// SPDX-License-Identifier: Apache-2.0
//! Signed LFS download grants.
//!
//! In provider mode the batch request is validated against the client's own
//! credential for its repo, and upstream's batch response lists only objects
//! that repo holds. Each rewritten download action carries a grant naming
//! (repo, oid, size, expiry); the object GET verifies it locally instead of
//! revalidating the credential, and since the grant names one repo it can never
//! pull another repo's oid out of the shared, content-addressed object cache.
//!
//! Two shapes, by whether the proxy needs a credential to re-fetch on a cache
//! miss:
//!
//! - `gcp1` (static upstream credential): an HMAC over (repo, oid, size,
//!   expiry). The proxy re-batches with its own `--upstream-auth-header`, so the
//!   grant carries no secret.
//! - `gcp2` (`client` upstream credential): an AEAD box whose plaintext is the
//!   requesting client's upstream credential and whose associated data is the
//!   same tuple. The object GET unseals it to re-batch upstream, so no per-object
//!   state is kept in the proxy and a grant still works across replicas sharing
//!   the key. The AEAD tag does the integrity job `gcp1`'s HMAC does, so a grant
//!   for one object cannot fetch another's. The AEAD key is derived from
//!   `--auth-signing-key` by HKDF with a distinct label, so it never shares
//!   material with the HMAC.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Result, anyhow, bail};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ring::aead::{self, Aad, LessSafeKey, NONCE_LEN, Nonce, UnboundKey};
use ring::hkdf;
use ring::hmac;
use ring::rand::{SecureRandom, SystemRandom};

/// How long a grant stays valid. git-lfs re-batches an action that expires
/// before its transfer starts, so this bounds how long a revoked credential can
/// keep pulling objects, not how long a transfer may take.
pub const LIFETIME: Duration = Duration::from_secs(15 * 60);

/// HMAC grant: no credential (static upstream mode).
const VERSION_HMAC: &str = "gcp1";
/// AEAD grant: seals the client's credential (client upstream mode).
const VERSION_AEAD: &str = "gcp2";
const MIN_KEY_LEN: usize = 32;

/// Domain-separates the AEAD key from the raw-key HMAC use, so the two never
/// share key material.
const AEAD_KDF_SALT: &[u8] = b"git-cache-proxy lfs grant";
const AEAD_KDF_INFO: &[u8] = b"credential seal v2";

/// Signs and verifies the download grants carried by LFS batch responses.
pub struct Signer {
    hmac_key: hmac::Key,
    aead_key: LessSafeKey,
    rng: SystemRandom,
}

impl Signer {
    /// A shared `key` lets every replica accept the grants of any other. `None`
    /// generates a per-process key, which only works with a single replica.
    pub fn new(key: Option<&str>) -> Result<Self> {
        let bytes = match key {
            Some(k) if k.len() < MIN_KEY_LEN => {
                bail!("--auth-signing-key must be at least {MIN_KEY_LEN} bytes")
            }
            Some(k) => k.as_bytes().to_vec(),
            None => {
                let mut b = vec![0; MIN_KEY_LEN];
                SystemRandom::new()
                    .fill(&mut b)
                    .map_err(|_| anyhow!("generate a random signing key"))?;
                b
            }
        };
        Ok(Self {
            hmac_key: hmac::Key::new(hmac::HMAC_SHA256, &bytes),
            aead_key: derive_aead_key(&bytes)?,
            rng: SystemRandom::new(),
        })
    }

    /// Issues a grant naming one object of `size` in `repo`, expiring after
    /// [`LIFETIME`]. `credential` is sealed inside a `gcp2` grant when present
    /// (client mode); `None` issues a credential-less `gcp1` grant (static mode).
    pub fn sign(&self, repo: &str, oid: &str, size: u64, credential: Option<&str>) -> String {
        self.sign_at(repo, oid, size, credential, unix_now())
    }

    /// Accepts a grant only for the exact repo, oid, and size it names, and only
    /// before it expires.
    pub fn verify(&self, grant: &str, repo: &str, oid: &str, size: u64) -> bool {
        self.open(grant, repo, oid, size).is_some()
    }

    /// Like [`verify`](Self::verify), but on success also returns the sealed
    /// credential: `Some(Some(cred))` for a valid `gcp2`, `Some(None)` for a
    /// valid `gcp1`, `None` for an invalid or expired grant.
    pub fn open(&self, grant: &str, repo: &str, oid: &str, size: u64) -> Option<Option<String>> {
        self.open_at(grant, repo, oid, size, unix_now())
    }

    fn sign_at(
        &self,
        repo: &str,
        oid: &str,
        size: u64,
        credential: Option<&str>,
        now: u64,
    ) -> String {
        let expires = now + LIFETIME.as_secs();
        match credential {
            None => {
                let tag = hmac::sign(
                    &self.hmac_key,
                    &message(VERSION_HMAC, repo, oid, size, expires),
                );
                format!(
                    "{VERSION_HMAC}.{expires}.{}",
                    URL_SAFE_NO_PAD.encode(tag.as_ref())
                )
            }
            Some(credential) => {
                let mut nonce_bytes = [0_u8; NONCE_LEN];
                self.rng
                    .fill(&mut nonce_bytes)
                    .expect("system rng fills a nonce");
                let aad = message(VERSION_AEAD, repo, oid, size, expires);
                let mut sealed = credential.as_bytes().to_vec();
                self.aead_key
                    .seal_in_place_append_tag(
                        Nonce::assume_unique_for_key(nonce_bytes),
                        Aad::from(&aad),
                        &mut sealed,
                    )
                    .expect("seal never fails for a valid key and nonce");
                let mut blob = nonce_bytes.to_vec();
                blob.append(&mut sealed);
                format!("{VERSION_AEAD}.{expires}.{}", URL_SAFE_NO_PAD.encode(&blob))
            }
        }
    }

    fn open_at(
        &self,
        grant: &str,
        repo: &str,
        oid: &str,
        size: u64,
        now: u64,
    ) -> Option<Option<String>> {
        let (version, rest) = grant.split_once('.')?;
        let (expires, payload) = rest.split_once('.')?;
        let expires = expires.parse::<u64>().ok()?;
        if expires <= now {
            return None;
        }
        let payload = URL_SAFE_NO_PAD.decode(payload).ok()?;
        match version {
            VERSION_HMAC => {
                hmac::verify(
                    &self.hmac_key,
                    &message(VERSION_HMAC, repo, oid, size, expires),
                    &payload,
                )
                .ok()?;
                Some(None)
            }
            VERSION_AEAD => {
                if payload.len() < NONCE_LEN {
                    return None;
                }
                let (nonce, sealed) = payload.split_at(NONCE_LEN);
                let nonce = Nonce::try_assume_unique_for_key(nonce).ok()?;
                let aad = message(VERSION_AEAD, repo, oid, size, expires);
                let mut sealed = sealed.to_vec();
                let plain = self
                    .aead_key
                    .open_in_place(nonce, Aad::from(&aad), &mut sealed)
                    .ok()?;
                Some(Some(String::from_utf8(plain.to_vec()).ok()?))
            }
            _ => None,
        }
    }
}

/// An AEAD key for `gcp2`, derived from the signing key by HKDF-SHA256 with a
/// distinct label so it never shares material with the HMAC key.
fn derive_aead_key(signing_key: &[u8]) -> Result<LessSafeKey> {
    let prk = hkdf::Salt::new(hkdf::HKDF_SHA256, AEAD_KDF_SALT).extract(signing_key);
    let okm = prk
        .expand(&[AEAD_KDF_INFO], hkdf::HKDF_SHA256)
        .map_err(|_| anyhow!("expand lfs grant aead key"))?;
    let mut derived = [0_u8; 32];
    okm.fill(&mut derived)
        .map_err(|_| anyhow!("fill lfs grant aead key"))?;
    let unbound = UnboundKey::new(&aead::CHACHA20_POLY1305, &derived)
        .map_err(|_| anyhow!("build lfs grant aead key"))?;
    Ok(LessSafeKey::new(unbound))
}

/// Binds the grant's version and tuple. NUL never occurs in a validated repo
/// name or an oid, so the fields cannot be re-split into a different tuple with
/// the same MAC/tag; the version keeps an HMAC and an AEAD grant from ever
/// sharing a bound message.
fn message(version: &str, repo: &str, oid: &str, size: u64, expires: u64) -> Vec<u8> {
    format!("{version}\0{repo}\0{oid}\0{size}\0{expires}").into_bytes()
}

/// Wall-clock seconds, not `tokio::time`: a grant must verify on any replica.
fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &str = "0123456789abcdef0123456789abcdef";

    #[test]
    fn an_hmac_grant_verifies_only_for_what_it_names() {
        let s = Signer::new(Some(KEY)).unwrap();
        let g = s.sign_at("g/r.git", "oid", 7, None, 1000);
        assert!(g.starts_with("gcp1."));
        assert_eq!(s.open_at(&g, "g/r.git", "oid", 7, 1000), Some(None));
        assert_eq!(s.open_at(&g, "g/other.git", "oid", 7, 1000), None);
        assert_eq!(s.open_at(&g, "g/r.git", "oid2", 7, 1000), None);
        assert_eq!(s.open_at(&g, "g/r.git", "oid", 8, 1000), None);
    }

    #[test]
    fn an_aead_grant_seals_and_unseals_the_credential() {
        let s = Signer::new(Some(KEY)).unwrap();
        let cred = "Authorization: Basic eC1hY2Nlc3MtdG9rZW46dG9r";
        let g = s.sign_at("g/r.git", "oid", 7, Some(cred), 1000);
        assert!(g.starts_with("gcp2."));
        // The credential is not readable without the key.
        assert!(!g.contains("Basic"));
        assert_eq!(
            s.open_at(&g, "g/r.git", "oid", 7, 1000),
            Some(Some(cred.to_string()))
        );
        // Bound to the exact tuple, like the HMAC grant.
        assert_eq!(s.open_at(&g, "g/other.git", "oid", 7, 1000), None);
        assert_eq!(s.open_at(&g, "g/r.git", "oid", 8, 1000), None);
    }

    #[test]
    fn grants_expire() {
        let s = Signer::new(Some(KEY)).unwrap();
        for cred in [None, Some("Authorization: Basic x")] {
            let g = s.sign_at("r", "oid", 1, cred, 1000);
            let expiry = 1000 + LIFETIME.as_secs();
            assert!(s.open_at(&g, "r", "oid", 1, expiry - 1).is_some());
            assert!(s.open_at(&g, "r", "oid", 1, expiry).is_none());
        }
    }

    #[test]
    fn replicas_sharing_a_key_accept_each_others_grants() {
        let a = Signer::new(Some(KEY)).unwrap();
        let b = Signer::new(Some(KEY)).unwrap();
        let cred = "Authorization: Basic x";
        assert_eq!(
            b.open(&a.sign("r", "oid", 1, Some(cred)), "r", "oid", 1),
            Some(Some(cred.to_string()))
        );
        let random = Signer::new(None).unwrap();
        assert!(!random.verify(&a.sign("r", "oid", 1, None), "r", "oid", 1));
    }

    #[test]
    fn tampered_or_malformed_grants_are_rejected() {
        let s = Signer::new(Some(KEY)).unwrap();
        for cred in [None, Some("Authorization: Basic x")] {
            let g = s.sign_at("r", "oid", 1, cred, 1000);
            // Flip the last payload byte: HMAC verify / AEAD tag must reject it.
            let mut bytes = g.into_bytes();
            let last = bytes.last_mut().unwrap();
            *last = if *last == b'A' { b'B' } else { b'A' };
            let tampered = String::from_utf8(bytes).unwrap();
            assert!(s.open_at(&tampered, "r", "oid", 1, 1000).is_none());
        }
        // `gcp2.<unexpired>.AAAA` reaches the AEAD arm but decodes to 3 bytes,
        // shorter than a nonce, so it is refused before any open.
        for bad in [
            "",
            "gcp1",
            "gcp1.x.y",
            "gcp2.4000000000.AAAA",
            "gcp3.1900.AAAA",
            "client-token",
        ] {
            assert!(
                s.open_at(bad, "r", "oid", 1, 1000).is_none(),
                "grant: {bad:?}"
            );
        }
    }

    #[test]
    fn short_keys_are_refused() {
        assert!(Signer::new(Some("short")).is_err());
    }
}
