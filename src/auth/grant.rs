// SPDX-License-Identifier: Apache-2.0
//! Signed LFS download grants.
//!
//! In provider mode the batch request is validated against the client's own
//! credential for its repo, and upstream's batch response lists only objects
//! that repo holds. Each rewritten download action carries a grant: an HMAC
//! over (repo, oid, size, expiry). The object GET verifies it locally instead
//! of revalidating the credential, and since the grant names one repo, it can
//! never pull another repo's oid out of the shared, content-addressed object
//! cache.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Result, anyhow, bail};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ring::hmac;
use ring::rand::{SecureRandom, SystemRandom};

/// How long a grant stays valid. git-lfs re-batches an action that expires
/// before its transfer starts, so this bounds how long a revoked credential can
/// keep pulling objects, not how long a transfer may take.
pub const LIFETIME: Duration = Duration::from_secs(15 * 60);

const VERSION: &str = "gcp1";
const MIN_KEY_LEN: usize = 32;

/// Signs and verifies the download grants carried by LFS batch responses.
pub struct Signer {
    key: hmac::Key,
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
            key: hmac::Key::new(hmac::HMAC_SHA256, &bytes),
        })
    }

    /// Issues a grant naming one object of `size` in `repo`, expiring after
    /// [`LIFETIME`].
    pub fn sign(&self, repo: &str, oid: &str, size: u64) -> String {
        self.sign_at(repo, oid, size, unix_now())
    }

    /// Accepts a grant only for the exact repo, oid, and size it names, and
    /// only before it expires.
    pub fn verify(&self, grant: &str, repo: &str, oid: &str, size: u64) -> bool {
        self.verify_at(grant, repo, oid, size, unix_now())
    }

    fn sign_at(&self, repo: &str, oid: &str, size: u64, now: u64) -> String {
        let expires = now + LIFETIME.as_secs();
        let tag = hmac::sign(&self.key, &message(repo, oid, size, expires));
        format!(
            "{VERSION}.{expires}.{}",
            URL_SAFE_NO_PAD.encode(tag.as_ref())
        )
    }

    fn verify_at(&self, grant: &str, repo: &str, oid: &str, size: u64, now: u64) -> bool {
        let Some((expires, tag)) = grant
            .strip_prefix(VERSION)
            .and_then(|g| g.strip_prefix('.'))
            .and_then(|g| g.split_once('.'))
        else {
            return false;
        };
        let (Ok(expires), Ok(tag)) = (expires.parse::<u64>(), URL_SAFE_NO_PAD.decode(tag)) else {
            return false;
        };
        expires > now && hmac::verify(&self.key, &message(repo, oid, size, expires), &tag).is_ok()
    }
}

/// NUL never occurs in a validated repo name or an oid, so the fields cannot be
/// re-split into a different tuple with the same MAC.
fn message(repo: &str, oid: &str, size: u64, expires: u64) -> Vec<u8> {
    format!("{VERSION}\0{repo}\0{oid}\0{size}\0{expires}").into_bytes()
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
    fn a_grant_verifies_only_for_what_it_names() {
        let s = Signer::new(Some(KEY)).unwrap();
        let g = s.sign_at("g/r.git", "oid", 7, 1000);
        assert!(s.verify_at(&g, "g/r.git", "oid", 7, 1000));
        assert!(!s.verify_at(&g, "g/other.git", "oid", 7, 1000));
        assert!(!s.verify_at(&g, "g/r.git", "oid2", 7, 1000));
        assert!(!s.verify_at(&g, "g/r.git", "oid", 8, 1000));
    }

    #[test]
    fn a_grant_expires() {
        let s = Signer::new(Some(KEY)).unwrap();
        let g = s.sign_at("r", "oid", 1, 1000);
        let expiry = 1000 + LIFETIME.as_secs();
        assert!(s.verify_at(&g, "r", "oid", 1, expiry - 1));
        assert!(!s.verify_at(&g, "r", "oid", 1, expiry));
    }

    #[test]
    fn replicas_sharing_a_key_accept_each_others_grants() {
        let a = Signer::new(Some(KEY)).unwrap();
        let b = Signer::new(Some(KEY)).unwrap();
        assert!(b.verify(&a.sign("r", "oid", 1), "r", "oid", 1));
        let random = Signer::new(None).unwrap();
        assert!(!random.verify(&a.sign("r", "oid", 1), "r", "oid", 1));
    }

    #[test]
    fn tampered_or_malformed_grants_are_rejected() {
        let s = Signer::new(Some(KEY)).unwrap();
        let g = s.sign_at("r", "oid", 1, 1000);
        let (head, _) = g.rsplit_once('.').unwrap();
        let later = head.replace("1900", "9900");
        let tag = g.rsplit_once('.').unwrap().1;
        assert!(!s.verify_at(&format!("{later}.{tag}"), "r", "oid", 1, 1000));
        for bad in ["", "gcp1", "gcp1.x.y", "gcp2.1900.AAAA", "client-token"] {
            assert!(!s.verify_at(bad, "r", "oid", 1, 1000), "grant: {bad:?}");
        }
    }

    #[test]
    fn short_keys_are_refused() {
        assert!(Signer::new(Some("short")).is_err());
    }
}
