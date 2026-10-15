// SPDX-License-Identifier: Apache-2.0
//! Client authentication for the serving surface.
//!
//! Modes, from `--auth-provider`:
//!
//! - `none` serves anonymously, for a network-restricted deployment.
//! - `shared` requires one shared bearer secret (`--serve-token`; also the
//!   legacy shape of passing only `--serve-token` with no explicit mode).
//! - `github` and `gitlab` check the client's own credential for the repo a
//!   request names, before any upstream or mirror work, so a client only
//!   reaches what that credential could clone directly. They ask upstream
//!   itself with a protocol v2 `info/refs` probe. LFS object GETs present a
//!   signed grant instead (see `grant`).
//!
//! Provider mode fails closed: unless the check gives a definitive verdict
//! (2xx allows, 401/403/404 rejects), the request is refused with 503 rather
//! than served unvalidated.

mod grant;

use std::collections::HashMap;
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use axum::http::header::{self, HeaderMap, HeaderValue};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use lru::LruCache;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tokio::sync::{OnceCell, Semaphore};
use tokio::time::Instant;

use crate::config::AuthProvider;
use crate::lfs::DownloadGrant;
use crate::repo;

/// Distinct `(credential, repo)` verdicts kept for `--auth-ttl-seconds`; least
/// recently used ones are evicted first.
const VERDICT_CAPACITY: NonZeroUsize = NonZeroUsize::new(4096).unwrap();

/// Most of a probe response read back so its connection can be pooled. A v2
/// capability advertisement fits easily; anything larger (a server ignoring
/// protocol v2) costs the connection instead of the bandwidth.
const DRAIN_LIMIT: usize = 64 * 1024;

const UPLOAD_PACK_ADVERTISEMENT: &str = "application/x-git-upload-pack-advertisement";

/// The outcome of authenticating one client request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// Serve: no auth configured, or the credential was accepted.
    Allow,
    /// Refuse with 401: the credential is missing or rejected for this repo.
    Deny,
    /// Refuse with 503: validity could not be determined, so fail closed.
    Unavailable,
}

/// What a request asks for, as classified by the router.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Target<'a> {
    /// A git or LFS batch request for this repo.
    Repo(&'a str),
    /// An LFS object GET, authorized by the grant its batch response issued.
    LfsObject {
        repo: &'a str,
        oid: &'a str,
        size: Option<u64>,
    },
    /// Not a git or LFS endpoint.
    Unknown,
}

/// Runtime auth settings, assembled by the binary from `Config`.
pub struct AuthConfig {
    pub provider: AuthProvider,
    /// `--serve-token`. Whitespace-only values collapse to `None`, like the
    /// upstream auth header.
    pub serve_token: Option<String>,
    /// Provider modes: HMAC key of LFS download grants, shared by every replica.
    /// `None` generates a per-process key.
    pub signing_key: Option<String>,
    /// Total budget for one validation request.
    pub timeout: Duration,
    /// How long a validation verdict may be reused per `(credential, repo)`.
    /// Zero disables reuse entirely.
    pub ttl: Duration,
    /// Validation requests in flight at once. Zero means unlimited.
    pub max_concurrent: usize,
    /// Accept an `http://` validation endpoint.
    pub allow_http: bool,
    /// `--upstream`, which `github` and `gitlab` probe.
    pub upstream_base: String,
}

pub struct Auth {
    mode: Mode,
}

enum Mode {
    Anonymous,
    Shared { token: String },
    Provider(Box<Provider>),
}

impl fmt::Debug for Auth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Only the mode label: never the shared secret.
        f.debug_struct("Auth")
            .field("mode", &self.describe())
            .finish()
    }
}

impl Auth {
    /// The `none` mode: serve anonymously.
    pub fn anonymous() -> Self {
        Self {
            mode: Mode::Anonymous,
        }
    }

    /// The `shared` mode: require one bearer secret, compared in constant time.
    pub fn shared(token: &str) -> Self {
        Self {
            mode: Mode::Shared {
                token: token.to_string(),
            },
        }
    }

    /// Resolve flags into an `Auth`, failing on any conflicting combination so
    /// misconfiguration surfaces at startup, not on first request.
    pub fn build(cfg: AuthConfig) -> Result<Self> {
        if cfg.timeout.is_zero() {
            bail!("--auth-timeout-seconds must be greater than zero");
        }
        // Set-but-blank is the one shape that silently changes the auth mode, so
        // it gets a warning; an absent flag is the documented anonymous default.
        if cfg
            .serve_token
            .as_deref()
            .is_some_and(|s| s.trim().is_empty())
        {
            tracing::warn!(
                "--serve-token is blank; serving anonymously - \
                 set a token, or unset the flag, to silence this"
            );
        }
        // An env var set to a blank string parses as Some(""); a missing flag must
        // not silently become an empty one.
        let trim = |v: &Option<String>| -> Option<String> {
            v.as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        };
        let serve_token = trim(&cfg.serve_token);
        let signing_key = trim(&cfg.signing_key);
        let provider = cfg.provider;

        let mode = match provider {
            AuthProvider::None | AuthProvider::Shared => {
                forbid(&signing_key, "--auth-signing-key", provider)?;
                match (serve_token, provider) {
                    // Legacy shape: only `--serve-token` given, no explicit mode.
                    (Some(token), _) => Mode::Shared { token },
                    (None, AuthProvider::None) => Mode::Anonymous,
                    (None, _) => bail!("--auth-provider shared requires --serve-token"),
                }
            }
            AuthProvider::Github | AuthProvider::Gitlab => {
                forbid(&serve_token, "--serve-token", provider)?;
                let base = probe_base(&cfg.upstream_base, cfg.allow_http)?;
                // Git hosts take a token as the Basic password; the username
                // is what each host documents for a bare token.
                let bearer_user = if provider == AuthProvider::Github {
                    BearerUser::Github
                } else {
                    BearerUser::Gitlab
                };
                provider_mode(&cfg, base, bearer_user, signing_key.as_deref())?
            }
        };
        Ok(Self { mode })
    }

    /// Authenticate one client request for `target`.
    pub async fn check(&self, headers: &HeaderMap, target: Target<'_>) -> Verdict {
        let provider = match &self.mode {
            Mode::Anonymous => return Verdict::Allow,
            Mode::Shared { token } => {
                return match bearer_token(headers) {
                    Some(provided) if token_matches(provided, token) => Verdict::Allow,
                    _ => Verdict::Deny,
                };
            }
            Mode::Provider(p) => p,
        };
        match target {
            Target::Repo(repo) => provider.check(headers, repo).await,
            Target::LfsObject {
                repo,
                oid,
                size: Some(size),
            } => match bearer_token(headers) {
                Some(g) if provider.grants.verify(g, repo, oid, size) => Verdict::Allow,
                _ => Verdict::Deny,
            },
            Target::LfsObject { size: None, .. } | Target::Unknown => Verdict::Deny,
        }
    }

    /// The credential a rewritten LFS download action carries for one object:
    /// the shared secret in shared mode, a signed grant in provider mode, `None`
    /// when anonymous.
    pub fn lfs_grant(&self, repo: &str, oid: &str, size: u64) -> Option<DownloadGrant> {
        match &self.mode {
            Mode::Anonymous => None,
            Mode::Shared { token } => Some(DownloadGrant {
                authorization: format!("Bearer {token}"),
                expires_in: None,
            }),
            Mode::Provider(p) => Some(DownloadGrant {
                authorization: format!("Bearer {}", p.grants.sign(repo, oid, size)),
                expires_in: Some(grant::LIFETIME),
            }),
        }
    }

    /// The `WWW-Authenticate` challenge of a 401. git only hands credential-helper
    /// or URL credentials to curl after a Basic challenge, and provider mode is
    /// the one that accepts Basic.
    pub fn challenge(&self) -> Option<&'static str> {
        match &self.mode {
            Mode::Provider(_) => Some(r#"Basic realm="git-cache-proxy""#),
            Mode::Anonymous | Mode::Shared { .. } => None,
        }
    }

    /// A short label for startup logging.
    pub fn describe(&self) -> &'static str {
        match &self.mode {
            Mode::Anonymous => "anonymous",
            Mode::Shared { .. } => "shared token",
            Mode::Provider(_) => "per-request token validation",
        }
    }
}

/// The username a git host documents for a bare token used as the Basic
/// password; the type pins the two strings to the hosts they belong to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BearerUser {
    Github,
    Gitlab,
}

impl BearerUser {
    fn as_str(self) -> &'static str {
        match self {
            Self::Github => "x-access-token",
            Self::Gitlab => "oauth2",
        }
    }
}

/// Checks a client credential against upstream's own `info/refs`, which only
/// advertises to a credential that can clone - unlike a metadata API, which
/// also answers Guest members and tokens without read access to the code.
struct Provider {
    client: reqwest::Client,
    /// `--upstream`, without its credentials.
    base: String,
    /// Username paired with a Bearer token when it is forwarded as Basic.
    bearer_user: BearerUser,
    /// `None` leaves validation requests unbounded.
    permits: Option<Semaphore>,
    /// Longest a check waits for one of `permits`.
    timeout: Duration,
    /// `None` revalidates every request.
    ttl: Option<Duration>,
    verdicts: Mutex<LruCache<CacheKey, (Verdict, Instant)>>,
    /// Concurrent checks of one `(credential, repo)` share a single call.
    inflight: Mutex<HashMap<CacheKey, Arc<OnceCell<Verdict>>>>,
    grants: grant::Signer,
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct CacheKey {
    /// sha256 of the client's `Authorization` header, so the raw secret never
    /// sits at rest in the cache.
    credential: [u8; 32],
    repo: String,
}

/// A client credential as presented on `Authorization`.
struct Credential<'a> {
    raw: &'a HeaderValue,
    scheme: Scheme,
    token: String,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Scheme {
    /// git's credential helpers and tokened clone URLs.
    Basic,
    /// git's `http.extraHeader`.
    Bearer,
}

impl Provider {
    async fn check(&self, headers: &HeaderMap, repo: &str) -> Verdict {
        let Some(cred) = credential(headers) else {
            return Verdict::Deny;
        };
        // Names serving would reject, or that need escaping, are denied
        // without spending a call.
        if repo::validate(repo).is_err() || !url_safe(repo) {
            return Verdict::Deny;
        }
        let key = CacheKey {
            credential: Sha256::digest(cred.raw.as_bytes()).into(),
            repo: repo.to_string(),
        };
        if let Some(v) = self.cached(&key) {
            return v;
        }
        let inflight = self.join(&key);
        *inflight
            .cell
            .get_or_init(|| async {
                let verdict = self.send(self.request(&cred, repo), repo).await;
                // Unavailable is never cached: the next request should retry.
                if verdict != Verdict::Unavailable {
                    self.remember(&key, verdict);
                }
                verdict
            })
            .await
    }

    fn request(&self, cred: &Credential<'_>, repo: &str) -> reqwest::RequestBuilder {
        // Basic goes upstream verbatim: the username can matter (GitLab job
        // tokens need `gitlab-ci-token`).
        let auth = match cred.scheme {
            Scheme::Basic => cred.raw.clone(),
            Scheme::Bearer => {
                let basic =
                    STANDARD.encode(format!("{}:{}", self.bearer_user.as_str(), cred.token));
                HeaderValue::from_str(&format!("Basic {basic}"))
                    .expect("base64 is a valid header value")
            }
        };
        self.client
            .get(format!(
                "{}/{repo}/info/refs?service=git-upload-pack",
                self.base
            ))
            .header(header::AUTHORIZATION, sensitive(auth))
            .header("git-protocol", "version=2")
    }

    async fn send(&self, req: reqwest::RequestBuilder, repo: &str) -> Verdict {
        // One deadline for the whole validation, so the slot wait and the HTTP
        // call share the configured budget instead of each getting a fresh one.
        let deadline = Instant::now() + self.timeout;
        let _permit = match &self.permits {
            Some(permits) => match tokio::time::timeout_at(deadline, permits.acquire()).await {
                Ok(Ok(permit)) => Some(permit),
                _ => {
                    tracing::warn!(repo, "token validation saturated (--auth-max-concurrent)");
                    return Verdict::Unavailable;
                }
            },
            None => None,
        };
        // Whatever the slot wait left of the budget. Zero would mean `no timeout`
        // to reqwest, so it is short-circuited as a failure instead.
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            tracing::warn!(repo, "token validation ran out of time waiting for a slot");
            return Verdict::Unavailable;
        }
        let mut resp = match req.timeout(remaining).send().await {
            Ok(r) => r,
            Err(e) => {
                // Without the URL: a custom endpoint's may carry userinfo, and
                // `repo` already names the target.
                tracing::warn!(repo, error = %e.without_url(), "token validation request failed");
                return Verdict::Unavailable;
            }
        };
        let status = resp.status().as_u16();
        let throttled = throttled(resp.headers());
        // A 200 that is not an advertisement (an SSO login page, say) says
        // nothing about the credential.
        let advertises = resp.headers().get(header::CONTENT_TYPE).is_some_and(|v| {
            v.as_bytes()
                .starts_with(UPLOAD_PACK_ADVERTISEMENT.as_bytes())
        });
        let mut read = 0;
        while read < DRAIN_LIMIT
            && let Ok(Some(chunk)) = resp.chunk().await
        {
            read += chunk.len();
        }
        let mut verdict = classify(status, throttled);
        if verdict == Verdict::Allow && !advertises {
            verdict = Verdict::Unavailable;
        }
        match verdict {
            Verdict::Unavailable => {
                tracing::warn!(repo, status, "token validation endpoint unhealthy");
            }
            Verdict::Deny => tracing::debug!(repo, "token rejected for this repo"),
            Verdict::Allow => {}
        }
        verdict
    }

    fn cached(&self, key: &CacheKey) -> Option<Verdict> {
        self.ttl?;
        let mut verdicts = self.verdicts.lock().expect("verdict cache lock");
        match verdicts.get(key) {
            Some(&(v, until)) if until > Instant::now() => Some(v),
            Some(_) => {
                verdicts.pop(key);
                None
            }
            None => None,
        }
    }

    fn remember(&self, key: &CacheKey, verdict: Verdict) {
        let Some(ttl) = self.ttl else { return };
        self.verdicts
            .lock()
            .expect("verdict cache lock")
            .put(key.clone(), (verdict, Instant::now() + ttl));
    }

    fn join(&self, key: &CacheKey) -> Inflight<'_> {
        let cell = self
            .inflight
            .lock()
            .expect("inflight lock")
            .entry(key.clone())
            .or_default()
            .clone();
        Inflight {
            map: &self.inflight,
            key: key.clone(),
            cell,
        }
    }
}

/// One caller's share of an in-flight validation.
struct Inflight<'a> {
    map: &'a Mutex<HashMap<CacheKey, Arc<OnceCell<Verdict>>>>,
    key: CacheKey,
    cell: Arc<OnceCell<Verdict>>,
}

impl Drop for Inflight<'_> {
    fn drop(&mut self) {
        let mut map = self.map.lock().expect("inflight lock");
        // Callers only clone the cell under this lock, so the count is exact: the
        // last one out removes the entry, also when its request was cancelled.
        if map
            .get(&self.key)
            .is_some_and(|c| Arc::ptr_eq(c, &self.cell))
            && Arc::strong_count(&self.cell) == 2
        {
            map.remove(&self.key);
        }
    }
}

/// Map a validation answer to a verdict: 2xx allows, {401, 403, 404} rejects.
/// Everything else - 5xx, 3xx, 429, a throttled 403 - is not a verdict on the
/// credential and fails closed.
fn classify(status: u16, throttled: bool) -> Verdict {
    if (200..300).contains(&status) {
        Verdict::Allow
    } else if !throttled && matches!(status, 401 | 403 | 404) {
        Verdict::Deny
    } else {
        Verdict::Unavailable
    }
}

/// GitHub and GitLab report an exhausted budget as a 403 with
/// `x-ratelimit-remaining: 0`, and a secondary rate limit as a 403 with
/// `retry-after`: an outage, not a verdict, so it must not deny every client.
fn throttled(headers: &HeaderMap) -> bool {
    headers.contains_key(header::RETRY_AFTER)
        || headers
            .get("x-ratelimit-remaining")
            .is_some_and(|v| v.as_bytes().trim_ascii() == b"0")
}

/// Reject a flag that does not apply to the requested provider, so a mistaken
/// combination surfaces at startup instead of being silently ignored.
fn forbid<T>(value: &Option<T>, flag: &str, provider: AuthProvider) -> Result<()> {
    if value.is_some() {
        bail!("{flag} does not apply to --auth-provider {provider}");
    }
    Ok(())
}

/// Every client credential rides to the validation endpoint, so plain http
/// needs an explicit opt-in.
fn require_tls(url: &reqwest::Url, flag: &str, allow_http: bool) -> Result<()> {
    match url.scheme() {
        "https" => Ok(()),
        "http" if allow_http => Ok(()),
        "http" => bail!(
            "{flag} is plain http and would carry client credentials in cleartext; \
             pass --auth-allow-http to accept that"
        ),
        other => bail!("{flag} must be an http(s) URL to validate against, not {other}"),
    }
}

/// The `--upstream` base a profile probes. Credentials embedded in it are the
/// proxy's own and must never vouch for a client.
fn probe_base(upstream: &str, allow_http: bool) -> Result<String> {
    let mut url = reqwest::Url::parse(upstream).context("parse --upstream")?;
    require_tls(&url, "--upstream", allow_http)?;
    let _ = url.set_username("");
    let _ = url.set_password(None);
    Ok(url.as_str().trim_end_matches('/').to_string())
}

/// Build the provider mode: the reqwest client is built once, at startup, so
/// the per-request path stays infallible.
fn provider_mode(
    cfg: &AuthConfig,
    base: String,
    bearer_user: BearerUser,
    signing_key: Option<&str>,
) -> Result<Mode> {
    let grants = grant::Signer::new(signing_key)?;
    if signing_key.is_none() {
        tracing::warn!(
            "no --auth-signing-key set; LFS download grants only verify on this replica"
        );
    }
    let client = reqwest::Client::builder()
        .user_agent(concat!("git-cache-proxy/", env!("CARGO_PKG_VERSION")))
        .timeout(cfg.timeout)
        .connect_timeout(cfg.timeout.min(Duration::from_secs(10)))
        // A redirect must not become an implicit allow (e.g. a login page
        // answering 200); refusing to follow sends it to `classify` as a 3xx,
        // which fails closed.
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .context("build token validation client")?;
    Ok(Mode::Provider(Box::new(Provider {
        client,
        base,
        bearer_user,
        permits: (cfg.max_concurrent != 0).then(|| Semaphore::new(cfg.max_concurrent)),
        timeout: cfg.timeout,
        ttl: (!cfg.ttl.is_zero()).then_some(cfg.ttl),
        verdicts: Mutex::new(LruCache::new(VERDICT_CAPACITY)),
        inflight: Mutex::new(HashMap::new()),
        grants,
    })))
}

/// Only RFC 3986 unreserved characters and `/`. Such a name reaches the
/// validation endpoint byte for byte as serving sends it upstream, so the repo
/// checked is the repo served: a `%` escape could otherwise decode to a
/// different path on one side than the other. GitHub and GitLab names fit.
fn url_safe(name: &str) -> bool {
    name.bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~' | b'/'))
}

/// The credential of `Authorization: Bearer <token>`: what shared mode accepts
/// and what an LFS grant rides in. No trimming: the token begins right after
/// the scheme's single space.
fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .filter(|t| !t.is_empty())
}

/// The client credential: `Authorization: Bearer <token>`, or the password of
/// `Authorization: Basic <user:token>` (the username when the password is
/// empty, the whole credential when it has no `:`).
fn credential(headers: &HeaderMap) -> Option<Credential<'_>> {
    let raw = headers.get(header::AUTHORIZATION)?;
    let (scheme, value) = raw.to_str().ok()?.trim().split_once(' ')?;
    let value = value.trim();
    let (scheme, token) = if scheme.eq_ignore_ascii_case("basic") {
        let decoded = String::from_utf8(STANDARD.decode(value).ok()?).ok()?;
        let token = match decoded.split_once(':') {
            // A tokened `https://<token>@host` URL, which GitHub accepts.
            Some((user, "")) => user.to_string(),
            Some((_user, password)) => password.to_string(),
            None => decoded,
        };
        (Scheme::Basic, token)
    } else if scheme.eq_ignore_ascii_case("bearer") {
        (Scheme::Bearer, value.to_string())
    } else {
        return None;
    };
    (!token.is_empty()).then_some(Credential { raw, scheme, token })
}

fn sensitive(mut value: HeaderValue) -> HeaderValue {
    value.set_sensitive(true);
    value
}

/// Compare a client-supplied bearer token against the expected one in constant
/// time, so response latency does not leak how many leading bytes matched. Only
/// the length can differ observably, which is not sensitive for a shared secret.
fn token_matches(provided: &str, expected: &str) -> bool {
    provided.as_bytes().ct_eq(expected.as_bytes()).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_and_explicit_shared_modes() {
        let legacy = Auth::build(config(AuthProvider::None)).unwrap();
        assert_eq!(legacy.describe(), "anonymous");
        let legacy = Auth::build(AuthConfig {
            serve_token: Some("s3cret".into()),
            ..config(AuthProvider::None)
        })
        .unwrap();
        assert_eq!(legacy.describe(), "shared token");
        let explicit = Auth::build(AuthConfig {
            serve_token: Some("s3cret".into()),
            ..config(AuthProvider::Shared)
        })
        .unwrap();
        assert_eq!(explicit.describe(), "shared token");
        // Blank collapses to absent, so it cannot enforce an empty secret.
        let blank = Auth::build(AuthConfig {
            serve_token: Some("   ".into()),
            ..config(AuthProvider::Shared)
        })
        .unwrap_err();
        assert!(blank.to_string().contains("requires --serve-token"));
    }

    #[test]
    fn conflicting_flag_combinations_are_rejected() {
        for provider in [AuthProvider::Github, AuthProvider::Gitlab] {
            let err = Auth::build(AuthConfig {
                serve_token: Some("s3cret".into()),
                ..config(provider)
            })
            .unwrap_err();
            assert!(err.to_string().contains("--serve-token"));
        }
    }

    #[test]
    fn profiles_probe_an_http_upstream_without_its_credentials() {
        let p = provider(AuthConfig {
            upstream_base: "https://proxy:secret@git.example.com/prefix/".into(),
            ..config(AuthProvider::Gitlab)
        });
        assert_eq!(p.base, "https://git.example.com/prefix");
        assert_eq!(p.bearer_user.as_str(), "oauth2");
        let err = Auth::build(AuthConfig {
            upstream_base: "file:///srv/git".into(),
            ..config(AuthProvider::Github)
        })
        .unwrap_err();
        assert!(err.to_string().contains("http(s)"));
    }

    #[test]
    fn plain_http_endpoints_need_an_opt_in() {
        let github = |allow_http| AuthConfig {
            upstream_base: "http://git.example.com".into(),
            allow_http,
            ..config(AuthProvider::Github)
        };
        let err = Auth::build(github(false)).unwrap_err();
        assert!(err.to_string().contains("--auth-allow-http"), "{err}");
        assert!(Auth::build(github(true)).is_ok());
    }

    #[test]
    fn timeout_must_be_positive() {
        let err = Auth::build(AuthConfig {
            timeout: Duration::from_secs(0),
            ..config(AuthProvider::Github)
        })
        .unwrap_err();
        assert!(err.to_string().contains("--auth-timeout-seconds"));
    }

    #[test]
    fn token_matches_only_the_exact_token() {
        assert!(token_matches("s3cret", "s3cret"));
        assert!(!token_matches("s3creT", "s3cret")); // last byte differs
        assert!(!token_matches("s3cre", "s3cret")); // prefix, shorter
        assert!(!token_matches("s3cret-extra", "s3cret")); // longer
        assert!(!token_matches("", "s3cret"));
        assert!(token_matches("", "")); // degenerate empty token
    }

    #[test]
    fn only_names_without_escapes_are_url_safe() {
        assert!(url_safe("owner/my-repo_v1.2~x.git"));
        // A `%` escape could decode differently at the endpoint and upstream.
        for name in [
            "a%2Fb.git",
            "a/%2e%2e/b.git",
            "a b.git",
            "a?b",
            "a#b",
            "a@b",
        ] {
            assert!(!url_safe(name), "name: {name:?}");
        }
    }

    #[test]
    fn credentials_read_bearer_and_basic() {
        let token = |h: &HeaderMap| credential(h).map(|c| c.token);
        assert_eq!(token(&headers("Bearer tok")).as_deref(), Some("tok"));
        assert_eq!(token(&headers("bearer tok")).as_deref(), Some("tok"));
        let basic = STANDARD.encode(b"x-access-token:tok");
        let h = headers(&format!("Basic {basic}"));
        let cred = credential(&h).unwrap();
        assert_eq!(cred.token, "tok");
        assert!(cred.scheme == Scheme::Basic);
        let username_only = STANDARD.encode(b"tok:");
        assert_eq!(
            token(&headers(&format!("Basic {username_only}"))).as_deref(),
            Some("tok")
        );
        assert_eq!(
            token(&headers(&format!("Basic {}", STANDARD.encode(b":")))),
            None
        );
        // Basic without a ':' separator: the credential taken whole.
        let bare = STANDARD.encode(b"tok");
        assert_eq!(
            token(&headers(&format!("Basic {bare}"))).as_deref(),
            Some("tok")
        );
        assert_eq!(token(&headers("Bearer ")), None);
        assert_eq!(token(&headers("Digest xyz")), None);
        assert_eq!(token(&HeaderMap::new()), None);
    }

    #[test]
    fn shared_mode_takes_bearer_only() {
        assert!(bearer_token(&headers("Bearer tok")).is_some());
        let basic = STANDARD.encode(b"user:tok");
        assert_eq!(bearer_token(&headers(&format!("Basic {basic}"))), None);
        // Empty is refused without comparing against the secret.
        assert_eq!(bearer_token(&headers("Bearer ")), None);
        // The token starts right after the scheme's single space; a doubled
        // space survives parsing but never matches a secret.
        let spaced_headers = headers("Bearer  spaced");
        let spaced = bearer_token(&spaced_headers).unwrap();
        assert!(!token_matches(spaced, "spaced"));
        assert_eq!(bearer_token(&HeaderMap::new()), None);
    }

    #[test]
    fn classification_maps_statuses_to_verdicts() {
        assert_eq!(classify(200, false), Verdict::Allow);
        assert_eq!(classify(204, false), Verdict::Allow);
        for status in [401, 403, 404] {
            assert_eq!(classify(status, false), Verdict::Deny);
        }
        assert_eq!(classify(403, true), Verdict::Unavailable);
        for status in [429, 500, 503, 302, 400] {
            assert_eq!(classify(status, false), Verdict::Unavailable);
        }
    }

    #[test]
    fn throttling_headers_are_recognized() {
        let mut h = HeaderMap::new();
        assert!(!throttled(&h));
        h.insert("x-ratelimit-remaining", HeaderValue::from_static("12"));
        assert!(!throttled(&h));
        h.insert("x-ratelimit-remaining", HeaderValue::from_static(" 0"));
        assert!(throttled(&h));
        let mut h = HeaderMap::new();
        h.insert(header::RETRY_AFTER, HeaderValue::from_static("60"));
        assert!(throttled(&h));
    }

    #[tokio::test]
    async fn lfs_objects_need_a_grant_for_exactly_that_object() {
        let auth = Auth::build(config(AuthProvider::Github)).unwrap();
        let grant = auth.lfs_grant("r.git", "oid", 7).unwrap();
        let h = headers(&grant.authorization);
        let object = |repo, size| Target::LfsObject {
            repo,
            oid: "oid",
            size,
        };
        assert_eq!(
            auth.check(&h, object("r.git", Some(7))).await,
            Verdict::Allow
        );
        assert_eq!(
            auth.check(&h, object("o.git", Some(7))).await,
            Verdict::Deny
        );
        assert_eq!(auth.check(&h, object("r.git", None)).await, Verdict::Deny);
        let client = headers("Bearer tok");
        assert_eq!(
            auth.check(&client, object("r.git", Some(7))).await,
            Verdict::Deny
        );
        assert_eq!(auth.check(&client, Target::Unknown).await, Verdict::Deny);
    }

    #[test]
    fn grants_are_issued_per_mode() {
        assert_eq!(Auth::anonymous().lfs_grant("r", "oid", 1), None);
        let shared = Auth::shared("s3cret").lfs_grant("r", "oid", 1).unwrap();
        assert_eq!(shared.authorization, "Bearer s3cret");
        assert_eq!(shared.expires_in, None);
        let auth = Auth::build(config(AuthProvider::Github)).unwrap();
        let grant = auth.lfs_grant("r", "oid", 1).unwrap();
        assert!(grant.authorization.starts_with("Bearer gcp1."));
        assert_eq!(grant.expires_in, Some(grant::LIFETIME));
    }

    #[test]
    fn a_signing_key_without_a_provider_is_rejected() {
        for provider in [AuthProvider::None, AuthProvider::Shared] {
            let err = Auth::build(AuthConfig {
                serve_token: Some("s3cret".into()),
                signing_key: Some(KEY.into()),
                ..config(provider)
            })
            .unwrap_err();
            assert!(err.to_string().contains("--auth-signing-key"));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn ttl_verdicts_expire() {
        let p = provider(AuthConfig {
            ttl: Duration::from_secs(1),
            ..config(AuthProvider::Github)
        });
        let key = key();
        assert_eq!(p.cached(&key), None);
        p.remember(&key, Verdict::Allow);
        assert_eq!(p.cached(&key), Some(Verdict::Allow));
        tokio::time::advance(Duration::from_secs(2)).await;
        assert_eq!(p.cached(&key), None);
        // The expired entry is dropped, not merely ignored.
        assert!(p.verdicts.lock().unwrap().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn ttl_zero_disables_reuse() {
        let p = provider(config(AuthProvider::Github));
        let key = key();
        p.remember(&key, Verdict::Allow);
        assert_eq!(p.cached(&key), None);
        assert!(p.verdicts.lock().unwrap().is_empty());
    }

    #[test]
    fn the_verdict_cache_is_bounded() {
        let p = provider(AuthConfig {
            ttl: Duration::from_secs(60),
            ..config(AuthProvider::Github)
        });
        for i in 0..VERDICT_CAPACITY.get() + 10 {
            let key = CacheKey {
                credential: [0; 32],
                repo: format!("r{i}.git"),
            };
            p.remember(&key, Verdict::Deny);
        }
        assert_eq!(p.verdicts.lock().unwrap().len(), VERDICT_CAPACITY.get());
    }

    #[tokio::test(start_paused = true)]
    async fn a_saturated_limit_fails_closed() {
        let p = provider(AuthConfig {
            max_concurrent: 1,
            ..config(AuthProvider::Github)
        });
        let _held = p.permits.as_ref().unwrap().acquire().await.unwrap();
        let start = Instant::now();
        assert_eq!(
            p.check(&headers("Bearer tok"), "r.git").await,
            Verdict::Unavailable
        );
        // Waited out the timeout for a slot, rather than failing on the
        // unreachable endpoint.
        assert_eq!(start.elapsed(), p.timeout);
    }

    #[test]
    fn a_zero_limit_is_unbounded() {
        let p = provider(AuthConfig {
            max_concurrent: 0,
            ..config(AuthProvider::Github)
        });
        assert!(p.permits.is_none());
    }

    #[test]
    fn the_last_inflight_caller_removes_the_entry() {
        let p = provider(config(AuthProvider::Github));
        let key = key();
        let first = p.join(&key);
        let second = p.join(&key);
        assert!(
            Arc::ptr_eq(&first.cell, &second.cell),
            "callers share a call"
        );
        drop(first);
        assert_eq!(p.inflight.lock().unwrap().len(), 1);
        drop(second);
        assert!(p.inflight.lock().unwrap().is_empty());
    }

    const KEY: &str = "0123456789abcdef0123456789abcdef";

    /// Every provider mode gets a shared signing key; `none` and `shared`
    /// reject one, so they get none.
    fn config(provider: AuthProvider) -> AuthConfig {
        AuthConfig {
            provider,
            serve_token: None,
            signing_key: matches!(provider, AuthProvider::Github | AuthProvider::Gitlab)
                .then(|| KEY.into()),
            timeout: Duration::from_secs(10),
            ttl: Duration::from_secs(0),
            max_concurrent: 32,
            allow_http: false,
            upstream_base: "https://upstream.invalid".into(),
        }
    }

    fn provider(cfg: AuthConfig) -> Box<Provider> {
        match Auth::build(cfg).unwrap().mode {
            Mode::Provider(p) => p,
            _ => panic!("not the provider mode"),
        }
    }

    fn key() -> CacheKey {
        CacheKey {
            credential: [1; 32],
            repo: "r.git".into(),
        }
    }

    fn headers(value: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(header::AUTHORIZATION, HeaderValue::from_str(value).unwrap());
        h
    }
}
