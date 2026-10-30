// SPDX-License-Identifier: Apache-2.0
//! Client authentication for the serving surface.
//!
//! Modes, from `--auth-provider`:
//!
//! - `none` serves anonymously, for a network-restricted deployment.
//! - `shared` requires one shared bearer secret (`--serve-token`; also the
//!   legacy shape of passing only `--serve-token` with no explicit mode).
//! - `github`, `gitlab`, and `custom` check the client's own credential for the
//!   repo a request names, before any upstream or mirror work, so a client only
//!   reaches what that credential could clone directly. `github` and `gitlab`
//!   ask upstream itself with a protocol v2 `info/refs` probe; `custom` asks
//!   any HTTP endpoint. LFS object GETs present a signed grant instead (see
//!   `grant`).
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
use axum::http::header::{self, HeaderMap, HeaderName, HeaderValue};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use lru::LruCache;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tokio::sync::{OnceCell, Semaphore};
use tokio::time::Instant;

use crate::config::{AuthProvider, RepoForm};
use crate::lfs::DownloadGrant;
use crate::metrics::{Metrics, ValidationResult};
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
    /// `custom` only: URL template containing `{repo}`.
    pub validate_url: Option<String>,
    /// `custom` only: header line template containing `{token}`.
    pub token_header: Option<String>,
    /// `custom` only: repo encoding. `None` means `path`.
    pub repo_form: Option<RepoForm>,
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

    /// Resolve flags and profiles into an `Auth`, failing on any conflicting
    /// combination so misconfiguration surfaces at startup, not on first request.
    pub fn build(cfg: AuthConfig, metrics: Arc<Metrics>) -> Result<Self> {
        if cfg.timeout.is_zero() {
            bail!("--auth-timeout-seconds must be greater than zero");
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
        let validate_url = trim(&cfg.validate_url);
        let token_header = trim(&cfg.token_header);
        let signing_key = trim(&cfg.signing_key);
        let provider = cfg.provider;
        let forbid_custom_flags = || -> Result<()> {
            forbid(&validate_url, "--auth-validate-url", provider)?;
            forbid(&token_header, "--auth-token-header", provider)?;
            forbid(&cfg.repo_form, "--auth-repo-form", provider)
        };

        let mode = match provider {
            AuthProvider::None | AuthProvider::Shared => {
                forbid_custom_flags()?;
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
                forbid_custom_flags()?;
                let check = Check::Upstream {
                    base: probe_base(&cfg.upstream_base, cfg.allow_http)?,
                    // Git hosts take a token as the Basic password; the username
                    // is what each host documents for a bare token.
                    bearer_user: if provider == AuthProvider::Github {
                        BearerUser::Github
                    } else {
                        BearerUser::Gitlab
                    },
                };
                provider_mode(&cfg, check, signing_key.as_deref(), metrics)?
            }
            AuthProvider::Custom => {
                forbid(&serve_token, "--serve-token", provider)?;
                let url =
                    validate_url.context("--auth-provider custom requires --auth-validate-url")?;
                if !url.contains("{repo}") {
                    bail!("--auth-validate-url must contain {{repo}}");
                }
                if url.contains("{token}") {
                    // Transport error messages echo the URL, so the token must
                    // never ride in it.
                    bail!(
                        "--auth-validate-url must not contain {{token}}; \
                         use --auth-token-header"
                    );
                }
                let parsed = reqwest::Url::parse(&url.replace("{repo}", "repo"))
                    .context("parse --auth-validate-url")?;
                require_tls(&parsed, "--auth-validate-url", cfg.allow_http)?;
                let (header_name, header_value) = parse_token_header(
                    token_header
                        .as_deref()
                        .unwrap_or("Authorization: Bearer {token}"),
                )?;
                let check = Check::Endpoint {
                    url,
                    header_name,
                    header_value,
                    repo_form: cfg.repo_form.unwrap_or(RepoForm::Path),
                };
                provider_mode(&cfg, check, signing_key.as_deref(), metrics)?
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

    /// The client's own credential as `github`/`gitlab` forward it upstream: the
    /// `Authorization` header value a git host accepts for that scheme (Basic
    /// verbatim, Bearer as Basic with the host's documented username).
    ///
    /// Sensitive. `None` outside `github`/`gitlab` - anonymous and shared mode
    /// never had a client credential to forward, and `custom` sends it to its own
    /// validation endpoint instead - and also when the request carries none.
    ///
    /// This is a pure reshape: it makes no request, reads no verdict, and decides
    /// nothing. Only a credential `check` already allowed should be forwarded, and
    /// it is distinct from the proxy's own `--upstream-auth-header`.
    #[must_use]
    pub fn forwarded_credential(&self, headers: &HeaderMap) -> Option<HeaderValue> {
        let Mode::Provider(provider) = &self.mode else {
            return None;
        };
        let Check::Upstream { bearer_user, .. } = &provider.check else {
            return None;
        };
        credential(headers)?.forwarded(*bearer_user)
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

struct Provider {
    client: reqwest::Client,
    check: Check,
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
    metrics: Arc<Metrics>,
}

/// Where a client credential is checked.
enum Check {
    /// `github`/`gitlab`: upstream's own `info/refs`, which only advertises to a
    /// credential that can clone - unlike a metadata API, which also answers
    /// Guest members and tokens without read access to the code.
    Upstream {
        base: String,
        /// Username paired with a Bearer token when it is forwarded as Basic.
        bearer_user: BearerUser,
    },
    /// `custom`: any endpoint, with the token interpolated into one header.
    Endpoint {
        url: String,
        header_name: HeaderName,
        /// Header value template containing `{token}`.
        header_value: String,
        repo_form: RepoForm,
    },
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

impl Credential<'_> {
    /// This credential in the `Authorization` form a git host takes for its
    /// scheme: Basic verbatim, Bearer re-encoded as Basic with the username that
    /// host documents, since a bare token is never sent as a Bearer token to a
    /// git endpoint.
    ///
    /// Sensitive on the way out, so no caller can forward a token in the clear.
    /// The `None` arm cannot be reached - base64 output is always a valid header
    /// value - and is kept so a value that somehow is not fails closed instead of
    /// panicking on a request path.
    fn forwarded(&self, bearer_user: BearerUser) -> Option<HeaderValue> {
        let value = match self.scheme {
            // The username can matter (GitLab job tokens need
            // `gitlab-ci-token`), so Basic is not re-split and reassembled.
            Scheme::Basic => self.raw.clone(),
            Scheme::Bearer => HeaderValue::from_str(&format!(
                "Basic {}",
                STANDARD.encode(format!("{}:{}", bearer_user.as_str(), self.token))
            ))
            .ok()?,
        };
        Some(sensitive(value))
    }
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
            self.metrics.record_auth_cache_hit();
            return v;
        }
        let inflight = self.join(&key);
        *inflight
            .cell
            .get_or_init(|| async {
                let verdict = match self.request(&cred, repo) {
                    Some(req) => self.send(req, repo).await,
                    // Not representable as a header: no endpoint could accept it.
                    None => Verdict::Deny,
                };
                // Unavailable is never cached: the next request should retry.
                if verdict != Verdict::Unavailable {
                    self.remember(&key, verdict);
                }
                verdict
            })
            .await
    }

    fn request(&self, cred: &Credential<'_>, repo: &str) -> Option<reqwest::RequestBuilder> {
        match &self.check {
            Check::Upstream { base, bearer_user } => {
                let auth = cred.forwarded(*bearer_user)?;
                let url = format!("{base}/{repo}/info/refs?service=git-upload-pack");
                Some(
                    self.client
                        .get(url)
                        .header(header::AUTHORIZATION, auth)
                        .header("git-protocol", "version=2"),
                )
            }
            Check::Endpoint {
                url,
                header_name,
                header_value,
                repo_form,
            } => {
                let value =
                    HeaderValue::from_str(&header_value.replace("{token}", &cred.token)).ok()?;
                let url = url.replace("{repo}", &repo_name(repo, *repo_form));
                Some(
                    self.client
                        .get(url)
                        .header(header_name.clone(), sensitive(value)),
                )
            }
        }
    }

    async fn send(&self, req: reqwest::RequestBuilder, repo: &str) -> Verdict {
        let _permit = match &self.permits {
            Some(permits) => match tokio::time::timeout(self.timeout, permits.acquire()).await {
                Ok(Ok(permit)) => Some(permit),
                _ => {
                    tracing::warn!(repo, "token validation saturated (--auth-max-concurrent)");
                    self.metrics.record_validation(ValidationResult::Saturated);
                    return Verdict::Unavailable;
                }
            },
            None => None,
        };
        let verdict = self.call(req, repo).await;
        self.metrics.record_validation(verdict.into());
        verdict
    }

    async fn call(&self, req: reqwest::RequestBuilder, repo: &str) -> Verdict {
        let mut resp = match req.send().await {
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
        if verdict == Verdict::Allow && matches!(self.check, Check::Upstream { .. }) && !advertises
        {
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

/// Split `--auth-token-header` into its header name and value template.
fn parse_token_header(line: &str) -> Result<(HeaderName, String)> {
    if !line.contains("{token}") {
        bail!("--auth-token-header must contain {{token}}");
    }
    let Some((name, value)) = line.split_once(':') else {
        bail!("--auth-token-header must be a `Name: value` header line");
    };
    let name = name.trim();
    let name = HeaderName::from_bytes(name.as_bytes())
        .with_context(|| format!("--auth-token-header: invalid header name {name:?}"))?;
    Ok((name, value.trim().to_string()))
}

/// Build the provider mode: the reqwest client is built once, at startup, so
/// the per-request path stays infallible.
fn provider_mode(
    cfg: &AuthConfig,
    check: Check,
    signing_key: Option<&str>,
    metrics: Arc<Metrics>,
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
        check,
        permits: (cfg.max_concurrent != 0).then(|| Semaphore::new(cfg.max_concurrent)),
        timeout: cfg.timeout,
        ttl: (!cfg.ttl.is_zero()).then_some(cfg.ttl),
        verdicts: Mutex::new(LruCache::new(VERDICT_CAPACITY)),
        inflight: Mutex::new(HashMap::new()),
        grants,
        metrics,
    })))
}

/// The repo name for a custom endpoint's `{repo}`: `.git` stripped, then
/// encoded per the configured form. `url_safe` names need nothing else.
fn repo_name(repo: &str, form: RepoForm) -> String {
    let name = repo.strip_suffix(".git").unwrap_or(repo);
    match form {
        RepoForm::Path => name.to_string(),
        RepoForm::Percent => name.replace('/', "%2F"),
    }
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
        let legacy = build(config(AuthProvider::None)).unwrap();
        assert_eq!(legacy.describe(), "anonymous");
        let legacy = build(AuthConfig {
            serve_token: Some("s3cret".into()),
            ..config(AuthProvider::None)
        })
        .unwrap();
        assert_eq!(legacy.describe(), "shared token");
        let explicit = build(AuthConfig {
            serve_token: Some("s3cret".into()),
            ..config(AuthProvider::Shared)
        })
        .unwrap();
        assert_eq!(explicit.describe(), "shared token");
        // Blank collapses to absent, so it cannot enforce an empty secret.
        let blank = build(AuthConfig {
            serve_token: Some("   ".into()),
            ..config(AuthProvider::Shared)
        })
        .unwrap_err();
        assert!(blank.to_string().contains("requires --serve-token"));
    }

    #[test]
    fn provider_flags_without_a_provider_are_rejected() {
        // A validate URL with the default `none` mode would otherwise serve
        // anonymously while looking configured.
        for provider in [AuthProvider::None, AuthProvider::Shared] {
            let err = build(AuthConfig {
                serve_token: Some("s3cret".into()),
                validate_url: Some("http://127.0.0.1:1/v/{repo}".into()),
                ..config(provider)
            })
            .unwrap_err();
            assert!(err.to_string().contains("--auth-validate-url"));
            let err = build(AuthConfig {
                serve_token: Some("s3cret".into()),
                signing_key: Some(KEY.into()),
                ..config(provider)
            })
            .unwrap_err();
            assert!(err.to_string().contains("--auth-signing-key"));
        }
    }

    #[test]
    fn conflicting_flag_combinations_are_rejected() {
        for provider in [
            AuthProvider::Github,
            AuthProvider::Gitlab,
            AuthProvider::Custom,
        ] {
            let err = build(AuthConfig {
                serve_token: Some("s3cret".into()),
                ..provider_config(provider)
            })
            .unwrap_err();
            assert!(err.to_string().contains("--serve-token"));
        }
    }

    #[test]
    fn custom_requires_a_repo_template() {
        let err = build(AuthConfig {
            validate_url: Some("http://127.0.0.1:1/validate".into()),
            ..provider_config(AuthProvider::Custom)
        })
        .unwrap_err();
        assert!(err.to_string().contains("{repo}"));

        let no_url = build(AuthConfig {
            validate_url: None,
            ..provider_config(AuthProvider::Custom)
        })
        .unwrap_err();
        assert!(no_url.to_string().contains("--auth-validate-url"));

        // The token must never ride in the URL: transport errors echo it.
        let leaked = build(AuthConfig {
            validate_url: Some("http://127.0.0.1:1/validate/{repo}?t={token}".into()),
            ..provider_config(AuthProvider::Custom)
        })
        .unwrap_err();
        assert!(leaked.to_string().contains("--auth-token-header"));

        // A header without `{token}` would send nothing, validating everyone.
        let headerless = build(AuthConfig {
            token_header: Some("Authorization: Bearer".into()),
            ..provider_config(AuthProvider::Custom)
        })
        .unwrap_err();
        assert!(headerless.to_string().contains("{token}"));
    }

    #[test]
    fn custom_only_flags_are_rejected_for_profiles() {
        for provider in [AuthProvider::Github, AuthProvider::Gitlab] {
            let err = build(AuthConfig {
                validate_url: Some("http://127.0.0.1:1/v/{repo}".into()),
                ..provider_config(provider)
            })
            .unwrap_err();
            assert!(err.to_string().contains("--auth-validate-url"));

            let err = build(AuthConfig {
                token_header: Some("Authorization: Bearer {token}".into()),
                ..provider_config(provider)
            })
            .unwrap_err();
            assert!(err.to_string().contains("--auth-token-header"));

            let err = build(AuthConfig {
                repo_form: Some(RepoForm::Percent),
                ..provider_config(provider)
            })
            .unwrap_err();
            assert!(err.to_string().contains("--auth-repo-form"));
        }
    }

    #[test]
    fn profiles_probe_an_http_upstream_without_its_credentials() {
        let p = provider(AuthConfig {
            upstream_base: "https://proxy:secret@git.example.com/prefix/".into(),
            ..provider_config(AuthProvider::Gitlab)
        });
        match &p.check {
            Check::Upstream { base, bearer_user } => {
                assert_eq!(base, "https://git.example.com/prefix");
                assert_eq!(bearer_user.as_str(), "oauth2");
            }
            Check::Endpoint { .. } => panic!("gitlab probes upstream"),
        }
        let err = build(AuthConfig {
            upstream_base: "file:///srv/git".into(),
            ..provider_config(AuthProvider::Github)
        })
        .unwrap_err();
        assert!(err.to_string().contains("http(s)"));
    }

    #[test]
    fn plain_http_endpoints_need_an_opt_in() {
        let cases: [fn(bool) -> AuthConfig; 2] = [
            |allow_http| AuthConfig {
                upstream_base: "http://git.example.com".into(),
                allow_http,
                ..provider_config(AuthProvider::Github)
            },
            |allow_http| AuthConfig {
                validate_url: Some("http://auth.example.com/v/{repo}".into()),
                allow_http,
                ..provider_config(AuthProvider::Custom)
            },
        ];
        for case in cases {
            let err = build(case(false)).unwrap_err();
            assert!(err.to_string().contains("--auth-allow-http"), "{err}");
            assert!(build(case(true)).is_ok());
        }
    }

    #[test]
    fn timeout_must_be_positive() {
        let err = build(AuthConfig {
            timeout: Duration::from_secs(0),
            ..provider_config(AuthProvider::Custom)
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
    fn repo_names_are_encoded_per_form() {
        assert_eq!(repo_name("owner/repo.git", RepoForm::Path), "owner/repo");
        assert_eq!(
            repo_name("owner/repo.git", RepoForm::Percent),
            "owner%2Frepo"
        );
        assert_eq!(repo_name("g/team/p.git", RepoForm::Percent), "g%2Fteam%2Fp");
        assert_eq!(repo_name("foo.bar.git", RepoForm::Path), "foo.bar");
        assert_eq!(repo_name("proj", RepoForm::Path), "proj");
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
    fn unrepresentable_tokens_are_denied_without_a_call() {
        // A Basic password may decode to bytes no header can carry.
        let p = provider(provider_config(AuthProvider::Custom));
        let basic = STANDARD.encode(b"user:bad\r\nInjected: 1");
        let h = headers(&format!("Basic {basic}"));
        assert!(p.request(&credential(&h).unwrap(), "r.git").is_none());
    }

    #[test]
    fn a_bearer_token_is_forwarded_as_basic_with_the_hosts_username() {
        let github = build(provider_config(AuthProvider::Github)).unwrap();
        let forwarded = github.forwarded_credential(&headers("Bearer tok")).unwrap();
        assert_eq!(
            forwarded,
            format!("Basic {}", STANDARD.encode(b"x-access-token:tok"))
        );
        let gitlab = build(provider_config(AuthProvider::Gitlab)).unwrap();
        let forwarded = gitlab.forwarded_credential(&headers("Bearer tok")).unwrap();
        assert_eq!(
            forwarded,
            format!("Basic {}", STANDARD.encode(b"oauth2:tok"))
        );
        // The token must not reach a log line or a debug print.
        assert!(
            github
                .forwarded_credential(&headers("Bearer tok"))
                .unwrap()
                .is_sensitive()
        );
    }

    #[test]
    fn basic_is_forwarded_verbatim_whatever_its_username() {
        let auth = build(provider_config(AuthProvider::Gitlab)).unwrap();
        // GitLab job tokens only authenticate with their own username, so
        // forwarding must not substitute the Bearer username.
        let basic = format!("Basic {}", STANDARD.encode(b"gitlab-ci-token:job-token"));
        let forwarded = auth.forwarded_credential(&headers(&basic)).unwrap();
        assert_eq!(forwarded, basic);
        assert!(forwarded.is_sensitive());
    }

    #[test]
    fn only_provider_upstream_mode_has_a_credential_to_forward() {
        let h = headers("Bearer tok");
        // Nothing to forward: no mode, or a mode whose credential is the proxy's
        // own. `custom` keeps the token for its own validation header.
        for auth in [
            Auth::anonymous(),
            Auth::shared("s3cret"),
            build(provider_config(AuthProvider::Custom)).unwrap(),
        ] {
            assert_eq!(auth.forwarded_credential(&h), None, "{}", auth.describe());
        }
        // Provider mode, but no usable credential on the request.
        let github = build(provider_config(AuthProvider::Github)).unwrap();
        assert_eq!(github.forwarded_credential(&HeaderMap::new()), None);
        assert_eq!(github.forwarded_credential(&headers("Bearer ")), None);
        assert_eq!(github.forwarded_credential(&headers("Digest xyz")), None);
    }

    #[test]
    fn the_probe_sends_the_credential_the_public_function_returns() {
        // What proves read access in `check` must be what a fetch would forward,
        // or a caller cannot rely on either.
        for provider in [AuthProvider::Github, AuthProvider::Gitlab] {
            let auth = build(provider_config(provider)).unwrap();
            let basic = format!("Basic {}", STANDARD.encode(b"x-access-token:tok"));
            for presented in ["Bearer tok", basic.as_str()] {
                let h = headers(presented);
                let expected = auth.forwarded_credential(&h).unwrap();
                let req = provider_of(&auth)
                    .request(&credential(&h).unwrap(), "r.git")
                    .unwrap()
                    .build()
                    .unwrap();
                assert_eq!(req.headers()[header::AUTHORIZATION], expected.as_bytes());
                assert_eq!(
                    req.url().as_str(),
                    "https://upstream.invalid/r.git/info/refs?service=git-upload-pack"
                );
            }
        }
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

    #[tokio::test(start_paused = true)]
    async fn ttl_verdicts_expire() {
        let p = provider(AuthConfig {
            ttl: Duration::from_secs(1),
            ..provider_config(AuthProvider::Custom)
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
        let p = provider(provider_config(AuthProvider::Custom));
        let key = key();
        p.remember(&key, Verdict::Allow);
        assert_eq!(p.cached(&key), None);
        assert!(p.verdicts.lock().unwrap().is_empty());
    }

    #[test]
    fn the_verdict_cache_is_bounded() {
        let p = provider(AuthConfig {
            ttl: Duration::from_secs(60),
            ..provider_config(AuthProvider::Custom)
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
            ..provider_config(AuthProvider::Custom)
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
        assert!(
            p.metrics
                .gather()
                .contains(r#"gitcacheproxy_auth_validations_total{result="saturated"} 1"#)
        );
    }

    #[test]
    fn a_zero_limit_is_unbounded() {
        let p = provider(AuthConfig {
            max_concurrent: 0,
            ..provider_config(AuthProvider::Custom)
        });
        assert!(p.permits.is_none());
    }

    #[test]
    fn the_last_inflight_caller_removes_the_entry() {
        let p = provider(provider_config(AuthProvider::Custom));
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

    #[tokio::test]
    async fn lfs_objects_need_a_grant_for_exactly_that_object() {
        let auth = build(provider_config(AuthProvider::Github)).unwrap();
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
        let auth = build(provider_config(AuthProvider::Custom)).unwrap();
        let grant = auth.lfs_grant("r", "oid", 1).unwrap();
        assert!(grant.authorization.starts_with("Bearer gcp1."));
        assert_eq!(grant.expires_in, Some(grant::LIFETIME));
    }

    const KEY: &str = "0123456789abcdef0123456789abcdef";

    fn config(provider: AuthProvider) -> AuthConfig {
        AuthConfig {
            provider,
            serve_token: None,
            validate_url: None,
            token_header: None,
            repo_form: None,
            signing_key: None,
            timeout: Duration::from_secs(10),
            ttl: Duration::from_secs(0),
            max_concurrent: 32,
            allow_http: false,
            upstream_base: "https://upstream.invalid".into(),
        }
    }

    /// A valid config for a provider mode: custom gets its endpoint, every
    /// provider a shared signing key.
    fn provider_config(provider: AuthProvider) -> AuthConfig {
        AuthConfig {
            validate_url: (provider == AuthProvider::Custom)
                .then(|| "https://127.0.0.1:1/validate/{repo}".into()),
            signing_key: Some(KEY.into()),
            ..config(provider)
        }
    }

    fn build(cfg: AuthConfig) -> Result<Auth> {
        Auth::build(cfg, Arc::new(Metrics::new()))
    }

    fn provider(cfg: AuthConfig) -> Box<Provider> {
        match build(cfg).unwrap().mode {
            Mode::Provider(p) => p,
            _ => panic!("not the provider mode"),
        }
    }

    fn provider_of(auth: &Auth) -> &Provider {
        match &auth.mode {
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
