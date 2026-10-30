// SPDX-License-Identifier: Apache-2.0
//! Command-line / environment configuration.

use std::fmt;
use std::path::PathBuf;

use clap::{Parser, ValueEnum};

/// Log output format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum LogFormat {
    /// Human-readable single-line text.
    Text,
    /// One JSON object per line (for log shippers).
    Json,
}

/// How clients authenticate to the serving surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum AuthProvider {
    /// Serve anonymously - for a network-restricted deployment.
    None,
    /// Require one shared bearer token, given by `--serve-token`.
    Shared,
    /// Validate each request's credential by probing `--upstream` with it
    /// (`GET {upstream}/{repo}/info/refs`, protocol v2); a bare token is sent
    /// as Basic `x-access-token:<token>`.
    Github,
    /// Like `github`; a bare token is sent as Basic `oauth2:<token>`.
    Gitlab,
    /// Validate each request's token against any HTTP endpoint given
    /// by `--auth-validate-url`.
    Custom,
}

impl fmt::Display for AuthProvider {
    /// The flag value, so messages name the provider exactly as it is passed.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.to_possible_value()
            .expect("no value is skipped")
            .get_name()
            .fmt(f)
    }
}

/// How the repo path of a request is presented to the validation endpoint.
/// Provider modes only admit names of RFC 3986 unreserved characters and `/`,
/// so no other byte ever needs encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum RepoForm {
    /// `owner/repo`: strip a `.git` suffix (REST-style: one path component per
    /// segment).
    Path,
    /// `owner%2Frepo`: strip `.git`, then encode each `/` (single-parameter
    /// style, as GitLab's `/projects/{id}` wants).
    Percent,
}

#[derive(Parser, Debug, Clone)]
#[command(
    name = "git-cache-proxy",
    version,
    about = "Read-only caching proxy for Git repositories"
)]
pub struct Config {
    /// Address to bind the HTTP server to.
    #[arg(long, env = "GITCACHEPROXY_BIND", default_value = "0.0.0.0:8080")]
    pub bind: String,

    /// Directory holding the bare mirror caches.
    #[arg(
        long,
        env = "GITCACHEPROXY_CACHE_ROOT",
        default_value = "/var/cache/git-cache-proxy"
    )]
    pub cache_root: PathBuf,

    /// Upstream git base URL, e.g. `https://git.example.com`. Requested repo
    /// paths are appended to this to locate the origin.
    #[arg(long, env = "GITCACHEPROXY_UPSTREAM")]
    pub upstream: String,

    /// Optional header injected on upstream clone/fetch for auth, e.g.
    /// `Authorization: Bearer <token>`. Read from the environment so the token
    /// never appears in argv (and it is passed to git via env, not `-c`, so it
    /// stays out of the child's argv too).
    #[arg(
        long,
        env = "GITCACHEPROXY_UPSTREAM_AUTH_HEADER",
        hide_env_values = true
    )]
    pub upstream_auth_header: Option<String>,

    /// Optional bearer token clients must present (`Authorization: Bearer ...`).
    /// Unset = serve anonymously (intended for a network-restricted deployment).
    #[arg(long, env = "GITCACHEPROXY_SERVE_TOKEN", hide_env_values = true)]
    pub serve_token: Option<String>,

    /// Client-auth mode. `github`/`gitlab`/`custom` validate each request's
    /// credential for the repo it names before any serving, so a client only
    /// reaches what its own credential could clone directly - the blast radius
    /// of one leaked runner token is that runner's repos, not the fleet's. The
    /// default `none` serves anonymously; passing only `--serve-token` keeps the
    /// legacy shared-secret behavior.
    #[arg(
        long,
        env = "GITCACHEPROXY_AUTH_PROVIDER",
        value_enum,
        default_value = "none"
    )]
    pub auth_provider: AuthProvider,

    /// Provider modes: HMAC key (at least 32 bytes) signing the LFS download
    /// grants of batch responses. Every replica behind one address needs the
    /// same key; unset, a random per-process key only suits a single replica.
    #[arg(long, env = "GITCACHEPROXY_AUTH_SIGNING_KEY", hide_env_values = true)]
    pub auth_signing_key: Option<String>,

    /// `custom` only: URL template of the validation endpoint, with `{repo}`
    /// interpolated (already encoded per `--auth-repo-form`). The token must go
    /// in `--auth-token-header`, not this URL.
    #[arg(long, env = "GITCACHEPROXY_AUTH_VALIDATE_URL")]
    pub auth_validate_url: Option<String>,

    /// `custom` only: header line template presenting the client token, with
    /// `{token}` interpolated - any scheme the endpoint wants, e.g.
    /// `PRIVATE-TOKEN: {token}`.
    #[arg(long, env = "GITCACHEPROXY_AUTH_TOKEN_HEADER")]
    pub auth_token_header: Option<String>,

    /// `custom` only: how the repo name is encoded into `--auth-validate-url`'s
    /// `{repo}` placeholder.
    #[arg(long, env = "GITCACHEPROXY_AUTH_REPO_FORM", value_enum)]
    pub auth_repo_form: Option<RepoForm>,

    /// Total budget for one token-validation request. A hung API must not pin
    /// a clone request on its own.
    #[arg(long, env = "GITCACHEPROXY_AUTH_TIMEOUT_SECONDS", default_value_t = 10)]
    pub auth_timeout_seconds: u64,

    /// Maximum number of token-validation requests in flight. Validation runs
    /// before `--max-concurrent-requests` admits a request, so this is what
    /// bounds the outbound calls a flood of junk credentials can trigger. A
    /// check waits up to `--auth-timeout-seconds` for a slot, then fails closed
    /// with 503. `0` = unlimited.
    #[arg(long, env = "GITCACHEPROXY_AUTH_MAX_CONCURRENT", default_value_t = 32)]
    pub auth_max_concurrent: usize,

    /// Provider modes: allow an `http://` validation endpoint (`--upstream` for
    /// `github`/`gitlab`, `--auth-validate-url` for `custom`). Off by default:
    /// every client credential would cross the network in cleartext.
    #[arg(long, env = "GITCACHEPROXY_AUTH_ALLOW_HTTP")]
    pub auth_allow_http: bool,

    /// Reuse a validation verdict per `(credential, repo)` for this many
    /// seconds, so the requests of one clone (info/refs, then an upload-pack
    /// POST each for `ls-refs` and `fetch` over protocol v2), or repeated
    /// fetches of the same submodules, validate once. `0` = validate
    /// every request, the safest default.
    #[arg(long, env = "GITCACHEPROXY_AUTH_TTL_SECONDS", default_value_t = 0)]
    pub auth_ttl_seconds: u64,

    /// Skip the upstream fetch on `info/refs` if the last fetch started within
    /// this many seconds. Coalesces bursts of clones for the same repo. `0` =
    /// always fetch (freshest, more upstream load).
    #[arg(long, env = "GITCACHEPROXY_FETCH_TTL_SECONDS", default_value_t = 10)]
    pub fetch_ttl_seconds: u64,

    /// Maximum number of requests handled concurrently, shared across all
    /// connections. Excess requests queue until a slot frees. This bounds the
    /// concurrent upstream clone/fetch work a burst can trigger (the slot is held
    /// for the handler, then released before the packfile streams, so it caps
    /// setup rather than in-flight streaming; see `max_concurrent_upload_packs`).
    /// `0` = unlimited. Complementary to any ingress-level rate limiting.
    #[arg(
        long,
        env = "GITCACHEPROXY_MAX_CONCURRENT_REQUESTS",
        default_value_t = 64
    )]
    pub max_concurrent_requests: usize,

    /// Maximum number of `git upload-pack` processes at once, to bound
    /// `pack-objects` memory on large repos. A slot frees when the process exits,
    /// so it covers the streamed pack. Excess clones queue while holding a
    /// `max_concurrent_requests` slot, so a full cap can also block `info/refs`.
    /// `0` = unlimited.
    #[arg(
        long,
        env = "GITCACHEPROXY_MAX_CONCURRENT_UPLOAD_PACKS",
        default_value_t = 0
    )]
    pub max_concurrent_upload_packs: usize,

    /// Maximum size, in MiB, of a decoded `git-upload-pack` request body (the
    /// client's want/have negotiation). Bounds in-memory buffering and defuses a
    /// gzip decompression bomb - a small compressed body can expand ~1000x. This
    /// caps only the negotiation request, never the streamed packfile response,
    /// so raising it is rarely needed even for very large repositories.
    #[arg(long, env = "GITCACHEPROXY_MAX_DECODED_BODY_MB", default_value_t = 512)]
    pub max_decoded_body_mb: u64,

    /// Maximum total size, in MiB, of the on-disk mirror cache. When a clone or
    /// fetch pushes the total over this, least-recently-used idle mirrors are
    /// evicted in the background until it is back under; an evicted mirror is
    /// transparently re-cloned on its next request. `0` = unlimited: no eviction
    /// and no accounting, so the cache grows without bound (the default).
    #[arg(long, env = "GITCACHEPROXY_CACHE_MAX_MB", default_value_t = 0)]
    pub cache_max_mb: u64,

    /// Maximum number of want-by-SHA pins a mirror retains. A client may `want` a
    /// commit that lives only under an unadvertised upstream ref (e.g. a GitHub PR
    /// merge commit); the proxy fetches it on demand and pins it so it can keep
    /// serving it. Beyond this cap the oldest pins are pruned so they cannot
    /// accumulate without bound. `0` = unlimited.
    #[arg(long, env = "GITCACHEPROXY_MAX_WANTS", default_value_t = 100)]
    pub max_wants: usize,

    /// Path to the git binary.
    #[arg(long, env = "GITCACHEPROXY_GIT_BINARY", default_value = "git")]
    pub git_binary: String,

    /// Cap the memory an upstream `git clone`/`fetch` uses to mirror a repo: blobs
    /// larger than this are streamed to disk instead of held in RAM for delta
    /// resolution, so a single very large repo can't OOM the proxy. Passed to git as
    /// `core.bigFileThreshold` (git's own default is 512m). Accepts git size units
    /// (`8m`, `512k`, `1g`).
    #[arg(long, env = "GITCACHEPROXY_BIG_FILE_THRESHOLD", default_value = "8m")]
    pub big_file_threshold: String,

    /// `pack.threads` for serving: threads per `pack-objects` delta search. Unset
    /// = git default (one per CPU). Each thread has its own delta window.
    #[arg(long, env = "GITCACHEPROXY_PACK_THREADS")]
    pub pack_threads: Option<u32>,

    /// `pack.windowMemory` for serving: delta window memory cap per
    /// `pack-objects` thread, in git size units (`64m`). Unset = git default
    /// (unlimited).
    #[arg(long, env = "GITCACHEPROXY_PACK_WINDOW_MEMORY")]
    pub pack_window_memory: Option<String>,

    /// Log filter directive (e.g. `info`, `git_cache_proxy=debug,tower=warn`).
    /// Overridden by the `RUST_LOG` environment variable when set.
    #[arg(long, env = "GITCACHEPROXY_LOG", default_value = "info")]
    pub log: String,

    /// Log output format.
    #[arg(
        long,
        value_enum,
        env = "GITCACHEPROXY_LOG_FORMAT",
        default_value = "text"
    )]
    pub log_format: LogFormat,
}
