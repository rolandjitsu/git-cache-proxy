// SPDX-License-Identifier: Apache-2.0
//! Command-line / environment configuration.

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

    /// Skip the upstream fetch on `info/refs` if the mirror was refreshed within
    /// this many seconds. Coalesces bursts of clones for the same repo. `0` =
    /// always fetch (freshest, more upstream load).
    #[arg(long, env = "GITCACHEPROXY_FETCH_TTL_SECONDS", default_value_t = 10)]
    pub fetch_ttl_seconds: u64,

    /// Maximum number of requests handled concurrently, shared across all
    /// connections. Excess requests queue until a slot frees. This bounds the
    /// concurrent upstream clone/fetch work a burst can trigger (the slot is held
    /// for the handler, then released before the packfile streams, so it caps
    /// setup rather than in-flight streaming). `0` = unlimited. Complementary to
    /// any ingress-level rate limiting.
    #[arg(
        long,
        env = "GITCACHEPROXY_MAX_CONCURRENT_REQUESTS",
        default_value_t = 64
    )]
    pub max_concurrent_requests: usize,

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

    /// Path to the git binary.
    #[arg(long, env = "GITCACHEPROXY_GIT_BINARY", default_value = "git")]
    pub git_binary: String,

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
