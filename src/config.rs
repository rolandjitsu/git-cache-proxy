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
