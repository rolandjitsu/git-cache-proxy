// SPDX-License-Identifier: Apache-2.0
//! git-cache-proxy: a read-only caching proxy for Git repositories.
//!
//! Sits between CI machines and an origin git server. On a clone/fetch it keeps
//! a local bare mirror fresh (incremental pull from upstream), then serves the
//! client from that mirror - so the bulk history is served locally/in-region and
//! only the delta crosses the WAN. Strictly pull-only: it never pushes and never
//! proactively replicates.

mod config;
mod git;
mod metrics;
mod repo;
mod server;

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use tracing_subscriber::EnvFilter;

use crate::config::Config;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cfg = Config::parse();

    tokio::fs::create_dir_all(&cfg.cache_root)
        .await
        .with_context(|| format!("create cache root {}", cfg.cache_root.display()))?;

    let git_cfg = git::GitConfig {
        git_binary: cfg.git_binary.clone(),
        upstream_auth_header: cfg.upstream_auth_header.clone(),
        fetch_ttl: Duration::from_secs(cfg.fetch_ttl_seconds),
    };

    let state = server::AppState {
        cache: Arc::new(git::GitCache::new(git_cfg)),
        upstream_base: cfg.upstream.trim_end_matches('/').to_string(),
        cache_root: cfg.cache_root.clone(),
        serve_token: cfg.serve_token.clone(),
        metrics: Arc::new(metrics::Metrics::new()),
    };

    let listener = tokio::net::TcpListener::bind(&cfg.bind)
        .await
        .with_context(|| format!("bind {}", cfg.bind))?;
    tracing::info!(
        "git-cache-proxy listening on {} (upstream {}, cache {})",
        cfg.bind,
        cfg.upstream,
        cfg.cache_root.display(),
    );

    axum::serve(listener, server::router(state))
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("http server")?;
    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("shutdown signal received");
}
