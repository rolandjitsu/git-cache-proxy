// SPDX-License-Identifier: Apache-2.0
//! git-cache-proxy: a read-only caching proxy for Git repositories.
//!
//! Sits between CI machines and an origin git server. On a clone/fetch it keeps
//! a local bare mirror fresh (incremental pull from upstream), then serves the
//! client from that mirror - so the bulk history is served locally/in-region and
//! only the delta crosses the WAN. Strictly pull-only: it never pushes and never
//! proactively replicates.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use tracing_subscriber::EnvFilter;

use git_cache_proxy::config::{Config, LogFormat};
use git_cache_proxy::{evict, git, lfs, metrics, repo, server};

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let cfg = Config::parse();
    // Hold the non-blocking writer's guard for the whole process; its drop flushes
    // any buffered log lines on exit.
    let _log_guard = init_tracing(&cfg.log, cfg.log_format);

    tokio::fs::create_dir_all(&cfg.cache_root)
        .await
        .with_context(|| format!("create cache root {}", cfg.cache_root.display()))?;

    // Normalize the upstream auth header: an env var set to an empty string
    // (the classic `-e VAR="$UNSET"` footgun) parses as `Some("")`, which would
    // inject a blank `http.extraHeader` - auth-less, but silently so. Collapse
    // any blank value to `None` and warn, so a missing token is visible at
    // startup instead of surfacing later as an upstream 401.
    let upstream_auth_header = cfg
        .upstream_auth_header
        .as_deref()
        .map(str::trim)
        .filter(|h| !h.is_empty())
        .map(str::to_string);
    if upstream_auth_header.is_none() {
        tracing::warn!(
            "no upstream auth header set; contacting upstream anonymously - \
             private repos will fail with an upstream 401 \
             (set --upstream-auth-header / GITCACHEPROXY_UPSTREAM_AUTH_HEADER)"
        );
    }

    let git_cfg = git::GitConfig {
        git_binary: cfg.git_binary.clone(),
        upstream_auth_header: upstream_auth_header.clone(),
        fetch_ttl: Duration::from_secs(cfg.fetch_ttl_seconds),
        max_wants: cfg.max_wants,
    };

    let metrics = Arc::new(metrics::Metrics::new());

    // Bound the cache on disk only when a cap is set; with `0` no index is built
    // and the eviction machinery stays inert, preserving the zero-overhead
    // unbounded-growth default. The startup scan runs here, before binding.
    let index = (cfg.cache_max_mb > 0).then(|| {
        tracing::info!("cache eviction enabled: cap {} MiB", cfg.cache_max_mb);
        evict::CacheIndex::new(
            cfg.cache_root.clone(),
            cfg.cache_max_mb.saturating_mul(1024 * 1024),
            metrics.clone(),
        )
    });

    let cache = Arc::new(git::GitCache::new(git_cfg, metrics.clone(), index.clone()));

    let upstream_base = cfg.upstream.trim_end_matches('/').to_string();

    // Clear any in-flight LFS downloads left by a crash so they never linger; the
    // objects themselves are content-addressed and re-fetched on demand.
    let _ = tokio::fs::remove_dir_all(
        cfg.cache_root
            .join(repo::LFS_OBJECTS_DIR)
            .join(lfs::INCOMING_DIR),
    )
    .await;

    let lfs = Arc::new(lfs::Lfs::new(
        lfs::LfsConfig {
            upstream_base: upstream_base.clone(),
            cache_root: cfg.cache_root.clone(),
            upstream_auth_header,
            serve_token: cfg.serve_token.clone(),
        },
        index.clone(),
    ));

    let state = server::AppState {
        cache: cache.clone(),
        lfs,
        upstream_base,
        cache_root: cfg.cache_root.clone(),
        serve_token: cfg.serve_token.clone(),
        max_decoded_body: (cfg.max_decoded_body_mb as usize).saturating_mul(1024 * 1024),
        max_concurrent: cfg.max_concurrent_requests,
        metrics: metrics.clone(),
    };

    // Spawn the event-driven evictor and keep its handle so shutdown can stop it
    // cleanly rather than leaving it dangling. `watch` carries the shutdown signal.
    let evictor = index.map(|idx| {
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let handle = tokio::spawn(evict::run(cache.clone(), idx, shutdown_rx));
        (shutdown_tx, handle)
    });

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

    // The server has drained; stop the evictor and wait for it to finish any
    // in-progress eviction before exiting.
    if let Some((shutdown_tx, handle)) = evictor {
        let _ = shutdown_tx.send(true);
        let _ = handle.await;
    }

    tracing::info!("shutdown complete");
    Ok(())
}

/// Initialize logging to a non-blocking stdout writer. Returns the writer's guard,
/// which must be held for the process lifetime (its drop flushes buffered logs).
/// The `RUST_LOG` env var, if set, overrides `filter`.
fn init_tracing(filter: &str, format: LogFormat) -> tracing_appender::non_blocking::WorkerGuard {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(filter));
    let (writer, guard) = tracing_appender::non_blocking(std::io::stdout());
    let base = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .with_writer(writer);
    match format {
        LogFormat::Json => base.json().flatten_event(true).init(),
        LogFormat::Text => base.init(),
    }
    guard
}

/// Completes on Ctrl-C or, on Unix, SIGTERM (Kubernetes/`docker stop` send SIGTERM
/// on shutdown), so the server drains in-flight requests before exiting either way.
async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let term = async {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(_) => std::future::pending().await,
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = term => {}
    }
    tracing::info!("shutdown signal received; draining");
}
