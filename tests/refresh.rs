// SPDX-License-Identifier: Apache-2.0
//! Public refresh configuration and startup behavior.

use std::future::Future;
use std::path::Path;
use std::process::Command;
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use clap::Parser;
use git_cache_proxy::auth::Auth;
use git_cache_proxy::config::{Config, UpstreamCredential};
use git_cache_proxy::git::{GitCache, GitConfig, RefreshConfig};
use git_cache_proxy::lfs::{Lfs, LfsConfig};
use git_cache_proxy::metrics::Metrics;
use git_cache_proxy::repo::resolve;
use git_cache_proxy::server::{AppState, router};
use tower::ServiceExt;

mod helpers;

#[tokio::test(start_paused = true)]
async fn refresh_default_disabled_does_not_start_a_task() {
    let cfg = Config::try_parse_from(["proxy", "--upstream", "https://example.com"]).unwrap();
    assert_eq!(cfg.refresh_interval_seconds, 0);
    assert_eq!(cfg.refresh_active_window_seconds, 604800);
    assert_eq!(cfg.max_concurrent_refreshes, 4);
    cfg.validate_refresh().unwrap();
    let cache = GitCache::new(
        GitConfig {
            git_binary: "missing-git-must-not-run".into(),
            fetch_ttl: Duration::from_secs(10),
            refresh: cfg.refresh_config(),
            ..helpers::test_git_config()
        },
        Arc::new(Metrics::new()),
        None,
    );
    assert!(cache.start_refresh().is_none());
    tokio::time::advance(Duration::from_secs(604800)).await;
    cache.shutdown();
    cache.wait_for_shutdown().await;
}

#[test]
fn refresh_config_maps_the_enabled_options() {
    let cfg = Config::try_parse_from([
        "proxy",
        "--upstream=https://example.com",
        "--refresh-interval-seconds=7200",
        "--refresh-active-window-seconds=86400",
        "--max-concurrent-refreshes=2",
    ])
    .unwrap();
    let refresh = cfg.refresh_config().unwrap();
    assert_eq!(refresh.interval, Duration::from_secs(7200));
    assert_eq!(refresh.active_window, Duration::from_secs(86400));
    assert_eq!(refresh.concurrency, 2);
}

#[test]
fn refresh_validation_ignores_inactive_options_when_disabled() {
    let mut cfg = Config::try_parse_from([
        "proxy",
        "--upstream",
        "https://example.com",
        "--max-concurrent-refreshes",
        "0",
    ])
    .unwrap();
    assert!(cfg.validate_refresh().is_ok());
    cfg.refresh_interval_seconds = 3599;
    assert!(cfg.validate_refresh().is_err());
    cfg.refresh_interval_seconds = 3600;
    assert!(cfg.validate_refresh().is_err());
    cfg.max_concurrent_refreshes = 1;
    assert!(cfg.validate_refresh().is_ok());
}

#[test]
fn refresh_validation_rejects_overflow_and_zero_active_window() {
    let mut cfg = Config::try_parse_from(["proxy", "--upstream", "https://example.com"]).unwrap();
    cfg.refresh_interval_seconds = u64::MAX;
    assert!(
        cfg.validate_refresh()
            .unwrap_err()
            .to_string()
            .contains("monotonic deadline")
    );
    cfg.refresh_interval_seconds = 3600;
    cfg.refresh_active_window_seconds = 0;
    assert!(
        cfg.validate_refresh()
            .unwrap_err()
            .to_string()
            .contains("--refresh-active-window-seconds")
    );
    cfg.refresh_interval_seconds = 0;
    assert!(cfg.validate_refresh().is_ok());
}

#[test]
fn refresh_invalid_settings_fail_before_creating_cache() {
    for (interval, concurrency, diagnostic) in [
        ("3599", "4", "--refresh-interval-seconds"),
        ("18446744073709551615", "4", "monotonic deadline"),
        ("3600", "0", "--max-concurrent-refreshes"),
    ] {
        let root = tempfile::tempdir().unwrap();
        let cache = root.path().join("not-created");
        let output = Command::new(env!("CARGO_BIN_EXE_git-cache-proxy"))
            .args([
                "--upstream",
                "https://example.com",
                "--refresh-interval-seconds",
                interval,
                "--max-concurrent-refreshes",
                concurrency,
                "--cache-root",
            ])
            .arg(&cache)
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains(diagnostic));
        assert!(!cache.exists());
    }
}

#[tokio::test(start_paused = true)]
async fn background_refresh_pulls_new_upstream_commits_without_a_client_request() {
    let up = tempfile::tempdir().unwrap();
    let work = tempfile::tempdir().unwrap();
    git(work.path(), &["init", "-q", "-b", "main", "."]);
    git(
        work.path(),
        &["commit", "-q", "--allow-empty", "-m", "first"],
    );
    let upstream_repo = up.path().join("repo.git");
    git(
        work.path(),
        &[
            "clone",
            "-q",
            "--bare",
            ".",
            upstream_repo.to_str().unwrap(),
        ],
    );

    let cache_root = tempfile::tempdir().unwrap();
    let metrics = Arc::new(Metrics::new());
    // The smallest interval the CLI accepts.
    let interval = Duration::from_secs(3600);
    let cache = Arc::new(GitCache::new(
        GitConfig {
            git_binary: "git".into(),
            upstream_auth_header: None,
            big_file_threshold: "8m".into(),
            fetch_ttl: Duration::ZERO,
            max_wants: 100,
            max_upload_packs: 0,
            pack_threads: None,
            pack_window_memory: None,
            refresh: Some(RefreshConfig {
                interval,
                active_window: Duration::from_secs(86400),
                concurrency: 1,
            }),
        },
        metrics.clone(),
        None,
    ));
    let refresher = cache.start_refresh().unwrap();
    let app = router(app_state(
        cache.clone(),
        metrics.clone(),
        up.path(),
        cache_root.path(),
    ));

    let response = without_auto_advance(
        app.oneshot(
            Request::get("/repo.git/info/refs?service=git-upload-pack")
                .header("git-protocol", "version=2")
                .body(Body::empty())
                .unwrap(),
        ),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let mirror = resolve("repo.git", "unused", cache_root.path())
        .unwrap()
        .cache_dir;
    let first = git(work.path(), &["rev-parse", "HEAD"]);
    assert_eq!(git(&mirror, &["rev-parse", "refs/heads/main"]), first);

    git(
        work.path(),
        &["commit", "-q", "--allow-empty", "-m", "second"],
    );
    let second = git(work.path(), &["rev-parse", "HEAD"]);
    git(
        work.path(),
        &["push", "-q", upstream_repo.to_str().unwrap(), "main"],
    );

    // Fire the first round. No further client request is made, so only the
    // scheduled refresh can move the mirror. The ref moves before the fetch
    // process exits, so also wait for the recorded result.
    tokio::time::advance(interval).await;
    wait_until(|| {
        let moved = git(&mirror, &["rev-parse", "refs/heads/main"]) == second;
        let recorded = metrics
            .gather()
            .contains(r#"gitcacheproxy_refresh_total{result="ok"}"#);
        moved && recorded
    })
    .await;

    let out = metrics.gather();
    assert!(
        out.contains(
            r#"gitcacheproxy_requests_total{kind="info_refs",repo="repo.git",result="ok"} 1"#
        ),
        "the mirror must have been refreshed without another client request"
    );

    cache.shutdown();
    refresher.await.unwrap();
    cache.wait_for_shutdown().await;
}

/// Hold the paused clock still while `work` waits on real subprocesses. With no
/// blocking task outstanding, tokio auto-advances an idle paused clock to the
/// next timer, here the upstream timeout, and the clone would fail.
async fn without_auto_advance<T>(work: impl Future<Output = T>) -> T {
    let (release, hold) = mpsc::channel::<()>();
    let guard = tokio::task::spawn_blocking(move || {
        let _ = hold.recv();
    });
    let out = work.await;
    drop(release);
    guard.await.unwrap();
    out
}

/// Poll `ready` every 10ms of paused time. Each check spawns `git`, which keeps
/// the virtual clock from racing far ahead of the real work it waits for (and
/// into the upstream timeout); the deadline is real time for the same reason.
async fn wait_until(mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !ready() {
        assert!(
            Instant::now() < deadline,
            "background refresh did not fetch the new upstream commit"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn app_state(
    cache: Arc<GitCache>,
    metrics: Arc<Metrics>,
    upstream_root: &Path,
    cache_root: &Path,
) -> AppState {
    let upstream_base = format!("file://{}", upstream_root.display());
    AppState {
        cache,
        lfs: Arc::new(Lfs::new(
            LfsConfig {
                upstream_base: upstream_base.clone(),
                cache_root: cache_root.to_path_buf(),
                upstream_auth_header: None,
            },
            None,
        )),
        upstream_base,
        cache_root: cache_root.to_path_buf(),
        auth: Arc::new(Auth::anonymous()),
        upstream_credential: UpstreamCredential::Static,
        max_decoded_body: 512 * 1024 * 1024,
        max_concurrent: 8,
        metrics,
    }
}

fn git(cwd: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .current_dir(cwd)
        .args(args)
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_AUTHOR_NAME", "t")
        .env("GIT_AUTHOR_EMAIL", "t@example.com")
        .env("GIT_COMMITTER_NAME", "t")
        .env("GIT_COMMITTER_EMAIL", "t@example.com")
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap().trim().to_string()
}
