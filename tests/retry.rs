// SPDX-License-Identifier: Apache-2.0
//! Real local Git operations with injected upstream failures.
//! Keep real time while child processes run; a paused clock can expire the total
//! operation deadline before the operating system schedules a child.
#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use git_cache_proxy::git::{CacheOutcome, GitCache, GitConfig};
use git_cache_proxy::metrics::Metrics;
use git_cache_proxy::repo::{RepoRef, resolve};

#[tokio::test]
async fn clone_retries_tls_eof_and_removes_partial_staging() {
    let f = Fixture::new();
    f.fail(
        "clone",
        1,
        "TLS connect error: unexpected eof while reading",
    );
    let started = tokio::time::Instant::now();
    assert!(matches!(
        f.cache
            .ensure_fresh(&f.repo, true, b"", None)
            .await
            .unwrap(),
        CacheOutcome::Cloned
    ));
    assert_eq!(2, f.attempts("clone"));
    assert!(started.elapsed() >= Duration::from_secs(1));
    assert!(!f.repo.cache_dir.join("partial").exists());
    assert!(matches!(
        f.cache
            .ensure_fresh(&f.repo, true, b"", None)
            .await
            .unwrap(),
        CacheOutcome::Cached
    ));
    assert_eq!(2, f.attempts("clone"));
}

#[tokio::test]
async fn fetch_retries_http_503_and_keeps_the_mirror() {
    let f = Fixture::new();
    f.seed_mirror();
    f.fail("fetch", 2, "The requested URL returned error: 503");
    let started = tokio::time::Instant::now();
    assert!(matches!(
        f.cache
            .ensure_fresh(&f.repo, true, b"", None)
            .await
            .unwrap(),
        CacheOutcome::Fetched
    ));
    assert_eq!(3, f.attempts("fetch"));
    assert!(started.elapsed() >= Duration::from_secs(3));
    assert!(f.repo.cache_dir.join("HEAD").exists());
}

#[tokio::test]
async fn retries_are_bounded_and_failure_does_not_refresh_ttl() {
    for op in ["clone", "fetch"] {
        let f = Fixture::new();
        if op == "fetch" {
            f.seed_mirror();
        }
        f.fail(op, 10, "Recv failure: Connection reset by peer");
        let started = tokio::time::Instant::now();
        assert!(
            f.cache
                .ensure_fresh(&f.repo, true, b"", None)
                .await
                .is_err()
        );
        assert_eq!(4, f.attempts(op));
        assert!(started.elapsed() >= Duration::from_secs(7));
        assert!(!f.root.path().join("cache/repo.git.__incoming__").exists());
        f.fail(op, 0, "unused");
        assert!(f.cache.ensure_fresh(&f.repo, true, b"", None).await.is_ok());
        assert_eq!(5, f.attempts(op));
    }
}

#[tokio::test]
async fn permanent_errors_are_not_retried() {
    for op in ["clone", "fetch"] {
        for message in [
            "remote: Repository not found.",
            "fatal: Authentication failed",
            "The requested URL returned error: 401",
            "The requested URL returned error: 403",
            "The requested URL returned error: 404",
            "SSL certificate problem: certificate has expired",
            "fatal: No space left on device",
            "fatal: Unable to create shallow.lock: File exists",
            "fatal: unknown failure",
        ] {
            let f = Fixture::new();
            if op == "fetch" {
                f.seed_mirror();
            }
            f.fail(op, 10, message);
            assert!(
                f.cache
                    .ensure_fresh(&f.repo, true, b"", None)
                    .await
                    .is_err(),
                "{message}"
            );
            assert_eq!(1, f.attempts(op), "{message}");
        }
    }
}

#[tokio::test]
async fn concurrent_clients_share_one_retry_sequence() {
    let f = Fixture::new();
    f.fail(
        "clone",
        1,
        "Failed to connect to github.com port 443: Connection timed out",
    );
    let (a, b) = tokio::join!(
        f.cache.ensure_fresh(&f.repo, true, b"", None),
        f.cache.ensure_fresh(&f.repo, true, b"", None)
    );
    let outcomes = [a.unwrap(), b.unwrap()];
    assert_eq!(
        1,
        outcomes
            .iter()
            .filter(|o| matches!(o, CacheOutcome::Cloned))
            .count()
    );
    assert_eq!(
        1,
        outcomes
            .iter()
            .filter(|o| matches!(o, CacheOutcome::Cached))
            .count()
    );
    assert_eq!(2, f.attempts("clone"));
}

#[tokio::test]
async fn waiter_behind_a_slow_fetch_refetches_once_the_ttl_has_passed() {
    // TTL is measured from the start of a fetch, the moment it reads upstream.
    // Three retries take 7s, so a caller queued behind them sees stale refs.
    let f = Fixture::with_ttl(Duration::from_secs(5));
    f.seed_mirror();
    f.fail("fetch", 3, "Recv failure: Connection reset by peer");
    let (a, b) = tokio::join!(
        f.cache.ensure_fresh(&f.repo, true, b"", None),
        f.cache.ensure_fresh(&f.repo, true, b"", None)
    );
    assert!(matches!(a.unwrap(), CacheOutcome::Fetched));
    assert!(matches!(b.unwrap(), CacheOutcome::Fetched));
    assert_eq!(5, f.attempts("fetch"));
}

#[tokio::test]
async fn noisy_stderr_is_drained_and_not_exposed_in_errors() {
    let f = Fixture::new();
    let message = format!(
        "{}\nTLS connect error: unexpected eof while reading",
        "private-upstream-diagnostic\n".repeat(8192)
    );
    f.fail("clone", 10, &message);
    let error = f
        .cache
        .ensure_fresh(&f.repo, true, b"", None)
        .await
        .unwrap_err()
        .to_string();
    assert_eq!(4, f.attempts("clone"));
    assert!(!error.contains("private-upstream-diagnostic"));
    assert!(error.contains("TLS connection interrupted"));
}

#[tokio::test]
async fn sha_backfill_propagates_auth_but_preserves_best_effort_failure() {
    for (message, auth) in [
        ("fatal: Authentication failed", true),
        ("fatal: not our ref", false),
    ] {
        let f = Fixture::new();
        f.seed_mirror();
        f.fail("fetch", 10, message);
        let result = f
            .cache
            .ensure_fresh(
                &f.repo,
                false,
                b"0032want 1111111111111111111111111111111111111111\n0000",
                None,
            )
            .await;
        if auth {
            assert!(git_cache_proxy::git::is_auth_failure(&result.unwrap_err()));
        } else {
            assert!(matches!(result.unwrap(), CacheOutcome::Cached));
        }
        assert_eq!(f.attempts("fetch"), 1);
    }
}

#[tokio::test]
async fn dropped_request_cancels_owner_and_releases_repository_lock() {
    let f = Fixture::new();
    std::fs::write(f.root.path().join("stall-clone"), "").unwrap();
    let ready = f.root.path().join("ready-clone");
    assert!(
        Command::new("mkfifo")
            .arg(&ready)
            .status()
            .unwrap()
            .success()
    );
    let cache = f.cache.clone();
    let repo = f.repo.clone();
    let request = tokio::spawn(async move { cache.ensure_fresh(&repo, true, b"", None).await });
    tokio::time::timeout(Duration::from_secs(5), tokio::fs::read_to_string(ready))
        .await
        .unwrap()
        .unwrap();
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
    std::fs::remove_file(f.root.path().join("stall-clone")).unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        f.cache.ensure_fresh(&f.repo, true, b"", None),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(matches!(result, CacheOutcome::Cloned));
    f.cache.shutdown();
    f.cache.wait_for_shutdown().await;
}

struct Fixture {
    root: tempfile::TempDir,
    repo: RepoRef,
    cache: GitCache,
}

impl Fixture {
    fn new() -> Self {
        Self::with_ttl(Duration::from_secs(60))
    }

    fn with_ttl(fetch_ttl: Duration) -> Self {
        let root = tempfile::tempdir().unwrap();
        let upstream = root.path().join("upstream");
        std::fs::create_dir(&upstream).unwrap();
        git(&upstream, &["init", "--bare", "-q", "repo.git"]);
        let binary = root.path().join("git-wrapper");
        std::fs::write(
            &binary,
            r#"#!/bin/sh
set -eu
base=$(dirname "$0")
op=$1
if test -e "$base/stall-$op"; then
    echo ready > "$base/ready-$op"
    sleep 30
fi
case "$op" in clone|fetch) test "$LC_ALL" = C ;; esac
count=0
test ! -f "$base/count-$op" || count=$(cat "$base/count-$op")
count=$((count + 1))
echo "$count" > "$base/count-$op"
limit=0
test ! -f "$base/fail-$op" || limit=$(cat "$base/fail-$op")
if test "$op" = clone && test -e "$5/partial"; then
    echo 'staging directory was not cleaned' >&2
    exit 128
fi
if test "$count" -le "$limit"; then
    if test "$op" = clone; then mkdir -p "$5"; touch "$5/partial"; fi
    cat "$base/error-$op" >&2
    exit 128
fi
exec git "$@"
"#,
        )
        .unwrap();
        std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o755)).unwrap();
        let repo = resolve(
            "repo.git",
            &format!("file://{}", upstream.display()),
            &root.path().join("cache"),
        )
        .unwrap();
        let cache = GitCache::new(
            GitConfig {
                git_binary: binary.to_str().unwrap().into(),
                upstream_auth_header: None,
                big_file_threshold: "8m".into(),
                fetch_ttl,
                max_wants: 0,
                max_upload_packs: 0,
                pack_threads: None,
                pack_window_memory: None,
            },
            Arc::new(Metrics::new()),
            None,
        );
        Self { root, repo, cache }
    }

    fn seed_mirror(&self) {
        git(
            self.root.path(),
            &[
                "clone",
                "--mirror",
                "-q",
                &self.repo.upstream_url,
                self.repo.cache_dir.to_str().unwrap(),
            ],
        );
    }

    fn fail(&self, op: &str, count: u32, message: &str) {
        std::fs::write(
            self.root.path().join(format!("fail-{op}")),
            count.to_string(),
        )
        .unwrap();
        std::fs::write(self.root.path().join(format!("error-{op}")), message).unwrap();
    }

    fn attempts(&self, op: &str) -> u32 {
        std::fs::read_to_string(self.root.path().join(format!("count-{op}")))
            .unwrap()
            .trim()
            .parse()
            .unwrap()
    }
}

fn git(cwd: &Path, args: &[&str]) {
    let output = Command::new("git")
        .current_dir(cwd)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
