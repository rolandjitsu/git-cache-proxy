// SPDX-License-Identifier: Apache-2.0
//! End-to-end test: a real `git` client clones through a live proxy instance
//! whose upstream is a local `file://` bare repo. Exercises the full path -
//! `clone --mirror`, ref advertisement, and the streamed `upload-pack` - plus
//! that all refs (branches/tags, not just HEAD) are served, that per-repo
//! upstream metrics are recorded, and that pushes are rejected over the wire.
//!
//! Requires `git` on PATH (the proxy's whole design delegates to it).

use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use git_cache_proxy::git::{GitCache, GitConfig};
use git_cache_proxy::metrics::Metrics;
use git_cache_proxy::server::{AppState, router};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Run a git command in `cwd` with a hermetic identity/config, asserting success.
fn git(cwd: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .current_dir(cwd)
        .args(args)
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_AUTHOR_NAME", "test")
        .env("GIT_AUTHOR_EMAIL", "test@example.invalid")
        .env("GIT_COMMITTER_NAME", "test")
        .env("GIT_COMMITTER_EMAIL", "test@example.invalid")
        .output()
        .expect("spawn git");
    assert!(
        out.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clones_through_proxy_serves_all_refs_and_rejects_push() {
    // --- Build an upstream bare repo with a branch and a tag, not just HEAD. ---
    let work = tempfile::tempdir().unwrap();
    git(work.path(), &["init", "-q", "-b", "main", "."]);
    std::fs::write(work.path().join("README.md"), "hello\n").unwrap();
    git(work.path(), &["add", "."]);
    git(work.path(), &["commit", "-q", "-m", "init"]);
    git(work.path(), &["branch", "feature"]);
    git(work.path(), &["tag", "v1"]);

    let up = tempfile::tempdir().unwrap();
    let upstream_repo = up.path().join("repo.git");
    // --mirror so the bare upstream carries every ref (branches + tags).
    git(
        work.path(),
        &[
            "clone",
            "-q",
            "--mirror",
            ".",
            upstream_repo.to_str().unwrap(),
        ],
    );

    // --- Bring up the proxy pointed at the file:// upstream. ---
    let cache = tempfile::tempdir().unwrap();
    let metrics = Arc::new(Metrics::new());
    let cfg = GitConfig {
        git_binary: "git".into(),
        upstream_auth_header: None,
        fetch_ttl: Duration::from_secs(0),
    };
    let state = AppState {
        cache: Arc::new(GitCache::new(cfg, metrics.clone())),
        upstream_base: format!("file://{}", up.path().display()),
        cache_root: cache.path().to_path_buf(),
        serve_token: None,
        metrics: metrics.clone(),
    };

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });

    // --- A real git client clones through the proxy. ---
    let dest = tempfile::tempdir().unwrap();
    let checkout = dest.path().join("checkout");
    let out = tokio::process::Command::new("git")
        .arg("clone")
        .arg("-q")
        .arg(format!("http://{addr}/repo.git"))
        .arg(&checkout)
        .env("GIT_TERMINAL_PROMPT", "0")
        .output()
        .await
        .expect("spawn git clone");
    assert!(
        out.status.success(),
        "clone through proxy failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        std::fs::read_to_string(checkout.join("README.md")).unwrap(),
        "hello\n"
    );

    // The mirror carried more than HEAD: the tag and the non-default branch are
    // both retrievable through the proxy.
    let tags = git(&checkout, &["tag"]);
    assert!(tags.contains("v1"), "expected tag v1, got: {tags:?}");
    let remotes = git(&checkout, &["branch", "-r"]);
    assert!(
        remotes.contains("feature"),
        "expected origin/feature, got: {remotes:?}"
    );

    // Per-repo upstream metrics were recorded for the on-demand clone.
    let scraped = metrics.gather();
    assert!(
        scraped.contains(r#"op="clone""#) && scraped.contains(r#"repo="repo.git""#),
        "missing per-repo clone metric:\n{scraped}"
    );

    // A second clone finds the mirror present and (fetch_ttl = 0) drives an
    // incremental fetch rather than a re-clone - exercising the fetch path and
    // recording a per-repo fetch metric.
    let checkout2 = dest.path().join("checkout2");
    let out = tokio::process::Command::new("git")
        .arg("clone")
        .arg("-q")
        .arg(format!("http://{addr}/repo.git"))
        .arg(&checkout2)
        .env("GIT_TERMINAL_PROMPT", "0")
        .output()
        .await
        .expect("spawn second git clone");
    assert!(
        out.status.success(),
        "second clone failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let scraped = metrics.gather();
    assert!(
        scraped.contains(r#"op="fetch""#) && scraped.contains(r#"repo="repo.git""#),
        "missing per-repo fetch metric after second clone:\n{scraped}"
    );

    // --- A push attempt is rejected over the wire (403). ---
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "GET /repo.git/info/refs?service=git-receive-pack HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n"
    );
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.unwrap();
    let resp = String::from_utf8_lossy(&buf);
    assert!(
        resp.starts_with("HTTP/1.1 403"),
        "expected 403 for receive-pack, got: {}",
        &resp[..resp.len().min(64)]
    );

    server.abort();
}
