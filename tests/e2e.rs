// SPDX-License-Identifier: Apache-2.0
//! End-to-end test: a real `git` client clones through a live proxy instance
//! whose upstream is a local `file://` bare repo. Exercises the full path -
//! `clone --mirror`, ref advertisement, and the streamed `upload-pack` - plus
//! that all refs (branches/tags, not just HEAD) are served, that per-repo
//! upstream metrics are recorded, and that pushes are rejected over the wire.
//!
//! Requires `git` on PATH (the proxy's whole design delegates to it).

use std::io::Write;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use git_cache_proxy::evict::CacheIndex;
use git_cache_proxy::git::{GitCache, GitConfig};
use git_cache_proxy::lfs::{Lfs, LfsConfig};
use git_cache_proxy::metrics::Metrics;
use git_cache_proxy::server::{AppState, router};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tower::ServiceExt; // for `oneshot`

/// Regression test for the gzip transport bug: git's smart-HTTP client
/// compresses the `git-upload-pack` request body and sends `Content-Encoding:
/// gzip`. The proxy must decode it before handing the bytes to `git
/// upload-pack`; before the fix it forwarded the gzip stream verbatim and
/// upload-pack died with "protocol error: bad line length character". The happy
/// path never caught this because git only gzips past a size threshold, so a tiny
/// clone slips through uncompressed. Here we build a protocol-v2 `ls-refs`
/// request, gzip it ourselves, and assert the proxy still serves the refs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upload_pack_decodes_gzip_encoded_request() {
    // Upstream bare repo with a known branch.
    let work = tempfile::tempdir().unwrap();
    git(work.path(), &["init", "-q", "-b", "main", "."]);
    std::fs::write(work.path().join("README.md"), "hello\n").unwrap();
    git(work.path(), &["add", "."]);
    git(work.path(), &["commit", "-q", "-m", "init"]);

    let up = tempfile::tempdir().unwrap();
    let upstream_repo = up.path().join("repo.git");
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

    // Proxy over the file:// upstream (in-process, driven via `oneshot`).
    let cache = tempfile::tempdir().unwrap();
    let metrics = Arc::new(Metrics::new());
    let cfg = GitConfig {
        git_binary: "git".into(),
        upstream_auth_header: None,
        big_file_threshold: "8m".into(),
        fetch_ttl: Duration::from_secs(0),
    };
    let state = AppState {
        cache: Arc::new(GitCache::new(cfg, metrics.clone(), None)),
        lfs: Arc::new(Lfs::new(
            LfsConfig {
                upstream_base: format!("file://{}", up.path().display()),
                cache_root: cache.path().to_path_buf(),
                upstream_auth_header: None,
                serve_token: None,
            },
            None,
        )),
        upstream_base: format!("file://{}", up.path().display()),
        cache_root: cache.path().to_path_buf(),
        serve_token: None,
        max_decoded_body: 512 * 1024 * 1024,
        max_concurrent: 8,
        metrics,
    };

    // Build a minimal protocol-v2 `ls-refs` request and gzip it, exactly as a
    // real client frames + compresses the POST body.
    let mut req = Vec::new();
    req.extend_from_slice(&pkt("command=ls-refs\n"));
    req.extend_from_slice(&pkt("object-format=sha1\n"));
    req.extend_from_slice(b"0001"); // delim-pkt: end of capabilities
    req.extend_from_slice(b"0000"); // flush-pkt: end of request
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    enc.write_all(&req).unwrap();
    let gz = enc.finish().unwrap();

    let resp = router(state)
        .oneshot(
            Request::post("/repo.git/git-upload-pack")
                .header("content-type", "application/x-git-upload-pack-request")
                .header("content-encoding", "gzip")
                .header("git-protocol", "version=2")
                .body(Body::from(gz))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "gzip-encoded upload-pack POST should be accepted"
    );
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let text = String::from_utf8_lossy(&body);
    assert!(
        text.contains("refs/heads/main"),
        "ls-refs response should list refs/heads/main; got: {text:?}"
    );
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
        big_file_threshold: "8m".into(),
        fetch_ttl: Duration::from_secs(0),
    };
    // Eviction enabled with an effectively unbounded cap: no mirror is ever
    // evicted, but the on-request index bookkeeping (touch on serve, mark-changed
    // on clone/fetch) runs, which the assertion below checks.
    let idx = CacheIndex::new(cache.path().to_path_buf(), u64::MAX, metrics.clone());
    let state = AppState {
        cache: Arc::new(GitCache::new(cfg, metrics.clone(), Some(idx.clone()))),
        lfs: Arc::new(Lfs::new(
            LfsConfig {
                upstream_base: format!("file://{}", up.path().display()),
                cache_root: cache.path().to_path_buf(),
                upstream_auth_header: None,
                serve_token: None,
            },
            Some(idx.clone()),
        )),
        upstream_base: format!("file://{}", up.path().display()),
        cache_root: cache.path().to_path_buf(),
        serve_token: None,
        max_decoded_body: 512 * 1024 * 1024,
        max_concurrent: 8,
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
    // The client-request counters carry the repo label too: a clone drives both
    // an info/refs advertisement and an upload-pack, each recorded per repo.
    assert!(
        scraped.contains(
            r#"gitcacheproxy_requests_total{kind="info_refs",repo="repo.git",result="ok"}"#
        ),
        "missing per-repo info_refs request metric:\n{scraped}"
    );
    assert!(
        scraped.contains(
            r#"gitcacheproxy_requests_total{kind="upload_pack",repo="repo.git",result="ok"}"#
        ),
        "missing per-repo upload_pack request metric:\n{scraped}"
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

    // The clone and fetch both flowed through the cache index: it tracks the one
    // mirror the proxy created.
    assert_eq!(
        idx.totals().1,
        1,
        "cache index should track the cloned repo"
    );

    // Latency histograms were recorded per repo for the synchronous ops: the
    // upstream clone + fetch, and the info/refs advertisement serve. (The streamed
    // upload-pack serve is timed on EOF and covered by a unit test in `git.rs`.)
    for series in [
        r#"gitcacheproxy_upstream_duration_seconds_count{op="clone",repo="repo.git"}"#,
        r#"gitcacheproxy_upstream_duration_seconds_count{op="fetch",repo="repo.git"}"#,
        r#"gitcacheproxy_serve_duration_seconds_count{kind="info_refs",repo="repo.git"}"#,
    ] {
        assert!(
            scraped.contains(series),
            "missing latency histogram {series}:\n{scraped}"
        );
    }

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

/// A `--depth 1` clone through the proxy must truncate history to the tip commit
/// and mark the clone shallow. Shallow serving works with no special server
/// config; this locks that in so a future serving-path change can't silently
/// regress it (no other test exercises shallow).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shallow_clone_through_proxy_truncates_history() {
    let work = tempfile::tempdir().unwrap();
    git(work.path(), &["init", "-q", "-b", "main", "."]);
    std::fs::write(work.path().join("README.md"), "one\n").unwrap();
    git(work.path(), &["add", "."]);
    git(work.path(), &["commit", "-q", "-m", "first"]);
    std::fs::write(work.path().join("README.md"), "two\n").unwrap();
    git(work.path(), &["commit", "-q", "-am", "second"]);

    let up = tempfile::tempdir().unwrap();
    let upstream_repo = up.path().join("repo.git");
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

    let proxy = start_proxy(up.path(), "8m").await;
    let dest = tempfile::tempdir().unwrap();
    let checkout = dest.path().join("checkout");
    let url = proxy.url("repo.git");
    let (ok, log) = git_try(
        dest.path(),
        &[
            "clone",
            "-q",
            "--depth",
            "1",
            url.as_str(),
            checkout.to_str().unwrap(),
        ],
    )
    .await;
    assert!(ok, "shallow clone failed: {log}");

    assert_eq!(
        std::fs::read_to_string(checkout.join("README.md")).unwrap(),
        "two\n"
    );
    // History truncated to the single tip commit, and git marks the repo shallow.
    assert_eq!(git(&checkout, &["rev-list", "--count", "HEAD"]).trim(), "1");
    assert_eq!(
        git(&checkout, &["rev-parse", "--is-shallow-repository"]).trim(),
        "true"
    );

    proxy.server.abort();
}

/// A `--filter=blob:none` clone through the proxy must be a *genuine* partial
/// clone: the server honors the filter (no "filtering not recognized" fallback),
/// an older blob HEAD doesn't need stays absent, and the HEAD checkout back-fills
/// its blob over HTTP (the promisor round-trip). Regression guard for
/// `uploadpack.allowFilter` on the serve path - without it the filter is silently
/// dropped and every blob is streamed. Note git records `partialclonefilter`
/// locally either way, so the *missing-blob count* is the real discriminator.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partial_clone_through_proxy_omits_blobs_and_backfills() {
    // Two commits so an older blob exists that HEAD's checkout does not need.
    let work = tempfile::tempdir().unwrap();
    git(work.path(), &["init", "-q", "-b", "main", "."]);
    std::fs::write(work.path().join("README.md"), "old\n").unwrap();
    git(work.path(), &["add", "."]);
    git(work.path(), &["commit", "-q", "-m", "first"]);
    std::fs::write(work.path().join("README.md"), "new\n").unwrap();
    git(work.path(), &["commit", "-q", "-am", "second"]);

    let up = tempfile::tempdir().unwrap();
    let upstream_repo = up.path().join("repo.git");
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

    let proxy = start_proxy(up.path(), "8m").await;
    let dest = tempfile::tempdir().unwrap();
    let checkout = dest.path().join("checkout");
    let url = proxy.url("repo.git");
    let (ok, log) = git_try(
        dest.path(),
        &[
            "clone",
            "-q",
            "--filter=blob:none",
            url.as_str(),
            checkout.to_str().unwrap(),
        ],
    )
    .await;
    assert!(ok, "partial clone failed: {log}");
    // The server honored the filter (else git warns and does a silent full clone),
    // and the HEAD checkout back-filled its blob over HTTP without erroring.
    assert!(
        !log.contains("filtering not recognized"),
        "server ignored the filter - is uploadpack.allowFilter enabled? log:\n{log}"
    );
    assert_eq!(
        std::fs::read_to_string(checkout.join("README.md")).unwrap(),
        "new\n"
    );
    // The older commit's blob is never needed by HEAD and stays genuinely absent -
    // the discriminator between a real partial clone and a silently-full one.
    let missing = git(
        &checkout,
        &["rev-list", "--objects", "--all", "--missing=print"],
    );
    let missing_blobs = missing.lines().filter(|l| l.starts_with('?')).count();
    assert!(
        missing_blobs >= 1,
        "expected >=1 omitted blob in a blob:none clone, got none:\n{missing}"
    );

    proxy.server.abort();
}

/// A clone forced onto the legacy protocol **v0** advertisement (modern clients
/// default to v2) must still work through the proxy, serving all refs. The
/// existing gzip test covers v2; this covers the v0/v1 path the README claims.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clone_over_protocol_v0_through_proxy() {
    let work = tempfile::tempdir().unwrap();
    git(work.path(), &["init", "-q", "-b", "main", "."]);
    std::fs::write(work.path().join("README.md"), "hello\n").unwrap();
    git(work.path(), &["add", "."]);
    git(work.path(), &["commit", "-q", "-m", "init"]);
    git(work.path(), &["tag", "v1"]);

    let up = tempfile::tempdir().unwrap();
    let upstream_repo = up.path().join("repo.git");
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

    let proxy = start_proxy(up.path(), "8m").await;
    let dest = tempfile::tempdir().unwrap();
    let checkout = dest.path().join("checkout");
    let url = proxy.url("repo.git");
    let (ok, log) = git_try(
        dest.path(),
        &[
            "-c",
            "protocol.version=0",
            "clone",
            "-q",
            url.as_str(),
            checkout.to_str().unwrap(),
        ],
    )
    .await;
    assert!(ok, "v0 clone failed: {log}");
    assert_eq!(
        std::fs::read_to_string(checkout.join("README.md")).unwrap(),
        "hello\n"
    );
    assert!(git(&checkout, &["tag"]).contains("v1"), "expected tag v1");

    proxy.server.abort();
}

/// A blob larger than `core.bigFileThreshold` makes the upstream clone stream it
/// to disk instead of buffering it in memory (the OOM-bounding path). Correctness
/// must survive that path: the proxy serves the large blob back byte-for-byte.
/// Guards the memory-bounding behavior against silent corruption regressions.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clone_with_low_big_file_threshold_serves_large_blob_intact() {
    let work = tempfile::tempdir().unwrap();
    git(work.path(), &["init", "-q", "-b", "main", "."]);
    let blob: Vec<u8> = (0..256 * 1024).map(|i| (i % 251) as u8).collect();
    std::fs::write(work.path().join("big.bin"), &blob).unwrap();
    git(work.path(), &["add", "."]);
    git(work.path(), &["commit", "-q", "-m", "big"]);

    let up = tempfile::tempdir().unwrap();
    let upstream_repo = up.path().join("repo.git");
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

    // Threshold well below the 256 KiB blob forces the streaming path.
    let proxy = start_proxy(up.path(), "64k").await;
    let dest = tempfile::tempdir().unwrap();
    let checkout = dest.path().join("checkout");
    let url = proxy.url("repo.git");
    let (ok, log) = git_try(
        dest.path(),
        &["clone", "-q", url.as_str(), checkout.to_str().unwrap()],
    )
    .await;
    assert!(ok, "clone with low bigFileThreshold failed: {log}");
    assert_eq!(
        std::fs::read(checkout.join("big.bin")).unwrap(),
        blob,
        "large blob served through the proxy was corrupted"
    );

    proxy.server.abort();
}

// --- Test helpers ---------------------------------------------------------

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

/// Run `git` in `cwd` returning `(success, combined stdout+stderr)` without
/// asserting - for cases that need to inspect output (e.g. a filter-ignored
/// warning) or drive a clone whose failure the test reports itself.
async fn git_try(cwd: &Path, args: &[&str]) -> (bool, String) {
    let out = tokio::process::Command::new("git")
        .current_dir(cwd)
        .args(args)
        .env("GIT_TERMINAL_PROMPT", "0")
        .output()
        .await
        .expect("spawn git");
    let mut log = String::from_utf8_lossy(&out.stdout).into_owned();
    log.push_str(&String::from_utf8_lossy(&out.stderr));
    (out.status.success(), log)
}

/// Frame a string as a single pkt-line, the way git builds a protocol-v2 request.
fn pkt(s: &str) -> Vec<u8> {
    let mut v = format!("{:04x}", s.len() + 4).into_bytes();
    v.extend_from_slice(s.as_bytes());
    v
}

/// A live proxy over a `file://` upstream, used by the protocol-coverage tests.
/// Holds the cache tempdir and server task so they outlive the test body.
struct Proxy {
    addr: std::net::SocketAddr,
    server: tokio::task::JoinHandle<()>,
    _cache: tempfile::TempDir,
}

impl Proxy {
    fn url(&self, repo: &str) -> String {
        format!("http://{}/{repo}", self.addr)
    }
}

/// Bring up the proxy pointed at `up` (the dir holding the bare upstream repo,
/// served over `file://`) with the given `core.bigFileThreshold`. Factors out the
/// inline proxy setup shared by the protocol-coverage tests.
async fn start_proxy(up: &Path, big_file_threshold: &str) -> Proxy {
    let cache = tempfile::tempdir().unwrap();
    let metrics = Arc::new(Metrics::new());
    let cfg = GitConfig {
        git_binary: "git".into(),
        upstream_auth_header: None,
        big_file_threshold: big_file_threshold.into(),
        fetch_ttl: Duration::from_secs(0),
    };
    let state = AppState {
        cache: Arc::new(GitCache::new(cfg, metrics.clone(), None)),
        lfs: Arc::new(Lfs::new(
            LfsConfig {
                upstream_base: format!("file://{}", up.display()),
                cache_root: cache.path().to_path_buf(),
                upstream_auth_header: None,
                serve_token: None,
            },
            None,
        )),
        upstream_base: format!("file://{}", up.display()),
        cache_root: cache.path().to_path_buf(),
        serve_token: None,
        max_decoded_body: 512 * 1024 * 1024,
        max_concurrent: 8,
        metrics,
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });
    Proxy {
        addr,
        server,
        _cache: cache,
    }
}
