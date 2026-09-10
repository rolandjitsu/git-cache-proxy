// SPDX-License-Identifier: Apache-2.0
//! End-to-end git-LFS tests: a mock upstream LFS server (batch API + object storage)
//! on a live socket, with the proxy driven in-process reaching it over real HTTP.
//! Verifies batch href rewriting, a cache miss (fetch + sha256 verify + store), a
//! cache hit served from disk without touching upstream, and rejection of a
//! corrupted object.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::body::Body;
use axum::extract::State;
use axum::http::{Method, Request, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use git_cache_proxy::evict::CacheIndex;
use git_cache_proxy::git::{GitCache, GitConfig};
use git_cache_proxy::lfs::{Lfs, LfsConfig};
use git_cache_proxy::metrics::Metrics;
use git_cache_proxy::server::{AppState, router};
use sha2::{Digest, Sha256};
use tower::ServiceExt; // for `oneshot`

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_rewrites_download_href_to_the_proxy() {
    let (addr, _batches) = spawn_upstream(Mode::Ok).await;
    let cache = tempfile::tempdir().unwrap();
    let metrics = Arc::new(Metrics::new());
    let state = proxy_state(addr, cache.path(), metrics);
    let oid = oid_of(PAYLOAD);

    let resp = router(state)
        .oneshot(
            Request::post(format!("/{REPO}/info/lfs/objects/batch"))
                .header(header::HOST, "proxy.test:8080")
                .header(header::CONTENT_TYPE, "application/vnd.git-lfs+json")
                .body(Body::from(batch_body(&oid, PAYLOAD.len())))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let json: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    let download = &json["objects"][0]["actions"]["download"];
    assert_eq!(
        download["href"],
        format!(
            "http://proxy.test:8080/{REPO}/info/lfs/objects/{oid}?size={}",
            PAYLOAD.len()
        )
    );
    assert!(
        download.get("header").is_none(),
        "the upstream object auth header must not leak to the client"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn object_miss_fetches_then_hit_serves_from_cache() {
    let (addr, batches) = spawn_upstream(Mode::Ok).await;
    let cache = tempfile::tempdir().unwrap();
    let metrics = Arc::new(Metrics::new());
    let state = proxy_state(addr, cache.path(), metrics.clone());
    let oid = oid_of(PAYLOAD);
    let uri = format!("/{REPO}/info/lfs/objects/{oid}?size={}", PAYLOAD.len());

    // First GET is a miss: the proxy re-batches upstream, downloads, verifies, caches.
    let resp = router(state.clone())
        .oneshot(Request::get(&uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_bytes(resp).await, PAYLOAD);
    let after_miss = batches.load(Ordering::SeqCst);
    assert!(after_miss >= 1, "a miss must re-batch upstream");

    // The object is now on disk, content-addressed by oid.
    assert!(
        git_cache_proxy::repo::lfs_object_path(cache.path(), &oid).exists(),
        "object should be cached after a miss"
    );

    // Second GET is a hit: served from disk, no further upstream batch.
    let resp = router(state)
        .oneshot(Request::get(&uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_bytes(resp).await, PAYLOAD);
    assert_eq!(
        batches.load(Ordering::SeqCst),
        after_miss,
        "a cache hit must not contact upstream"
    );

    let scraped = metrics.gather();
    assert!(scraped.contains(r#"gitcacheproxy_lfs_objects_total{result="miss"} 1"#));
    assert!(scraped.contains(r#"gitcacheproxy_lfs_objects_total{result="hit"} 1"#));
    // The cached object counts toward the shared on-disk cache budget.
    assert!(
        scraped.contains(&format!("gitcacheproxy_cache_bytes {}", PAYLOAD.len())),
        "cached object bytes should be accounted:\n{scraped}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corrupted_object_fails_the_integrity_check() {
    // Upstream hands back bytes that do not hash to the requested oid; the proxy must
    // reject them (502) and cache nothing.
    let (addr, _batches) = spawn_upstream(Mode::Corrupt).await;
    let cache = tempfile::tempdir().unwrap();
    let metrics = Arc::new(Metrics::new());
    let state = proxy_state(addr, cache.path(), metrics.clone());
    let oid = oid_of(PAYLOAD);
    let uri = format!("/{REPO}/info/lfs/objects/{oid}?size={}", PAYLOAD.len());

    let resp = router(state)
        .oneshot(Request::get(&uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
    assert!(
        !git_cache_proxy::repo::lfs_object_path(cache.path(), &oid).exists(),
        "a corrupted object must not be cached"
    );
    assert!(
        metrics
            .gather()
            .contains(r#"gitcacheproxy_lfs_objects_total{result="error"} 1"#)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn object_download_failure_returns_bad_gateway() {
    // The batch succeeds but the object GET 404s; the proxy surfaces a 502, records
    // the error, and caches nothing (the temp download is cleaned up).
    let (addr, _batches) = spawn_upstream(Mode::Missing).await;
    let cache = tempfile::tempdir().unwrap();
    let metrics = Arc::new(Metrics::new());
    let state = proxy_state(addr, cache.path(), metrics.clone());
    let oid = oid_of(PAYLOAD);
    let uri = format!("/{REPO}/info/lfs/objects/{oid}?size={}", PAYLOAD.len());

    let resp = router(state)
        .oneshot(Request::get(&uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
    assert!(
        !git_cache_proxy::repo::lfs_object_path(cache.path(), &oid).exists(),
        "a failed download must not leave a cached object"
    );
    assert!(
        metrics
            .gather()
            .contains(r#"gitcacheproxy_lfs_objects_total{result="error"} 1"#)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upload_batch_is_rejected() {
    // A read-only proxy refuses an upload batch before any upstream contact.
    let (addr, batches) = spawn_upstream(Mode::Ok).await;
    let cache = tempfile::tempdir().unwrap();
    let metrics = Arc::new(Metrics::new());
    let state = proxy_state(addr, cache.path(), metrics);

    let body = serde_json::to_vec(&serde_json::json!({
        "operation": "upload",
        "objects": [{ "oid": oid_of(PAYLOAD), "size": PAYLOAD.len() }],
    }))
    .unwrap();
    let resp = router(state)
        .oneshot(
            Request::post(format!("/{REPO}/info/lfs/objects/batch"))
                .header(header::HOST, "proxy.test:8080")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    assert_eq!(
        batches.load(Ordering::SeqCst),
        0,
        "an upload must be refused before contacting upstream"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_upstream_failure_returns_bad_gateway() {
    // The upstream is not listening, so the batch POST fails and the proxy 502s.
    let cache = tempfile::tempdir().unwrap();
    let metrics = Arc::new(Metrics::new());
    let dead: SocketAddr = "127.0.0.1:1".parse().unwrap();
    let state = proxy_state(dead, cache.path(), metrics.clone());
    let resp = router(state)
        .oneshot(
            Request::post(format!("/{REPO}/info/lfs/objects/batch"))
                .header(header::HOST, "proxy.test:8080")
                .body(Body::from(batch_body(&oid_of(PAYLOAD), PAYLOAD.len())))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
    assert!(metrics.gather().contains(
        r#"gitcacheproxy_requests_total{kind="lfs_batch",repo="-",result="upstream_error"} 1"#
    ));
}

// --- test helpers --------------------------------------------------------

const REPO: &str = "group/repo.git";
const PAYLOAD: &[u8] = b"the quick brown fox jumps over the lazy dog\n";
/// A header the mock puts in the batch download action; the storage endpoint asserts
/// the proxy forwards it, proving batch-supplied object auth is honored.
const OBJECT_AUTH: &str = "X-Object-Auth: let-me-in";

/// How the mock upstream serves object storage, exercising the success and failure
/// paths (a corrupted object fails the integrity check; a missing one fails the GET).
#[derive(Clone, Copy)]
enum Mode {
    Ok,
    Corrupt,
    Missing,
}

/// Shared state of the mock upstream: how many batch calls it has served (so a test
/// can prove a cache hit did not re-contact upstream) and how it serves objects.
#[derive(Clone)]
struct Upstream {
    addr: SocketAddr,
    batches: Arc<AtomicUsize>,
    mode: Mode,
}

fn oid_of(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    let mut s = String::new();
    use std::fmt::Write as _;
    for b in h.finalize() {
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// The whole mock upstream: `POST <repo>/info/lfs/objects/batch` returns a download
/// action per requested object pointing at `GET <repo>/storage/<oid>`, which serves
/// the object bytes (or corrupted bytes) after checking the batch-supplied header.
async fn upstream_handler(State(up): State<Upstream>, req: Request<Body>) -> Response {
    let (parts, body) = req.into_parts();
    let path = parts.uri.path().to_string();

    if parts.method == Method::POST && path.ends_with("/info/lfs/objects/batch") {
        up.batches.fetch_add(1, Ordering::SeqCst);
        let repo = path
            .trim_start_matches('/')
            .strip_suffix("/info/lfs/objects/batch")
            .unwrap_or("")
            .to_string();
        let bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
        let req_json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let objects: Vec<serde_json::Value> = req_json["objects"]
            .as_array()
            .unwrap()
            .iter()
            .map(|o| {
                let oid = o["oid"].as_str().unwrap();
                let size = o["size"].as_u64().unwrap();
                let (k, v) = OBJECT_AUTH.split_once(": ").unwrap();
                serde_json::json!({
                    "oid": oid,
                    "size": size,
                    "actions": {
                        "download": {
                            "href": format!("http://{}/{repo}/storage/{oid}", up.addr),
                            "header": { k: v }
                        }
                    }
                })
            })
            .collect();
        let resp = serde_json::json!({ "transfer": "basic", "objects": objects });
        return (
            [(header::CONTENT_TYPE, "application/vnd.git-lfs+json")],
            serde_json::to_vec(&resp).unwrap(),
        )
            .into_response();
    }

    if parts.method == Method::GET && path.contains("/storage/") {
        // Prove the proxy forwarded the batch-supplied object authorization header.
        let (k, v) = OBJECT_AUTH.split_once(": ").unwrap();
        if parts.headers.get(k).and_then(|h| h.to_str().ok()) != Some(v) {
            return (StatusCode::UNAUTHORIZED, "missing object auth").into_response();
        }
        return match up.mode {
            Mode::Ok => (StatusCode::OK, PAYLOAD.to_vec()).into_response(),
            Mode::Corrupt => (StatusCode::OK, b"tampered".to_vec()).into_response(),
            Mode::Missing => (StatusCode::NOT_FOUND, "gone").into_response(),
        };
    }

    (StatusCode::NOT_FOUND, "mock: unknown path").into_response()
}

/// Bind the mock upstream on an ephemeral port and start serving; returns its addr
/// and the shared batch counter.
async fn spawn_upstream(mode: Mode) -> (SocketAddr, Arc<AtomicUsize>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let batches = Arc::new(AtomicUsize::new(0));
    let state = Upstream {
        addr,
        batches: batches.clone(),
        mode,
    };
    let app = axum::Router::new()
        .fallback(any(upstream_handler))
        .with_state(state);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (addr, batches)
}

/// A proxy `AppState` whose LFS upstream is the mock at `addr`, cached under `cache`.
fn proxy_state(addr: SocketAddr, cache: &std::path::Path, metrics: Arc<Metrics>) -> AppState {
    let upstream = format!("http://{addr}");
    let idx = CacheIndex::new(cache.to_path_buf(), u64::MAX, metrics.clone());
    let cfg = GitConfig {
        git_binary: "git".into(),
        upstream_auth_header: None,
        big_file_threshold: "8m".into(),
        fetch_ttl: Duration::from_secs(10),
        max_wants: 100,
    };
    AppState {
        cache: Arc::new(GitCache::new(cfg, metrics.clone(), Some(idx.clone()))),
        lfs: Arc::new(Lfs::new(
            LfsConfig {
                upstream_base: upstream.clone(),
                cache_root: cache.to_path_buf(),
                upstream_auth_header: None,
                serve_token: None,
            },
            Some(idx),
        )),
        upstream_base: upstream,
        cache_root: cache.to_path_buf(),
        serve_token: None,
        max_decoded_body: 512 * 1024 * 1024,
        max_concurrent: 8,
        metrics,
    }
}

fn batch_body(oid: &str, size: usize) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "operation": "download",
        "transfers": ["basic"],
        "objects": [{ "oid": oid, "size": size }],
    }))
    .unwrap()
}

async fn body_bytes(resp: Response) -> Vec<u8> {
    axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap()
        .to_vec()
}
