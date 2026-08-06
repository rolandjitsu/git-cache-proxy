// SPDX-License-Identifier: Apache-2.0
//! HTTP handler tests that exercise the router in-process (no live socket).
//! Most cover the request paths that terminate *before* any upstream work: auth,
//! read-only push rejection, request validation, health, and metrics. One drives
//! the upstream-failure path (a clone against a bogus upstream) to check the 502
//! and error accounting. The happy-path end-to-end git flow lives in `e2e.rs`.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use git_cache_proxy::git::{GitCache, GitConfig};
use git_cache_proxy::metrics::Metrics;
use git_cache_proxy::server::{AppState, router};
use tower::ServiceExt; // for `oneshot`

fn state(serve_token: Option<String>) -> AppState {
    // None of these tests reach the cache dir; a path under the temp dir that is
    // never created is fine, and keeps the test filesystem-free.
    let cache_root = std::env::temp_dir().join("git-cache-proxy-tests-unused");
    let metrics = Arc::new(Metrics::new());
    let cfg = GitConfig {
        git_binary: "git".into(),
        upstream_auth_header: None,
        fetch_ttl: Duration::from_secs(10),
    };
    AppState {
        cache: Arc::new(GitCache::new(cfg, metrics.clone())),
        upstream_base: "https://upstream.invalid".into(),
        cache_root,
        serve_token,
        metrics,
    }
}

async fn get(state: AppState, uri: &str) -> axum::http::Response<Body> {
    router(state)
        .oneshot(Request::get(uri).body(Body::empty()).unwrap())
        .await
        .unwrap()
}

async fn body_string(resp: axum::http::Response<Body>) -> String {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    String::from_utf8_lossy(&bytes).into_owned()
}

#[tokio::test]
async fn health_endpoints_return_ok() {
    assert_eq!(get(state(None), "/healthz").await.status(), StatusCode::OK);
    let resp = get(state(None), "/readyz").await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_string(resp).await, "ok");
}

#[tokio::test]
async fn push_over_get_is_rejected() {
    let resp = get(state(None), "/repo.git/info/refs?service=git-receive-pack").await;
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn push_over_post_is_rejected() {
    let resp = router(state(None))
        .oneshot(
            Request::post("/repo.git/git-receive-pack")
                .body(Body::from("0000"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn info_refs_requires_upload_pack_service() {
    // No `service=` param -> dumb-http request, which we don't support.
    let resp = get(state(None), "/repo.git/info/refs").await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn unknown_path_is_not_found() {
    let resp = get(state(None), "/not/a/git/endpoint").await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn serve_token_required_when_configured() {
    let st = state(Some("s3cret".into()));

    // Missing token -> 401.
    let resp = get(st.clone(), "/repo.git/info/refs?service=git-upload-pack").await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // Wrong token -> 401.
    let resp = router(st.clone())
        .oneshot(
            Request::get("/repo.git/info/refs?service=git-upload-pack")
                .header(header::AUTHORIZATION, "Bearer wrong")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn upstream_failure_returns_bad_gateway_and_records_error() {
    // A real (but doomed) clone: the upstream points at a path that does not
    // exist, so `git clone --mirror` fails and the request surfaces a 502.
    let cache = tempfile::tempdir().unwrap();
    let metrics = Arc::new(Metrics::new());
    let cfg = GitConfig {
        git_binary: "git".into(),
        upstream_auth_header: None,
        fetch_ttl: Duration::from_secs(10),
    };
    let st = AppState {
        cache: Arc::new(GitCache::new(cfg, metrics.clone())),
        upstream_base: "file:///nonexistent/git-cache-proxy-upstream".into(),
        cache_root: cache.path().to_path_buf(),
        serve_token: None,
        metrics: metrics.clone(),
    };

    let resp = get(st, "/missing.git/info/refs?service=git-upload-pack").await;
    assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);

    let scraped = metrics.gather();
    assert!(
        scraped.contains(r#"op="clone",repo="missing.git",result="error""#),
        "expected clone error metric, got:\n{scraped}"
    );
    assert!(
        scraped.contains(r#"result="upstream_error""#),
        "expected request upstream_error metric, got:\n{scraped}"
    );
}

#[tokio::test]
async fn metrics_endpoint_renders_recorded_series() {
    let st = state(None);
    // Drive an auth rejection so at least one series exists, then scrape.
    let _ = get(st.clone(), "/repo.git/info/refs?service=git-receive-pack").await;

    let resp = get(st.clone(), "/metrics").await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok()),
        Some("text/plain; version=0.0.4")
    );
    let body = body_string(resp).await;
    assert!(
        body.contains("gitcacheproxy_requests_total"),
        "metrics body missing request counter: {body}"
    );
}
