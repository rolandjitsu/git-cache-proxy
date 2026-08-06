// SPDX-License-Identifier: Apache-2.0
//! HTTP surface: the git smart-HTTP endpoints plus health/metrics.
//!
//! Routing is method + path suffix based (git paths have arbitrary depth), so
//! the git handler is registered as the router fallback and dispatches:
//!   GET  <repo>/info/refs?service=git-upload-pack  -> ref advertisement
//!   POST <repo>/git-upload-pack                    -> packfile (streamed)
//!   anything git-receive-pack                      -> 403 (read-only)

use std::path::PathBuf;
use std::sync::Arc;

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{HeaderMap, Method, Request, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;

use crate::git::{CacheOutcome, GitCache};
use crate::metrics::Metrics;
use crate::repo;

const MAX_BODY: usize = 64 * 1024 * 1024;
const UPLOAD_PACK: &str = "git-upload-pack";
const RECEIVE_PACK: &str = "git-receive-pack";

#[derive(Clone)]
pub struct AppState {
    pub cache: Arc<GitCache>,
    pub upstream_base: String,
    pub cache_root: PathBuf,
    pub serve_token: Option<String>,
    pub metrics: Arc<Metrics>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/readyz", get(|| async { "ok" }))
        .route("/metrics", get(metrics_handler))
        .fallback(handle_git)
        .with_state(state)
}

async fn metrics_handler(State(st): State<AppState>) -> Response {
    Response::builder()
        .header(header::CONTENT_TYPE, "text/plain; version=0.0.4")
        .body(Body::from(st.metrics.gather()))
        .expect("valid response")
}

async fn handle_git(State(st): State<AppState>, req: Request<Body>) -> Response {
    let (parts, body) = req.into_parts();
    let path = parts.uri.path().to_string();
    let query = parts.uri.query().unwrap_or("").to_string();
    let git_protocol = parts
        .headers
        .get("git-protocol")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);

    if let Some(resp) = check_auth(&st, &parts.headers) {
        return resp;
    }

    // Read-only: refuse anything that would write upstream.
    if path.ends_with(&format!("/{RECEIVE_PACK}"))
        || query.contains(&format!("service={RECEIVE_PACK}"))
    {
        return err(
            StatusCode::FORBIDDEN,
            "read-only proxy: pushes are not allowed",
        );
    }

    if parts.method == Method::GET && path.ends_with("/info/refs") {
        if !query.contains(&format!("service={UPLOAD_PACK}")) {
            return err(
                StatusCode::BAD_REQUEST,
                "only smart-http git-upload-pack is supported",
            );
        }
        return info_refs(st, &path, git_protocol.as_deref()).await;
    }

    if parts.method == Method::POST && path.ends_with(&format!("/{UPLOAD_PACK}")) {
        let body = match axum::body::to_bytes(body, MAX_BODY).await {
            Ok(b) => b,
            Err(_) => return err(StatusCode::BAD_REQUEST, "failed to read request body"),
        };
        return upload_pack(st, &path, git_protocol.as_deref(), body).await;
    }

    err(StatusCode::NOT_FOUND, "not a git smart-http endpoint")
}

async fn info_refs(st: AppState, path: &str, git_protocol: Option<&str>) -> Response {
    let Some(name) = repo::repo_name_from_path(path, "/info/refs") else {
        return err(StatusCode::NOT_FOUND, "bad path");
    };
    let repo = match repo::resolve(&name, &st.upstream_base, &st.cache_root) {
        Ok(r) => r,
        Err(e) => return err(StatusCode::BAD_REQUEST, &e.to_string()),
    };

    match st.cache.ensure_fresh(&repo, true).await {
        Ok(outcome) => record_upstream(&st.metrics, outcome),
        Err(e) => {
            st.metrics
                .requests
                .with_label_values(&["info_refs", "upstream_error"])
                .inc();
            tracing::warn!(repo = %name, error = %e, "ensure_fresh failed");
            return err(StatusCode::BAD_GATEWAY, "upstream fetch failed");
        }
    }

    match st.cache.advertise_refs(&repo, git_protocol).await {
        Ok(body) => {
            st.metrics
                .requests
                .with_label_values(&["info_refs", "ok"])
                .inc();
            Response::builder()
                .header(
                    header::CONTENT_TYPE,
                    "application/x-git-upload-pack-advertisement",
                )
                .header(header::CACHE_CONTROL, "no-cache")
                .body(Body::from(body))
                .expect("valid response")
        }
        Err(e) => {
            tracing::warn!(repo = %name, error = %e, "advertise_refs failed");
            err(StatusCode::INTERNAL_SERVER_ERROR, "advertise-refs failed")
        }
    }
}

async fn upload_pack(
    st: AppState,
    path: &str,
    git_protocol: Option<&str>,
    body: Bytes,
) -> Response {
    let Some(name) = repo::repo_name_from_path(path, &format!("/{UPLOAD_PACK}")) else {
        return err(StatusCode::NOT_FOUND, "bad path");
    };
    let repo = match repo::resolve(&name, &st.upstream_base, &st.cache_root) {
        Ok(r) => r,
        Err(e) => return err(StatusCode::BAD_REQUEST, &e.to_string()),
    };

    // The preceding info/refs already refreshed; here just ensure the mirror is
    // present (a client could POST against a not-yet-cloned repo).
    if let Err(e) = st.cache.ensure_fresh(&repo, false).await {
        tracing::warn!(repo = %name, error = %e, "ensure mirror exists failed");
        return err(StatusCode::BAD_GATEWAY, "upstream unavailable");
    }

    match st.cache.upload_pack_rpc(&repo, git_protocol, body).await {
        Ok(stream) => {
            st.metrics
                .requests
                .with_label_values(&["upload_pack", "ok"])
                .inc();
            Response::builder()
                .header(header::CONTENT_TYPE, "application/x-git-upload-pack-result")
                .header(header::CACHE_CONTROL, "no-cache")
                .body(Body::from_stream(stream))
                .expect("valid response")
        }
        Err(e) => {
            tracing::warn!(repo = %name, error = %e, "upload_pack_rpc failed");
            err(StatusCode::INTERNAL_SERVER_ERROR, "upload-pack failed")
        }
    }
}

fn record_upstream(m: &Metrics, outcome: CacheOutcome) {
    match outcome {
        CacheOutcome::Cloned => m.upstream.with_label_values(&["clone", "ok"]).inc(),
        CacheOutcome::Fetched => m.upstream.with_label_values(&["fetch", "ok"]).inc(),
        CacheOutcome::Cached => {}
    }
}

/// When a serve token is configured, require `Authorization: Bearer <token>`.
/// Returns `Some(401)` to short-circuit, `None` to allow.
fn check_auth(st: &AppState, headers: &HeaderMap) -> Option<Response> {
    let expected = st.serve_token.as_ref()?;
    let ok = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .is_some_and(|t| t == expected);
    if ok {
        None
    } else {
        Some(err(
            StatusCode::UNAUTHORIZED,
            "missing or invalid bearer token",
        ))
    }
}

fn err(status: StatusCode, msg: &str) -> Response {
    (status, format!("{msg}\n")).into_response()
}
