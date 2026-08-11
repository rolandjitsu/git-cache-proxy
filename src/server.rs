// SPDX-License-Identifier: Apache-2.0
//! HTTP surface: the git smart-HTTP endpoints plus health/metrics.
//!
//! Routing is method + path suffix based (git paths have arbitrary depth), so
//! the git handler is registered as the router fallback and dispatches:
//!   GET  <repo>/info/refs?service=git-upload-pack  -> ref advertisement
//!   POST <repo>/git-upload-pack                    -> packfile (streamed)
//!   anything git-receive-pack                      -> 403 (read-only)

use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{HeaderMap, Method, Request, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use subtle::ConstantTimeEq;
use tower::limit::GlobalConcurrencyLimitLayer;

use crate::git::GitCache;
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
    /// Upper bound (bytes) on a decoded upload-pack request body. See
    /// `Config::max_decoded_body_mb`.
    pub max_decoded_body: usize,
    /// Max concurrent in-flight requests (`0` = unlimited). See
    /// `Config::max_concurrent_requests`.
    pub max_concurrent: usize,
    pub metrics: Arc<Metrics>,
}

pub fn router(state: AppState) -> Router {
    let max_concurrent = state.max_concurrent;

    // Observability endpoints are deliberately kept *out* of the concurrency
    // limit below: a liveness/readiness probe or a metrics scrape must stay
    // responsive even when a burst of clones has saturated the semaphore -
    // otherwise a healthy-but-busy pod fails its probes and gets restarted.
    let observability = Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics_handler))
        .with_state(state.clone());

    // The git smart-HTTP surface has arbitrary-depth paths, so it is the router
    // fallback, and it is the only thing the concurrency limit wraps (`0`
    // disables it). One global semaphore shared across every per-connection
    // clone of the service (axum clones it per connection) makes the cap
    // process-wide rather than per-connection. `merge` keeps the (limited)
    // fallback as the merged fallback, while the observability routes above are
    // matched first and bypass it.
    let mut git = Router::new().fallback(handle_git).with_state(state);
    if max_concurrent != 0 {
        git = git.layer(GlobalConcurrencyLimitLayer::new(max_concurrent));
    }

    observability.merge(git)
}

async fn metrics_handler(State(st): State<AppState>) -> Response {
    Response::builder()
        .header(header::CONTENT_TYPE, "text/plain; version=0.0.4")
        .body(Body::from(st.metrics.gather()))
        .expect("valid response")
}

/// Readiness probe. Liveness (`/healthz`) only says the process is up; readiness
/// additionally verifies the proxy can do its one job - write bare mirrors into
/// the cache root - so a detached, unmounted, read-only, or unwritable cache
/// volume surfaces as `503 Service Unavailable` here instead of a later flood of
/// upstream `502`s. Kept out of the concurrency limit (see `router`).
async fn readyz(State(st): State<AppState>) -> Response {
    match cache_writable(&st.cache_root).await {
        Ok(()) => (StatusCode::OK, "ok").into_response(),
        Err(e) => {
            tracing::warn!(
                cache_root = %st.cache_root.display(),
                error = %e,
                "readiness check failed"
            );
            err(StatusCode::SERVICE_UNAVAILABLE, "cache root not writable")
        }
    }
}

/// Confirm the cache root exists and is writable by creating and removing a probe
/// file - the same directory the mirrors live in, so it tests the real target
/// rather than a proxy for it. A single create + unlink is cheap enough to run
/// per probe and catches the failure modes that make "ready" a lie: a missing
/// directory, a read-only remount, or a permissions problem.
async fn cache_writable(cache_root: &Path) -> std::io::Result<()> {
    tokio::fs::create_dir_all(cache_root).await?;
    let probe = cache_root.join(".readyz-probe");
    tokio::fs::write(&probe, b"").await?;
    let _ = tokio::fs::remove_file(&probe).await;
    Ok(())
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
        st.metrics.record_request("auth", "unauthorized", "-");
        return resp;
    }

    // Read-only: refuse anything that would write upstream.
    if path.ends_with(&format!("/{RECEIVE_PACK}"))
        || query.contains(&format!("service={RECEIVE_PACK}"))
    {
        st.metrics.record_request("receive_pack", "rejected", "-");
        return err(
            StatusCode::FORBIDDEN,
            "read-only proxy: pushes are not allowed",
        );
    }

    if parts.method == Method::GET && path.ends_with("/info/refs") {
        if !query.contains(&format!("service={UPLOAD_PACK}")) {
            st.metrics.record_request("info_refs", "error", "-");
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
        // Git's smart-HTTP client gzips the upload-pack request body (any
        // normally-sized want/have negotiation) and sends `Content-Encoding:
        // gzip`. `git upload-pack` reads its stdin as raw pkt-lines, so we must
        // undo the transport encoding before handing the body over - otherwise
        // it chokes on the gzip magic with "bad line length character".
        let content_encoding = parts
            .headers
            .get(header::CONTENT_ENCODING)
            .and_then(|v| v.to_str().ok());
        let body = match decode_body(content_encoding, body, st.max_decoded_body) {
            Ok(b) => b,
            Err(_) => return err(StatusCode::BAD_REQUEST, "failed to decode request body"),
        };
        return upload_pack(st, &path, git_protocol.as_deref(), body).await;
    }

    err(StatusCode::NOT_FOUND, "not a git smart-http endpoint")
}

async fn info_refs(st: AppState, path: &str, git_protocol: Option<&str>) -> Response {
    let Some(name) = repo::repo_name_from_path(path, "/info/refs") else {
        st.metrics.record_request("info_refs", "error", "-");
        return err(StatusCode::NOT_FOUND, "bad path");
    };
    let repo = match repo::resolve(&name, &st.upstream_base, &st.cache_root) {
        Ok(r) => r,
        Err(e) => {
            st.metrics.record_request("info_refs", "error", "-");
            return err(StatusCode::BAD_REQUEST, &e.to_string());
        }
    };

    // The upstream clone/fetch counters (per repo) are recorded inside `GitCache`;
    // here we only account for the client request. The `repo` label is emitted only
    // once a request is served; failures use `-` so a flood of distinct but doomed
    // repo paths cannot inflate label cardinality (see `metrics`).
    if let Err(e) = st.cache.ensure_fresh(&repo, true).await {
        st.metrics
            .record_request("info_refs", "upstream_error", "-");
        tracing::warn!(repo = %name, error = %e, "ensure_fresh failed");
        return err(StatusCode::BAD_GATEWAY, "upstream fetch failed");
    }

    match st.cache.advertise_refs(&repo, git_protocol).await {
        Ok(body) => {
            st.metrics.record_request("info_refs", "ok", &name);
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
            st.metrics.record_request("info_refs", "error", "-");
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
        st.metrics.record_request("upload_pack", "error", "-");
        return err(StatusCode::NOT_FOUND, "bad path");
    };
    let repo = match repo::resolve(&name, &st.upstream_base, &st.cache_root) {
        Ok(r) => r,
        Err(e) => {
            st.metrics.record_request("upload_pack", "error", "-");
            return err(StatusCode::BAD_REQUEST, &e.to_string());
        }
    };

    // The preceding info/refs already refreshed; here just ensure the mirror is
    // present (a client could POST against a not-yet-cloned repo).
    if let Err(e) = st.cache.ensure_fresh(&repo, false).await {
        st.metrics
            .record_request("upload_pack", "upstream_error", "-");
        tracing::warn!(repo = %name, error = %e, "ensure mirror exists failed");
        return err(StatusCode::BAD_GATEWAY, "upstream unavailable");
    }

    match st.cache.upload_pack_rpc(&repo, git_protocol, body).await {
        Ok(stream) => {
            st.metrics.record_request("upload_pack", "ok", &name);
            Response::builder()
                .header(header::CONTENT_TYPE, "application/x-git-upload-pack-result")
                .header(header::CACHE_CONTROL, "no-cache")
                .body(Body::from_stream(stream))
                .expect("valid response")
        }
        Err(e) => {
            st.metrics.record_request("upload_pack", "error", "-");
            tracing::warn!(repo = %name, error = %e, "upload_pack_rpc failed");
            err(StatusCode::INTERNAL_SERVER_ERROR, "upload-pack failed")
        }
    }
}

/// When a serve token is configured, require `Authorization: Bearer <token>`.
/// Returns `Some(401)` to short-circuit, `None` to allow.
fn check_auth(st: &AppState, headers: &HeaderMap) -> Option<Response> {
    let expected = st.serve_token.as_ref()?;
    let provided = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    if provided.is_some_and(|t| token_matches(t, expected)) {
        None
    } else {
        Some(err(
            StatusCode::UNAUTHORIZED,
            "missing or invalid bearer token",
        ))
    }
}

/// Compare a client-supplied bearer token against the expected one in constant
/// time, so response latency does not leak how many leading bytes matched. Only
/// the length can differ observably, which is not sensitive for a shared secret.
fn token_matches(provided: &str, expected: &str) -> bool {
    provided.as_bytes().ct_eq(expected.as_bytes()).into()
}

/// Undo the request's `Content-Encoding`. Git only ever gzips, so that is the
/// single encoding we decode; an absent/`identity` header passes through
/// untouched, and any other encoding is rejected by the caller as a bad body.
///
/// The decoded size is capped at `max_decoded`: DEFLATE reaches ~1000:1, so an
/// unbounded read here would let a small compressed body expand into an
/// out-of-memory kill (a decompression bomb). We read at most `max_decoded + 1`
/// bytes so we can tell "exactly at the limit" from "over it" and reject the
/// latter.
fn decode_body(
    content_encoding: Option<&str>,
    body: Bytes,
    max_decoded: usize,
) -> std::io::Result<Bytes> {
    match content_encoding.map(str::trim) {
        Some(enc) if enc.eq_ignore_ascii_case("gzip") || enc.eq_ignore_ascii_case("x-gzip") => {
            let mut out = Vec::new();
            let limit = max_decoded as u64 + 1;
            flate2::read::GzDecoder::new(&body[..])
                .take(limit)
                .read_to_end(&mut out)?;
            within_limit(Bytes::from(out), max_decoded)
        }
        // Uncompressed: no expansion risk, but still enforce the cap so
        // `--max-decoded-body-mb` bounds an identity request the same as a gzipped
        // one - otherwise only the coarser transport cap (`MAX_BODY`) would apply.
        None => within_limit(body, max_decoded),
        Some(enc) if enc.is_empty() || enc.eq_ignore_ascii_case("identity") => {
            within_limit(body, max_decoded)
        }
        Some(other) => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("unsupported content-encoding: {other}"),
        )),
    }
}

/// Reject a decoded body that exceeds the configured cap. Shared by every encoding
/// branch so the limit is enforced uniformly, not just when decoding gzip.
fn within_limit(body: Bytes, max_decoded: usize) -> std::io::Result<Bytes> {
    if body.len() > max_decoded {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "decoded request body exceeds limit",
        ));
    }
    Ok(body)
}

fn err(status: StatusCode, msg: &str) -> Response {
    (status, format!("{msg}\n")).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn gzip(bytes: &[u8]) -> Bytes {
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::best());
        enc.write_all(bytes).unwrap();
        Bytes::from(enc.finish().unwrap())
    }

    #[test]
    fn identity_and_absent_encoding_pass_through() {
        let raw = Bytes::from_static(b"want ...\n");
        assert_eq!(decode_body(None, raw.clone(), 1024).unwrap(), raw);
        assert_eq!(
            decode_body(Some("identity"), raw.clone(), 1024).unwrap(),
            raw
        );
        assert_eq!(decode_body(Some(""), raw.clone(), 1024).unwrap(), raw);
    }

    #[test]
    fn identity_body_over_limit_is_rejected() {
        // An uncompressed body must honor the decoded-body cap too, not just the
        // gzip path. Exactly at the limit is accepted; one byte over is rejected.
        let body = Bytes::from(vec![b'x'; 2048]);
        for enc in [None, Some("identity"), Some("")] {
            assert!(decode_body(enc, body.clone(), 2048).is_ok());
            assert!(decode_body(enc, body.clone(), 2047).is_err());
        }
    }

    #[test]
    fn gzip_within_limit_decodes() {
        let payload = b"command=ls-refs\n";
        let decoded = decode_body(Some("gzip"), gzip(payload), 1024).unwrap();
        assert_eq!(&decoded[..], payload);
        // Case-insensitive and the `x-gzip` alias both decode.
        assert_eq!(
            &decode_body(Some("GZIP"), gzip(payload), 1024).unwrap()[..],
            payload
        );
        assert_eq!(
            &decode_body(Some("x-gzip"), gzip(payload), 1024).unwrap()[..],
            payload
        );
    }

    #[test]
    fn gzip_decompression_bomb_is_rejected() {
        // 1 MiB of zeros compresses to ~1 KiB but must not be allowed to expand
        // past the cap. Exactly-at-limit is accepted; one byte over is rejected.
        let big = vec![0u8; 1024 * 1024];
        assert!(decode_body(Some("gzip"), gzip(&big), 1024).is_err());
        assert!(decode_body(Some("gzip"), gzip(&big), big.len()).is_ok());
        assert!(decode_body(Some("gzip"), gzip(&big), big.len() - 1).is_err());
    }

    #[test]
    fn unsupported_encoding_is_rejected() {
        assert!(decode_body(Some("br"), Bytes::from_static(b"x"), 1024).is_err());
        assert!(decode_body(Some("deflate"), Bytes::from_static(b"x"), 1024).is_err());
    }

    #[test]
    fn token_matches_only_the_exact_token() {
        assert!(token_matches("s3cret", "s3cret"));
        assert!(!token_matches("s3creT", "s3cret")); // last byte differs
        assert!(!token_matches("s3cre", "s3cret")); // prefix, shorter
        assert!(!token_matches("s3cret-extra", "s3cret")); // longer
        assert!(!token_matches("", "s3cret"));
        assert!(token_matches("", "")); // degenerate empty token
    }
}
