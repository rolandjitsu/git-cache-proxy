// SPDX-License-Identifier: Apache-2.0
//! Shared helpers for the suites that exercise per-request token validation:
//! a mock upstream `info/refs` on a live local socket, and the `github` auth
//! config that probes it. Each suite pulls it in with `mod support;`, so
//! Cargo does not build it as a test target of its own.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::State;
use axum::http::{Request, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use git_cache_proxy::auth::AuthConfig;
use git_cache_proxy::config::AuthProvider;
use tokio::sync::{Mutex, Notify};

/// The signing key the test replicas share, long enough for HMAC-SHA256.
pub const SIGNING_KEY: &str = "0123456789abcdef0123456789abcdef";

/// What the mock answers, so each classification branch is testable.
// Each suite builds its own copy of this module and uses a different subset
// of variants; the sharing, not a real gap, is what dead-code analysis sees.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reply {
    /// 200 with a git advertisement: the credential is accepted.
    Ok,
    /// The advertisement only for a Basic credential whose password is the
    /// given one; 401 otherwise.
    Password(&'static str),
    /// 401: the credential is rejected.
    Reject,
    /// 500: unhealthy, so the proxy must fail closed.
    Unhealthy,
    /// 302: a redirect must not become an implicit allow.
    Redirect,
    /// 403 with `retry-after`: a secondary rate limit, not a verdict.
    Throttled,
    /// 200 with an HTML page, as an SSO front answers.
    LoginPage,
    /// Never answers a credential containing `slow`; `Ok` otherwise.
    HangOnSlowToken,
}

/// One observed validation request.
// Same as `Reply`: not every suite reads every field.
#[allow(dead_code)]
pub struct Hit {
    pub uri: String,
    pub authorization: Option<String>,
    pub git_protocol: Option<String>,
}

#[derive(Clone)]
struct Validator {
    reply: Reply,
    hits: Arc<Mutex<Vec<Hit>>>,
    /// Signalled once per recorded request.
    arrived: Arc<Notify>,
}

async fn validate_handler(State(v): State<Validator>, req: Request<Body>) -> Response {
    let (parts, _) = req.into_parts();
    let read = |name: &str| {
        parts
            .headers
            .get(name)
            .and_then(|h| h.to_str().ok())
            .map(str::to_string)
    };
    let hit = Hit {
        uri: parts.uri.to_string(),
        authorization: read("authorization"),
        git_protocol: read("git-protocol"),
    };
    let hang = v.reply == Reply::HangOnSlowToken
        && hit
            .authorization
            .as_deref()
            .is_some_and(|a| presented_token(a).contains("slow"));
    let advertisement = [(
        header::CONTENT_TYPE,
        "application/x-git-upload-pack-advertisement",
    )];
    // The answer is built before `hit` is recorded, since the `Password` arm
    // reads it.
    let answer = match v.reply {
        Reply::Ok | Reply::HangOnSlowToken => (advertisement, "0000").into_response(),
        Reply::Password(want) => {
            let advertised = hit
                .authorization
                .as_deref()
                .is_some_and(|a| presented_token(a).ends_with(&format!(":{want}")));
            if advertised {
                (advertisement, "0000").into_response()
            } else {
                StatusCode::UNAUTHORIZED.into_response()
            }
        }
        Reply::Reject => StatusCode::UNAUTHORIZED.into_response(),
        Reply::Unhealthy => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        Reply::Redirect => StatusCode::FOUND.into_response(),
        Reply::Throttled => (StatusCode::FORBIDDEN, [(header::RETRY_AFTER, "60")]).into_response(),
        Reply::LoginPage => ([(header::CONTENT_TYPE, "text/html")], "<html>").into_response(),
    };
    v.hits.lock().await.push(hit);
    v.arrived.notify_one();
    if hang {
        std::future::pending::<()>().await;
    }
    answer
}

/// The token of an `Authorization` value: a Bearer token as is, a Basic one
/// decoded.
fn presented_token(authorization: &str) -> String {
    match authorization.strip_prefix("Basic ") {
        Some(b64) => String::from_utf8(STANDARD.decode(b64).unwrap()).unwrap(),
        None => authorization.to_string(),
    }
}

/// A live mock on an ephemeral port, recording every request it answers.
pub async fn spawn_validator(reply: Reply) -> (String, Arc<Mutex<Vec<Hit>>>) {
    let (base, hits, _arrived) = spawn_validator_with(reply).await;
    (base, hits)
}

/// Like [`spawn_validator`], but also returns the `Notify` signalled once per
/// recorded request, for tests that must observe a request before it is
/// answered.
pub async fn spawn_validator_with(reply: Reply) -> (String, Arc<Mutex<Vec<Hit>>>, Arc<Notify>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let hits = Arc::new(Mutex::new(Vec::new()));
    let arrived = Arc::new(Notify::new());
    let state = Validator {
        reply,
        hits: hits.clone(),
        arrived: arrived.clone(),
    };
    let app = axum::Router::new()
        .fallback(any(validate_handler))
        .with_state(state);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), hits, arrived)
}

/// A `github` provider config probing the mock at `base` as its upstream, with
/// its verdict reused for `ttl_secs` (0: revalidated on every request).
pub fn github_auth_config(base: &str, ttl_secs: u64) -> AuthConfig {
    AuthConfig {
        provider: AuthProvider::Github,
        serve_token: None,
        validate_url: None,
        token_header: None,
        repo_form: None,
        signing_key: Some(SIGNING_KEY.into()),
        timeout: Duration::from_secs(5),
        ttl: Duration::from_secs(ttl_secs),
        max_concurrent: 32,
        // The mocks speak plain http on loopback.
        allow_http: true,
        upstream_base: base.to_string(),
    }
}
