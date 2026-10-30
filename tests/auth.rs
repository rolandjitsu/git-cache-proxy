// SPDX-License-Identifier: Apache-2.0
//! Per-request token validation tests: the provider modes run against a mock
//! upstream `info/refs` on a live local socket, with the router driven
//! in-process. A request that passes auth and then fails as an upstream clone
//! (502 against a bogus upstream) proves the credential was accepted - auth
//! passed, serving did not.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use axum::response::Response;
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use git_cache_proxy::auth::{Auth, AuthConfig};
use git_cache_proxy::config::{AuthProvider, RepoForm};
use git_cache_proxy::git::{GitCache, GitConfig};
use git_cache_proxy::lfs::{Lfs, LfsConfig};
use git_cache_proxy::metrics::Metrics;
use git_cache_proxy::server::{AppState, router};
use tower::ServiceExt; // for `oneshot`

mod support;

use support::{Reply, github_auth_config, spawn_validator, spawn_validator_with};

const REFS: &str = "/repo.git/info/refs?service=git-upload-pack";

#[tokio::test]
async fn missing_token_is_challenged_without_a_validation_call() {
    let (base, hits) = spawn_validator(Reply::Ok).await;
    let resp = get(state(build(&base, 0)), REFS, None).await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    // Without a Basic challenge git never sends URL or helper credentials.
    assert_eq!(
        resp.headers().get(header::WWW_AUTHENTICATE).unwrap(),
        r#"Basic realm="git-cache-proxy""#
    );
    assert!(
        hits.lock().await.is_empty(),
        "a missing token must not hit upstream"
    );
}

#[tokio::test]
async fn github_probes_upstream_info_refs_over_protocol_v2() {
    let (base, hits) = spawn_validator(Reply::Ok).await;
    let resp = get(
        state(build(&base, 0)),
        "/owner/repo.git/info/refs?service=git-upload-pack",
        Some("Bearer good-token"),
    )
    .await;
    assert_eq!(
        resp.status(),
        StatusCode::BAD_GATEWAY,
        "an accepted token must clear auth and reach the doomed upstream clone"
    );
    let hits = hits.lock().await;
    assert_eq!(hits.len(), 1);
    assert_eq!(
        hits[0].uri,
        "/owner/repo.git/info/refs?service=git-upload-pack"
    );
    assert_eq!(hits[0].git_protocol.as_deref(), Some("version=2"));
    // A bare token goes upstream the way a git host takes one: as a Basic password.
    let basic = STANDARD.encode(b"x-access-token:good-token");
    assert_eq!(
        hits[0].authorization.as_deref(),
        Some(format!("Basic {basic}").as_str())
    );
}

#[tokio::test]
async fn gitlab_forwards_basic_credentials_verbatim() {
    let (base, hits) = spawn_validator(Reply::Ok).await;
    let auth = Arc::new(
        Auth::build(
            AuthConfig {
                provider: AuthProvider::Gitlab,
                ..github_auth_config(&base, 0)
            },
            Arc::new(Metrics::new()),
        )
        .unwrap(),
    );
    // GitLab job tokens only authenticate with their `gitlab-ci-token` username.
    let basic = format!("Basic {}", STANDARD.encode(b"gitlab-ci-token:job-token"));
    let resp = get(
        state(auth),
        "/g/team/p.git/info/refs?service=git-upload-pack",
        Some(&basic),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_GATEWAY, "auth must clear");
    let hits = hits.lock().await;
    assert_eq!(
        hits[0].uri,
        "/g/team/p.git/info/refs?service=git-upload-pack"
    );
    assert_eq!(hits[0].authorization.as_deref(), Some(basic.as_str()));
}

#[tokio::test]
async fn rejected_token_yields_401() {
    let (base, hits) = spawn_validator(Reply::Reject).await;
    let resp = get(state(build(&base, 0)), REFS, Some("Bearer bad-token")).await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(hits.lock().await.len(), 1);
}

#[tokio::test]
async fn non_verdicts_fail_closed_with_503() {
    for reply in [
        Reply::Unhealthy,
        Reply::Redirect,
        Reply::Throttled,
        Reply::LoginPage,
    ] {
        let (base, hits) = spawn_validator(reply).await;
        let resp = get(state(build(&base, 0)), REFS, Some("Bearer good-token")).await;
        assert_eq!(
            resp.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "{reply:?} must neither allow nor deny"
        );
        assert_eq!(hits.lock().await.len(), 1);
    }
}

#[tokio::test]
async fn unreachable_endpoint_fails_closed_with_503() {
    // Bind, then drop, so the address refuses connections.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    let resp = get(state(build(&base, 0)), REFS, Some("Bearer good-token")).await;
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn ttl_reuses_a_verdict_within_its_window() {
    let (base, hits) = spawn_validator(Reply::Ok).await;
    let st = state(build(&base, 3600));
    for _ in 0..2 {
        let resp = get(st.clone(), REFS, Some("Bearer good-token")).await;
        assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
    }
    assert_eq!(
        hits.lock().await.len(),
        1,
        "the second request must reuse the first verdict"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hung_validation_does_not_hold_a_serving_permit() {
    let (base, _hits, arrived) = spawn_validator_with(Reply::HangOnSlowToken).await;
    let mut st = state(build(&base, 0));
    st.max_concurrent = 1;
    let slow = tokio::spawn(get(st.clone(), REFS, Some("Bearer slow-token")));
    arrived.notified().await;
    // Another credential clears auth, then takes the only permit and fails as
    // an upstream clone: it must not queue behind the hung validation.
    let resp = get(st, REFS, Some("Bearer good-token")).await;
    assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
    assert!(
        !slow.is_finished(),
        "the slow validation is still in flight"
    );
    slow.abort();
}

#[tokio::test]
async fn pushes_are_refused_without_a_validation_call() {
    let (base, hits) = spawn_validator(Reply::Ok).await;
    let resp = get(
        state(build(&base, 0)),
        "/repo.git/info/refs?service=git-receive-pack",
        Some("Bearer good-token"),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    assert!(hits.lock().await.is_empty());
}

#[tokio::test]
async fn dumb_http_is_refused_without_a_validation_call() {
    let (base, hits) = spawn_validator(Reply::Ok).await;
    let resp = get(
        state(build(&base, 0)),
        "/repo.git/info/refs",
        Some("Bearer good-token"),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    assert!(hits.lock().await.is_empty());
}

#[tokio::test]
async fn non_git_paths_are_denied() {
    let (base, hits) = spawn_validator(Reply::Ok).await;
    let resp = get(
        state(build(&base, 0)),
        "/not/a/git/endpoint",
        Some("Bearer good-token"),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    assert!(hits.lock().await.is_empty());
}

#[tokio::test]
async fn invalid_repo_names_are_denied_without_validation() {
    let (base, hits) = spawn_validator(Reply::Ok).await;
    for uri in [
        "/../evil.git/info/refs?service=git-upload-pack",
        "/a/../../b.git/info/refs?service=git-upload-pack",
        "/repo.__incoming__.git/info/refs?service=git-upload-pack",
        // Escapes could decode to a different repo at upstream than was checked.
        "/a/%2e%2e/b.git/info/refs?service=git-upload-pack",
        "/a%2Fb.git/info/refs?service=git-upload-pack",
    ] {
        let resp = get(state(build(&base, 0)), uri, Some("Bearer good-token")).await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED, "uri: {uri}");
    }
    assert!(
        hits.lock().await.is_empty(),
        "names serving would reject must not reach the validation endpoint"
    );
}

#[tokio::test]
async fn validation_calls_and_cache_hits_are_counted() {
    let (base, _hits) = spawn_validator(Reply::Ok).await;
    let metrics = Arc::new(Metrics::new());
    let auth = Auth::build(github_auth_config(&base, 3600), metrics.clone()).unwrap();
    let st = state(Arc::new(auth));
    for token in [
        "Bearer good-token",
        "Bearer good-token",
        "Bearer other-token",
    ] {
        let resp = get(st.clone(), REFS, Some(token)).await;
        assert_eq!(resp.status(), StatusCode::BAD_GATEWAY, "auth must clear");
    }
    let out = metrics.gather();
    assert!(out.contains(r#"gitcacheproxy_auth_validations_total{result="allow"} 2"#));
    assert!(out.contains("gitcacheproxy_auth_cache_hits_total 1"));
}

#[tokio::test]
async fn custom_validates_against_its_endpoint() {
    let (base, hits) = spawn_validator(Reply::Ok).await;
    let resp = get(state(custom(&base, None)), REFS, Some("Bearer good-token")).await;
    assert_eq!(resp.status(), StatusCode::BAD_GATEWAY, "auth must clear");
    let hits = hits.lock().await;
    assert_eq!(hits.len(), 1);
    // The repo from the request path reached the endpoint, `.git` stripped, and
    // the client token rode along as a bearer.
    assert_eq!(hits[0].uri, "/validate/repo");
    assert_eq!(hits[0].authorization.as_deref(), Some("Bearer good-token"));
}

#[tokio::test]
async fn custom_presents_a_basic_password_as_the_token() {
    let (base, hits) = spawn_validator(Reply::Ok).await;
    // What git's credential helpers produce: any username, the token as the
    // password (the actions/checkout shape is x-access-token).
    let basic = STANDARD.encode(b"x-access-token:good-token");
    let resp = get(
        state(custom(&base, None)),
        REFS,
        Some(&format!("Basic {basic}")),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_GATEWAY, "auth must clear");
    let hits = hits.lock().await;
    assert_eq!(hits.len(), 1);
    assert_eq!(
        hits[0].authorization.as_deref(),
        Some("Bearer good-token"),
        "the password reached the endpoint"
    );
}

#[tokio::test]
async fn unrepresentable_tokens_are_denied_without_a_call() {
    let (base, hits) = spawn_validator(Reply::Ok).await;
    let basic = STANDARD.encode(b"user:bad\r\nInjected: 1");
    let resp = get(
        state(custom(&base, None)),
        REFS,
        Some(&format!("Basic {basic}")),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    assert!(hits.lock().await.is_empty());
}

#[tokio::test]
async fn custom_repo_forms_reach_the_endpoint() {
    let (base, hits) = spawn_validator(Reply::Ok).await;
    let resp = get(
        state(custom(&base, Some(RepoForm::Percent))),
        "/g/p.git/info/refs?service=git-upload-pack",
        Some("Bearer good-token"),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_GATEWAY, "auth must clear");
    assert_eq!(hits.lock().await[0].uri, "/validate/g%2Fp");
}

// --- test helpers --------------------------------------------------------

fn build(base: &str, ttl: u64) -> Arc<Auth> {
    Arc::new(Auth::build(github_auth_config(base, ttl), Arc::new(Metrics::new())).unwrap())
}

/// `custom` validating against the mock's `/validate/{repo}`.
fn custom(base: &str, repo_form: Option<RepoForm>) -> Arc<Auth> {
    Arc::new(
        Auth::build(
            AuthConfig {
                provider: AuthProvider::Custom,
                validate_url: Some(format!("{base}/validate/{{repo}}")),
                repo_form,
                upstream_base: "https://upstream.invalid".into(),
                ..github_auth_config(base, 0)
            },
            Arc::new(Metrics::new()),
        )
        .unwrap(),
    )
}

fn state(auth: Arc<Auth>) -> AppState {
    // The upstream does not exist: a request that clears auth then fails as an
    // upstream clone (502), which is what proves the credential was accepted.
    let cache_root = std::env::temp_dir().join("git-cache-proxy-auth-tests-unused");
    let metrics = Arc::new(Metrics::new());
    let cfg = GitConfig {
        git_binary: "git".into(),
        upstream_auth_header: None,
        big_file_threshold: "8m".into(),
        fetch_ttl: Duration::from_secs(10),
        max_wants: 100,
        max_upload_packs: 0,
        pack_threads: None,
        pack_window_memory: None,
    };
    let lfs = Arc::new(Lfs::new(
        LfsConfig {
            upstream_base: "file:///nonexistent/git-cache-proxy-upstream".into(),
            cache_root: cache_root.clone(),
            upstream_auth_header: None,
        },
        None,
    ));
    AppState {
        cache: Arc::new(GitCache::new(cfg, metrics.clone(), None)),
        lfs,
        upstream_base: "file:///nonexistent/git-cache-proxy-upstream".into(),
        cache_root,
        auth,
        max_decoded_body: 512 * 1024 * 1024,
        max_concurrent: 0,
        metrics,
    }
}

async fn get(st: AppState, uri: &str, authorization: Option<&str>) -> Response {
    let mut req = Request::get(uri);
    if let Some(value) = authorization {
        req = req.header(header::AUTHORIZATION, value);
    }
    router(st)
        .oneshot(req.body(Body::empty()).unwrap())
        .await
        .unwrap()
}
