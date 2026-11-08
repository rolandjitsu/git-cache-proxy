// SPDX-License-Identifier: Apache-2.0
//! End-to-end tests for `--upstream-credential client`: the proxy fetches
//! upstream with the requesting client's own credential, reshaped as a git host
//! takes it. The upstream here is a real `git http-backend` behind an auth gate
//! that requires and records the `Authorization` header, so these prove a clone
//! actually succeeds with the forwarded credential (not merely that a header was
//! sent), and that an upstream rejection on the fetch becomes a client 401.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use axum::body::Body;
use axum::extract::State;
use axum::http::{Request, Response, StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::any;
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use git_cache_proxy::auth::{Auth, AuthConfig};
use git_cache_proxy::config::{AuthProvider, UpstreamCredential};
use git_cache_proxy::git::{GitCache, GitConfig};
use git_cache_proxy::lfs::{Lfs, LfsConfig};
use git_cache_proxy::metrics::Metrics;
use git_cache_proxy::server::{AppState, router};
use tokio::io::AsyncWriteExt;
use tower::ServiceExt; // for `oneshot`

mod helpers;

const REFS: &str = "/repo.git/info/refs?service=git-upload-pack";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn github_fetch_forwards_a_bearer_token_as_basic_and_clones() {
    let up = make_upstream();
    let backend = Backend::spawn(up.path().to_path_buf()).await;
    let st = client_state(&backend.base).await;

    let resp = get(st, REFS, "Bearer s3cret-token").await;

    // A real clone succeeded through the auth-gated upstream, so the proxy serves
    // the advertisement - not a 502 (clone failed) or 401 (auth denied).
    assert_eq!(resp.status, StatusCode::OK, "body: {}", resp.body);
    assert!(
        resp.body.contains("refs/heads/main"),
        "advertisement should list the upstream ref: {}",
        resp.body
    );
    // GitHub takes a bare token as the Basic password under `x-access-token`, and
    // the upstream saw exactly that - both on the validation probe and the fetch.
    let want = format!("Basic {}", STANDARD.encode(b"x-access-token:s3cret-token"));
    let seen = backend.authorizations();
    assert!(
        !seen.is_empty() && seen.iter().all(|a| a == &want),
        "upstream must see the reshaped credential on every request: {seen:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gitlab_fetch_forwards_a_bearer_token_as_oauth2() {
    let up = make_upstream();
    let backend = Backend::spawn(up.path().to_path_buf()).await;
    let mut st = client_state(&backend.base).await;
    st.auth = Arc::new(
        Auth::build(
            AuthConfig {
                provider: AuthProvider::Gitlab,
                ..helpers::github_auth_config(&backend.base, 0)
            },
            Arc::new(Metrics::new()),
        )
        .unwrap(),
    );

    let resp = get(st, REFS, "Bearer s3cret-token").await;

    assert_eq!(resp.status, StatusCode::OK, "body: {}", resp.body);
    let want = format!("Basic {}", STANDARD.encode(b"oauth2:s3cret-token"));
    assert!(
        backend.authorizations().iter().all(|a| a == &want),
        "gitlab forwards a bearer token as Basic oauth2:<token>"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn basic_credentials_are_forwarded_verbatim() {
    let up = make_upstream();
    let backend = Backend::spawn(up.path().to_path_buf()).await;
    let st = client_state(&backend.base).await;

    // A GitLab CI job token only authenticates with its `gitlab-ci-token`
    // username, so the proxy must forward Basic exactly as the client sent it.
    let basic = format!("Basic {}", STANDARD.encode(b"gitlab-ci-token:job-token"));
    let resp = get(st, REFS, &basic).await;

    assert_eq!(resp.status, StatusCode::OK, "body: {}", resp.body);
    assert!(backend.authorizations().iter().all(|a| a == &basic));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_missing_credential_is_denied_before_upstream() {
    let up = make_upstream();
    let backend = Backend::spawn(up.path().to_path_buf()).await;
    let st = client_state(&backend.base).await;

    let resp = router(st)
        .oneshot(Request::get(REFS).body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    assert!(
        backend.authorizations().is_empty(),
        "a request with no credential must not reach upstream"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_upstream_auth_failure_on_the_fetch_becomes_a_client_401() {
    let up = make_upstream();
    let backend = Backend::spawn(up.path().to_path_buf()).await;
    // Reuse a verdict so the second request's check is a cache hit (Allow) while
    // the fetch itself still runs and fails - the token-revoked-mid-window case.
    let mut st = client_state(&backend.base).await;
    st.auth = Arc::new(
        Auth::build(
            AuthConfig {
                ttl: Duration::from_secs(3600),
                ..helpers::github_auth_config(&backend.base, 3600)
            },
            Arc::new(Metrics::new()),
        )
        .unwrap(),
    );

    // First request clones successfully and caches the Allow.
    let resp = get(st.clone(), REFS, "Bearer s3cret-token").await;
    assert_eq!(resp.status, StatusCode::OK, "body: {}", resp.body);

    // Upstream now rejects every credential; the cached Allow still stands, but
    // the fetch fails with an auth diagnostic.
    backend.reject(true);
    let resp = get(st, REFS, "Bearer s3cret-token").await;
    assert_eq!(
        resp.status,
        StatusCode::UNAUTHORIZED,
        "an upstream auth failure on the fetch is the client's 401, not a 502: {}",
        resp.body
    );
    assert_eq!(
        resp.www_authenticate.as_deref(),
        Some(r#"Basic realm="git-cache-proxy""#),
        "the 401 carries a Basic challenge so git re-sends credentials"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_upload_pack_clone_that_fails_auth_becomes_a_client_401() {
    let up = make_upstream();
    let backend = Backend::spawn(up.path().to_path_buf()).await;
    let mut st = client_state(&backend.base).await;
    st.auth = Arc::new(
        Auth::build(
            helpers::github_auth_config(&backend.base, 3600),
            Arc::new(Metrics::new()),
        )
        .unwrap(),
    );

    // A successful info/refs clones the mirror and caches the Allow (no eviction
    // of the verdict, since it did not fail).
    assert_eq!(
        get(st.clone(), REFS, "Bearer s3cret-token").await.status,
        StatusCode::OK
    );

    // Evict the mirror and revoke upstream: the next request must re-clone, but
    // the cached Allow still lets it past the gate.
    std::fs::remove_dir_all(&st.cache_root).unwrap();
    backend.reject(true);

    // The upload-pack handler's clone now fails with an auth diagnostic, which in
    // client mode is the client's 401 rather than a 502.
    let resp = post_upload_pack(st, "Bearer s3cret-token").await;
    assert_eq!(resp.status, StatusCode::UNAUTHORIZED, "{}", resp.body);
    assert_eq!(
        resp.www_authenticate.as_deref(),
        Some(r#"Basic realm="git-cache-proxy""#)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_lfs_object_get_re_batches_with_the_sealed_credential() {
    let up = make_upstream();
    let backend = Backend::spawn(up.path().to_path_buf()).await;
    let st = client_state(&backend.base).await;
    let oid = "a".repeat(64);
    let sealed = "Basic c2VhbGVk";
    // The batch response would seal the client credential into each object's
    // grant; mint one directly and present it on the object GET.
    let grant = st
        .auth
        .lfs_grant(
            "repo.git",
            &oid,
            7,
            Some(&format!("Authorization: {sealed}")),
        )
        .unwrap();
    let uri = format!("/repo.git/info/lfs/objects/{oid}?size=7");

    let resp = get(st, &uri, &grant.authorization).await;

    // gate accepted the grant (not a 401) and unsealed the credential; the cache
    // miss then re-batches upstream carrying it (the git-only upstream has no LFS
    // endpoint, so the batch itself fails afterward).
    assert_ne!(resp.status, StatusCode::UNAUTHORIZED, "{}", resp.body);
    assert!(
        backend.authorizations().iter().any(|a| a == sealed),
        "the LFS re-batch must carry the unsealed credential: {:?}",
        backend.authorizations()
    );
}

// ---- helpers (below the tests that use them) ----

/// A bare upstream repo with one commit on `main`.
fn make_upstream() -> tempfile::TempDir {
    let work = tempfile::tempdir().unwrap();
    git(work.path(), &["init", "-q", "-b", "main", "."]);
    std::fs::write(work.path().join("README.md"), "hello\n").unwrap();
    git(work.path(), &["add", "."]);
    git(
        work.path(),
        &[
            "-c",
            "user.email=t@t",
            "-c",
            "user.name=t",
            "commit",
            "-q",
            "-m",
            "init",
        ],
    );
    let up = tempfile::tempdir().unwrap();
    let mirror = up.path().join("repo.git");
    let mirror = mirror.to_string_lossy();
    git(
        work.path(),
        &["clone", "-q", "--mirror", ".", mirror.as_ref()],
    );
    up
}

/// Proxy `AppState` in `client` upstream-credential mode, github provider,
/// probing and fetching the auth-gated upstream at `base` over plain HTTP.
async fn client_state(base: &str) -> AppState {
    let cache = tempfile::tempdir().unwrap();
    // Leak the cache dir: the state outlives this call and the test only needs it
    // to exist for the process.
    let cache_root = cache.keep();
    let metrics = Arc::new(Metrics::new());
    let cfg = GitConfig {
        git_binary: "git".into(),
        upstream_auth_header: None,
        big_file_threshold: "8m".into(),
        fetch_ttl: Duration::from_secs(0), // always fetch, so a revoked token is seen
        max_wants: 100,
        max_upload_packs: 0,
        pack_threads: None,
        pack_window_memory: None,
    };
    let auth =
        Arc::new(Auth::build(helpers::github_auth_config(base, 0), metrics.clone()).unwrap());
    AppState {
        cache: Arc::new(GitCache::new(cfg, metrics.clone(), None)),
        lfs: Arc::new(Lfs::new(
            LfsConfig {
                upstream_base: base.to_string(),
                cache_root: cache_root.clone(),
                upstream_auth_header: None,
            },
            None,
        )),
        upstream_base: base.to_string(),
        cache_root,
        auth,
        upstream_credential: UpstreamCredential::Client,
        max_decoded_body: 512 * 1024 * 1024,
        max_concurrent: 8,
        metrics,
    }
}

struct Resp {
    status: StatusCode,
    body: String,
    www_authenticate: Option<String>,
}

async fn get(st: AppState, uri: &str, authorization: &str) -> Resp {
    send(
        st,
        Request::get(uri)
            .header(header::AUTHORIZATION, authorization)
            .body(Body::empty())
            .unwrap(),
    )
    .await
}

async fn post_upload_pack(st: AppState, authorization: &str) -> Resp {
    send(
        st,
        Request::post("/repo.git/git-upload-pack")
            .header(header::AUTHORIZATION, authorization)
            .header(
                header::CONTENT_TYPE,
                "application/x-git-upload-pack-request",
            )
            .body(Body::empty())
            .unwrap(),
    )
    .await
}

async fn send(st: AppState, req: Request<Body>) -> Resp {
    let resp = router(st).oneshot(req).await.unwrap();
    let status = resp.status();
    let www_authenticate = resp
        .headers()
        .get(header::WWW_AUTHENTICATE)
        .and_then(|v| v.to_str().ok())
        .map(String::from);
    let body = axum::body::to_bytes(resp.into_body(), 64 * 1024 * 1024)
        .await
        .map(|b| String::from_utf8_lossy(&b).into_owned())
        .unwrap_or_default();
    Resp {
        status,
        body,
        www_authenticate,
    }
}

fn git(dir: &Path, args: &[&str]) {
    let status = std::process::Command::new("git")
        .args(args)
        .current_dir(dir)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("spawn git");
    assert!(status.success(), "git {args:?} failed");
}

/// A live upstream: `git http-backend` behind an auth gate that records every
/// `Authorization` and can be flipped to reject all requests.
#[derive(Clone)]
struct Backend {
    base: String,
    seen: Arc<Mutex<Vec<String>>>,
    reject: Arc<AtomicBool>,
}

impl Backend {
    async fn spawn(project_root: PathBuf) -> Self {
        let state = Backend {
            base: String::new(),
            seen: Arc::new(Mutex::new(Vec::new())),
            reject: Arc::new(AtomicBool::new(false)),
        };
        let app = axum::Router::new().fallback(any(serve)).with_state((
            project_root,
            state.seen.clone(),
            state.reject.clone(),
        ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Backend {
            base: format!("http://{addr}"),
            ..state
        }
    }

    fn authorizations(&self) -> Vec<String> {
        self.seen.lock().unwrap().clone()
    }

    fn reject(&self, yes: bool) {
        self.reject.store(yes, Ordering::SeqCst);
    }
}

type BackendState = (PathBuf, Arc<Mutex<Vec<String>>>, Arc<AtomicBool>);

async fn serve(
    State((root, seen, reject)): State<BackendState>,
    req: Request<Body>,
) -> Response<Body> {
    let (parts, body) = req.into_parts();
    let auth = parts
        .headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .map(String::from);
    if let Some(a) = &auth {
        seen.lock().unwrap().push(a.clone());
    }
    if reject.load(Ordering::SeqCst) || auth.is_none() {
        return (
            StatusCode::UNAUTHORIZED,
            [(header::WWW_AUTHENTICATE, r#"Basic realm="upstream""#)],
            "denied",
        )
            .into_response();
    }
    let body = axum::body::to_bytes(body, 64 * 1024 * 1024)
        .await
        .unwrap_or_default();
    run_http_backend(&root, &parts, &body).await
}

/// Run `git http-backend` as CGI for one request and translate its output.
async fn run_http_backend(
    root: &Path,
    parts: &axum::http::request::Parts,
    body: &[u8],
) -> Response<Body> {
    let header = |name: &str| {
        parts
            .headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(String::from)
    };
    let mut cmd = tokio::process::Command::new("git");
    cmd.arg("http-backend")
        .env("GIT_PROJECT_ROOT", root)
        .env("GIT_HTTP_EXPORT_ALL", "1")
        .env("REQUEST_METHOD", parts.method.as_str())
        .env("PATH_INFO", parts.uri.path())
        .env("QUERY_STRING", parts.uri.query().unwrap_or(""))
        .env("CONTENT_LENGTH", body.len().to_string())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    if let Some(ct) = header("content-type") {
        cmd.env("CONTENT_TYPE", ct);
    }
    if let Some(gp) = header("git-protocol") {
        cmd.env("GIT_PROTOCOL", gp);
    }
    let mut child = cmd.spawn().expect("spawn git http-backend");
    child
        .stdin
        .take()
        .unwrap()
        .write_all(body)
        .await
        .expect("write http-backend stdin");
    let out = child
        .wait_with_output()
        .await
        .expect("git http-backend output");
    parse_cgi(&out.stdout)
}

/// Split CGI headers from the body and build the HTTP response. `git
/// http-backend` emits `Status`/`Content-Type` headers, then a blank line.
fn parse_cgi(stdout: &[u8]) -> Response<Body> {
    let boundary = stdout
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|i| (i, i + 4))
        .or_else(|| {
            stdout
                .windows(2)
                .position(|w| w == b"\n\n")
                .map(|i| (i, i + 2))
        });
    let (head, body) = match boundary {
        Some((end, start)) => (&stdout[..end], &stdout[start..]),
        None => (&b""[..], stdout),
    };
    let mut status = StatusCode::OK;
    let mut content_type: Option<String> = None;
    for line in String::from_utf8_lossy(head).lines() {
        if let Some(v) = line.strip_prefix("Status:") {
            if let Some(code) = v
                .split_whitespace()
                .next()
                .and_then(|c| c.parse::<u16>().ok())
                .and_then(|c| StatusCode::from_u16(c).ok())
            {
                status = code;
            }
        } else if let Some(v) = line
            .strip_prefix("Content-Type:")
            .or_else(|| line.strip_prefix("Content-type:"))
        {
            content_type = Some(v.trim().to_string());
        }
    }
    let mut resp = Response::builder().status(status);
    if let Some(ct) = content_type {
        resp = resp.header(header::CONTENT_TYPE, ct);
    }
    resp.body(Body::from(body.to_vec())).unwrap()
}
