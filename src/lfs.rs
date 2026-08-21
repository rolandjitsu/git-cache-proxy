// SPDX-License-Identifier: Apache-2.0
//! git-LFS caching: proxy the batch API to upstream and cache objects on disk.
//!
//! git-cache-proxy serves the git wire protocol (see `git`), but LFS objects use a
//! different HTTP API: a JSON "batch" negotiation that hands back a per-object
//! download URL, then a content-addressed GET of the object itself. Because clones
//! are routed through the proxy, a client's git-lfs derives its LFS endpoint from the
//! proxy URL and talks LFS to us - so we must answer it, or every LFS-tracked file
//! fails to check out.
//!
//! Flow:
//!   POST <repo>/info/lfs/objects/batch
//!     -> forward to upstream, then rewrite every download href to point back here
//!        (`<repo>/info/lfs/objects/<oid>`) so the object fetch is cached.
//!   GET <repo>/info/lfs/objects/<oid>
//!     -> serve from the on-disk cache (content-addressed by oid), or on a miss fetch
//!        it once (re-batch for a fresh authorized URL, download, verify sha256 ==
//!        oid, store) then serve. Objects are immutable, so a cached object is never
//!        stale and is shared across every repo that references the same oid.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use reqwest::header::{ACCEPT, CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};
use serde_json::Value;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

use crate::evict::CacheIndex;
use crate::repo;

/// The git-LFS batch API content type, sent and expected on the batch endpoint.
const LFS_CONTENT_TYPE: &str = "application/vnd.git-lfs+json";

/// Reserved subdir of the LFS store holding in-flight downloads before their atomic
/// rename into place. On the same filesystem as the final object so the rename is
/// atomic; skipped by the eviction scan.
pub const INCOMING_DIR: &str = ".incoming";

/// Whether an object request was served from cache or fetched from upstream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Hit,
    Miss,
}

#[derive(Clone)]
pub struct LfsConfig {
    /// Upstream base URL, trailing slash trimmed (same value the git side uses).
    pub upstream_base: String,
    pub cache_root: PathBuf,
    /// Full upstream auth header line (e.g. `Authorization: Basic <b64>`), or `None`
    /// for anonymous.
    pub upstream_auth_header: Option<String>,
    /// The proxy's own serve token, if configured; embedded in each rewritten download
    /// action so git-lfs re-presents it on the (auth-checked) object GET.
    pub serve_token: Option<String>,
}

pub struct Lfs {
    cfg: LfsConfig,
    /// One shared client (connection pooling) using rustls+ring - no native-tls, so
    /// the binary stays static and free of an OpenSSL dependency.
    client: reqwest::Client,
    /// Present only when a cache cap is configured; cached objects are recorded here
    /// so they share the mirrors' byte budget and LRU eviction.
    index: Option<Arc<CacheIndex>>,
    /// Per-oid single-flight: collapses a burst of concurrent misses for one object
    /// into a single upstream download. Only populated on a miss (a hit returns
    /// before locking), so it holds at most one entry per distinct object fetched
    /// since startup - each tiny, and never on the hot cached path.
    slots: Mutex<HashMap<String, Arc<Mutex<()>>>>,
    /// Names the per-download temp file the object is streamed to before its atomic
    /// rename into the content-addressed cache path.
    tmp_counter: AtomicU64,
}

impl Lfs {
    pub fn new(cfg: LfsConfig, index: Option<Arc<CacheIndex>>) -> Self {
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(30))
            .build()
            .expect("build reqwest client");
        Self {
            cfg,
            client,
            index,
            slots: Mutex::new(HashMap::new()),
            tmp_counter: AtomicU64::new(0),
        }
    }

    /// Proxy a client batch request to upstream and rewrite the download hrefs so the
    /// objects are fetched back through this proxy (and thus cached). `advertise_base`
    /// is the proxy's own `scheme://host` as the client reached it. Returns the
    /// rewritten batch JSON.
    pub async fn batch(&self, repo: &str, body: &[u8], advertise_base: &str) -> Result<Vec<u8>> {
        let url = format!("{}/{repo}/info/lfs/objects/batch", self.cfg.upstream_base);
        let resp = self
            .post_batch(&url, body)
            .await
            .context("upstream lfs batch")?;
        let mut json: Value = serde_json::from_slice(&resp).context("parse lfs batch response")?;
        rewrite_download_hrefs(
            &mut json,
            advertise_base,
            repo,
            self.cfg.serve_token.as_deref(),
        );
        serde_json::to_vec(&json).context("serialize lfs batch response")
    }

    /// Ensure the object identified by `oid` is on disk and return its path plus
    /// whether it was a cache hit. On a miss `size` is required (the upstream batch
    /// API needs it); it is not needed for a hit.
    pub async fn ensure_object(
        &self,
        repo: &str,
        oid: &str,
        size: Option<u64>,
    ) -> Result<(PathBuf, Outcome)> {
        let path = repo::lfs_object_path(&self.cfg.cache_root, oid);
        if self.cached(&path).await {
            self.touch(oid);
            return Ok((path, Outcome::Hit));
        }
        // Serialize concurrent misses for the same oid onto one download.
        let slot = self.slot(oid).await;
        let _guard = slot.lock().await;
        if self.cached(&path).await {
            self.touch(oid); // another task fetched it while we waited on the lock
            return Ok((path, Outcome::Hit));
        }
        let size = size.context("cache miss without an object size")?;
        self.fetch_object(repo, oid, size, &path).await?;
        Ok((path, Outcome::Miss))
    }

    async fn cached(&self, path: &Path) -> bool {
        tokio::fs::try_exists(path).await.unwrap_or(false)
    }

    fn touch(&self, oid: &str) {
        if let Some(idx) = &self.index {
            idx.touch(&repo::lfs_object_key(oid));
        }
    }

    /// Download one object from upstream into the cache: re-batch for a fresh,
    /// authorized download URL (the batch JWT is short-lived, so it is fetched per
    /// download, never cached), stream it to a temp file, verify the content hashes
    /// to `oid`, then atomically move it into place.
    async fn fetch_object(
        &self,
        repo: &str,
        oid: &str,
        size: u64,
        final_path: &Path,
    ) -> Result<()> {
        let (href, headers) = self.download_action(repo, oid, size).await?;
        let tmp = self.tmp_path().await?;
        if let Err(e) = self.download_to_file(&href, headers, &tmp).await {
            let _ = tokio::fs::remove_file(&tmp).await;
            return Err(e);
        }
        let got = sha256_file(tmp.clone()).await?;
        if got != oid {
            let _ = tokio::fs::remove_file(&tmp).await;
            bail!("lfs object {oid} failed integrity check (upstream returned {got})");
        }
        if let Some(parent) = final_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .context("create lfs shard dir")?;
        }
        let bytes = tokio::fs::metadata(&tmp)
            .await
            .map(|m| m.len())
            .unwrap_or(0);
        tokio::fs::rename(&tmp, final_path)
            .await
            .context("store lfs object")?;
        if let Some(idx) = &self.index {
            idx.record_blob(&repo::lfs_object_key(oid), bytes);
        }
        Ok(())
    }

    /// Re-batch upstream for a single object and return its download href plus the
    /// headers to send when fetching it (the batch response embeds a short-lived
    /// authorization for the object URL).
    async fn download_action(
        &self,
        repo: &str,
        oid: &str,
        size: u64,
    ) -> Result<(String, HeaderMap)> {
        let url = format!("{}/{repo}/info/lfs/objects/batch", self.cfg.upstream_base);
        let req = serde_json::json!({
            "operation": "download",
            "transfers": ["basic"],
            "objects": [{ "oid": oid, "size": size }],
        });
        let body = serde_json::to_vec(&req).context("build lfs re-batch request")?;
        let resp = self
            .post_batch(&url, &body)
            .await
            .context("upstream lfs re-batch")?;
        let json: Value = serde_json::from_slice(&resp).context("parse lfs re-batch response")?;
        parse_download_action(&json)
    }

    /// POST `body` to an upstream LFS batch URL with the LFS content type and (if set)
    /// the upstream credential, and return the response bytes.
    async fn post_batch(&self, url: &str, body: &[u8]) -> Result<Vec<u8>> {
        let mut req = self
            .client
            .post(url)
            .header(CONTENT_TYPE, LFS_CONTENT_TYPE)
            .header(ACCEPT, LFS_CONTENT_TYPE)
            .body(body.to_vec());
        if let Some(line) = &self.cfg.upstream_auth_header
            && let Some((name, value)) = parse_header_line(line)
        {
            req = req.header(name, value);
        }
        let resp = req
            .send()
            .await
            .context("send lfs batch")?
            .error_for_status()
            .context("lfs batch http status")?;
        Ok(resp.bytes().await.context("read lfs batch body")?.to_vec())
    }

    /// Stream an object href to `out`, sending the batch-supplied `headers`. reqwest
    /// follows redirects (LFS hrefs commonly redirect to object storage) and drops the
    /// auth header on a cross-host hop.
    async fn download_to_file(&self, url: &str, headers: HeaderMap, out: &Path) -> Result<()> {
        let mut resp = self
            .client
            .get(url)
            .headers(headers)
            .send()
            .await
            .context("send lfs download")?
            .error_for_status()
            .context("lfs download http status")?;
        let mut file = tokio::fs::File::create(out)
            .await
            .context("create lfs temp file")?;
        while let Some(chunk) = resp.chunk().await.context("read lfs object chunk")? {
            file.write_all(&chunk)
                .await
                .context("write lfs object chunk")?;
        }
        file.flush().await.context("flush lfs object")?;
        Ok(())
    }

    async fn tmp_path(&self) -> Result<PathBuf> {
        let dir = self
            .cfg
            .cache_root
            .join(repo::LFS_OBJECTS_DIR)
            .join(INCOMING_DIR);
        tokio::fs::create_dir_all(&dir)
            .await
            .context("create lfs incoming dir")?;
        let n = self.tmp_counter.fetch_add(1, Ordering::Relaxed);
        Ok(dir.join(format!("{}-{n}", std::process::id())))
    }

    async fn slot(&self, oid: &str) -> Arc<Mutex<()>> {
        self.slots
            .lock()
            .await
            .entry(oid.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }
}

/// Rewrite each object's `download` href to point back at this proxy so the fetch is
/// cached, and replace the upstream authorization header with the proxy's serve token
/// (or drop it when the proxy serves anonymously) - the client authenticates to the
/// proxy, not upstream. Objects carrying an `error`, or an `upload` action, are left
/// untouched. Malformed entries are skipped rather than failing the batch.
fn rewrite_download_hrefs(
    json: &mut Value,
    advertise_base: &str,
    repo: &str,
    serve_token: Option<&str>,
) {
    let Some(objects) = json.get_mut("objects").and_then(Value::as_array_mut) else {
        return;
    };
    for obj in objects {
        let Some(oid) = obj.get("oid").and_then(Value::as_str).map(str::to_string) else {
            continue;
        };
        let size = obj.get("size").and_then(Value::as_u64).unwrap_or(0);
        let Some(download) = obj
            .get_mut("actions")
            .and_then(|a| a.get_mut("download"))
            .and_then(Value::as_object_mut)
        else {
            continue;
        };
        download.insert(
            "href".to_string(),
            Value::String(format!(
                "{advertise_base}/{repo}/info/lfs/objects/{oid}?size={size}"
            )),
        );
        match serve_token {
            Some(token) => {
                download.insert(
                    "header".to_string(),
                    serde_json::json!({ "Authorization": format!("Bearer {token}") }),
                );
            }
            None => {
                download.remove("header");
            }
        }
    }
}

/// Extract the download href and object-transfer headers from a batch response for a
/// single requested object. Errors if upstream reported the object missing or omitted
/// a usable download action.
fn parse_download_action(json: &Value) -> Result<(String, HeaderMap)> {
    let obj = json
        .get("objects")
        .and_then(Value::as_array)
        .and_then(|a| a.first())
        .context("lfs batch: no objects in response")?;
    if let Some(err) = obj.get("error") {
        bail!("lfs batch: upstream object error {err}");
    }
    let download = obj
        .get("actions")
        .and_then(|a| a.get("download"))
        .context("lfs batch: no download action")?;
    let href = download
        .get("href")
        .and_then(Value::as_str)
        .context("lfs batch: download action has no href")?
        .to_string();
    let mut headers = HeaderMap::new();
    if let Some(map) = download.get("header").and_then(Value::as_object) {
        for (k, v) in map {
            if let (Ok(name), Some(val)) = (HeaderName::from_bytes(k.as_bytes()), v.as_str())
                && let Ok(value) = HeaderValue::from_str(val)
            {
                headers.insert(name, value);
            }
        }
    }
    Ok((href, headers))
}

/// Parse a full header line (`Name: value`) into a typed name/value pair, marking the
/// value sensitive so it is redacted from any debug output. `None` if either half is
/// not a valid header token.
fn parse_header_line(line: &str) -> Option<(HeaderName, HeaderValue)> {
    let (name, value) = line.split_once(':')?;
    let name = HeaderName::from_bytes(name.trim().as_bytes()).ok()?;
    let mut value = HeaderValue::from_str(value.trim()).ok()?;
    value.set_sensitive(true);
    Some((name, value))
}

/// Hex-encoded sha256 of a file, computed on the blocking pool (the object can be
/// large, and this runs off the request's async path).
async fn sha256_file(path: PathBuf) -> Result<String> {
    tokio::task::spawn_blocking(move || -> Result<String> {
        use std::io::Read;

        use sha2::{Digest, Sha256};

        let mut f = std::fs::File::open(&path).context("open lfs object to hash")?;
        let mut hasher = Sha256::new();
        let mut buf = [0u8; 64 * 1024];
        loop {
            let n = f.read(&mut buf).context("read lfs object to hash")?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }
        let mut hex = String::with_capacity(64);
        for b in hasher.finalize() {
            let _ = write!(hex, "{b:02x}");
        }
        Ok(hex)
    })
    .await
    .context("join sha256 task")?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewrites_download_href_and_strips_auth() {
        let mut json = serde_json::json!({
            "transfer": "basic",
            "objects": [{
                "oid": "abc123",
                "size": 42,
                "actions": {
                    "download": {
                        "href": "https://upstream.example/storage/abc123",
                        "header": { "Authorization": "Bearer secret" }
                    }
                }
            }],
        });
        rewrite_download_hrefs(&mut json, "http://proxy:8080", "g/r.git", None);
        let dl = &json["objects"][0]["actions"]["download"];
        assert_eq!(
            dl["href"],
            "http://proxy:8080/g/r.git/info/lfs/objects/abc123?size=42"
        );
        assert!(
            dl.get("header").is_none(),
            "upstream auth header must be stripped when serving anonymously"
        );
    }

    #[test]
    fn embeds_serve_token_in_download_header() {
        let mut json = serde_json::json!({
            "objects": [{
                "oid": "abc123",
                "size": 1,
                "actions": { "download": {
                    "href": "https://upstream/storage/abc123",
                    "header": { "Authorization": "Bearer upstream-secret" }
                }}
            }],
        });
        rewrite_download_hrefs(
            &mut json,
            "http://proxy:8080",
            "g/r.git",
            Some("serve-secret"),
        );
        let dl = &json["objects"][0]["actions"]["download"];
        // The upstream credential is replaced by the proxy's serve token so git-lfs
        // re-presents it on the (auth-checked) object GET.
        assert_eq!(dl["header"]["Authorization"], "Bearer serve-secret");
    }

    #[test]
    fn leaves_error_and_upload_objects_untouched() {
        let mut json = serde_json::json!({
            "objects": [
                { "oid": "bad", "size": 0, "error": { "code": 404, "message": "missing" } },
                { "oid": "up", "size": 1, "actions": { "upload": { "href": "https://upstream/put" } } },
                { "size": 2, "actions": { "download": { "href": "https://x" } } } // no oid -> skipped
            ],
        });
        let before = json.clone();
        rewrite_download_hrefs(&mut json, "http://proxy:8080", "g/r.git", None);
        assert_eq!(json, before, "no download action -> nothing rewritten");
    }

    #[test]
    fn parse_download_action_extracts_href_and_headers() {
        let json = serde_json::json!({
            "objects": [{
                "oid": "abc",
                "size": 3,
                "actions": { "download": {
                    "href": "https://storage/abc",
                    "header": { "Authorization": "Bearer jwt", "X-Extra": "1" }
                }}
            }],
        });
        let (href, headers) = parse_download_action(&json).unwrap();
        assert_eq!(href, "https://storage/abc");
        assert_eq!(headers.get("authorization").unwrap(), "Bearer jwt");
        assert_eq!(headers.get("x-extra").unwrap(), "1");
    }

    #[test]
    fn parse_download_action_reports_upstream_and_shape_errors() {
        // An object upstream flagged as an error propagates as an error.
        let err_obj = serde_json::json!({
            "objects": [{ "oid": "x", "size": 0, "error": { "code": 404, "message": "gone" } }]
        });
        assert!(parse_download_action(&err_obj).is_err());
        // No download action (e.g. an upload-only response) is an error.
        let no_action = serde_json::json!({ "objects": [{ "oid": "x", "size": 0 }] });
        assert!(parse_download_action(&no_action).is_err());
        // A download action without an href is an error.
        let no_href = serde_json::json!({
            "objects": [{ "oid": "x", "size": 0, "actions": { "download": {} } }]
        });
        assert!(parse_download_action(&no_href).is_err());
        // An empty response has no first object.
        assert!(parse_download_action(&serde_json::json!({ "objects": [] })).is_err());
    }

    #[test]
    fn parse_download_action_tolerates_a_missing_header_map() {
        let json = serde_json::json!({
            "objects": [{ "oid": "x", "size": 0, "actions": { "download": { "href": "https://s/x" } } }]
        });
        let (href, headers) = parse_download_action(&json).unwrap();
        assert_eq!(href, "https://s/x");
        assert!(headers.is_empty());
    }

    #[test]
    fn parse_header_line_splits_and_trims() {
        let (name, value) = parse_header_line("Authorization: Basic abc123").unwrap();
        assert_eq!(name, "authorization");
        assert_eq!(value, "Basic abc123");
        assert!(value.is_sensitive());
        // A line with no colon is not a header.
        assert!(parse_header_line("not a header").is_none());
    }

    #[tokio::test]
    async fn sha256_matches_known_vectors() {
        // The well-known empty and "abc" sha256 digests.
        let dir = std::env::temp_dir();
        let empty = dir.join(format!("gcp-lfs-empty-{}", std::process::id()));
        tokio::fs::write(&empty, b"").await.unwrap();
        assert_eq!(
            sha256_file(empty.clone()).await.unwrap(),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        let abc = dir.join(format!("gcp-lfs-abc-{}", std::process::id()));
        tokio::fs::write(&abc, b"abc").await.unwrap();
        assert_eq!(
            sha256_file(abc.clone()).await.unwrap(),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        let _ = tokio::fs::remove_file(&empty).await;
        let _ = tokio::fs::remove_file(&abc).await;
    }
}
