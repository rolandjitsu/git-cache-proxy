// SPDX-License-Identifier: Apache-2.0
//! Git plumbing: keep a bare mirror fresh and serve `upload-pack` from it.
//!
//! All git work is delegated to the system `git` binary, so protocol
//! correctness - including protocol v2, shallow and partial (filtered) clones -
//! comes for free. The proxy never reimplements the wire format; it only:
//!   1. maps a request to a bare mirror under the cache root,
//!   2. runs an incremental `git fetch` from upstream (coalesced per repo),
//!   3. streams `git upload-pack` output from the local mirror to the client.
//!
//! It is strictly read-only: only `git-upload-pack` (clone/fetch) is served, and
//! upstream is only ever *pulled* from - nothing is pushed or replicated
//! proactively. A miss transparently pulls from upstream, so the cache is never
//! stale for the ref the client actually asked for.

use std::collections::HashMap;
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use tokio::io::AsyncWriteExt;
use tokio::process::{ChildStdout, Command};
use tokio::sync::Mutex;
use tokio_util::io::ReaderStream;

use crate::metrics::Metrics;
use crate::repo::RepoRef;

/// What `ensure_fresh` did - for metrics.
#[derive(Debug, Clone, Copy)]
pub enum CacheOutcome {
    /// Mirror did not exist; a full `clone --mirror` ran.
    Cloned,
    /// Mirror existed and an incremental `fetch` ran.
    Fetched,
    /// Mirror was fresh within the TTL; no upstream call.
    Cached,
}

#[derive(Clone)]
pub struct GitConfig {
    pub git_binary: String,
    /// Optional header for upstream auth, e.g. `Authorization: Bearer <token>`.
    /// Injected via env (not argv) so it never shows up in `ps`. Never logged.
    pub upstream_auth_header: Option<String>,
    /// Skip the upstream fetch if the mirror was refreshed within this window.
    pub fetch_ttl: Duration,
}

/// Per-repo serialization point. `fetch_lock` is a `Mutex`, not an `RwLock`, on
/// purpose: it does double duty as (1) the mutual-exclusion point that collapses a
/// burst of concurrent clones for one repo into a single upstream pull, and (2) the
/// guard for the last-fetch `Instant` it holds. Both uses need *exclusive* access
/// while a fetch runs - there is no read-only fast path to share - so an `RwLock`
/// would only add overhead and never grant a concurrent reader.
struct RepoSlot {
    fetch_lock: Mutex<Option<Instant>>,
}

pub struct GitCache {
    cfg: GitConfig,
    metrics: Arc<Metrics>,
    /// Map of repo name -> its serialization slot. A plain `Mutex` (not `RwLock`)
    /// because every access is a get-or-insert (`entry`), which needs a write lock
    /// anyway, and the critical section is a single O(1) map operation - far too
    /// short for reader/writer separation to pay off.
    slots: Mutex<HashMap<String, Arc<RepoSlot>>>,
}

impl GitCache {
    pub fn new(cfg: GitConfig, metrics: Arc<Metrics>) -> Self {
        Self {
            cfg,
            metrics,
            slots: Mutex::new(HashMap::new()),
        }
    }

    async fn slot(&self, name: &str) -> Arc<RepoSlot> {
        self.slots
            .lock()
            .await
            .entry(name.to_string())
            .or_insert_with(|| {
                Arc::new(RepoSlot {
                    fetch_lock: Mutex::new(None),
                })
            })
            .clone()
    }

    /// Ensure the mirror exists and (when `want_fetch`) is fresh. Concurrent
    /// callers for the same repo are serialized; the first does the work, the
    /// rest see it already fresh.
    pub async fn ensure_fresh(&self, repo: &RepoRef, want_fetch: bool) -> Result<CacheOutcome> {
        let slot = self.slot(&repo.name).await;
        let mut last = slot.fetch_lock.lock().await;

        if !repo.cache_dir.join("HEAD").exists() {
            self.clone_mirror(repo).await?;
            *last = Some(Instant::now());
            return Ok(CacheOutcome::Cloned);
        }
        if want_fetch {
            let stale = match *last {
                None => true,
                Some(t) => self.cfg.fetch_ttl.is_zero() || t.elapsed() >= self.cfg.fetch_ttl,
            };
            if stale {
                self.fetch(repo).await?;
                *last = Some(Instant::now());
                return Ok(CacheOutcome::Fetched);
            }
        }
        Ok(CacheOutcome::Cached)
    }

    /// `git upload-pack --advertise-refs`, wrapped with the smart-HTTP service
    /// header - i.e. the `GET .../info/refs?service=git-upload-pack` body.
    pub async fn advertise_refs(
        &self,
        repo: &RepoRef,
        git_protocol: Option<&str>,
    ) -> Result<Bytes> {
        let mut cmd = self.local_cmd(git_protocol);
        cmd.arg("upload-pack")
            .arg("--stateless-rpc")
            .arg("--advertise-refs")
            .arg(&repo.cache_dir);
        let out = cmd
            .output()
            .await
            .context("spawn git upload-pack --advertise-refs")?;
        if !out.status.success() {
            bail!(
                "advertise-refs failed for {}: {}",
                repo.name,
                String::from_utf8_lossy(&out.stderr)
            );
        }
        let mut body = pkt_line("# service=git-upload-pack\n");
        body.extend_from_slice(b"0000"); // flush-pkt
        body.extend_from_slice(&out.stdout);
        Ok(Bytes::from(body))
    }

    /// Stream `git upload-pack --stateless-rpc` - i.e. the
    /// `POST .../git-upload-pack` response (the packfile). The request `body`
    /// (the client's want/have negotiation) is small and buffered; the response
    /// can be gigabytes, so it is streamed straight from the child's stdout.
    pub async fn upload_pack_rpc(
        &self,
        repo: &RepoRef,
        git_protocol: Option<&str>,
        body: Bytes,
    ) -> Result<ReaderStream<ChildStdout>> {
        let mut cmd = self.local_cmd(git_protocol);
        cmd.arg("upload-pack")
            .arg("--stateless-rpc")
            .arg(&repo.cache_dir)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());
        let mut child = cmd.spawn().context("spawn git upload-pack")?;

        let mut stdin = child.stdin.take().context("upload-pack: no stdin")?;
        let stdout = child.stdout.take().context("upload-pack: no stdout")?;
        stdin
            .write_all(&body)
            .await
            .context("write upload-pack request")?;
        drop(stdin); // EOF so upload-pack starts producing the pack

        // The response is the stdout stream we return; the caller (and ultimately
        // the HTTP client) drives it to completion by reading to EOF. We can't make
        // this method block until the child exits without buffering the whole
        // (potentially multi-GB) packfile in memory first, which defeats streaming.
        // So we detach a task purely to *reap* the child - dropping `child` after
        // `take()` neither kills it nor waits on it, leaving a zombie - and to log a
        // non-zero exit. Its completion carries no result the caller needs: a failed
        // upload-pack simply truncates the stream, which the git client detects as a
        // broken pack. This is best-effort observability, not control flow.
        let name = repo.name.clone();
        tokio::spawn(async move {
            match child.wait().await {
                Ok(s) if !s.success() => {
                    tracing::warn!(repo = %name, "git upload-pack exited: {s}")
                }
                Err(e) => tracing::warn!(repo = %name, "git upload-pack wait failed: {e}"),
                _ => {}
            }
        });
        Ok(ReaderStream::new(stdout))
    }

    async fn clone_mirror(&self, repo: &RepoRef) -> Result<()> {
        if let Some(parent) = repo.cache_dir.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("create cache parent for {}", repo.name))?;
        }
        // Clone into a staging dir and atomically rename, so a crashed clone never
        // leaves a half-populated mirror that looks valid. The staging path
        // *appends* a reserved suffix to the full cache path rather than replacing
        // the extension: `with_extension("tmp")` would map a repo literally named
        // `foo.tmp` onto its own cache dir, and could collide with a sibling repo's
        // dir - which hash to different fetch-lock slots and so are not serialized.
        // `repo::resolve` rejects any client path containing the suffix, so this
        // can never alias a real repo.
        let mut tmp = repo.cache_dir.clone().into_os_string();
        tmp.push(crate::repo::INCOMING_SUFFIX);
        let tmp = std::path::PathBuf::from(tmp);
        let _ = tokio::fs::remove_dir_all(&tmp).await;
        tracing::info!(repo = %repo.name, "cloning mirror from upstream");
        // `--mirror` copies *every* ref (all branches, tags, and notes) into a bare
        // repo, not just HEAD, and maps them 1:1 so a later `fetch` keeps them in
        // sync. The client then negotiates whatever ref it wants via upload-pack, so
        // the mirror can serve any branch/tag/sha the origin has - never HEAD-only.
        let status = self
            .fetch_cmd()
            .arg("clone")
            .arg("--mirror")
            .arg("--quiet")
            .arg(&repo.upstream_url)
            .arg(&tmp)
            .status()
            .await
            .context("spawn git clone --mirror")?;
        if !status.success() {
            let _ = tokio::fs::remove_dir_all(&tmp).await;
            // `-` not the repo name: a failed clone must not mint a per-repo series
            // for an arbitrary client-supplied path (see `metrics`). The failing
            // repo is still named in the returned error, which the caller logs.
            self.metrics.record_upstream("clone", "error", "-");
            bail!("git clone --mirror failed for {}", repo.name);
        }
        tokio::fs::rename(&tmp, &repo.cache_dir)
            .await
            .context("rename mirror into place")?;
        self.metrics.record_upstream("clone", "ok", &repo.name);
        Ok(())
    }

    async fn fetch(&self, repo: &RepoRef) -> Result<()> {
        tracing::debug!(repo = %repo.name, "fetching updates from upstream");
        // Run inside the bare mirror and fetch `origin`: `clone --mirror` above sets
        // up exactly one remote named `origin` (git's default remote name) pointing at
        // the upstream URL, with a mirror refspec that updates all refs. So `origin`
        // is not an assumption about the client - it is the remote this proxy created.
        let status = self
            .fetch_cmd()
            .current_dir(&repo.cache_dir)
            .arg("fetch")
            .arg("--prune")
            .arg("--quiet")
            .arg("origin")
            .status()
            .await
            .context("spawn git fetch")?;
        if !status.success() {
            self.metrics.record_upstream("fetch", "error", "-");
            bail!("git fetch failed for {}", repo.name);
        }
        self.metrics.record_upstream("fetch", "ok", &repo.name);
        Ok(())
    }

    /// Command for upstream operations (clone/fetch): injects the auth header via
    /// env-based git config so the token stays out of argv.
    fn fetch_cmd(&self) -> Command {
        let mut c = Command::new(&self.cfg.git_binary);
        c.env("GIT_TERMINAL_PROMPT", "0"); // fail instead of hanging on a prompt
        if let Some(h) = &self.cfg.upstream_auth_header {
            c.env("GIT_CONFIG_COUNT", "1")
                .env("GIT_CONFIG_KEY_0", "http.extraHeader")
                .env("GIT_CONFIG_VALUE_0", h);
        }
        c
    }

    /// Command for local operations (upload-pack): no upstream, no auth. Forwards
    /// the client's protocol version so v2 clients get a v2 advertisement.
    fn local_cmd(&self, git_protocol: Option<&str>) -> Command {
        let mut c = Command::new(&self.cfg.git_binary);
        if let Some(p) = git_protocol {
            c.env("GIT_PROTOCOL", p);
        }
        c
    }
}

/// Encode a string as a single pkt-line (4-hex length prefix + payload).
fn pkt_line(s: &str) -> Vec<u8> {
    let mut v = format!("{:04x}", s.len() + 4).into_bytes();
    v.extend_from_slice(s.as_bytes());
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pkt_line_encodes_length() {
        assert_eq!(pkt_line("a"), b"0005a");
        assert_eq!(&pkt_line("# service=git-upload-pack\n")[..4], b"001e");
    }
}
