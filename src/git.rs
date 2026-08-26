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
//!
//! Want-by-SHA: a client can `want` a commit that lives only under an unadvertised
//! upstream ref (e.g. a GitHub PR merge commit), which `clone --mirror` never
//! captured. Such wants are fetched from upstream on demand and pinned under
//! `PROXY_WANTS_NS` so the mirror can serve them.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::pin::Pin;
use std::process::Stdio;
use std::sync::Arc;
use std::task::Poll;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncWriteExt, ReadBuf};
use tokio::process::{ChildStdout, Command};
use tokio::sync::Mutex;
use tokio_util::io::ReaderStream;

use crate::metrics::{Metrics, ServeKind, Status, UpstreamOp};
use crate::repo::RepoRef;

/// Reserved ref namespace for objects fetched by bare SHA on a want-miss.
const PROXY_WANTS_NS: &str = "refs/proxy-wants";

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
    /// Maximum want-by-SHA pins a mirror retains; oldest pruned beyond it (`0` = unlimited).
    pub max_wants: usize,
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
    /// Cache-size index for LRU eviction, or `None` when no cap is configured.
    /// When present it is `touch`ed on every request and `record`ed after each
    /// clone/fetch; when absent the eviction machinery is entirely inert.
    index: Option<Arc<crate::evict::CacheIndex>>,
}

impl GitCache {
    pub fn new(
        cfg: GitConfig,
        metrics: Arc<Metrics>,
        index: Option<Arc<crate::evict::CacheIndex>>,
    ) -> Self {
        Self {
            cfg,
            metrics,
            slots: Mutex::new(HashMap::new()),
            index,
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

    /// Flag a mirror as changed after a clone/fetch so the background evictor
    /// (re)measures it and rebalances the cache. O(1) bookkeeping only - the size
    /// walk and any eviction run off this request path. No-op when eviction is
    /// disabled.
    fn mark_changed(&self, repo: &RepoRef) {
        if let Some(idx) = &self.index {
            idx.mark_changed(&repo.name);
        }
    }

    /// Evict a mirror: rename it out of the way, then remove it. Serialized against
    /// clone/fetch for the same repo via its slot lock, so it never races the work
    /// that populates the mirror. The rename is atomic and fast; the (possibly slow)
    /// removal runs after the lock is released. `ensure_fresh` keys off
    /// `HEAD.exists()`, so the next request for an evicted repo transparently
    /// re-clones. An `upload-pack` already streaming from the old directory keeps
    /// its open file descriptors and drains cleanly (POSIX unlink semantics).
    pub async fn evict(&self, name: &str, cache_dir: &Path) -> Result<()> {
        let slot = self.slot(name).await;
        let guard = slot.fetch_lock.lock().await;
        if !cache_dir.join("HEAD").exists() {
            return Ok(()); // already gone (raced a prior eviction or manual removal)
        }
        // Rename to a reserved sibling (rejected as a client path by `repo::resolve`)
        // and remove any leftover from a crashed prior eviction first. Appending the
        // suffix to the full path mirrors `clone_mirror`'s staging discipline.
        let mut trash = cache_dir.as_os_str().to_owned();
        trash.push(crate::repo::EVICTING_SUFFIX);
        let trash = std::path::PathBuf::from(trash);
        let _ = tokio::fs::remove_dir_all(&trash).await;
        tokio::fs::rename(cache_dir, &trash)
            .await
            .with_context(|| format!("rename mirror for eviction: {name}"))?;
        drop(guard); // the mirror is gone from its path; free the slot before the slow delete
        let _ = tokio::fs::remove_dir_all(&trash).await;
        Ok(())
    }

    /// Ensure the mirror exists and (when `want_fetch`) is fresh, then satisfy any
    /// `want` in `body` for a SHA the mirror never captured. Concurrent callers for
    /// the same repo are serialized; the first does the work, the rest see it already
    /// fresh. `body` is the client's upload-pack request (empty for `info/refs`),
    /// scanned for want-by-SHA misses - see `ensure_wanted_oids`.
    pub async fn ensure_fresh(
        &self,
        repo: &RepoRef,
        want_fetch: bool,
        body: &[u8],
    ) -> Result<CacheOutcome> {
        let slot = self.slot(&repo.name).await;
        // Mark the repo used on every request - a served cache hit counts as much as
        // a fetch - so the eviction index keeps a truthful last-access ordering.
        if let Some(idx) = &self.index {
            idx.touch(&repo.name);
        }
        let mut last = slot.fetch_lock.lock().await;

        let outcome = if !repo.cache_dir.join("HEAD").exists() {
            self.clone_mirror(repo).await?;
            *last = Some(Instant::now());
            CacheOutcome::Cloned
        } else if want_fetch
            && match *last {
                None => true,
                Some(t) => self.cfg.fetch_ttl.is_zero() || t.elapsed() >= self.cfg.fetch_ttl,
            }
        {
            self.fetch(repo).await?;
            *last = Some(Instant::now());
            CacheOutcome::Fetched
        } else {
            CacheOutcome::Cached
        };

        // Best-effort: on failure, fall through and let upload-pack surface the
        // normal "not our ref" error for any want it still cannot satisfy.
        if let Err(e) = self.ensure_wanted_oids(repo, body).await {
            tracing::warn!(repo = %repo.name, error = %e, "ensure wanted oids failed");
        }
        Ok(outcome)
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
        let started = Instant::now();
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
        self.metrics.observe_serve(
            ServeKind::InfoRefs,
            &repo.name,
            started.elapsed().as_secs_f64(),
        );
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
    ) -> Result<ReaderStream<TimedReader<ChildStdout>>> {
        // Serve duration spans the whole RPC: spawn, negotiation write, and the
        // streamed packfile, recorded when the stream reaches EOF (see `TimedReader`).
        let started = Instant::now();
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
        let timed = TimedReader {
            inner: stdout,
            repo: repo.name.clone(),
            recorder: Some((self.metrics.clone(), started)),
        };
        Ok(ReaderStream::new(timed))
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
        let started = Instant::now();
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
            self.metrics
                .record_upstream(UpstreamOp::Clone, Status::Error, "-");
            bail!("git clone --mirror failed for {}", repo.name);
        }
        let elapsed = started.elapsed().as_secs_f64();
        tokio::fs::rename(&tmp, &repo.cache_dir)
            .await
            .context("rename mirror into place")?;
        self.metrics
            .record_upstream(UpstreamOp::Clone, Status::Ok, &repo.name);
        self.metrics
            .observe_upstream(UpstreamOp::Clone, &repo.name, elapsed);
        self.mark_changed(repo);
        Ok(())
    }

    async fn fetch(&self, repo: &RepoRef) -> Result<()> {
        tracing::debug!(repo = %repo.name, "fetching updates from upstream");
        // Run inside the bare mirror and fetch `origin`: `clone --mirror` above sets
        // up exactly one remote named `origin` (git's default remote name) pointing at
        // the upstream URL, with a mirror refspec that updates all refs. So `origin`
        // is not an assumption about the client - it is the remote this proxy created.
        let started = Instant::now();
        let status = self
            .fetch_cmd()
            .current_dir(&repo.cache_dir)
            .arg("fetch")
            .arg("--prune")
            .arg("--quiet")
            .arg("origin")
            // Mirror refspec, minus the local-only pin namespace: `--prune` would
            // otherwise drop those refs as "not on origin" (see `prune_wants`).
            .arg("+refs/*:refs/*")
            .arg(format!("^{PROXY_WANTS_NS}/*"))
            .status()
            .await
            .context("spawn git fetch")?;
        if !status.success() {
            self.metrics
                .record_upstream(UpstreamOp::Fetch, Status::Error, "-");
            bail!("git fetch failed for {}", repo.name);
        }
        self.metrics
            .record_upstream(UpstreamOp::Fetch, Status::Ok, &repo.name);
        self.metrics.observe_upstream(
            UpstreamOp::Fetch,
            &repo.name,
            started.elapsed().as_secs_f64(),
        );
        self.mark_changed(repo);
        Ok(())
    }

    /// Fetch and pin under `PROXY_WANTS_NS` any `want`ed object the mirror lacks - a
    /// commit reachable only by bare SHA, e.g. a GitHub PR merge commit. A no-op
    /// unless the body names a missing object, so an ordinary clone pays only a cheap
    /// `cat-file` check. Relies on the upstream serving arbitrary SHAs
    /// (`uploadpack.allowAnySHA1InWant`).
    async fn ensure_wanted_oids(&self, repo: &RepoRef, body: &[u8]) -> Result<()> {
        let wants = parse_wants(body);
        if wants.is_empty() {
            return Ok(());
        }
        let mut missing = self.missing_oids(repo, &wants).await?;
        if missing.is_empty() {
            return Ok(());
        }
        // Never fetch more than the mirror will retain: anything past the pin cap
        // would be pruned straight away (see `prune_wants`), so fetching it is pure
        // waste. The excess is dropped and upload-pack rejects those wants.
        if self.cfg.max_wants > 0 && missing.len() > self.cfg.max_wants {
            tracing::warn!(
                repo = %repo.name,
                requested = missing.len(),
                cap = self.cfg.max_wants,
                "capping want-by-sha fetch; excess wants left unserved"
            );
            missing.truncate(self.cfg.max_wants);
        }
        tracing::info!(
            repo = %repo.name,
            count = missing.len(),
            "fetching want-by-sha objects missing from mirror (e.g. PR merge refs)"
        );
        let started = Instant::now();
        let mut cmd = self.fetch_cmd();
        cmd.current_dir(&repo.cache_dir)
            .arg("fetch")
            .arg("--no-tags")
            .arg("--quiet")
            .arg("origin");
        for oid in &missing {
            // Pin under a reserved ref (force: the ref name is the SHA and may
            // already exist), making the object a `want` tip safe from `git gc`.
            cmd.arg(format!("+{oid}:{PROXY_WANTS_NS}/{oid}"));
        }
        let status = cmd
            .status()
            .await
            .context("spawn git fetch (want-by-sha)")?;
        if !status.success() {
            // Upstream would not serve one of these SHAs (e.g. arbitrary-SHA fetches
            // are disabled). Leave it: upload-pack will reject the want with "not
            // our ref".
            self.metrics
                .record_upstream(UpstreamOp::WantFetch, Status::Error, "-");
            tracing::warn!(repo = %repo.name, "want-by-sha fetch from upstream failed");
            return Ok(());
        }
        self.metrics
            .record_upstream(UpstreamOp::WantFetch, Status::Ok, &repo.name);
        self.metrics.observe_upstream(
            UpstreamOp::WantFetch,
            &repo.name,
            started.elapsed().as_secs_f64(),
        );
        self.mark_changed(repo);
        // Best-effort: a failed prune is not worth failing the request over.
        if let Err(e) = self.prune_wants(repo).await {
            tracing::warn!(repo = %repo.name, error = %e, "pruning want-by-sha pins failed");
        }
        Ok(())
    }

    /// Cap how many want-by-SHA pins a mirror keeps, deleting the oldest by creation
    /// date beyond `max_wants`. Pins are excluded from `fetch --prune` and `git gc`,
    /// so this is what bounds their growth - the cache-size LRU scoped to a mirror's
    /// pins. A dropped pin is re-fetched on the next want-miss for its SHA.
    async fn prune_wants(&self, repo: &RepoRef) -> Result<()> {
        if self.cfg.max_wants == 0 {
            return Ok(());
        }
        let out = Command::new(&self.cfg.git_binary)
            .current_dir(&repo.cache_dir)
            .arg("for-each-ref")
            .arg("--sort=-creatordate")
            .arg("--format=%(refname)")
            .arg(format!("{PROXY_WANTS_NS}/"))
            .output()
            .await
            .context("spawn git for-each-ref (proxy-wants)")?;
        if !out.status.success() {
            bail!("git for-each-ref failed for {}", repo.name);
        }
        let listing = String::from_utf8_lossy(&out.stdout);
        let refs: Vec<&str> = listing.lines().collect();
        if refs.len() <= self.cfg.max_wants {
            return Ok(());
        }
        // `update-ref --stdin` deletes the stale tail in one atomic transaction.
        let mut deletions = String::new();
        for r in &refs[self.cfg.max_wants..] {
            deletions.push_str("delete ");
            deletions.push_str(r);
            deletions.push('\n');
        }
        let mut child = Command::new(&self.cfg.git_binary)
            .current_dir(&repo.cache_dir)
            .arg("update-ref")
            .arg("--stdin")
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .context("spawn git update-ref --stdin")?;
        child
            .stdin
            .take()
            .context("update-ref: no stdin")?
            .write_all(deletions.as_bytes())
            .await
            .context("write update-ref deletions")?;
        let status = child.wait().await.context("git update-ref --stdin")?;
        if !status.success() {
            bail!("git update-ref --stdin failed for {}", repo.name);
        }
        tracing::info!(
            repo = %repo.name,
            pruned = refs.len() - self.cfg.max_wants,
            "pruned oldest want-by-sha pins over cap"
        );
        Ok(())
    }

    /// Which of `oids` are absent from the mirror's object store, via a single
    /// `git cat-file --batch-check` (prints `<oid> missing` for an absent object).
    /// A present object is serveable: it entered the mirror via a real ref or a pin.
    async fn missing_oids(&self, repo: &RepoRef, oids: &HashSet<String>) -> Result<Vec<String>> {
        let mut child = Command::new(&self.cfg.git_binary)
            .current_dir(&repo.cache_dir)
            .arg("cat-file")
            .arg("--batch-check")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .context("spawn git cat-file --batch-check")?;
        let mut stdin = child.stdin.take().context("cat-file: no stdin")?;
        let mut query = String::with_capacity(oids.len() * 41);
        for oid in oids {
            query.push_str(oid);
            query.push('\n');
        }
        stdin
            .write_all(query.as_bytes())
            .await
            .context("write cat-file query")?;
        drop(stdin); // EOF so cat-file finishes and exits
        let out = child
            .wait_with_output()
            .await
            .context("git cat-file --batch-check")?;
        if !out.status.success() {
            bail!("git cat-file --batch-check failed for {}", repo.name);
        }
        Ok(String::from_utf8_lossy(&out.stdout)
            .lines()
            .filter_map(|l| l.strip_suffix(" missing"))
            .map(str::to_string)
            .collect())
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
        // Hide the pin namespace from the advertisement so it never leaks into a
        // client's ref list; a hidden ref is still honored as a `want` tip.
        c.arg("-c")
            .arg(format!("uploadpack.hideRefs={PROXY_WANTS_NS}"));
        if let Some(p) = git_protocol {
            c.env("GIT_PROTOCOL", p);
        }
        c
    }
}

/// Wraps `upload-pack`'s stdout to record how long the packfile took to serve.
/// The duration spans from the RPC starting to the stream reaching EOF - or the
/// client disconnecting, caught by `Drop` - so it includes the client's read
/// speed: this is serve latency, not pure generation time. Recorded exactly once
/// (the `Option` is `take`n on the first of EOF or drop).
pub struct TimedReader<R> {
    inner: R,
    repo: String,
    recorder: Option<(Arc<Metrics>, Instant)>,
}

impl<R> TimedReader<R> {
    fn record(&mut self) {
        if let Some((metrics, started)) = self.recorder.take() {
            metrics.observe_serve(
                ServeKind::UploadPack,
                &self.repo,
                started.elapsed().as_secs_f64(),
            );
        }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for TimedReader<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        let before = buf.filled().len();
        let poll = Pin::new(&mut this.inner).poll_read(cx, buf);
        // A ready read that produced no bytes is EOF: the packfile is fully served.
        if let Poll::Ready(Ok(())) = &poll
            && buf.filled().len() == before
        {
            this.record();
        }
        poll
    }
}

impl<R> Drop for TimedReader<R> {
    fn drop(&mut self) {
        // Covers a client that hung up before EOF; a no-op if EOF already recorded.
        self.record();
    }
}

/// Encode a string as a single pkt-line (4-hex length prefix + payload).
fn pkt_line(s: &str) -> Vec<u8> {
    let mut v = format!("{:04x}", s.len() + 4).into_bytes();
    v.extend_from_slice(s.as_bytes());
    v
}

/// Extract the unique object IDs from the `want` lines of an `upload-pack` request
/// body (protocol v0/v1: `want <oid> [capabilities]`; v2: `want <oid>` among the
/// fetch-command args). The body is pkt-line framed, so we walk the frames rather
/// than scanning raw bytes - a `want ` byte sequence inside packed negotiation data
/// must not be mistaken for a request line. Malformed framing stops the walk,
/// missing at worst a want that upload-pack then rejects.
fn parse_wants(body: &[u8]) -> HashSet<String> {
    let mut oids: HashSet<String> = HashSet::new();
    let mut pos = 0;
    while pos + 4 <= body.len() {
        let Some(len) = std::str::from_utf8(&body[pos..pos + 4])
            .ok()
            .and_then(|h| usize::from_str_radix(h, 16).ok())
        else {
            break; // not a hex length prefix: malformed, stop
        };
        // 0000 flush / 0001 delim / 0002 response-end carry no payload.
        if len < 4 {
            pos += 4;
            continue;
        }
        let end = pos + len;
        if end > body.len() {
            break; // truncated frame
        }
        if let Some(rest) = body[pos + 4..end].strip_prefix(b"want ") {
            let oid: Vec<u8> = rest
                .iter()
                .copied()
                .take_while(u8::is_ascii_hexdigit)
                .collect();
            // sha1 (40) or sha256 (64); ignore anything else (e.g. a stray token).
            if oid.len() == 40 || oid.len() == 64 {
                oids.insert(String::from_utf8(oid).expect("ascii hex is valid utf-8"));
            }
        }
        pos = end;
    }
    oids
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Collect an iterator of oids into a `HashSet` for order-insensitive assertions
    /// against `parse_wants`.
    fn want_set<I: IntoIterator<Item = String>>(oids: I) -> HashSet<String> {
        oids.into_iter().collect()
    }

    #[test]
    fn pkt_line_encodes_length() {
        assert_eq!(pkt_line("a"), b"0005a");
        assert_eq!(&pkt_line("# service=git-upload-pack\n")[..4], b"001e");
    }

    #[test]
    fn parse_wants_extracts_oids_from_both_protocol_versions() {
        let sha_a = "a".repeat(40);
        let sha_b = "b".repeat(40);
        // Protocol v0/v1: first want carries capabilities after the oid.
        let mut v1 = Vec::new();
        v1.extend_from_slice(&pkt_line(&format!(
            "want {sha_a} multi_ack side-band-64k\n"
        )));
        v1.extend_from_slice(&pkt_line(&format!("want {sha_b}\n")));
        v1.extend_from_slice(b"0000");
        assert_eq!(parse_wants(&v1), want_set([sha_a.clone(), sha_b.clone()]));

        // Protocol v2: a fetch command with want args framed by a delim-pkt.
        let mut v2 = Vec::new();
        v2.extend_from_slice(&pkt_line("command=fetch\n"));
        v2.extend_from_slice(b"0001");
        v2.extend_from_slice(&pkt_line(&format!("want {sha_a}\n")));
        v2.extend_from_slice(&pkt_line("have cccccccccccccccccccccccccccccccccccccccc\n"));
        v2.extend_from_slice(&pkt_line("done\n"));
        v2.extend_from_slice(b"0000");
        assert_eq!(parse_wants(&v2), want_set([sha_a.clone()]));
    }

    #[test]
    fn parse_wants_dedups_and_ignores_non_wants() {
        let sha = "0123456789abcdef0123456789abcdef01234567".to_string();
        let mut body = Vec::new();
        body.extend_from_slice(&pkt_line(&format!("want {sha}\n")));
        body.extend_from_slice(&pkt_line(&format!("want {sha}\n"))); // duplicate
        body.extend_from_slice(&pkt_line("command=ls-refs\n")); // not a want
        body.extend_from_slice(b"0000");
        assert_eq!(parse_wants(&body), want_set([sha]));

        // A request with no want lines (e.g. a bare ls-refs) yields nothing, so the
        // caller skips the cat-file check and any upstream call entirely.
        let mut ls = Vec::new();
        ls.extend_from_slice(&pkt_line("command=ls-refs\n"));
        ls.extend_from_slice(b"0000");
        assert!(parse_wants(&ls).is_empty());
    }

    #[test]
    fn parse_wants_tolerates_malformed_framing() {
        assert!(parse_wants(b"").is_empty());
        assert!(parse_wants(b"xyz").is_empty()); // too short for a length prefix
        assert!(parse_wants(b"zzzz").is_empty()); // non-hex length prefix
        // A length that runs past the buffer is ignored rather than panicking.
        assert!(parse_wants(b"0099want ").is_empty());
    }

    #[tokio::test]
    async fn timed_reader_records_serve_duration_at_eof() {
        use tokio::io::AsyncReadExt;

        let metrics = Arc::new(Metrics::new());
        let mut reader = TimedReader {
            inner: &b"packfile bytes"[..],
            repo: "group/foo.git".into(),
            recorder: Some((metrics.clone(), Instant::now())),
        };
        // Reading to EOF drives the final zero-byte read, which records once.
        let mut sink = Vec::new();
        reader.read_to_end(&mut sink).await.unwrap();
        assert!(metrics.gather().contains(
            r#"gitcacheproxy_serve_duration_seconds_count{kind="upload_pack",repo="group/foo.git"} 1"#
        ));
    }
}
