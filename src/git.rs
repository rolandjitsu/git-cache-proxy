// SPDX-License-Identifier: Apache-2.0
//! Git plumbing: keep a bare mirror fresh and serve `upload-pack` from it.
//!
//! All git work is delegated to the system `git` binary, so protocol
//! correctness - protocol v2, shallow and partial (filtered) clones - comes from
//! `git` itself, not a reimplemented wire format. (Partial clone additionally
//! needs `uploadpack.allowFilter`, which the serve path sets; see `local_cmd`.)
//! The proxy only:
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
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::process::Stdio;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, ReadBuf};
use tokio::process::{Child, ChildStdout, Command};
use tokio::sync::{Mutex, Semaphore, oneshot, watch};
use tokio::time::Instant;
use tokio_util::io::ReaderStream;

use crate::metrics::{Metrics, ServeKind, Status, UpstreamOp};
use crate::repo::RepoRef;

/// Reserved ref namespace for objects fetched by bare SHA on a want-miss.
const PROXY_WANTS_NS: &str = "refs/proxy-wants";

const UPSTREAM_RETRY_DELAYS: [Duration; 3] = [
    Duration::from_secs(1),
    Duration::from_secs(2),
    Duration::from_secs(4),
];
const MAX_UPSTREAM_RETRIES: usize = UPSTREAM_RETRY_DELAYS.len();
const MAX_UPSTREAM_STDERR_BYTES: usize = 64 * 1024;

// Negotiation is bounded independently of pack size. Once input is written,
// only inactivity is bounded: active downloads have no total duration limit.
const UPLOAD_PACK_REQUEST_WRITE_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const UPLOAD_PACK_RESPONSE_IDLE_TIMEOUT: Duration = Duration::from_secs(30 * 60);

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

/// Upstream refused the credential on a clone/fetch (an auth diagnostic, or a
/// 401/403/404). Carried out of the fetch so a caller in `client`
/// upstream-credential mode can answer the client a 401, not a 502: the
/// credential is the client's own.
#[derive(Debug)]
pub struct UpstreamAuthFailed;

impl std::fmt::Display for UpstreamAuthFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("upstream rejected the credential")
    }
}

impl std::error::Error for UpstreamAuthFailed {}

/// Whether `err` carries an [`UpstreamAuthFailed`] anywhere in its chain.
pub fn is_auth_failure(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| cause.is::<UpstreamAuthFailed>())
}

#[derive(Clone)]
pub struct GitConfig {
    pub git_binary: String,
    /// Optional header for upstream auth, e.g. `Authorization: Bearer <token>`.
    /// Injected via env (not argv) so it never shows up in `ps`. Never logged.
    pub upstream_auth_header: Option<String>,
    /// git `core.bigFileThreshold` for upstream clone/fetch: blobs above it are
    /// streamed to disk rather than held in memory, bounding `index-pack` RSS on
    /// very large repos. See `Config::big_file_threshold`.
    pub big_file_threshold: String,
    /// Skip the upstream fetch if the last one started within this window.
    pub fetch_ttl: Duration,
    /// Maximum want-by-SHA pins a mirror retains; oldest pruned beyond it (`0` = unlimited).
    pub max_wants: usize,
    /// Maximum concurrent `upload-pack` processes (`0` = unlimited). See
    /// `Config::max_concurrent_upload_packs`.
    pub max_upload_packs: usize,
    /// git `pack.threads` for upload-pack (`None` = git default).
    pub pack_threads: Option<u32>,
    /// git `pack.windowMemory` for upload-pack (`None` = git default).
    pub pack_window_memory: Option<String>,
}

/// Per-repo serialization point. `fetch_lock` is a `Mutex`, not an `RwLock`, on
/// purpose: it does double duty as (1) the mutual-exclusion point that collapses a
/// burst of concurrent clones for one repo into a single upstream pull, and (2) the
/// guard for the start `Instant` of the last upstream pull. Both uses need *exclusive* access
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
    /// `None` when `max_upload_packs` is `0` (unlimited).
    upload_packs: Option<Arc<Semaphore>>,
}

impl GitCache {
    pub fn new(
        cfg: GitConfig,
        metrics: Arc<Metrics>,
        index: Option<Arc<crate::evict::CacheIndex>>,
    ) -> Self {
        let upload_packs =
            (cfg.max_upload_packs > 0).then(|| Arc::new(Semaphore::new(cfg.max_upload_packs)));
        Self {
            cfg,
            metrics,
            slots: Mutex::new(HashMap::new()),
            index,
            upload_packs,
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
        cred: Option<&str>,
    ) -> Result<CacheOutcome> {
        let slot = self.slot(&repo.name).await;
        // Mark the repo used on every request - a served cache hit counts as much as
        // a fetch - so the eviction index keeps a truthful last-access ordering.
        if let Some(idx) = &self.index {
            idx.touch(&repo.name);
        }
        let mut last = slot.fetch_lock.lock().await;

        // Stamp the start: upstream is read then, so a ref pushed mid-fetch is
        // missing, and a caller queued behind a slow fetch must see it as stale.
        let started = Instant::now();
        let outcome = if !repo.cache_dir.join("HEAD").exists() {
            self.clone_mirror(repo, cred).await?;
            *last = Some(started);
            CacheOutcome::Cloned
        } else if want_fetch
            && match *last {
                None => true,
                Some(t) => self.cfg.fetch_ttl.is_zero() || t.elapsed() >= self.cfg.fetch_ttl,
            }
        {
            self.fetch(repo, cred).await?;
            *last = Some(started);
            CacheOutcome::Fetched
        } else {
            CacheOutcome::Cached
        };

        // Best-effort: on failure, fall through and let upload-pack surface the
        // normal "not our ref" error for any want it still cannot satisfy.
        if let Err(e) = self.ensure_wanted_oids(repo, body, cred).await {
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
    /// (the client's want/have negotiation) is buffered; the response
    /// can be gigabytes, so it is streamed straight from the child's stdout.
    pub async fn upload_pack_rpc(
        &self,
        repo: &RepoRef,
        git_protocol: Option<&str>,
        body: Bytes,
    ) -> Result<ReaderStream<TimedReader<ChildStdout>>> {
        let permit = match &self.upload_packs {
            Some(s) => Some(s.clone().acquire_owned().await.context("upload-pack cap")?),
            None => None,
        };
        // Serve duration spans the whole RPC: spawn, negotiation write, and the
        // streamed packfile, recorded when the stream reaches EOF (see `TimedReader`).
        let started = Instant::now();
        let mut cmd = self.local_cmd(git_protocol);
        cmd.arg("upload-pack")
            .arg("--stateless-rpc")
            .arg(&repo.cache_dir)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);
        // Own process group, so a kill also reaches `pack-objects`, which writes
        // nothing while it counts and compresses and so never gets SIGPIPE.
        #[cfg(unix)]
        cmd.process_group(0);
        let mut child = cmd.spawn().context("spawn git upload-pack")?;
        let mut stdin = child.stdin.take().context("upload-pack: no stdin")?;
        let stdout = child.stdout.take().context("upload-pack: no stdout")?;

        let (read_progress_tx, read_progress_rx) = watch::channel(ReadProgress {
            last_read_at: started,
            eof: false,
        });
        tracing::debug!(repo = %repo.name, pid = child.id(), request_bytes = body.len(), "start upload-pack");
        let writer = async move { write_upload_pack_request(&mut stdin, &body).await };
        // One owner polls the writer and retains the permit until wait.
        // Stdout goes directly to the response, including during negotiation.
        let name = repo.name.clone();
        let (completion_tx, completion_rx) = oneshot::channel();
        tokio::spawn(async move {
            let _permit = permit;
            let result = run_upload_pack(
                child,
                writer,
                read_progress_rx,
                UPLOAD_PACK_RESPONSE_IDLE_TIMEOUT,
            )
            .await;
            if let Err(error) = &result {
                if error.kind() == std::io::ErrorKind::ConnectionAborted {
                    tracing::debug!(repo = %name, %error, "upload-pack lifecycle ended");
                } else {
                    tracing::warn!(repo = %name, %error, "upload-pack lifecycle failed");
                }
            }
            let _ = completion_tx.send(result);
        });
        let timed = TimedReader {
            inner: stdout,
            repo: repo.name.clone(),
            recorder: Some((self.metrics.clone(), started)),
            lifecycle: Some(ResponseLifecycle {
                read_progress_tx,
                completion_rx,
            }),
            eof: false,
        };
        Ok(ReaderStream::new(timed))
    }

    async fn clone_mirror(&self, repo: &RepoRef, cred: Option<&str>) -> Result<()> {
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
        tracing::info!(repo = %repo.name, "cloning mirror from upstream");
        // `--mirror` copies *every* ref (all branches, tags, and notes) into a bare
        // repo, not just HEAD, and maps them 1:1 so a later `fetch` keeps them in
        // sync. The client then negotiates whatever ref it wants via upload-pack, so
        // the mirror can serve any branch/tag/sha the origin has - never HEAD-only.
        let started = Instant::now();
        let mut cmd = self.fetch_cmd(cred);
        cmd.arg("clone")
            .arg("--mirror")
            .arg("--quiet")
            .arg(&repo.upstream_url)
            .arg(&tmp);
        if let Err(error) = run_upstream(&mut cmd, "clone", &repo.name, Some(&tmp)).await {
            // `-` not the repo name: a failed clone must not mint a per-repo series
            // for an arbitrary client-supplied path (see `metrics`). The failing
            // repo is still named in the returned error, which the caller logs.
            self.metrics
                .record_upstream(UpstreamOp::Clone, Status::Error, "-");
            return Err(error);
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

    async fn fetch(&self, repo: &RepoRef, cred: Option<&str>) -> Result<()> {
        tracing::debug!(repo = %repo.name, "fetching updates from upstream");
        // Run inside the bare mirror and fetch `origin`: `clone --mirror` above sets
        // up exactly one remote named `origin` (git's default remote name) pointing at
        // the upstream URL, with a mirror refspec that updates all refs. So `origin`
        // is not an assumption about the client - it is the remote this proxy created.
        let started = Instant::now();
        let mut cmd = self.fetch_cmd(cred);
        cmd.current_dir(&repo.cache_dir)
            .arg("fetch")
            .arg("--prune")
            .arg("--quiet")
            .arg("origin")
            // Mirror refspec, minus the local-only pin namespace: `--prune` would
            // otherwise drop those refs as "not on origin" (see `prune_wants`).
            .arg("+refs/*:refs/*")
            .arg(format!("^{PROXY_WANTS_NS}/*"));
        if let Err(error) = run_upstream(&mut cmd, "fetch", &repo.name, None).await {
            self.metrics
                .record_upstream(UpstreamOp::Fetch, Status::Error, "-");
            return Err(error);
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
    async fn ensure_wanted_oids(
        &self,
        repo: &RepoRef,
        body: &[u8],
        cred: Option<&str>,
    ) -> Result<()> {
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
        let mut cmd = self.fetch_cmd(cred);
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
            .kill_on_drop(true)
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
        let (_, out) = tokio::try_join!(
            async {
                stdin
                    .write_all(query.as_bytes())
                    .await
                    .context("write cat-file query")?;
                drop(stdin); // EOF so cat-file finishes and exits
                Ok::<(), anyhow::Error>(())
            },
            async {
                child
                    .wait_with_output()
                    .await
                    .context("git cat-file --batch-check")
            }
        )?;
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
    /// env-based git config so the token stays out of argv. A per-request `cred`
    /// (the `client` upstream-credential mode) takes precedence over the static
    /// `--upstream-auth-header`; `None` falls back to the static one.
    fn fetch_cmd(&self, cred: Option<&str>) -> Command {
        let mut c = Command::new(&self.cfg.git_binary);
        c.env("GIT_TERMINAL_PROMPT", "0"); // fail instead of hanging on a prompt
        c.env("LC_ALL", "C"); // Retry classification relies on Git's English diagnostics.
        for (k, v) in git_config_env(
            &self.cfg.big_file_threshold,
            cred.or(self.cfg.upstream_auth_header.as_deref()),
        ) {
            c.env(k, v);
        }
        c
    }

    /// Command for local operations (upload-pack): no upstream, no auth. Forwards
    /// the client's protocol version so v2 clients get a v2 advertisement.
    ///
    /// `uploadpack.allowFilter` is enabled so clients can request *partial*
    /// (filtered) clones - e.g. `--filter=blob:none`. Without it `git upload-pack`
    /// silently drops the filter and streams every object (the client warns
    /// "filtering not recognized by server, ignoring"), so partial clone would not
    /// actually work through the proxy. We always serve from a full local mirror,
    /// so honoring the filter - and the promisor back-fill of omitted objects it
    /// triggers - is always safe. Passed as a `-c` override (before the
    /// `upload-pack` subcommand) rather than persisted in each mirror's config, so
    /// it holds regardless of mirror state.
    fn local_cmd(&self, git_protocol: Option<&str>) -> Command {
        let mut c = Command::new(&self.cfg.git_binary);
        c.arg("-c").arg("uploadpack.allowFilter=true");
        // Hide the pin namespace from the advertisement so it never leaks into a
        // client's ref list; a hidden ref is still honored as a `want` tip.
        c.arg("-c")
            .arg(format!("uploadpack.hideRefs={PROXY_WANTS_NS}"));
        if let Some(n) = self.cfg.pack_threads {
            c.arg("-c").arg(format!("pack.threads={n}"));
        }
        if let Some(m) = &self.cfg.pack_window_memory {
            c.arg("-c").arg(format!("pack.windowMemory={m}"));
        }
        if let Some(p) = git_protocol {
            c.env("GIT_PROTOCOL", p);
        }
        c
    }
}

// The caller holds the repository lock throughout retries. Never retry local
// upload-pack: once its response starts streaming it cannot be replayed safely.
async fn run_upstream(
    cmd: &mut Command,
    operation: &str,
    repo: &str,
    staging: Option<&Path>,
) -> Result<()> {
    cmd.stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    for attempt in 0..=MAX_UPSTREAM_RETRIES {
        if let Some(path) = staging {
            clean_staging(path).await?;
        }
        let mut child = cmd.spawn().context("spawn upstream git")?;
        let mut stderr = child.stderr.take().context("upstream git: no stderr")?;
        // Drain even after the cap so a noisy child cannot block on its pipe.
        // Only retain the tail; credentials and raw remote output are never logged.
        let mut tail = Vec::new();
        let mut chunk = [0_u8; 4096];
        loop {
            let count = stderr
                .read(&mut chunk)
                .await
                .context("read upstream git stderr")?;
            if count == 0 {
                break;
            }
            tail.extend_from_slice(&chunk[..count]);
            if tail.len() > MAX_UPSTREAM_STDERR_BYTES {
                tail.drain(..tail.len() - MAX_UPSTREAM_STDERR_BYTES);
            }
        }
        let status = child.wait().await.context("wait for upstream git")?;
        if status.success() {
            return Ok(());
        }
        if let Some(path) = staging {
            clean_staging(path).await?;
        }
        let reason = transient_upstream_error(&tail);
        if let Some(reason) = reason
            && status.code().is_some()
            && let Some(delay) = UPSTREAM_RETRY_DELAYS.get(attempt)
        {
            tracing::warn!(
                repo,
                operation,
                attempt = attempt + 1,
                max_attempts = MAX_UPSTREAM_RETRIES + 1,
                delay_seconds = delay.as_secs(),
                reason,
                "retrying upstream git"
            );
            tokio::time::sleep(*delay).await;
            continue;
        }
        // An upstream credential rejection is carried out typed, so a `client`-mode
        // caller can map it to a client 401 rather than a blanket 502.
        if reason.is_none() && upstream_auth_diagnostic(&tail) {
            return Err(anyhow::Error::new(UpstreamAuthFailed).context(format!(
                "git {operation} failed for {repo}: upstream rejected the credential"
            )));
        }
        bail!(
            "git {operation} failed for {repo} after {} attempt(s) ({}, status {status})",
            attempt + 1,
            reason.unwrap_or("permanent or unrecognized error")
        );
    }
    unreachable!("final attempt returns an error or succeeds")
}

async fn clean_staging(path: &Path) -> Result<()> {
    match tokio::fs::remove_dir_all(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).context("remove upstream clone staging directory"),
    }
}

/// Whether a non-retryable failure is upstream refusing the credential, as
/// opposed to a proxy-side permanent error (disk, lock, local cert). GitHub and
/// GitLab return 404 for a repo a credential cannot see, so a not-found reads as
/// an auth failure here. Checked only after `transient_upstream_error` has ruled
/// the error permanent.
fn upstream_auth_diagnostic(stderr: &[u8]) -> bool {
    let text = String::from_utf8_lossy(stderr).to_ascii_lowercase();
    [
        "authentication failed",
        "could not read username",
        "repository not found",
        "returned error: 401",
        "returned error: 403",
        "returned error: 404",
    ]
    .iter()
    .any(|message| text.contains(message))
}

fn transient_upstream_error(stderr: &[u8]) -> Option<&'static str> {
    let text = String::from_utf8_lossy(stderr).to_ascii_lowercase();
    // Permanent errors take precedence if Git reports more than one diagnostic.
    if [
        "authentication failed",
        "repository not found",
        "could not read username",
        "certificate problem",
        "certificate verify failed",
        "certificate verification failed",
        "server certificate verification failed",
        "no space left on device",
        "permission denied",
        "read-only file system",
        ".lock",
        "returned error: 401",
        "returned error: 403",
        "returned error: 404",
    ]
    .iter()
    .any(|message| text.contains(message))
    {
        return None;
    }
    if [408, 429, 500, 502, 503, 504].iter().any(|code| {
        text.lines().any(|line| {
            line.trim_end()
                .ends_with(&format!("returned error: {code}"))
        })
    }) {
        return Some("transient HTTP status");
    }
    if [
        "unexpected eof while reading",
        "gnutls_handshake() failed: the tls connection was non-properly terminated",
        "gnutls recv error (-110)",
        "ssl_error_syscall",
    ]
    .iter()
    .any(|message| text.contains(message))
    {
        return Some("TLS connection interrupted");
    }
    if [
        "connection reset",
        "connection timed out",
        "connection refused",
        "failed to connect to",
        "operation timed out",
        "connection timeout",
        "empty reply from server",
        "recv failure:",
        "send failure:",
        "curl 18 ",
        "curl 28 ",
        "curl 52 ",
        "curl 55 ",
        "curl 56 ",
        "curl 92 ",
    ]
    .iter()
    .any(|message| text.contains(message))
    {
        return Some("connection interrupted or timed out");
    }
    if [
        "temporary failure in name resolution",
        "could not resolve host:",
        "could not resolve proxy:",
    ]
    .iter()
    .any(|message| text.contains(message))
    {
        return Some("DNS resolution failed");
    }
    None
}

async fn write_upload_pack_request(
    stdin: &mut (impl tokio::io::AsyncWrite + Unpin),
    body: &[u8],
) -> std::io::Result<()> {
    tokio::time::timeout(UPLOAD_PACK_REQUEST_WRITE_TIMEOUT, stdin.write_all(body))
        .await
        .map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "upload-pack request write timed out",
            )
        })?
}

#[derive(Clone, Copy)]
struct ReadProgress {
    last_read_at: Instant,
    eof: bool,
}

/// Own the writer future and child until cancellation or complete delivery.
async fn run_upload_pack(
    mut child: Child,
    writer: impl Future<Output = std::io::Result<()>>,
    mut read_progress_rx: watch::Receiver<ReadProgress>,
    idle_timeout: Duration,
) -> std::io::Result<()> {
    tokio::pin!(writer);
    let mut request_written = false;
    let result = async {
        let exit = wait_upload_pack(&mut child);
        tokio::pin!(exit);
        let mut exited = false;
        loop {
            let read_progress = *read_progress_rx.borrow_and_update();
            if exited && request_written && read_progress.eof {
                return Ok(());
            }
            tokio::select! {
                biased;
                status = &mut exit, if !exited && (UPLOAD_PACK_CAN_OBSERVE_EXIT_BEFORE_EOF || (request_written && read_progress.eof)) => {
                    let status = status?;
                    if !status.success() {
                        return Err(std::io::Error::other(format!("git upload-pack exited: {status}")));
                    }
                    // A successful leader can exit with valid stdout still in
                    // the pipe. Drain it before reporting successful delivery.
                    exited = true;
                }
                result = &mut writer, if !request_written => {
                    result?;
                    request_written = true;
                }
                changed = read_progress_rx.changed() => {
                    changed.map_err(|_| std::io::Error::new(
                        std::io::ErrorKind::ConnectionAborted,
                        "upload-pack response dropped",
                    ))?;
                }
                _ = tokio::time::sleep_until(read_progress.last_read_at + idle_timeout) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "upload-pack response idle timed out",
                    ));
                }
            }
        }
    }
    .await;
    // Drop cancels the writer future before the leases are released. There is
    // no separate writer task that can outlive this owner.
    if result.is_err() && child.id().is_some() {
        if let Err(error) = kill_group(&child) {
            tracing::warn!("kill upload-pack process group failed: {error}");
            let _ = child.start_kill();
        }
        child.wait().await?;
    }
    result
}

// Targets without waitid(NOWAIT) retain EOF-first waiting: reaping early
// there would lose the process-group identity needed for safe cancellation.
const UPLOAD_PACK_CAN_OBSERVE_EXIT_BEFORE_EOF: bool = cfg!(all(
    unix,
    not(any(
        target_os = "cygwin",
        target_os = "horizon",
        target_os = "openbsd",
        target_os = "redox",
    ))
));

// Subscribe before probing so an exit between waitid and recv is not missed.
// Keep the leader unreaped until its descendants have been stopped.
#[cfg(all(
    unix,
    not(any(
        target_os = "cygwin",
        target_os = "horizon",
        target_os = "openbsd",
        target_os = "redox",
    ))
))]
async fn wait_upload_pack(child: &mut Child) -> std::io::Result<std::process::ExitStatus> {
    use rustix::process::{Pid, WaitId, WaitIdOptions, waitid};
    use tokio::signal::unix::{SignalKind, signal};

    let pid = child
        .id()
        .and_then(|id| Pid::from_raw(id as i32))
        .ok_or_else(|| std::io::Error::other("upload-pack already reaped"))?;
    let mut exits = signal(SignalKind::child())?;
    loop {
        if waitid(
            WaitId::Pid(pid),
            WaitIdOptions::EXITED | WaitIdOptions::NOHANG | WaitIdOptions::NOWAIT,
        )?
        .is_some()
        {
            let _ = kill_group(child);
            return child.wait().await;
        }
        exits
            .recv()
            .await
            .ok_or_else(|| std::io::Error::other("upload-pack exit signal closed"))?;
    }
}

#[cfg(not(all(
    unix,
    not(any(
        target_os = "cygwin",
        target_os = "horizon",
        target_os = "openbsd",
        target_os = "redox",
    ))
)))]
async fn wait_upload_pack(child: &mut Child) -> std::io::Result<std::process::ExitStatus> {
    child.wait().await
}

/// Call before reaping: an unreaped child still owns its group ID, so the
/// signal cannot reach a recycled group.
#[cfg(unix)]
fn kill_group(child: &Child) -> std::io::Result<()> {
    use rustix::process::{Pid, Signal, kill_process_group};

    let pid = child
        .id()
        .and_then(|id| i32::try_from(id).ok())
        .and_then(Pid::from_raw)
        .ok_or_else(|| std::io::Error::other("upload-pack already reaped"))?;
    Ok(kill_process_group(pid, Signal::KILL)?)
}

#[cfg(not(unix))]
fn kill_group(_child: &Child) -> std::io::Result<()> {
    Err(std::io::Error::other("process groups are unix-only"))
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
    lifecycle: Option<ResponseLifecycle>,
    eof: bool,
}

struct ResponseLifecycle {
    read_progress_tx: watch::Sender<ReadProgress>,
    completion_rx: oneshot::Receiver<std::io::Result<()>>,
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
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if let Some(lifecycle) = &mut this.lifecycle {
            match Pin::new(&mut lifecycle.completion_rx).poll(cx) {
                Poll::Ready(result) => {
                    let result = result.unwrap_or_else(|_| {
                        Err(std::io::Error::other("upload-pack lifecycle task failed"))
                    });
                    this.lifecycle = None;
                    if let Err(error) = result {
                        this.eof = true;
                        this.record();
                        return Poll::Ready(Err(error));
                    }
                }
                Poll::Pending => {}
            }
        }
        if this.eof {
            return if this.lifecycle.is_some() {
                Poll::Pending
            } else {
                this.record();
                Poll::Ready(Ok(()))
            };
        }
        let before = buf.filled().len();
        match Pin::new(&mut this.inner).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                this.eof = buf.filled().len() == before;
                if let Some(lifecycle) = &this.lifecycle {
                    lifecycle.read_progress_tx.send_replace(ReadProgress {
                        last_read_at: Instant::now(),
                        eof: this.eof,
                    });
                }
                if this.eof {
                    // EOF is successful only after stdin and child exit succeed.
                    cx.waker().wake_by_ref();
                    if this.lifecycle.is_some() {
                        return Poll::Pending;
                    }
                    this.record();
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(error)) => {
                this.lifecycle = None;
                this.eof = true;
                this.record();
                Poll::Ready(Err(error))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<R> Drop for TimedReader<R> {
    fn drop(&mut self) {
        // Covers a client that hung up before EOF; a no-op if EOF already recorded.
        self.record();
    }
}

/// Assemble the `GIT_CONFIG_*` environment for an upstream git command: the
/// memory-bounding options, then the optional auth header, numbered as git's
/// env-based config protocol requires (`GIT_CONFIG_COUNT` + `KEY_i`/`VALUE_i`).
/// Everything goes via env, not argv, so the auth header never shows up in `ps`.
/// `core.bigFileThreshold` streams large blobs to disk instead of holding them in
/// memory, and `core.deltaBaseCacheLimit` caps index-pack's delta-base cache - so a
/// single very large repo's clone cannot balloon RSS and OOM the proxy.
fn git_config_env(big_file_threshold: &str, auth_header: Option<&str>) -> Vec<(String, String)> {
    let mut pairs: Vec<(&str, &str)> = vec![
        ("core.bigFileThreshold", big_file_threshold),
        ("core.deltaBaseCacheLimit", "128m"),
    ];
    if let Some(h) = auth_header {
        pairs.push(("http.extraHeader", h));
    }
    let mut env = vec![("GIT_CONFIG_COUNT".to_string(), pairs.len().to_string())];
    for (i, (k, v)) in pairs.into_iter().enumerate() {
        env.push((format!("GIT_CONFIG_KEY_{i}"), k.to_string()));
        env.push((format!("GIT_CONFIG_VALUE_{i}"), v.to_string()));
    }
    env
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

    #[tokio::test]
    async fn missing_oids_drains_more_than_a_pipe_buffer() {
        let dir = tempfile::tempdir().unwrap();
        let status = std::process::Command::new("git")
            .args(["init", "--bare", "--quiet"])
            .arg(dir.path())
            .status()
            .unwrap();
        assert!(status.success());
        let cache = GitCache::new(
            GitConfig {
                git_binary: "git".into(),
                upstream_auth_header: None,
                big_file_threshold: "8m".into(),
                fetch_ttl: Duration::from_secs(60),
                max_wants: 100,
                max_upload_packs: 0,
                pack_threads: None,
                pack_window_memory: None,
            },
            Arc::new(Metrics::new()),
            None,
        );
        let repo = RepoRef {
            name: "test/repo.git".into(),
            upstream_url: String::new(),
            cache_dir: dir.path().to_path_buf(),
        };
        let oids = (0..20_000).map(|n| format!("{n:040x}")).collect();
        let missing =
            tokio::time::timeout(Duration::from_secs(10), cache.missing_oids(&repo, &oids))
                .await
                .expect("cat-file must not deadlock")
                .unwrap();
        assert_eq!(missing.len(), oids.len());
    }

    #[test]
    fn upstream_retry_classifies_transient_diagnostics() {
        for message in [
            "TLS connect error: error:0A000126:SSL routines::unexpected eof while reading",
            "gnutls_handshake() failed: The TLS connection was non-properly terminated.",
            "GnuTLS recv error (-110): The TLS connection was non-properly terminated.",
            "OpenSSL SSL_connect: SSL_ERROR_SYSCALL in connection to github.com:443",
            "Recv failure: Connection reset by peer",
            "Failed to connect to github.com port 443: Connection refused",
            "Operation timed out after 30000 milliseconds",
            "Empty reply from server",
            "Send failure: Broken pipe",
            "Temporary failure in name resolution",
            "Could not resolve host: github.com",
            "Could not resolve proxy: proxy.example",
            "error: RPC failed; curl 18 transfer closed with outstanding read data remaining",
            "error: RPC failed; curl 92 HTTP/2 stream was not closed cleanly: CANCEL",
        ] {
            assert!(
                transient_upstream_error(message.as_bytes()).is_some(),
                "{message}"
            );
        }
        for code in [408, 429, 500, 502, 503, 504] {
            let message = format!(
                "fatal: unable to access 'https://github.com/a/b/': The requested URL returned error: {code}\n"
            );
            assert_eq!(
                Some("transient HTTP status"),
                transient_upstream_error(message.as_bytes())
            );
        }
    }

    #[test]
    fn upstream_retry_rejects_permanent_and_ambiguous_diagnostics() {
        for message in [
            "The requested URL returned error: 501",
            "The requested URL returned error: 5020",
            "TLS connect error: certificate verify failed",
            "TLS connect error: unsupported protocol",
            "Recv failure: Connection reset by peer\nfatal: No space left on device",
            "Connection reset\nSSL certificate problem: self-signed certificate",
            "fatal: Authentication failed\nThe requested URL returned error: 503",
            "error: cannot lock ref refs/heads/main",
            "fatal: protocol error: bad line length character",
            "fatal: early EOF",
            "fatal: unknown error",
            "",
        ] {
            assert_eq!(
                None,
                transient_upstream_error(message.as_bytes()),
                "{message}"
            );
        }
        assert_eq!(
            [1, 2, 4],
            UPSTREAM_RETRY_DELAYS.map(|delay| delay.as_secs())
        );
    }

    #[test]
    fn git_config_env_numbers_options_and_appends_auth() {
        // No auth: just the two memory bounds, numbered from 0.
        let env = git_config_env("8m", None);
        assert!(env.contains(&("GIT_CONFIG_COUNT".into(), "2".into())));
        assert!(env.contains(&("GIT_CONFIG_KEY_0".into(), "core.bigFileThreshold".into())));
        assert!(env.contains(&("GIT_CONFIG_VALUE_0".into(), "8m".into())));
        assert!(env.contains(&("GIT_CONFIG_KEY_1".into(), "core.deltaBaseCacheLimit".into())));

        // With auth: appended as the last numbered entry, count bumps to 3.
        let env = git_config_env("16m", Some("Authorization: Basic xyz"));
        assert!(env.contains(&("GIT_CONFIG_COUNT".into(), "3".into())));
        assert!(env.contains(&("GIT_CONFIG_VALUE_0".into(), "16m".into())));
        assert!(env.contains(&("GIT_CONFIG_KEY_2".into(), "http.extraHeader".into())));
        assert!(env.contains(&(
            "GIT_CONFIG_VALUE_2".into(),
            "Authorization: Basic xyz".into()
        )));
    }

    #[test]
    fn upstream_credential_rejections_are_auth_diagnostics() {
        for stderr in [
            "fatal: Authentication failed for 'https://host/r.git/'",
            "fatal: could not read Username for 'https://host': terminal prompts disabled",
            "remote: Repository not found.",
            "The requested URL returned error: 401",
            "The requested URL returned error: 403",
            "The requested URL returned error: 404",
        ] {
            assert!(upstream_auth_diagnostic(stderr.as_bytes()), "{stderr}");
            // All are permanent, so the typed auth error, not a retry, is returned.
            assert_eq!(
                transient_upstream_error(stderr.as_bytes()),
                None,
                "{stderr}"
            );
        }
        // A proxy-side or transient failure is not a client auth failure.
        for stderr in [
            "fatal: write error: No space left on device",
            "The requested URL returned error: 503",
            "fatal: unable to access: Could not resolve host: host",
            "error: cannot lock ref refs/heads/main",
        ] {
            assert!(!upstream_auth_diagnostic(stderr.as_bytes()), "{stderr}");
        }
    }

    #[test]
    fn is_auth_failure_sees_the_typed_error_under_context() {
        let typed = anyhow::Error::new(UpstreamAuthFailed).context("git clone failed for r");
        assert!(is_auth_failure(&typed));
        assert!(!is_auth_failure(&anyhow::anyhow!("upstream unavailable")));
        // The error renders without leaking a URL or credential.
        assert_eq!(
            UpstreamAuthFailed.to_string(),
            "upstream rejected the credential"
        );
    }

    #[tokio::test]
    async fn upload_pack_large_v0_negotiation_drains_both_pipes() {
        let dir = tempfile::tempdir().unwrap();
        for args in [
            vec!["init", "--quiet"],
            vec![
                "-c",
                "user.name=Test",
                "-c",
                "user.email=test@localhost",
                "commit",
                "--quiet",
                "--allow-empty",
                "-m",
                "init",
            ],
        ] {
            assert!(
                std::process::Command::new("git")
                    .current_dir(dir.path())
                    .args(args)
                    .status()
                    .unwrap()
                    .success()
            );
        }
        let head = std::process::Command::new("git")
            .current_dir(dir.path())
            .args(["rev-parse", "HEAD"])
            .output()
            .unwrap();
        assert!(head.status.success());
        let oid = String::from_utf8(head.stdout).unwrap();
        let oid = oid.trim();
        let mut request = pkt_line(&format!("want {oid} multi_ack_detailed\n"));
        request.extend_from_slice(b"0000");
        // Known haves make Git emit ACKs before it has consumed all input.
        for _ in 0..30_000 {
            request.extend(pkt_line(&format!("have {oid}\n")));
        }
        request.extend_from_slice(b"0000");
        assert!(request.len() > 1_000_000);

        let cache = GitCache::new(
            GitConfig {
                git_binary: "git".into(),
                upstream_auth_header: None,
                big_file_threshold: "8m".into(),
                fetch_ttl: Duration::from_secs(60),
                max_wants: 100,
                max_upload_packs: 1,
                pack_threads: None,
                pack_window_memory: None,
            },
            Arc::new(Metrics::new()),
            None,
        );
        let repo = RepoRef {
            name: "test/repo.git".into(),
            upstream_url: String::new(),
            cache_dir: dir.path().to_path_buf(),
        };
        let mut expected = Vec::new();
        let ack = pkt_line(&format!("ACK {oid} common\n"));
        for _ in 0..30_000 {
            expected.extend_from_slice(&ack);
        }
        expected.extend(pkt_line(&format!("ACK {oid} ready\n")));
        expected.extend_from_slice(b"0008NAK\n");

        // The same request completes when both pipes are driven concurrently.
        let mut child = cache
            .local_cmd(Some("version=0"))
            .arg("upload-pack")
            .arg("--stateless-rpc")
            .arg(dir.path())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut stdin = child.stdin.take().unwrap();
        let (_, output) = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::join!(
                async {
                    stdin.write_all(&request).await.unwrap();
                    drop(stdin);
                },
                child.wait_with_output(),
            )
        })
        .await
        .expect("concurrent Git control must complete");
        let output = output.unwrap();
        assert!(output.status.success());
        assert_eq!(output.stdout, expected);

        let result = tokio::time::timeout(Duration::from_secs(10), async {
            let stream = cache
                .upload_pack_rpc(&repo, Some("version=0"), Bytes::from(request))
                .await
                .unwrap();
            axum::body::to_bytes(axum::body::Body::from_stream(stream), 2_000_000)
                .await
                .unwrap()
        })
        .await;

        // Wait for cleanup even on timeout before unwinding the runtime.
        let permit = tokio::time::timeout(
            Duration::from_secs(10),
            cache.upload_packs.as_ref().unwrap().clone().acquire_owned(),
        )
        .await
        .expect("upload-pack must release its permit")
        .unwrap();
        drop(permit);
        let response = result.expect("upload-pack must drain stdout while writing stdin");
        assert_eq!(response, expected);

        let mut small = pkt_line(&format!("want {oid} multi_ack_detailed\n"));
        small.extend_from_slice(b"0000");
        small.extend(pkt_line(&format!("have {oid}\n")));
        small.extend_from_slice(b"0000");
        let mut expected_small = ack;
        expected_small.extend(pkt_line(&format!("ACK {oid} ready\n")));
        expected_small.extend_from_slice(b"0008NAK\n");
        let stream = cache
            .upload_pack_rpc(&repo, Some("version=0"), Bytes::from(small))
            .await
            .unwrap();
        assert_eq!(
            axum::body::to_bytes(axum::body::Body::from_stream(stream), 4096)
                .await
                .unwrap(),
            expected_small
        );

        let mut v2 = pkt_line("command=ls-refs\n");
        v2.extend_from_slice(b"00010000");
        let stream = cache
            .upload_pack_rpc(&repo, Some("version=2"), Bytes::from(v2))
            .await
            .unwrap();
        let refs = axum::body::to_bytes(axum::body::Body::from_stream(stream), 4096)
            .await
            .unwrap();
        assert!(
            refs.windows(oid.len() + 5)
                .any(|bytes| bytes == format!("{oid} HEAD").as_bytes())
        );
        assert!(refs.ends_with(b"0000"));
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
            lifecycle: None,
            eof: false,
        };
        // Reading to EOF drives the final zero-byte read, which records once.
        let mut sink = Vec::new();
        reader.read_to_end(&mut sink).await.unwrap();
        assert!(metrics.gather().contains(
            r#"gitcacheproxy_serve_duration_seconds_count{kind="upload_pack",repo="group/foo.git"} 1"#
        ));
    }

    #[tokio::test]
    async fn timed_reader_zero_capacity_read_does_not_signal_eof() {
        let (read_progress_tx, read_progress_rx) = watch::channel(ReadProgress {
            last_read_at: Instant::now(),
            eof: false,
        });
        let (_completion_tx, completion_rx) = oneshot::channel();
        let mut reader = lifecycle_reader(&b"pack"[..], read_progress_tx, completion_rx);
        assert_eq!(reader.read(&mut []).await.unwrap(), 0);
        assert!(!reader.eof);
        assert!(!read_progress_rx.borrow().eof);
        assert!(!read_progress_rx.has_changed().unwrap());
        let mut bytes = [0; 4];
        reader.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"pack");
        assert!(!read_progress_rx.borrow().eof);
    }

    #[tokio::test]
    async fn upload_pack_reader_reports_lifecycle_task_loss_before_and_after_eof() {
        for stdout_eof in [false, true] {
            let metrics = Arc::new(Metrics::new());
            let (read_progress_tx, read_progress_rx) = watch::channel(ReadProgress {
                last_read_at: Instant::now(),
                eof: false,
            });
            let (completion_tx, completion_rx) = oneshot::channel();
            let (mut stdout_writer, stdout_reader) = tokio::io::duplex(16);
            if stdout_eof {
                stdout_writer.shutdown().await.unwrap();
            }
            let mut reader = lifecycle_reader(stdout_reader, read_progress_tx, completion_rx);
            reader.recorder = Some((metrics.clone(), Instant::now()));
            let mut bytes = Vec::new();
            let read = reader.read_to_end(&mut bytes);
            tokio::pin!(read);
            for _ in 0..2 {
                assert!(matches!(
                    std::future::poll_fn(|cx| Poll::Ready(read.as_mut().poll(cx))).await,
                    Poll::Pending
                ));
            }
            assert_eq!(read_progress_rx.borrow().eof, stdout_eof);
            drop(completion_tx);
            let error = read.await.unwrap_err();
            assert_eq!(error.kind(), std::io::ErrorKind::Other);
            assert_eq!(error.to_string(), "upload-pack lifecycle task failed");
            assert!(bytes.is_empty());
            drop(reader);
            assert!(read_progress_rx.has_changed().is_err());
            assert!(metrics.gather().contains(
                r#"gitcacheproxy_serve_duration_seconds_count{kind="upload_pack",repo="repo.git"} 1"#
            ));
        }
    }

    #[cfg(unix)]
    #[tokio::test(start_paused = true)]
    async fn upload_pack_deadlines_fail_response_and_reap_child() {
        for request_blocked in [false, true] {
            let child = sleeping_child(true);
            let pid = rustix::process::Pid::from_raw(child.id().unwrap() as i32).unwrap();
            let (read_progress_tx, read_progress_rx) = watch::channel(ReadProgress {
                last_read_at: Instant::now(),
                eof: false,
            });
            let (mut stdin, _stdin_reader) = tokio::io::duplex(1);
            let writer = async move {
                if request_blocked {
                    write_upload_pack_request(&mut stdin, b"0000").await
                } else {
                    Ok(())
                }
            };
            let (completion_tx, completion_rx) = oneshot::channel();
            let owner = tokio::spawn(async move {
                let result = run_upload_pack(
                    child,
                    writer,
                    read_progress_rx,
                    UPLOAD_PACK_RESPONSE_IDLE_TIMEOUT,
                )
                .await;
                let _ = completion_tx.send(result);
            });
            // EOF must wait for child exit; a request timeout must still reach the body.
            let mut reader = lifecycle_reader(&b""[..], read_progress_tx, completion_rx);
            let error = reader.read_to_end(&mut Vec::new()).await.unwrap_err();
            assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
            assert_eq!(
                error.to_string(),
                if request_blocked {
                    "upload-pack request write timed out"
                } else {
                    "upload-pack response idle timed out"
                }
            );
            owner.await.unwrap();
            assert_eq!(
                rustix::process::waitpid(Some(pid), rustix::process::WaitOptions::NOHANG)
                    .unwrap_err(),
                rustix::io::Errno::CHILD
            );
        }
    }

    #[cfg(all(
        unix,
        not(any(
            target_os = "cygwin",
            target_os = "horizon",
            target_os = "openbsd",
            target_os = "redox",
        ))
    ))]
    #[tokio::test]
    async fn upload_pack_exit_observer_refuses_a_reaped_child() {
        let mut child = Command::new("true").process_group(0).spawn().unwrap();
        child.wait().await.unwrap();
        let error = wait_upload_pack(&mut child).await.unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::Other);
        assert_eq!(error.to_string(), "upload-pack already reaped");
    }

    #[tokio::test(start_paused = true)]
    async fn upload_pack_request_write_has_a_deadline() {
        let (mut writer, _reader) = tokio::io::duplex(1);
        let started = Instant::now();
        let error = write_upload_pack_request(&mut writer, b"0000")
            .await
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
        assert_eq!(started.elapsed(), UPLOAD_PACK_REQUEST_WRITE_TIMEOUT);
    }

    #[cfg(unix)]
    #[tokio::test(start_paused = true)]
    async fn upload_pack_lifecycle_bounds_an_unconsumed_response_and_drops_writer() {
        let child = sleeping_child(true);
        let (read_progress_tx, read_progress_rx) = watch::channel(ReadProgress {
            last_read_at: Instant::now(),
            eof: false,
        });
        let (writer_alive, writer_dropped) = oneshot::channel::<()>();
        let writer = async move {
            let _owned = writer_alive;
            std::future::pending::<std::io::Result<()>>().await
        };
        let started = Instant::now();
        let error = run_upload_pack(
            child,
            writer,
            read_progress_rx,
            UPLOAD_PACK_RESPONSE_IDLE_TIMEOUT,
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
        assert_eq!(started.elapsed(), UPLOAD_PACK_RESPONSE_IDLE_TIMEOUT);
        assert!(writer_dropped.await.is_err());
        drop(read_progress_tx);
    }

    #[cfg(unix)]
    #[tokio::test(start_paused = true)]
    async fn upload_pack_lifecycle_progress_extends_idle_deadline() {
        let child = sleeping_child(true);
        let (read_progress_tx, read_progress_rx) = watch::channel(ReadProgress {
            last_read_at: Instant::now(),
            eof: false,
        });
        let owner = run_upload_pack(
            child,
            std::future::ready(Ok(())),
            read_progress_rx,
            UPLOAD_PACK_RESPONSE_IDLE_TIMEOUT,
        );
        tokio::pin!(owner);
        for _ in 0..3 {
            assert!(matches!(
                std::future::poll_fn(|cx| Poll::Ready(owner.as_mut().poll(cx))).await,
                Poll::Pending
            ));
            tokio::time::advance(UPLOAD_PACK_RESPONSE_IDLE_TIMEOUT / 2).await;
            read_progress_tx.send_replace(ReadProgress {
                last_read_at: Instant::now(),
                eof: false,
            });
        }
        assert!(matches!(
            std::future::poll_fn(|cx| Poll::Ready(owner.as_mut().poll(cx))).await,
            Poll::Pending
        ));
        drop(read_progress_tx);
        assert_eq!(
            owner.await.unwrap_err().kind(),
            std::io::ErrorKind::ConnectionAborted
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn upload_pack_lifecycle_writer_error_reaps_child_and_reaches_reader() {
        let child = sleeping_child(true);
        let (read_progress_tx, read_progress_rx) = watch::channel(ReadProgress {
            last_read_at: Instant::now(),
            eof: false,
        });
        let lifecycle_task = tokio::spawn(run_upload_pack(
            child,
            async {
                Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "write failed",
                ))
            },
            read_progress_rx,
            UPLOAD_PACK_RESPONSE_IDLE_TIMEOUT,
        ));
        let (completion_tx, completion_rx) = oneshot::channel();
        completion_tx.send(lifecycle_task.await.unwrap()).unwrap();
        let mut reader = lifecycle_reader(&b""[..], read_progress_tx, completion_rx);
        assert_eq!(
            reader
                .read_to_end(&mut Vec::new())
                .await
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::BrokenPipe
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn upload_pack_lifecycle_reader_error_cancels_even_if_reader_is_retained() {
        let child = sleeping_child(true);
        let (read_progress_tx, read_progress_rx) = watch::channel(ReadProgress {
            last_read_at: Instant::now(),
            eof: false,
        });
        let owner = tokio::spawn(run_upload_pack(
            child,
            std::future::pending(),
            read_progress_rx,
            UPLOAD_PACK_RESPONSE_IDLE_TIMEOUT,
        ));
        let (_completion_tx, completion_rx) = oneshot::channel();
        let mut reader = lifecycle_reader(FailingReader, read_progress_tx, completion_rx);
        assert!(reader.read_to_end(&mut Vec::new()).await.is_err());
        assert_eq!(
            owner.await.unwrap().unwrap_err().kind(),
            std::io::ErrorKind::ConnectionAborted
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn upload_pack_lifecycle_cancels_after_eof_while_child_is_running() {
        // No process group: also exercises the leader-kill fallback.
        let child = sleeping_child(false);
        let (read_progress_tx, read_progress_rx) = watch::channel(ReadProgress {
            last_read_at: Instant::now(),
            eof: true,
        });
        let owner = tokio::spawn(run_upload_pack(
            child,
            std::future::ready(Ok(())),
            read_progress_rx,
            UPLOAD_PACK_RESPONSE_IDLE_TIMEOUT,
        ));
        drop(read_progress_tx);
        assert_eq!(
            owner.await.unwrap().unwrap_err().kind(),
            std::io::ErrorKind::ConnectionAborted
        );
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn upload_pack_failed_leader_is_observed_before_descendant_stdout_eof() {
        use rustix::process::{Pid, WaitId, WaitIdOptions, waitid};

        let child = Command::new("sh")
            .args(["-c", "sleep 300 & exit 7"])
            .process_group(0)
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let pid = Pid::from_raw(child.id().unwrap() as i32).unwrap();
        assert!(
            waitid(
                WaitId::Pid(pid),
                WaitIdOptions::EXITED | WaitIdOptions::NOWAIT
            )
            .unwrap()
            .is_some()
        );
        let (read_progress_tx, read_progress_rx) = watch::channel(ReadProgress {
            last_read_at: Instant::now(),
            eof: false,
        });
        let owner = run_upload_pack(
            child,
            std::future::ready(Ok(())),
            read_progress_rx,
            UPLOAD_PACK_RESPONSE_IDLE_TIMEOUT,
        );
        tokio::pin!(owner);
        let result = tokio::time::timeout(Duration::from_secs(1), &mut owner).await;
        // Clean up the failing implementation before asserting the regression.
        if result.is_err() {
            drop(read_progress_tx);
            let _ = owner.await;
        }
        let error = result
            .expect("leader failure must not wait for descendant stdout EOF")
            .unwrap_err();
        assert!(error.to_string().contains("exit status: 7"), "{error}");
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn upload_pack_leader_exit_signal_wakes_owner_before_stdout_eof() {
        let mut child = Command::new("sh")
            .args(["-c", "read gate; sleep 300 & exit 7"])
            .process_group(0)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let mut stdin = child.stdin.take().unwrap();
        let (read_progress_tx, read_progress_rx) = watch::channel(ReadProgress {
            last_read_at: Instant::now(),
            eof: false,
        });
        let owner = run_upload_pack(
            child,
            std::future::ready(Ok(())),
            read_progress_rx,
            UPLOAD_PACK_RESPONSE_IDLE_TIMEOUT,
        );
        tokio::pin!(owner);
        assert!(matches!(
            std::future::poll_fn(|cx| Poll::Ready(owner.as_mut().poll(cx))).await,
            Poll::Pending
        ));
        stdin.write_all(b"go\n").await.unwrap();
        drop(stdin);
        let result = tokio::time::timeout(Duration::from_secs(1), &mut owner).await;
        if result.is_err() {
            drop(read_progress_tx);
            let _ = owner.await;
        }
        let error = result
            .expect("exit signal must wake the owner without response reads")
            .unwrap_err();
        assert!(error.to_string().contains("exit status: 7"), "{error}");
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn upload_pack_leader_exit_preserves_successful_stdout_and_fails_error_stream() {
        for status in [0, 7] {
            let mut child = Command::new("sh")
                .args([
                    "-c",
                    &format!("printf 'buffered output'; sleep 300 & exit {status}"),
                ])
                .process_group(0)
                .stdout(Stdio::piped())
                .spawn()
                .unwrap();
            let stdout = child.stdout.take().unwrap();
            let (read_progress_tx, read_progress_rx) = watch::channel(ReadProgress {
                last_read_at: Instant::now(),
                eof: false,
            });
            let (completion_tx, completion_rx) = oneshot::channel();
            let owner = tokio::spawn(async move {
                let result = run_upload_pack(
                    child,
                    std::future::ready(Ok(())),
                    read_progress_rx,
                    UPLOAD_PACK_RESPONSE_IDLE_TIMEOUT,
                )
                .await;
                let _ = completion_tx.send(result);
            });
            let mut reader = lifecycle_reader(stdout, read_progress_tx, completion_rx);
            let mut body = Vec::new();
            let result =
                tokio::time::timeout(Duration::from_secs(1), reader.read_to_end(&mut body)).await;
            drop(reader);
            owner.await.unwrap();
            let result = result
                .expect("leader exit must close descendant stdout without losing buffered bytes");
            if status == 0 {
                result.unwrap();
                assert_eq!(body, b"buffered output");
            } else {
                assert!(result.unwrap_err().to_string().contains("exit status: 7"));
            }
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn upload_pack_lifecycle_reports_nonzero_child_exit_after_stdout_eof() {
        let mut cmd = Command::new("sh");
        cmd.args(["-c", "exit 7"]).process_group(0);
        let child = cmd.spawn().unwrap();
        let (read_progress_tx, read_progress_rx) = watch::channel(ReadProgress {
            last_read_at: Instant::now(),
            eof: true,
        });
        let error = run_upload_pack(
            child,
            std::future::ready(Ok(())),
            read_progress_rx,
            UPLOAD_PACK_RESPONSE_IDLE_TIMEOUT,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("exit status: 7"));
        drop(read_progress_tx);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn dropped_upload_pack_response_releases_permit_after_reaping() {
        let dir = tempfile::tempdir().unwrap();
        let script = dir.path().join("git");
        use std::os::unix::fs::PermissionsExt;
        std::fs::write(&script, "#!/bin/sh\nexec sleep 300\n").unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        let cache = upload_pack_test_cache(script.to_str().unwrap());
        let repo = RepoRef {
            name: "repo.git".into(),
            upstream_url: String::new(),
            cache_dir: dir.path().to_owned(),
        };
        let stream = cache
            .upload_pack_rpc(&repo, None, Bytes::from(vec![0; 1024 * 1024]))
            .await
            .unwrap();
        assert_eq!(cache.upload_packs.as_ref().unwrap().available_permits(), 0);
        drop(stream);
        let permit = tokio::time::timeout(
            Duration::from_secs(10),
            cache.upload_packs.as_ref().unwrap().clone().acquire_owned(),
        )
        .await
        .expect("cancelled upload-pack must release permit")
        .unwrap();
        drop(permit);
        assert_eq!(cache.upload_packs.as_ref().unwrap().available_permits(), 1);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn upload_pack_stream_surfaces_real_stdin_broken_pipe() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let script = dir.path().join("git");
        std::fs::write(&script, "#!/bin/sh\nexec 0<&-\nexec sleep 300\n").unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        let cache = upload_pack_test_cache(script.to_str().unwrap());
        let repo = RepoRef {
            name: "repo.git".into(),
            upstream_url: String::new(),
            cache_dir: dir.path().to_owned(),
        };
        let stream = cache
            .upload_pack_rpc(&repo, None, Bytes::from(vec![0; 1024 * 1024]))
            .await
            .unwrap();
        let error = tokio::time::timeout(
            Duration::from_secs(10),
            axum::body::to_bytes(axum::body::Body::from_stream(stream), usize::MAX),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert!(error.to_string().contains("Broken pipe"), "{error}");
        assert_eq!(cache.upload_packs.as_ref().unwrap().available_permits(), 1);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn upload_pack_eof_does_not_hide_a_later_writer_error() {
        let child = Command::new("true").process_group(0).spawn().unwrap();
        let (_read_progress_tx, read_progress_rx) = watch::channel(ReadProgress {
            last_read_at: Instant::now(),
            eof: true,
        });
        let (writer_result_tx, writer_result_rx) = oneshot::channel();
        let owner = run_upload_pack(
            child,
            async { writer_result_rx.await.unwrap() },
            read_progress_rx,
            UPLOAD_PACK_RESPONSE_IDLE_TIMEOUT,
        );
        tokio::pin!(owner);
        assert!(matches!(
            std::future::poll_fn(|cx| Poll::Ready(owner.as_mut().poll(cx))).await,
            Poll::Pending
        ));
        writer_result_tx
            .send(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "late writer error",
            )))
            .unwrap();
        assert_eq!(
            owner.await.unwrap_err().kind(),
            std::io::ErrorKind::BrokenPipe
        );
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn upload_pack_normal_eof_cleans_descendants_before_reaping_leader() {
        let mut child = Command::new("sh")
            .args([
                "-c",
                "sleep 300 </dev/null >/dev/null 2>&1 & echo $!; exit 0",
            ])
            .process_group(0)
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let mut pid = String::new();
        child
            .stdout
            .take()
            .unwrap()
            .read_to_string(&mut pid)
            .await
            .unwrap();
        let (_read_progress_tx, read_progress_rx) = watch::channel(ReadProgress {
            last_read_at: Instant::now(),
            eof: true,
        });
        run_upload_pack(
            child,
            std::future::ready(Ok(())),
            read_progress_rx,
            UPLOAD_PACK_RESPONSE_IDLE_TIMEOUT,
        )
        .await
        .unwrap();
        // An orphan may be a zombie until PID 1 reaps it; it must stop running.
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let status = tokio::fs::read_to_string(format!("/proc/{}/stat", pid.trim())).await;
                match status {
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
                    Ok(status) if status.split(") ").nth(1).unwrap().starts_with('Z') => break,
                    _ => tokio::time::sleep(Duration::from_millis(10)).await,
                }
            }
        })
        .await
        .expect("descendant must stop after leader exits");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn kill_group_refuses_a_reaped_child() {
        let mut child = Command::new("true").spawn().unwrap();
        child.wait().await.unwrap();
        assert!(kill_group(&child).is_err());
    }

    /// Collect an iterator of oids into a `HashSet` for order-insensitive assertions
    /// against `parse_wants`.
    fn want_set<I: IntoIterator<Item = String>>(oids: I) -> HashSet<String> {
        oids.into_iter().collect()
    }

    #[cfg(unix)]
    fn sleeping_child(group: bool) -> Child {
        let mut cmd = Command::new("sleep");
        cmd.arg("300");
        if group {
            cmd.process_group(0);
        }
        cmd.spawn().unwrap()
    }

    fn lifecycle_reader<R>(
        inner: R,
        read_progress_tx: watch::Sender<ReadProgress>,
        completion_rx: oneshot::Receiver<std::io::Result<()>>,
    ) -> TimedReader<R> {
        TimedReader {
            inner,
            repo: "repo.git".into(),
            recorder: None,
            lifecycle: Some(ResponseLifecycle {
                read_progress_tx,
                completion_rx,
            }),
            eof: false,
        }
    }

    struct FailingReader;

    impl AsyncRead for FailingReader {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Ready(Err(std::io::Error::other("read failed")))
        }
    }

    fn upload_pack_test_cache(binary: &str) -> GitCache {
        GitCache::new(
            GitConfig {
                git_binary: binary.into(),
                upstream_auth_header: None,
                big_file_threshold: "8m".into(),
                fetch_ttl: Duration::from_secs(60),
                max_wants: 100,
                max_upload_packs: 1,
                pack_threads: None,
                pack_window_memory: None,
            },
            Arc::new(Metrics::new()),
            None,
        )
    }
}
