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
use std::ffi::{OsStr, OsString};
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::process::{ExitStatus, Stdio};
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
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::metrics::{Metrics, RefreshResult, ServeKind, Status, UpstreamOp};
use crate::repo::RepoRef;

mod error;

use error::{
    FailureKind, UpstreamFailed, UpstreamStopped, failure_kind, redact_stderr,
    transient_upstream_error, upstream_auth_diagnostic,
};
pub use error::{UpstreamAuthFailed, is_auth_failure, upstream_failure_logged};

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
    /// Background refresh of recently used mirrors (`None` = disabled).
    pub refresh: Option<RefreshConfig>,
}

/// Settings for [`GitCache::start_refresh`]. Callers validate them with
/// `Config::validate_refresh` first.
#[derive(Clone, Copy)]
pub struct RefreshConfig {
    /// Pause between round starts.
    pub interval: Duration,
    /// Only mirrors used within this window are refreshed.
    pub active_window: Duration,
    /// Maximum background fetches in flight.
    pub concurrency: usize,
}

/// Per-repo serialization point. `fetch_lock` is a `Mutex`, not an `RwLock`, on
/// purpose: it does double duty as (1) the mutual-exclusion point that collapses a
/// burst of concurrent clones for one repo into a single upstream pull, and (2) the
/// guard for the start `Instant` of the last upstream pull. Both uses need *exclusive* access
/// while a fetch runs - there is no read-only fast path to share - so an `RwLock`
/// would only add overhead and never grant a concurrent reader.
struct RepoSlot {
    fetch_lock: Mutex<Option<Instant>>,
    refresh: Mutex<RefreshState>,
    /// Bumped by `evict` before it waits for `fetch_lock`, so a background
    /// refresh holding the lock can yield instead of fetching data that is
    /// about to be deleted.
    evict_requests: watch::Sender<u64>,
}

#[derive(Default)]
struct RefreshState {
    used: Option<(RepoRef, Instant)>,
    failures: u32,
    retry_after: Option<Instant>,
    auth_blocked: bool,
}

impl RefreshState {
    fn succeeded(&mut self, server_credential: bool) {
        self.failures = 0;
        self.retry_after = None;
        if server_credential {
            self.auth_blocked = false;
        }
    }
}

#[derive(Clone)]
pub struct GitCache {
    shutdown: CancellationToken,
    owners: TaskTracker,
    upstream_timeout: Duration,
    cfg: GitConfig,
    metrics: Arc<Metrics>,
    /// Map of repo name -> its serialization slot. A plain `Mutex` (not `RwLock`)
    /// because every access is a get-or-insert (`entry`), which needs a write lock
    /// anyway, and the critical section is a single O(1) map operation - far too
    /// short for reader/writer separation to pay off.
    slots: Arc<Mutex<HashMap<String, Arc<RepoSlot>>>>,
    /// Cache-size index for LRU eviction, or `None` when no cap is configured.
    /// Requests promote entries; background refresh only schedules remeasurement.
    index: Option<Arc<crate::evict::CacheIndex>>,
    /// `None` when `max_upload_packs` is `0` (unlimited).
    upload_packs: Option<Arc<Semaphore>>,
}

impl GitCache {
    pub fn new(
        mut cfg: GitConfig,
        metrics: Arc<Metrics>,
        index: Option<Arc<crate::evict::CacheIndex>>,
    ) -> Self {
        // Settings that cannot run a round count as disabled, so request use is
        // never recorded for a refresh task that will not exist.
        if let Some(refresh) = cfg.refresh
            && (refresh.interval.is_zero() || refresh.concurrency == 0)
        {
            tracing::warn!("refresh interval and concurrency must be at least 1; refresh disabled");
            cfg.refresh = None;
        }
        let upload_packs =
            (cfg.max_upload_packs > 0).then(|| Arc::new(Semaphore::new(cfg.max_upload_packs)));
        Self {
            cfg,
            metrics,
            slots: Arc::new(Mutex::new(HashMap::new())),
            shutdown: CancellationToken::new(),
            owners: TaskTracker::new(),
            upstream_timeout: Duration::from_secs(3600),
            index,
            upload_packs,
        }
    }

    /// Set the total budget for each upstream operation, including retries.
    pub fn with_upstream_timeout(mut self, timeout: Duration) -> Result<Self> {
        validate_upstream_timeout(timeout)?;
        self.upstream_timeout = timeout;
        Ok(self)
    }

    /// Reject new upstream work and cancel active operations before HTTP drain.
    /// Local upload-pack processes continue to drain.
    pub fn shutdown(&self) {
        self.shutdown.cancel();
        self.owners.close();
    }

    /// Wait for request upstream owners to reap children and release repository locks.
    /// Also await the handle returned by start_refresh to finish refresh cleanup.
    pub async fn wait_for_shutdown(&self) {
        self.owners.wait().await;
    }

    async fn slot(&self, name: &str) -> Arc<RepoSlot> {
        self.slots
            .lock()
            .await
            .entry(name.to_string())
            .or_insert_with(|| {
                Arc::new(RepoSlot {
                    fetch_lock: Mutex::new(None),
                    refresh: Mutex::new(RefreshState::default()),
                    evict_requests: watch::channel(0).0,
                })
            })
            .clone()
    }

    async fn record_fetch_success(&self, repo: &RepoRef, cred: Option<&str>) {
        if self.cfg.refresh.is_none() {
            return;
        }
        self.slot(&repo.name)
            .await
            .refresh
            .lock()
            .await
            .succeeded(cred.is_none() || cred == self.cfg.upstream_auth_header.as_deref());
    }

    async fn record_client_use(&self, repo: &RepoRef, finished: Instant) {
        let slot = self.slot(&repo.name).await;
        let mut state = slot.refresh.lock().await;
        if !self.shutdown.is_cancelled()
            && state
                .used
                .as_ref()
                .is_none_or(|(_, previous)| *previous <= finished)
        {
            state.used = Some((repo.clone(), finished));
        }
    }

    /// Start optional refresh work from `GitConfig::refresh`. Disabled refresh
    /// creates no task. Callers should validate the settings with Config first.
    pub fn start_refresh(&self) -> Option<tokio::task::JoinHandle<()>> {
        let RefreshConfig {
            interval,
            concurrency,
            ..
        } = self.cfg.refresh?;
        let cache = self.clone();
        Some(tokio::spawn(async move {
            refresh_loop(interval, cache.shutdown.child_token(), |deadline| {
                let cache = &cache;
                async move {
                    let candidates = cache.refresh_candidates().await;
                    let total = candidates.len();
                    let started = refresh_round(
                        candidates.into_iter().map(|(repo, _)| repo),
                        concurrency,
                        deadline,
                        cache.shutdown.child_token(),
                        |repo| {
                            let worker = cache.clone();
                            async move {
                                worker.refresh_repo(&repo, deadline).await;
                            }
                        },
                    )
                    .await;
                    // Candidates the round never reached would otherwise be invisible.
                    let unstarted = total - started;
                    if unstarted > 0 && !cache.shutdown.is_cancelled() {
                        cache
                            .metrics
                            .record_refresh(RefreshResult::Skipped, unstarted as u64);
                    }
                }
            })
            .await;
        }))
    }

    async fn refresh_candidates(&self) -> Vec<(RepoRef, Instant)> {
        let Some(RefreshConfig { active_window, .. }) = self.cfg.refresh else {
            return Vec::new();
        };
        let mut candidates = Vec::new();
        for slot in self.slots.lock().await.values() {
            let state = slot.refresh.lock().await;
            if let Some((repo, used)) = &state.used
                && used.elapsed() <= active_window
            {
                candidates.push((repo.clone(), *used));
            }
        }
        candidates.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.name.cmp(&b.0.name)));
        candidates
    }

    /// Refresh an existing mirror without recording client use or LRU access.
    pub(crate) async fn refresh_repo(&self, repo: &RepoRef, deadline: Instant) {
        let Some(RefreshConfig {
            interval,
            active_window,
            ..
        }) = self.cfg.refresh
        else {
            return;
        };
        let slot = self.slot(&repo.name).await;
        // Subscribe before taking the lock so an eviction queued behind us is seen.
        let mut evict_requested = slot.evict_requests.subscribe();
        let mut last = tokio::select! {
            biased;
            _ = self.shutdown.cancelled() => return,
            _ = tokio::time::sleep_until(deadline) => {
                self.metrics.record_refresh(RefreshResult::Skipped, 1);
                return;
            }
            guard = slot.fetch_lock.lock() => guard,
        };
        let skip = {
            let state = slot.refresh.lock().await;
            Instant::now() >= deadline
                || state.auth_blocked
                || state.retry_after.is_some_and(|t| t > Instant::now())
                || state
                    .used
                    .as_ref()
                    .is_none_or(|(_, used)| used.elapsed() > active_window)
                || !repo.cache_dir.join("HEAD").exists()
                || last.is_some_and(|t| {
                    !self.cfg.fetch_ttl.is_zero() && t.elapsed() < self.cfg.fetch_ttl
                })
        };
        if self.shutdown.is_cancelled() {
            return;
        }
        if skip {
            self.metrics.record_refresh(RefreshResult::Skipped, 1);
            return;
        }
        let started = Instant::now();
        let mut worker = self.clone();
        worker.shutdown = self.shutdown.child_token();
        let cancel = worker.shutdown.clone();
        let preempt = CancellationToken::new();
        let fetch = worker.fetch_inner(repo, None, Some(&preempt));
        tokio::pin!(fetch);
        // Cancel rather than drop the fetch so its child is still killed and reaped.
        let result = tokio::select! {
            result = &mut fetch => result,
            _ = evict_requested.changed() => {
                preempt.cancel();
                cancel.cancel();
                fetch.await
            }
        };
        if preempt.is_cancelled() {
            self.metrics.record_refresh(RefreshResult::Preempted, 1);
            return;
        }
        match result {
            Ok(()) => {
                self.metrics.record_refresh(RefreshResult::Ok, 1);
                *last = Some(started);
                if let Some(index) = &self.index {
                    index.mark_dirty(&repo.name);
                }
            }
            Err(error) => {
                if self.shutdown.is_cancelled() {
                    return;
                }
                if !upstream_failure_logged(&error) {
                    let (repo, error) = (repo.name.as_str(), format!("{error:#}"));
                    tracing::warn!(repo, error, "background refresh failed");
                }
                let auth_rejected = is_auth_failure(&error);
                self.metrics.record_refresh(
                    if auth_rejected {
                        RefreshResult::AuthRejected
                    } else {
                        RefreshResult::Error
                    },
                    1,
                );
                let mut state = slot.refresh.lock().await;
                state.auth_blocked |= auth_rejected;
                state.failures = state.failures.saturating_add(1);
                let multiplier = 1u32 << state.failures.saturating_sub(1).min(5);
                state.retry_after = if state.failures == 1 {
                    None
                } else {
                    // Startup validation reserves space for the maximum backoff.
                    Some(
                        Instant::now()
                            .checked_add(interval.saturating_mul(multiplier))
                            .expect("validated refresh interval fits maximum backoff"),
                    )
                };
            }
        }
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
        slot.evict_requests.send_modify(|n| *n = n.wrapping_add(1));
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
    /// Refresh use is recorded after successful preparation, including TTL hits,
    /// independently of later HTTP response or pack-stream success.
    pub async fn ensure_fresh(
        &self,
        repo: &RepoRef,
        want_fetch: bool,
        body: &[u8],
        cred: Option<&str>,
    ) -> Result<CacheOutcome> {
        let track_use = self.cfg.refresh.is_some();
        let mut owner = self.clone();
        owner.shutdown = self.shutdown.child_token();
        let _cancel_on_drop = owner.shutdown.clone().drop_guard();
        let owner_repo = repo.clone();
        let wants = parse_wants(body);
        let cred = cred.map(str::to_owned);
        let outcome = self
            .owners
            .spawn(async move {
                owner
                    .ensure_fresh_owned(&owner_repo, want_fetch, &wants, cred.as_deref())
                    .await
            })
            .await
            .context("join upstream owner")??;
        if track_use {
            self.record_client_use(repo, Instant::now()).await;
        }
        Ok(outcome)
    }

    async fn ensure_fresh_owned(
        &self,
        repo: &RepoRef,
        want_fetch: bool,
        wants: &HashSet<String>,
        cred: Option<&str>,
    ) -> Result<CacheOutcome> {
        let slot = self.slot(&repo.name).await;
        // Mark the repo used on every request - a served cache hit counts as much as
        // a fetch - so the eviction index keeps a truthful last-access ordering.
        if let Some(idx) = &self.index {
            idx.touch(&repo.name);
        }
        let mut last = tokio::select! {
            biased;
            _ = self.shutdown.cancelled() => return Err(UpstreamStopped::Cancelled.into()),
            guard = slot.fetch_lock.lock() => guard,
        };

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
            self.mark_changed(repo);
            *last = Some(started);
            CacheOutcome::Fetched
        } else {
            CacheOutcome::Cached
        };

        // Best-effort: on failure, fall through and let upload-pack surface the
        // normal "not our ref" error for any want it still cannot satisfy.
        if let Err(e) = self.ensure_wanted_oids(repo, wants, cred).await {
            if is_auth_failure(&e) || self.shutdown.is_cancelled() || e.is::<UpstreamStopped>() {
                return Err(e);
            }
            if !upstream_failure_logged(&e) {
                let (repo, error) = (repo.name.as_str(), format!("{e:#}"));
                tracing::warn!(repo, error, "ensure wanted oids failed");
            }
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
        // tokio's `output()` inherits stdin, unlike std's.
        let out = cmd
            .stdin(Stdio::null())
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
        let writer = async move {
            write_upload_pack_request(&mut stdin, &body).await?;
            drop(stdin); // EOF so upload-pack starts producing the pack
            Ok(())
        };
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
        if let Err(error) = run_upstream(
            &mut cmd,
            "clone",
            &repo.name,
            Some(&tmp),
            self.upstream_timeout,
            &self.shutdown,
        )
        .await
        {
            self.upstream_failed(UpstreamOp::Clone, "request", repo, cred, started, &error);
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
        self.record_fetch_success(repo, cred).await;
        Ok(())
    }

    async fn fetch(&self, repo: &RepoRef, cred: Option<&str>) -> Result<()> {
        self.fetch_inner(repo, cred, None).await
    }

    /// `preempt` is cancelled when an eviction, not a failure, stops the fetch; that
    /// cancellation must not inflate the upstream error count.
    async fn fetch_inner(
        &self,
        repo: &RepoRef,
        cred: Option<&str>,
        preempt: Option<&CancellationToken>,
    ) -> Result<()> {
        tracing::debug!(repo = %repo.name, "fetching updates from upstream");
        // A reused mirror can still record an older upstream in origin.
        let started = Instant::now();
        let mut cmd = self.fetch_cmd(cred);
        cmd.current_dir(&repo.cache_dir)
            .arg("fetch")
            .arg("--prune")
            .arg("--quiet")
            .arg(&repo.upstream_url)
            // Mirror refspec, minus the local-only pin namespace: `--prune` would
            // otherwise drop those refs as "not on upstream" (see `prune_wants`).
            .arg("+refs/*:refs/*")
            .arg(format!("^{PROXY_WANTS_NS}/*"));
        if let Err(error) = run_upstream(
            &mut cmd,
            "fetch",
            &repo.name,
            None,
            self.upstream_timeout,
            &self.shutdown,
        )
        .await
        {
            // Only background refresh passes `preempt`.
            let trigger = if preempt.is_some() {
                "refresh"
            } else {
                "request"
            };
            if !preempt.is_some_and(CancellationToken::is_cancelled) {
                self.upstream_failed(UpstreamOp::Fetch, trigger, repo, cred, started, &error);
            }
            return Err(error);
        }
        self.metrics
            .record_upstream(UpstreamOp::Fetch, Status::Ok, &repo.name);
        self.metrics.observe_upstream(
            UpstreamOp::Fetch,
            &repo.name,
            started.elapsed().as_secs_f64(),
        );
        self.record_fetch_success(repo, cred).await;
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
        wants: &HashSet<String>,
        cred: Option<&str>,
    ) -> Result<()> {
        if wants.is_empty() {
            return Ok(());
        }
        let mut missing = self.missing_oids(repo, wants).await?;
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
            .arg(&repo.upstream_url);
        for oid in &missing {
            // Pin under a reserved ref (force: the ref name is the SHA and may
            // already exist), making the object a `want` tip safe from `git gc`.
            cmd.arg(format!("+{oid}:{PROXY_WANTS_NS}/{oid}"));
        }
        if let Err(error) = run_upstream(
            &mut cmd,
            "fetch (want-by-sha)",
            &repo.name,
            None,
            self.upstream_timeout,
            &self.shutdown,
        )
        .await
        {
            self.upstream_failed(
                UpstreamOp::WantFetch,
                "request",
                repo,
                cred,
                started,
                &error,
            );
            return Err(error);
        }
        self.metrics
            .record_upstream(UpstreamOp::WantFetch, Status::Ok, &repo.name);
        self.metrics.observe_upstream(
            UpstreamOp::WantFetch,
            &repo.name,
            started.elapsed().as_secs_f64(),
        );
        self.mark_changed(repo);
        self.record_fetch_success(repo, cred).await;
        // Best-effort: a failed prune is not worth failing the request over.
        if let Err(e) = self.prune_wants(repo).await {
            tracing::warn!(repo = %repo.name, error = %e, "pruning want-by-sha pins failed");
        }
        Ok(())
    }

    /// Count and log an upstream operation's final failure. This is the one place
    /// that sees the operation, the repo, and the credential needed to redact
    /// stderr, so callers skip errors for which `upstream_failure_logged` holds.
    fn upstream_failed(
        &self,
        op: UpstreamOp,
        trigger: &'static str,
        repo: &RepoRef,
        cred: Option<&str>,
        started: Instant,
        error: &anyhow::Error,
    ) {
        let kind = failure_kind(error);
        // A client that went away, or shutdown, is not an upstream failure.
        if kind != Some(FailureKind::Cancelled) {
            // `-` not the repo name: a failure must not mint a per-repo series for
            // an arbitrary client-supplied path (see `metrics`). The log names it.
            self.metrics.record_upstream(op, Status::Error, "-");
        }
        let Some(kind) = kind else {
            return;
        };
        let elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        let (op, repo) = (op.as_str(), repo.name.as_str());
        if kind == FailureKind::Cancelled {
            tracing::debug!(op, repo, elapsed_ms, "upstream operation cancelled");
            return;
        }
        let secret = cred.or(self.cfg.upstream_auth_header.as_deref());
        let stderr = error
            .chain()
            .find_map(|cause| cause.downcast_ref::<UpstreamFailed>())
            .map(|failed| redact_stderr(&failed.stderr, secret))
            .unwrap_or_default();
        let (error_kind, error) = (kind.as_str(), format!("{error:#}"));
        let message = "upstream operation failed";
        tracing::warn!(
            op, trigger, repo, error_kind, elapsed_ms, stderr, error, message
        );
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
            .stdin(Stdio::null())
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
        let mut stdin = child.stdin.take().context("update-ref: no stdin")?;
        let written = stdin.write_all(deletions.as_bytes()).await;
        drop(stdin); // EOF so update-ref commits the transaction
        let status = child.wait().await.context("git update-ref --stdin")?;
        // Report the exit status first: a child that fails early closes its
        // stdin, so the write error is only a symptom of that failure.
        if !status.success() {
            bail!("git update-ref --stdin failed for {}", repo.name);
        }
        written.context("write update-ref deletions")?;
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
        // Trace output would bypass redaction and can carry request headers.
        remove_trace_vars(&mut c, std::env::vars_os().map(|(key, _)| key));
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

/// Validate the budget for both library configuration and CLI parsing.
pub fn validate_upstream_timeout(timeout: Duration) -> Result<()> {
    if timeout.is_zero() || Instant::now().checked_add(timeout).is_none() {
        bail!("upstream timeout must be positive and fit in a monotonic deadline");
    }
    Ok(())
}

// The caller holds the repository lock throughout retries. Never retry local
// upload-pack: once its response starts streaming it cannot be replayed safely.
async fn run_upstream(
    cmd: &mut Command,
    operation: &str,
    repo: &str,
    staging: Option<&Path>,
    budget: Duration,
    cancellation: &CancellationToken,
) -> Result<()> {
    let deadline = Instant::now() + budget;
    cmd.stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    #[cfg(unix)]
    cmd.process_group(0);
    for attempt in 0..=MAX_UPSTREAM_RETRIES {
        if cancellation.is_cancelled() {
            return Err(UpstreamStopped::Cancelled.into());
        }
        if Instant::now() >= deadline {
            return Err(UpstreamStopped::TimedOut.into());
        }
        if let Some(path) = staging {
            clean_staging(path).await?;
        }
        let mut child = cmd.spawn().context("spawn upstream git")?;
        let mut stderr = child.stderr.take().context("upstream git: no stderr")?;
        let operation_result = async {
            // Drain all output but retain only a bounded diagnostic tail.
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
            Ok::<_, anyhow::Error>((status, tail))
        };
        let result = tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(UpstreamStopped::Cancelled.into()),
            _ = tokio::time::sleep_until(deadline) => Err(UpstreamStopped::TimedOut.into()),
            result = operation_result => result,
        };
        let (status, tail) = match result {
            Ok(result) => result,
            Err(error) => {
                stop_upstream(&mut child).await;
                if let Some(path) = staging {
                    clean_staging(path).await?;
                }
                return Err(error);
            }
        };
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
            let reason = reason.reason();
            tracing::warn!(
                repo,
                operation,
                attempt = attempt + 1,
                max_attempts = MAX_UPSTREAM_RETRIES + 1,
                delay_seconds = delay.as_secs(),
                reason,
                "retrying upstream git"
            );
            wait_upstream_retry(*delay, deadline, cancellation).await?;
            continue;
        }
        // An upstream credential rejection is carried out typed, so a `client`-mode
        // caller can map it to a client 401 rather than a blanket 502.
        let kind = match reason {
            Some(kind) => kind,
            None if upstream_auth_diagnostic(&tail) => FailureKind::Auth,
            None => FailureKind::Unknown,
        };
        return Err(UpstreamFailed {
            operation: operation.to_owned(),
            repo: repo.to_owned(),
            attempts: attempt + 1,
            status,
            kind,
            stderr: tail,
        }
        .into());
    }
    unreachable!("final attempt returns an error or succeeds")
}

async fn stop_upstream(child: &mut Child) {
    if kill_group(child).is_err() {
        let _ = child.start_kill();
    }
    let _ = child.wait().await;
}

async fn wait_upstream_retry(
    delay: Duration,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<()> {
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => Err(UpstreamStopped::Cancelled.into()),
        _ = tokio::time::sleep_until(deadline) => Err(UpstreamStopped::TimedOut.into()),
        _ = tokio::time::sleep(delay) => Ok(()),
    }
}

async fn clean_staging(path: &Path) -> Result<()> {
    match tokio::fs::remove_dir_all(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).context("remove upstream clone staging directory"),
    }
}

fn remove_trace_vars(cmd: &mut Command, keys: impl IntoIterator<Item = OsString>) {
    for key in keys {
        if is_trace_var(&key) {
            cmd.env_remove(key);
        }
    }
}

/// Git environment variables that send trace output to stderr.
fn is_trace_var(key: &OsStr) -> bool {
    key.to_str()
        .is_some_and(|key| key.starts_with("GIT_TRACE") || key == "GIT_CURL_VERBOSE")
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
async fn wait_upload_pack(child: &mut Child) -> std::io::Result<ExitStatus> {
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
        // Option is a legacy Tokio API shape; recv() guarantees Some,
        // so a None error branch would be unreachable.
        exits.recv().await;
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
async fn wait_upload_pack(child: &mut Child) -> std::io::Result<ExitStatus> {
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

async fn refresh_loop<F, Fut>(interval: Duration, shutdown: CancellationToken, round: F)
where
    F: Fn(Instant) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let Some(mut next) = Instant::now().checked_add(interval) else {
        tracing::warn!("refresh interval exceeds the monotonic deadline range");
        return;
    };
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,
            _ = tokio::time::sleep_until(next) => {}
        }
        let Some(deadline) = Instant::now().checked_add(interval) else {
            tracing::warn!("refresh interval exceeds the monotonic deadline range");
            break;
        };
        round(deadline).await;
        next = deadline;
    }
}

async fn refresh_round<F, Fut>(
    candidates: impl IntoIterator<Item = RepoRef>,
    concurrency: usize,
    deadline: Instant,
    shutdown: CancellationToken,
    fetch: F,
) -> usize
where
    F: Fn(RepoRef) -> Fut,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let mut started = 0;
    let mut jobs = tokio::task::JoinSet::new();
    for repo in candidates {
        if jobs.len() == concurrency
            && let Some(Err(error)) = jobs.join_next().await
        {
            tracing::warn!(%error, "background refresh job failed");
        }
        if Instant::now() >= deadline || shutdown.is_cancelled() {
            break;
        }
        jobs.spawn(fetch(repo));
        started += 1;
    }
    while let Some(result) = jobs.join_next().await {
        if let Err(error) = result {
            tracing::warn!(%error, "background refresh job failed");
        }
    }
    started
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;

    use crate::test_util::test_git_config;
    #[cfg(unix)]
    use crate::test_util::write_test_script;

    #[tokio::test(start_paused = true)]
    async fn refresh_usage_tracks_success_and_ttl_hits_only() {
        let root = tempfile::tempdir().unwrap();
        let repo = refresh_test_mirror(root.path(), "repo.git");
        let mut cfg = test_git_config();
        cfg.git_binary = "true".into();
        let mut cache = GitCache::new(cfg, Arc::new(Metrics::new()), None);
        enable_refresh(&mut cache, 3600, 604800, 1);
        let handle = cache.start_refresh().unwrap();
        let slot = cache.slot(&repo.name).await;
        cache
            .clone()
            .ensure_fresh(&repo, false, b"", None)
            .await
            .unwrap();
        let first = slot.refresh.lock().await.used.as_ref().unwrap().1;
        *slot.fetch_lock.lock().await = Some(Instant::now());
        tokio::time::advance(Duration::from_secs(2)).await;
        assert!(matches!(
            cache.ensure_fresh(&repo, true, b"", None).await.unwrap(),
            CacheOutcome::Cached
        ));
        let latest = slot.refresh.lock().await.used.as_ref().unwrap().1;
        assert!(latest > first);
        cache
            .refresh_repo_with(
                &repo,
                Duration::from_secs(3600),
                Duration::from_secs(604800),
                Instant::now() + Duration::from_secs(3600),
            )
            .await;
        assert_eq!(slot.refresh.lock().await.used.as_ref().unwrap().1, latest);
        cache.shutdown();
        handle.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn refresh_failed_preparation_does_not_add_or_replace_usage() {
        let root = tempfile::tempdir().unwrap();
        let (cache, repo) = refresh_spawn_failure_fixture(root.path()).await;
        let slot = cache.slot(&repo.name).await;
        let previous = slot.refresh.lock().await.used.as_ref().unwrap().1;
        tokio::time::advance(Duration::from_secs(2)).await;
        assert!(cache.ensure_fresh(&repo, true, b"", None).await.is_err());
        assert_eq!(slot.refresh.lock().await.used.as_ref().unwrap().1, previous);
        let unseen = refresh_test_mirror(root.path(), "unseen.git");
        assert!(cache.ensure_fresh(&unseen, true, b"", None).await.is_err());
        assert!(
            cache
                .slot(&unseen.name)
                .await
                .refresh
                .lock()
                .await
                .used
                .is_none()
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn refresh_auth_failed_preparation_preserves_successful_usage() {
        let root = tempfile::tempdir().unwrap();
        let (cache, repo) = refresh_fixture(
            root.path(),
            "echo 'fatal: Authentication failed' >&2\nexit 1\n",
        )
        .await;
        let slot = cache.slot(&repo.name).await;
        let previous = slot.refresh.lock().await.used.as_ref().unwrap().1;
        let error = cache
            .ensure_fresh(&repo, true, b"", None)
            .await
            .unwrap_err();
        assert!(is_auth_failure(&error));
        assert_eq!(slot.refresh.lock().await.used.as_ref().unwrap().1, previous);
        let unseen = refresh_test_mirror(root.path(), "unseen.git");
        assert!(is_auth_failure(
            &cache
                .ensure_fresh(&unseen, true, b"", None)
                .await
                .unwrap_err()
        ));
        assert!(
            cache
                .slot(&unseen.name)
                .await
                .refresh
                .lock()
                .await
                .used
                .is_none()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn refresh_cancelled_preparation_preserves_usage() {
        for previously_used in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let (cache, repo) = refresh_spawn_failure_fixture(root.path()).await;
            let slot = cache.slot(&repo.name).await;
            if !previously_used {
                slot.refresh.lock().await.used = None;
            }
            let previous = slot
                .refresh
                .lock()
                .await
                .used
                .as_ref()
                .map(|(_, used)| *used);
            let guard = slot.fetch_lock.lock().await;
            let worker = cache.clone();
            let mirror = repo.clone();
            let request =
                tokio::spawn(async move { worker.ensure_fresh(&mirror, false, b"", None).await });
            tokio::time::sleep(Duration::from_millis(1)).await;
            request.abort();
            assert!(request.await.unwrap_err().is_cancelled());
            cache.shutdown();
            drop(guard);
            cache.wait_for_shutdown().await;
            assert_eq!(
                slot.refresh
                    .lock()
                    .await
                    .used
                    .as_ref()
                    .map(|(_, used)| *used),
                previous
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn refresh_usage_cannot_move_backwards() {
        let root = tempfile::tempdir().unwrap();
        let (cache, repo) = refresh_spawn_failure_fixture(root.path()).await;
        let older = Instant::now();
        tokio::time::advance(Duration::from_secs(2)).await;
        let newer = Instant::now();
        tokio::join!(
            cache.record_client_use(&repo, newer),
            cache.record_client_use(&repo, older)
        );
        assert_eq!(
            cache
                .slot(&repo.name)
                .await
                .refresh
                .lock()
                .await
                .used
                .as_ref()
                .unwrap()
                .1,
            newer
        );
    }

    #[tokio::test(start_paused = true)]
    async fn refresh_loop_rejects_an_overflowing_deadline_without_panicking() {
        let round = |_| async { panic!("test refresh round") };
        let valid_round = tokio::spawn(async move {
            refresh_loop(Duration::from_secs(1), CancellationToken::new(), round).await;
        });
        assert!(valid_round.await.unwrap_err().is_panic());
        let now = Instant::now();
        let (mut valid, mut invalid) = (0, u64::MAX);
        while invalid - valid > 1 {
            let seconds = valid + (invalid - valid) / 2;
            if now.checked_add(Duration::from_secs(seconds)).is_some() {
                valid = seconds;
            } else {
                invalid = seconds;
            }
        }
        let interval = Duration::from_secs(invalid);
        assert!(now.checked_add(Duration::from_secs(valid)).is_some());
        assert!(now.checked_add(interval).is_none());
        refresh_loop(interval, CancellationToken::new(), round).await;
        let interval = Duration::from_secs(1);
        let token = CancellationToken::new();
        let refresh = refresh_loop(interval, token.clone(), round);
        tokio::pin!(refresh);
        std::future::poll_fn(|cx| {
            assert!(refresh.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        tokio::time::advance(Duration::from_secs(valid)).await;
        assert!(Instant::now().checked_add(interval).is_none());
        refresh.await;
    }

    #[tokio::test(start_paused = true)]
    async fn start_refresh_attempts_request_recorded_repos_in_recency_order() {
        let root = tempfile::tempdir().unwrap();
        let (mut cache, repo) = refresh_spawn_failure_fixture(root.path()).await;
        let stale = refresh_test_mirror(root.path(), "stale.git");
        cache.ensure_fresh(&stale, false, b"", None).await.unwrap();
        let cold = refresh_test_mirror(root.path(), "cold.git");
        tokio::time::advance(Duration::from_secs(200)).await;
        cache.ensure_fresh(&repo, false, b"", None).await.unwrap();
        tokio::time::advance(Duration::from_secs(1)).await;
        let newest_z = refresh_test_mirror(root.path(), "z.git");
        let newest_a = refresh_test_mirror(root.path(), "a.git");
        for mirror in [&newest_z, &newest_a] {
            cache.ensure_fresh(mirror, false, b"", None).await.unwrap();
        }
        let output = RefreshLog::default();
        let writer = output.clone();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);
        enable_refresh(&mut cache, 10, 100, 1);
        let handle = cache.start_refresh().unwrap();
        tokio::time::sleep(Duration::from_millis(10001)).await;
        cache.shutdown();
        handle.await.unwrap();
        let log = String::from_utf8(output.0.lock().unwrap().clone()).unwrap();
        let attempts: Vec<_> = log
            .lines()
            .filter(|line| line.contains("background refresh failed"))
            .collect();
        assert_eq!(attempts.len(), 3, "{log}");
        for (line, name) in attempts.iter().zip(["a.git", "z.git", "repo.git"]) {
            assert!(line.contains(&format!("repo=\"{name}\" ")), "{line}");
            assert_eq!(cache.slot(name).await.refresh.lock().await.failures, 1);
        }
        assert_eq!(
            cache.slot(&stale.name).await.refresh.lock().await.failures,
            0
        );
        assert!(!cache.slots.lock().await.contains_key(&cold.name));
    }

    #[tokio::test(start_paused = true)]
    async fn refresh_job_panics_are_logged_and_other_jobs_finish() {
        let root = tempfile::tempdir().unwrap();
        let output = RefreshLog::default();
        let writer = output.clone();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);
        for names in [["a.git", "b.git"], ["b.git", "a.git"]] {
            let candidates = names.map(|name| refresh_test_mirror(root.path(), name));
            let completed = Arc::new(Mutex::new(Vec::new()));
            refresh_round(
                candidates,
                1,
                Instant::now() + Duration::from_secs(10),
                CancellationToken::new(),
                |repo| {
                    let completed = completed.clone();
                    async move {
                        if repo.name == "a.git" {
                            panic!("test refresh panic");
                        }
                        completed.lock().await.push(repo.name);
                    }
                },
            )
            .await;
            assert_eq!(*completed.lock().await, ["b.git"]);
        }
        std::io::Write::flush(&mut output.clone()).unwrap();
        let log = String::from_utf8(output.0.lock().unwrap().clone()).unwrap();
        assert!(log.contains("WARN"));
        assert_eq!(log.matches("background refresh job failed").count(), 2);
    }

    #[tokio::test]
    async fn refresh_without_config_has_no_candidates_and_skips_the_fetch() {
        let cache = GitCache::new(test_git_config(), Arc::new(Metrics::new()), None);
        let root = tempfile::tempdir().unwrap();
        let repo = crate::repo::resolve("repo.git", "https://example.com", root.path()).unwrap();
        cache.slot(&repo.name).await.refresh.lock().await.used =
            Some((repo.clone(), Instant::now()));
        assert!(cache.refresh_candidates().await.is_empty());
        cache
            .refresh_repo(&repo, Instant::now() + Duration::from_secs(10))
            .await;
        assert_eq!(refresh_count(&cache, "ok"), 0);
        assert!(!repo.cache_dir.exists());
    }

    #[tokio::test(start_paused = true)]
    async fn refresh_candidates_use_client_recency_and_window_boundary() {
        let mut cache = GitCache::new(test_git_config(), Arc::new(Metrics::new()), None);
        enable_refresh(&mut cache, 3600, 20, 1);
        let root = tempfile::tempdir().unwrap();
        for name in ["old.git", "recent.git", "newest.git"] {
            let repo = crate::repo::resolve(name, "https://example.com", root.path()).unwrap();
            cache.slot(name).await.refresh.lock().await.used = Some((repo, Instant::now()));
            tokio::time::advance(Duration::from_secs(10)).await;
        }
        let candidates = cache.refresh_candidates().await;
        assert_eq!(
            candidates
                .iter()
                .map(|(repo, _)| repo.name.as_str())
                .collect::<Vec<_>>(),
            ["newest.git", "recent.git"]
        );
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(cache.refresh_candidates().await.len(), 1);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn refresh_want_fetch_success_clears_backoff_with_credential_rules() {
        let root = tempfile::tempdir().unwrap();
        let (cache, repo) = refresh_fixture(root.path(),
            "if [ \"$1\" = cat-file ]; then while read -r oid; do printf '%s missing\\n' \"$oid\"; done; fi\n").await;
        let slot = cache.slot(&repo.name).await;
        for cred in [Some("Authorization: client"), None] {
            {
                let mut state = slot.refresh.lock().await;
                state.failures = 3;
                state.retry_after = Some(Instant::now() + Duration::from_secs(100));
                state.auth_blocked = true;
            }
            let body = pkt_line(&format!("want {}\n", "a".repeat(40)));
            cache.ensure_fresh(&repo, false, &body, cred).await.unwrap();
            let state = slot.refresh.lock().await;
            assert_eq!(state.failures, 0);
            assert!(state.retry_after.is_none());
            assert_eq!(state.auth_blocked, cred.is_some());
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn refresh_shutdown_reaps_running_fetch_and_releases_lock() {
        let root = tempfile::tempdir().unwrap();
        let (cache, repo) =
            refresh_fixture(root.path(), "printf x > started\nexec sleep 30\n").await;
        assert!(
            std::process::Command::new("mkfifo")
                .arg(repo.cache_dir.join("started"))
                .status()
                .unwrap()
                .success()
        );
        let worker = cache.clone();
        let mirror = repo.clone();
        let job = tokio::spawn(async move {
            worker
                .refresh_repo_with(
                    &mirror,
                    Duration::from_secs(10),
                    Duration::from_secs(100),
                    Instant::now() + Duration::from_secs(30),
                )
                .await;
        });
        tokio::fs::File::open(repo.cache_dir.join("started"))
            .await
            .unwrap()
            .read_exact(&mut [0])
            .await
            .unwrap();
        cache.shutdown();
        tokio::time::timeout(Duration::from_secs(5), job)
            .await
            .unwrap()
            .unwrap();
        let slot = cache.slot(&repo.name).await;
        assert!(slot.fetch_lock.try_lock().is_ok());
        assert_eq!(slot.refresh.lock().await.failures, 0);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn eviction_preempts_a_running_refresh_fetch() {
        let root = tempfile::tempdir().unwrap();
        let (cache, repo) =
            refresh_fixture(root.path(), "printf x > started\nexec sleep 30\n").await;
        assert!(
            std::process::Command::new("mkfifo")
                .arg(repo.cache_dir.join("started"))
                .status()
                .unwrap()
                .success()
        );
        let worker = cache.clone();
        let mirror = repo.clone();
        let job = tokio::spawn(async move {
            worker
                .refresh_repo_with(
                    &mirror,
                    Duration::from_secs(10),
                    Duration::from_secs(100),
                    Instant::now() + Duration::from_secs(30),
                )
                .await;
        });
        tokio::fs::File::open(repo.cache_dir.join("started"))
            .await
            .unwrap()
            .read_exact(&mut [0])
            .await
            .unwrap();
        tokio::time::timeout(
            Duration::from_secs(5),
            cache.evict(&repo.name, &repo.cache_dir),
        )
        .await
        .unwrap()
        .unwrap();
        tokio::time::timeout(Duration::from_secs(5), job)
            .await
            .unwrap()
            .unwrap();
        assert!(!repo.cache_dir.join("HEAD").exists());
        let slot = cache.slot(&repo.name).await;
        let state = slot.refresh.lock().await;
        assert_eq!(state.failures, 0);
        assert!(!state.auth_blocked);
        assert_eq!(refresh_count(&cache, "preempted"), 1);
        assert!(
            !cache
                .metrics
                .gather()
                .contains(r#"upstream_ops_total{op="fetch",repo="-",result="error"}"#),
            "an eviction must not count as an upstream error"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn refresh_uses_static_auth_and_client_waits_for_refresh() {
        let root = tempfile::tempdir().unwrap();
        let (cache, repo) = refresh_fixture(root.path(),
            "printf '%s' \"$GIT_CONFIG_VALUE_2\" > auth\nprintf '%s\\n' \"$@\" > args\nprintf x >> fetches\nprintf x > started\ncat release >/dev/null\n").await;
        for name in ["started", "release"] {
            assert!(
                std::process::Command::new("mkfifo")
                    .arg(repo.cache_dir.join(name))
                    .status()
                    .unwrap()
                    .success()
            );
        }
        let worker = cache.clone();
        let mirror = repo.clone();
        let job = tokio::spawn(async move {
            worker
                .refresh_repo_with(
                    &mirror,
                    Duration::from_secs(10),
                    Duration::from_secs(100),
                    Instant::now() + Duration::from_secs(30),
                )
                .await;
        });
        let mut signal = tokio::fs::File::open(repo.cache_dir.join("started"))
            .await
            .unwrap();
        signal.read_exact(&mut [0]).await.unwrap();
        assert!(cache.slot(&repo.name).await.fetch_lock.try_lock().is_err());
        let request = cache.ensure_fresh(&repo, true, b"", Some("Authorization: client"));
        tokio::pin!(request);
        std::future::poll_fn(|cx| {
            assert!(request.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        tokio::time::pause();
        tokio::time::sleep(Duration::from_millis(1)).await;
        std::future::poll_fn(|cx| {
            assert!(request.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        tokio::time::resume();
        assert_eq!(
            tokio::fs::read(repo.cache_dir.join("fetches"))
                .await
                .unwrap(),
            b"x"
        );
        assert_eq!(
            tokio::fs::read_to_string(repo.cache_dir.join("auth"))
                .await
                .unwrap(),
            "Authorization: server"
        );
        let args = tokio::fs::read_to_string(repo.cache_dir.join("args"))
            .await
            .unwrap();
        assert!(args.lines().any(|arg| arg == repo.upstream_url));
        assert!(!args.lines().any(|arg| arg == "origin"));
        tokio::fs::write(repo.cache_dir.join("release"), b"done")
            .await
            .unwrap();
        job.await.unwrap();
        assert!(matches!(request.await.unwrap(), CacheOutcome::Cached));
        assert_eq!(
            tokio::fs::read(repo.cache_dir.join("fetches"))
                .await
                .unwrap(),
            b"x"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn refresh_failure_backoff_auth_rejection_and_request_recovery() {
        let root = tempfile::tempdir().unwrap();
        let (mut cache, repo) = refresh_fixture(root.path(), "exit 1\n").await;
        let interval = Duration::from_secs(10);
        let slot = cache.slot(&repo.name).await;
        for failures in 1..=8 {
            slot.refresh.lock().await.retry_after = None;
            let before = Instant::now();
            cache
                .refresh_repo_with(&repo, interval, Duration::from_secs(100), before + interval)
                .await;
            let state = slot.refresh.lock().await;
            assert_eq!(state.failures, failures);
            let delay = interval * (1 << (failures - 1).min(5));
            if failures == 1 {
                assert!(state.retry_after.is_none());
            } else {
                let retry_after = state.retry_after.unwrap();
                assert!(retry_after >= before + delay);
                if failures >= 6 {
                    assert!(retry_after <= Instant::now() + interval * 32);
                }
            }
            assert!(!state.auth_blocked);
        }
        assert_eq!(refresh_count(&cache, "error"), 8);
        assert!(
            cache
                .metrics
                .gather()
                .contains(r#"upstream_ops_total{op="fetch",repo="-",result="error"} 8"#)
        );
        cache.cfg.git_binary = "true".into();
        cache
            .ensure_fresh(&repo, true, b"", Some("Authorization: client"))
            .await
            .unwrap();
        assert_eq!(slot.refresh.lock().await.failures, 0);
        cache.cfg.git_binary = root.path().join("git-wrapper").to_str().unwrap().into();
        write_test_script(
            Path::new(&cache.cfg.git_binary),
            "#!/bin/sh\necho 'fatal: Authentication failed' >&2\nexit 1\n",
        );
        *slot.fetch_lock.lock().await = None;
        cache
            .refresh_repo_with(
                &repo,
                interval,
                Duration::from_secs(100),
                Instant::now() + interval,
            )
            .await;
        assert!(slot.refresh.lock().await.auth_blocked);
        assert_eq!(slot.refresh.lock().await.failures, 1);
        assert_eq!(refresh_count(&cache, "auth_rejected"), 1);
        cache
            .refresh_repo_with(
                &repo,
                interval,
                Duration::from_secs(100),
                Instant::now() + interval,
            )
            .await;
        assert_eq!(slot.refresh.lock().await.failures, 1);
        assert_eq!(refresh_count(&cache, "skipped"), 1);
        assert_eq!(refresh_count(&cache, "error"), 8);
        cache.cfg.git_binary = "true".into();
        cache
            .ensure_fresh(&repo, true, b"", Some("Authorization: client"))
            .await
            .unwrap();
        assert!(slot.refresh.lock().await.auth_blocked);
        *slot.fetch_lock.lock().await = None;
        cache.ensure_fresh(&repo, true, b"", None).await.unwrap();
        assert!(!slot.refresh.lock().await.auth_blocked);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn refresh_failure_is_logged_once_with_its_trigger() {
        let root = tempfile::tempdir().unwrap();
        let script = "echo 'fatal: Authentication failed' >&2\nexit 1\n";
        let (cache, repo) = refresh_fixture(root.path(), script).await;
        let output = RefreshLog::default();
        let writer = output.clone();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish();
        // A global subscriber that wants every callsite: without one, a parallel
        // test thread with no subscriber can cache a callsite as disabled.
        let _ = tracing::subscriber::set_global_default(tracing_subscriber::registry());
        let _guard = tracing::subscriber::set_default(subscriber);
        let interval = Duration::from_secs(10);
        cache
            .refresh_repo_with(&repo, interval, interval * 10, Instant::now() + interval)
            .await;
        let log = String::from_utf8(output.0.lock().unwrap().clone()).unwrap();
        assert_eq!(1, log.matches(" WARN ").count(), "{log}");
        assert!(log.contains("upstream operation failed"), "{log}");
        assert!(log.contains("trigger=\"refresh\""), "{log}");
    }

    #[tokio::test(start_paused = true)]
    async fn refresh_backoff_stops_at_thirty_two_intervals() {
        let root = tempfile::tempdir().unwrap();
        let (cache, repo) = refresh_spawn_failure_fixture(root.path()).await;
        let slot = cache.slot(&repo.name).await;
        let interval = Duration::from_secs(10);
        for failures in 1..=8 {
            slot.refresh.lock().await.retry_after = None;
            cache
                .refresh_repo_with(
                    &repo,
                    interval,
                    Duration::from_secs(100),
                    Instant::now() + interval,
                )
                .await;
            let state = slot.refresh.lock().await;
            assert_eq!(state.failures, failures);
            let expected = if failures == 1 {
                None
            } else {
                Some(Instant::now() + interval * (1 << (failures - 1).min(5)))
            };
            assert_eq!(state.retry_after, expected);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn refresh_first_failure_retries_in_the_next_round() {
        let root = tempfile::tempdir().unwrap();
        let (cache, repo) = refresh_spawn_failure_fixture(root.path()).await;
        let interval = Duration::from_secs(10);
        let token = CancellationToken::new();
        let slot = cache.slot(&repo.name).await;
        let started = Instant::now();
        tokio::time::timeout(
            Duration::from_secs(30),
            refresh_loop(interval, token.clone(), |deadline| {
                let (cache, repo, slot, token) = (&cache, &repo, &slot, &token);
                async move {
                    tokio::time::sleep(Duration::from_secs(3)).await;
                    cache
                        .refresh_repo_with(repo, interval, Duration::from_secs(100), deadline)
                        .await;
                    if slot.refresh.lock().await.failures == 2 {
                        token.cancel();
                    }
                }
            }),
        )
        .await
        .expect("first failure must retry in round two");
        assert_eq!(started.elapsed(), Duration::from_secs(23));
        assert_eq!(slot.refresh.lock().await.failures, 2);
    }

    #[tokio::test(start_paused = true)]
    async fn refresh_second_failure_waits_two_intervals() {
        let root = tempfile::tempdir().unwrap();
        let (cache, repo) = refresh_spawn_failure_fixture(root.path()).await;
        let interval = Duration::from_secs(10);
        let slot = cache.slot(&repo.name).await;
        for _ in 0..2 {
            cache
                .refresh_repo_with(
                    &repo,
                    interval,
                    Duration::from_secs(100),
                    Instant::now() + interval,
                )
                .await;
        }
        let retry_after = slot.refresh.lock().await.retry_after.unwrap();
        assert_eq!(retry_after, Instant::now() + interval * 2);
        tokio::time::advance(interval).await;
        cache
            .refresh_repo_with(
                &repo,
                interval,
                Duration::from_secs(100),
                Instant::now() + interval,
            )
            .await;
        assert_eq!(slot.refresh.lock().await.failures, 2);
        tokio::time::advance(interval).await;
        cache
            .refresh_repo_with(
                &repo,
                interval,
                Duration::from_secs(100),
                Instant::now() + interval,
            )
            .await;
        assert_eq!(slot.refresh.lock().await.failures, 3);
    }

    #[tokio::test(start_paused = true)]
    async fn refresh_schedule_spaces_short_rounds_and_waits_for_overruns() {
        for length in [3, 15] {
            let token = CancellationToken::new();
            let starts = Mutex::new(Vec::new());
            let start = Instant::now();
            refresh_loop(Duration::from_secs(10), token.clone(), |_| async {
                starts.lock().await.push(Instant::now());
                tokio::time::sleep(Duration::from_secs(length)).await;
                if starts.lock().await.len() == 3 {
                    token.cancel();
                }
            })
            .await;
            let starts = starts.lock().await;
            assert_eq!(starts[0] - start, Duration::from_secs(10));
            assert_eq!(starts[1] - starts[0], Duration::from_secs(length.max(10)));
            assert_eq!(starts[2] - starts[1], Duration::from_secs(length.max(10)));
        }
    }

    #[tokio::test]
    async fn refresh_disabled_creates_no_task_or_request_state_access() {
        let root = tempfile::tempdir().unwrap();
        let repo = refresh_test_mirror(root.path(), "repo.git");
        let mut cfg = test_git_config();
        cfg.git_binary = "true".into();
        let cache = GitCache::new(cfg, Arc::new(Metrics::new()), None);
        let slot = cache.slot(&repo.name).await;
        let guard = slot.refresh.lock().await;
        assert!(cache.start_refresh().is_none());
        tokio::time::timeout(
            Duration::from_secs(1),
            cache.ensure_fresh(&repo, true, b"", None),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(guard.used.is_none());
        assert_eq!(guard.failures, 0);
        assert!(cache.owners.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn refresh_counts_candidates_the_round_deadline_never_started() {
        let mut cache = GitCache::new(test_git_config(), Arc::new(Metrics::new()), None);
        let root = tempfile::tempdir().unwrap();
        let older = crate::repo::resolve("older.git", "https://example.com", root.path()).unwrap();
        let newer = crate::repo::resolve("newer.git", "https://example.com", root.path()).unwrap();
        let older_slot = cache.slot(&older.name).await;
        older_slot.refresh.lock().await.used = Some((older.clone(), Instant::now()));
        tokio::time::advance(Duration::from_secs(1)).await;
        let newer_slot = cache.slot(&newer.name).await;
        newer_slot.refresh.lock().await.used = Some((newer.clone(), Instant::now()));
        // The most recent candidate runs first and waits on its lock until the
        // deadline; with one slot the other is never started.
        let _held = newer_slot.fetch_lock.lock().await;
        enable_refresh(&mut cache, 10, 1000, 1);
        let handle = cache.start_refresh().unwrap();
        // Covers the first round only: the next one ends after this sleep.
        tokio::time::sleep(Duration::from_secs(25)).await;
        assert_eq!(refresh_count(&cache, "skipped"), 2);
        cache.shutdown();
        handle.await.unwrap();
        assert_eq!(
            refresh_count(&cache, "skipped"),
            2,
            "shutdown is not counted"
        );
    }

    #[tokio::test]
    async fn refresh_unusable_settings_count_as_disabled() {
        for (interval, concurrency) in [(3600, 0), (0, 1)] {
            let mut cfg = test_git_config();
            cfg.refresh = Some(RefreshConfig {
                interval: Duration::from_secs(interval),
                active_window: Duration::from_secs(1),
                concurrency,
            });
            let cache = GitCache::new(cfg, Arc::new(Metrics::new()), None);
            assert!(cache.cfg.refresh.is_none());
            assert!(cache.start_refresh().is_none());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn refresh_round_deadline_drains_running_jobs_without_starting_waiters() {
        let root = tempfile::tempdir().unwrap();
        let candidates = ["new.git", "middle.git", "old.git"]
            .map(|name| crate::repo::resolve(name, "https://example.com", root.path()).unwrap());
        let events = Arc::new(Mutex::new(Vec::new()));
        let start = Instant::now();
        let token = CancellationToken::new();
        let started = refresh_round(
            candidates,
            2,
            start + Duration::from_secs(10),
            token.clone(),
            |repo| {
                let events = events.clone();
                async move {
                    events.lock().await.push((repo.name.clone(), false));
                    tokio::time::sleep(Duration::from_secs(15)).await;
                    events.lock().await.push((repo.name, true));
                }
            },
        )
        .await;
        assert_eq!(started, 2);
        assert_eq!(start.elapsed(), Duration::from_secs(15));
        let events = events.lock().await;
        assert_eq!(
            &events[..2],
            &[("new.git".into(), false), ("middle.git".into(), false)]
        );
        let mut completed = events[2..].to_vec();
        completed.sort();
        assert_eq!(
            completed,
            [("middle.git".into(), true), ("new.git".into(), true)]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn refresh_round_short_jobs_start_in_order_with_bounded_concurrency() {
        let root = tempfile::tempdir().unwrap();
        let candidates = ["a.git", "b.git", "c.git"]
            .map(|name| crate::repo::resolve(name, "https://example.com", root.path()).unwrap());
        let events = Arc::new(Mutex::new(Vec::new()));
        let start = Instant::now();
        refresh_round(
            candidates,
            1,
            start + Duration::from_secs(10),
            CancellationToken::new(),
            |repo| {
                let events = events.clone();
                async move {
                    events.lock().await.push((repo.name, Instant::now()));
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            },
        )
        .await;
        assert_eq!(start.elapsed(), Duration::from_secs(3));
        let events = events.lock().await;
        for (i, (name, when)) in events.iter().enumerate() {
            assert_eq!(name, &["a.git", "b.git", "c.git"][i]);
            assert_eq!(*when - start, Duration::from_secs(i as u64));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn refresh_lock_deadline_and_shutdown_leave_failure_state_unchanged() {
        let cache = GitCache::new(test_git_config(), Arc::new(Metrics::new()), None);
        let root = tempfile::tempdir().unwrap();
        let repo = crate::repo::resolve("repo.git", "https://example.com", root.path()).unwrap();
        let slot = cache.slot(&repo.name).await;
        slot.refresh.lock().await.failures = 3;
        let guard = slot.fetch_lock.lock().await;
        let start = Instant::now();
        cache
            .refresh_repo_with(
                &repo,
                Duration::from_secs(10),
                Duration::from_secs(100),
                start + Duration::from_secs(10),
            )
            .await;
        assert_eq!(start.elapsed(), Duration::from_secs(10));
        assert_eq!(slot.refresh.lock().await.failures, 3);
        assert_eq!(refresh_count(&cache, "skipped"), 1);
        cache.shutdown();
        cache
            .refresh_repo_with(
                &repo,
                Duration::from_secs(10),
                Duration::from_secs(100),
                Instant::now() + Duration::from_secs(10),
            )
            .await;
        assert_eq!(slot.refresh.lock().await.failures, 3);
        assert_eq!(
            refresh_count(&cache, "skipped"),
            1,
            "shutdown is not counted"
        );
        drop(guard);
        assert!(slot.fetch_lock.try_lock().is_ok());
    }

    #[tokio::test]
    async fn refresh_success_shares_ttl_and_preserves_client_usage() {
        let root = tempfile::tempdir().unwrap();
        let repo = crate::repo::resolve("repo.git", "https://example.com", root.path()).unwrap();
        std::fs::create_dir_all(&repo.cache_dir).unwrap();
        std::fs::write(repo.cache_dir.join("HEAD"), "ref: refs/heads/main").unwrap();
        let mut cfg = test_git_config();
        cfg.git_binary = "true".into();
        cfg.fetch_ttl = Duration::from_secs(60);
        let mut cache = GitCache::new(cfg, Arc::new(Metrics::new()), None);
        enable_refresh(&mut cache, 3600, 604800, 1);
        let slot = cache.slot(&repo.name).await;
        let used = Instant::now();
        slot.refresh.lock().await.used = Some((repo.clone(), used));
        slot.refresh.lock().await.failures = 3;
        cache
            .refresh_repo_with(
                &repo,
                Duration::from_secs(10),
                Duration::from_secs(100),
                Instant::now() + Duration::from_secs(10),
            )
            .await;
        assert!(slot.fetch_lock.lock().await.is_some());
        assert_eq!(slot.refresh.lock().await.used.as_ref().unwrap().1, used);
        assert_eq!(slot.refresh.lock().await.failures, 0);
        assert_eq!(refresh_count(&cache, "ok"), 1);
        assert!(matches!(
            cache.ensure_fresh(&repo, true, b"", None).await.unwrap(),
            CacheOutcome::Cached
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn refresh_skips_inactive_missing_auth_blocked_and_backoff_repos() {
        let cache = GitCache::new(test_git_config(), Arc::new(Metrics::new()), None);
        let root = tempfile::tempdir().unwrap();
        let repo = crate::repo::resolve("repo.git", "https://example.com", root.path()).unwrap();
        let slot = cache.slot(&repo.name).await;
        let used = Instant::now();
        slot.refresh.lock().await.used = Some((repo.clone(), used));
        tokio::time::advance(Duration::from_secs(11)).await;
        cache
            .refresh_repo_with(
                &repo,
                Duration::from_secs(10),
                Duration::from_secs(10),
                Instant::now() + Duration::from_secs(10),
            )
            .await;
        assert!(!repo.cache_dir.exists());
        assert_eq!(slot.refresh.lock().await.failures, 0);
        slot.refresh.lock().await.used = Some((repo.clone(), Instant::now()));
        cache
            .refresh_repo_with(
                &repo,
                Duration::from_secs(10),
                Duration::from_secs(10),
                Instant::now() + Duration::from_secs(10),
            )
            .await;
        assert!(!repo.cache_dir.exists());
        std::fs::create_dir_all(&repo.cache_dir).unwrap();
        std::fs::write(repo.cache_dir.join("HEAD"), "ref: refs/heads/main").unwrap();
        slot.refresh.lock().await.auth_blocked = true;
        cache
            .refresh_repo_with(
                &repo,
                Duration::from_secs(10),
                Duration::from_secs(10),
                Instant::now() + Duration::from_secs(10),
            )
            .await;
        assert_eq!(slot.refresh.lock().await.failures, 0);
        slot.refresh.lock().await.auth_blocked = false;
        slot.refresh.lock().await.retry_after = Some(Instant::now() + Duration::from_secs(20));
        cache
            .refresh_repo_with(
                &repo,
                Duration::from_secs(10),
                Duration::from_secs(10),
                Instant::now() + Duration::from_secs(10),
            )
            .await;
        assert_eq!(slot.refresh.lock().await.failures, 0);
        assert_eq!(refresh_count(&cache, "skipped"), 4);
        assert_eq!(
            refresh_count(&cache, "ok") + refresh_count(&cache, "error"),
            0
        );
    }

    #[tokio::test]
    async fn refresh_auth_mark_requires_server_credential_success() {
        let mut cfg = test_git_config();
        cfg.upstream_auth_header = Some("Authorization: server".into());
        let mut cache = GitCache::new(cfg, Arc::new(Metrics::new()), None);
        enable_refresh(&mut cache, 3600, 604800, 1);
        let root = tempfile::tempdir().unwrap();
        let repo = crate::repo::resolve("repo.git", "https://example.com", root.path()).unwrap();
        let slot = cache.slot(&repo.name).await;
        {
            let mut state = slot.refresh.lock().await;
            state.auth_blocked = true;
            state.failures = 4;
            state.retry_after = Some(Instant::now() + Duration::from_secs(100));
        }
        cache
            .record_fetch_success(&repo, Some("Authorization: client"))
            .await;
        {
            let state = slot.refresh.lock().await;
            assert!(state.auth_blocked);
            assert_eq!(state.failures, 0);
            assert!(state.retry_after.is_none());
        }
        cache.record_fetch_success(&repo, None).await;
        assert!(!slot.refresh.lock().await.auth_blocked);
        slot.refresh.lock().await.auth_blocked = true;
        cache
            .record_fetch_success(&repo, Some("Authorization: server"))
            .await;
        assert!(!slot.refresh.lock().await.auth_blocked);
    }

    #[tokio::test]
    async fn missing_oids_drains_more_than_a_pipe_buffer() {
        let dir = tempfile::tempdir().unwrap();
        let status = std::process::Command::new("git")
            .args(["init", "--bare", "--quiet"])
            .arg(dir.path())
            .status()
            .unwrap();
        assert!(status.success());
        let cache = GitCache::new(test_git_config(), Arc::new(Metrics::new()), None);
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

    #[tokio::test(start_paused = true)]
    async fn cancelled_upstream_does_not_spawn_a_command() {
        let dir = tempfile::tempdir().unwrap();
        let mut cmd = Command::new(dir.path().join("missing-git"));
        let token = CancellationToken::new();
        token.cancel();
        let error = run_upstream(
            &mut cmd,
            "test",
            "test",
            None,
            Duration::from_secs(60),
            &token,
        )
        .await
        .unwrap_err();
        assert!(
            matches!(
                error.downcast_ref::<UpstreamStopped>(),
                Some(UpstreamStopped::Cancelled)
            ),
            "cancellation must precede spawn: {error}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn upstream_cleanup_reaps_leader_when_process_group_is_missing() {
        use std::os::unix::process::ExitStatusExt;
        let mut child = Command::new("sleep").arg("30").spawn().unwrap();
        tokio::time::timeout(Duration::from_secs(5), stop_upstream(&mut child))
            .await
            .expect("fallback must terminate and reap the direct child");
        assert!(
            child.id().is_none(),
            "child must be reaped before cleanup returns"
        );
        assert_eq!(child.wait().await.unwrap().signal(), Some(9));
    }

    #[tokio::test(start_paused = true)]
    async fn retry_wait_obeys_shared_deadline_and_cancellation() {
        let token = CancellationToken::new();
        let started = Instant::now();
        let deadline = started + Duration::from_secs(3);
        wait_upstream_retry(Duration::from_secs(2), deadline, &token)
            .await
            .unwrap();
        assert!(
            wait_upstream_retry(Duration::from_secs(2), deadline, &token)
                .await
                .is_err()
        );
        assert_eq!(started.elapsed(), Duration::from_secs(3));
        let started = Instant::now();
        let deadline = started + Duration::from_secs(20);
        let cancel = async {
            tokio::time::sleep(Duration::from_secs(1)).await;
            token.cancel();
        };
        let (result, ()) = tokio::join!(
            wait_upstream_retry(Duration::from_secs(10), deadline, &token),
            cancel
        );
        assert!(result.is_err());
        assert_eq!(started.elapsed(), Duration::from_secs(1));
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_cancels_repository_waiters_and_rejects_admission() {
        let cache = GitCache::new(test_git_config(), Arc::new(Metrics::new()), None);
        let root = tempfile::tempdir().unwrap();
        let repo = crate::repo::resolve("repo.git", "https://example.com", root.path()).unwrap();
        let slot = cache.slot(&repo.name).await;
        let guard = slot.fetch_lock.lock().await;
        let wants = HashSet::new();
        let waiter = cache.ensure_fresh_owned(&repo, true, &wants, None);
        let shutdown = async {
            cache.shutdown();
        };
        let (result, ()) = tokio::join!(waiter, shutdown);
        assert!(result.is_err());
        cache.wait_for_shutdown().await;
        drop(guard);
        assert!(slot.fetch_lock.try_lock().is_ok());
        assert!(cache.ensure_fresh(&repo, true, b"", None).await.is_err());
        cache.wait_for_shutdown().await;
        assert!(cache.owners.is_empty());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn upstream_shutdown_preserves_active_upload_and_releases_its_permit() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let release = root.path().join("release");
        assert!(
            std::process::Command::new("mkfifo")
                .arg(&release)
                .status()
                .unwrap()
                .success()
        );
        let binary = root.path().join("git-wrapper");
        write_test_script(
            &binary,
            &format!(
                "#!/bin/sh\ncat >/dev/null\ncat '{}' >/dev/null\nprintf pack\n",
                release.display()
            ),
        );
        std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o755)).unwrap();
        let mut config = test_git_config();
        config.git_binary = binary.to_str().unwrap().into();
        config.max_upload_packs = 1;
        let cache = GitCache::new(config, Arc::new(Metrics::new()), None);
        let repo = crate::repo::resolve("repo.git", "https://example.com", root.path()).unwrap();
        let stream = cache
            .upload_pack_rpc(&repo, None, Bytes::new())
            .await
            .unwrap();
        assert_eq!(cache.upload_packs.as_ref().unwrap().available_permits(), 0);
        cache.shutdown();
        cache.wait_for_shutdown().await;
        tokio::fs::write(&release, b"").await.unwrap();
        let mut body = Vec::new();
        tokio::time::timeout(
            Duration::from_secs(5),
            tokio_util::io::StreamReader::new(stream).read_to_end(&mut body),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(body, b"pack");
        let permit = tokio::time::timeout(
            Duration::from_secs(5),
            cache.upload_packs.as_ref().unwrap().acquire(),
        )
        .await
        .unwrap()
        .unwrap();
        drop(permit);
        assert_eq!(cache.upload_packs.as_ref().unwrap().available_permits(), 1);
    }

    #[test]
    fn upstream_budget_rejects_zero_and_overflow() {
        assert!(validate_upstream_timeout(Duration::ZERO).is_err());
        assert!(validate_upstream_timeout(Duration::MAX).is_err());
        assert!(validate_upstream_timeout(Duration::from_secs(3600)).is_ok());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn upstream_budget_stops_stalled_and_noisy_commands() {
        for script in ["sleep 30", "while :; do echo private-diagnostic >&2; done"] {
            let token = CancellationToken::new();
            let mut cmd = Command::new("sh");
            cmd.args(["-c", script]);
            let error = run_upstream(
                &mut cmd,
                "test",
                "test",
                None,
                Duration::from_millis(100),
                &token,
            )
            .await
            .unwrap_err();
            assert_eq!(failure_kind(&error), Some(FailureKind::Timeout));
            assert!(!error.to_string().contains("private-diagnostic"));
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn upstream_cancellation_reaps_child_and_stops_descendants() {
        let root = tempfile::tempdir().unwrap();
        let pidfile = root.path().join("pids");
        assert!(
            std::process::Command::new("mkfifo")
                .arg(&pidfile)
                .status()
                .unwrap()
                .success()
        );
        let token = CancellationToken::new();
        let mut cmd = Command::new("sh");
        cmd.arg("-c")
            .arg("sleep 30 & echo \"$$ $!\" > \"$1\"; wait")
            .arg("test")
            .arg(&pidfile);
        let cancel = async {
            let pids = tokio::fs::read_to_string(&pidfile).await.unwrap();
            token.cancel();
            pids
        };
        let (result, pids) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(
                run_upstream(
                    &mut cmd,
                    "test",
                    "test",
                    None,
                    Duration::from_secs(30),
                    &token
                ),
                cancel
            )
        })
        .await
        .unwrap();
        assert_eq!(
            failure_kind(&result.unwrap_err()),
            Some(FailureKind::Cancelled)
        );
        let mut pids = pids.split_whitespace();
        let leader = pids.next().unwrap();
        assert!(
            process_state(leader).await.is_empty(),
            "leader must be reaped"
        );
        for pid in pids {
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let state = process_state(pid).await;
                    if state.is_empty() || state.starts_with('Z') {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .expect("descendant must stop after group kill");
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn upstream_budget_is_shared_across_retry_backoff() {
        let token = CancellationToken::new();
        let mut cmd = Command::new("sh");
        cmd.args([
            "-c",
            "echo 'Recv failure: Connection reset by peer' >&2; exit 128",
        ]);
        let started = Instant::now();
        let error = run_upstream(
            &mut cmd,
            "test",
            "test",
            None,
            Duration::from_millis(100),
            &token,
        )
        .await
        .unwrap_err();
        assert!(error.is::<UpstreamStopped>());
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn upstream_success_within_budget_and_typed_auth_error() {
        let token = CancellationToken::new();
        let mut cmd = Command::new("sh");
        cmd.args(["-c", "sleep 0.1"]);
        run_upstream(
            &mut cmd,
            "test",
            "test",
            None,
            Duration::from_secs(2),
            &token,
        )
        .await
        .unwrap();
        let mut cmd = Command::new("sh");
        cmd.args(["-c", "echo 'fatal: Authentication failed' >&2; exit 128"]);
        let error = run_upstream(
            &mut cmd,
            "test",
            "test",
            None,
            Duration::from_secs(2),
            &token,
        )
        .await
        .unwrap_err();
        assert!(is_auth_failure(&error));
    }

    #[tokio::test]
    async fn clean_staging_handles_missing_path_and_reports_errors() {
        let root = tempfile::tempdir().unwrap();
        clean_staging(&root.path().join("missing")).await.unwrap();
        let dir = root.path().join("staging");
        tokio::fs::create_dir(&dir).await.unwrap();
        clean_staging(&dir).await.unwrap();
        assert!(!dir.exists());
        let file = root.path().join("file");
        tokio::fs::write(&file, b"test").await.unwrap();
        let err = clean_staging(&file).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("remove upstream clone staging directory")
        );
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

    #[cfg(unix)]
    #[tokio::test]
    async fn upstream_failure_keeps_stderr_out_of_the_error() {
        let token = CancellationToken::new();
        let mut cmd = Command::new("sh");
        cmd.args(["-c", "echo 'remote: private-diagnostic' >&2; exit 128"]);
        let error = run_upstream(
            &mut cmd,
            "fetch",
            "repo.git",
            None,
            Duration::from_secs(5),
            &token,
        )
        .await
        .unwrap_err();
        assert_eq!(failure_kind(&error), Some(FailureKind::Unknown));
        assert!(upstream_failure_logged(&error));
        let failed = error.downcast_ref::<UpstreamFailed>().unwrap();
        assert_eq!(failed.attempts, 1);
        let debug = format!("{failed:?}");
        assert!(debug.contains("Unknown"), "{debug}");
        assert!(!debug.contains("private-diagnostic"), "{debug}");
        assert!(String::from_utf8_lossy(&failed.stderr).contains("private-diagnostic"));
        for rendered in [format!("{error:#}"), format!("{error:?}")] {
            assert!(!rendered.contains("private-diagnostic"), "{rendered}");
            assert!(
                rendered.contains("permanent or unrecognized error"),
                "{rendered}"
            );
        }

        let mut cmd = Command::new("sh");
        cmd.args([
            "-c",
            "echo 'fatal: The requested URL returned error: 404' >&2; exit 128",
        ]);
        let error = run_upstream(
            &mut cmd,
            "fetch",
            "repo.git",
            None,
            Duration::from_secs(5),
            &token,
        )
        .await
        .unwrap_err();
        assert!(is_auth_failure(&error));
        assert_eq!(failure_kind(&error), Some(FailureKind::Auth));
        assert!(format!("{error:#}").ends_with("upstream rejected the credential"));
    }

    #[test]
    fn local_errors_are_left_to_the_caller_to_log() {
        let error = anyhow::anyhow!("disk full").context("spawn upstream git");
        assert_eq!(failure_kind(&error), None);
        assert!(!upstream_failure_logged(&error));
        let stopped = anyhow::Error::new(UpstreamStopped::TimedOut).context("outer");
        assert_eq!(failure_kind(&stopped), Some(FailureKind::Timeout));
    }

    #[test]
    fn trace_variables_are_recognized() {
        for key in [
            "GIT_TRACE",
            "GIT_TRACE_CURL",
            "GIT_TRACE_PACKET",
            "GIT_TRACE2_EVENT",
            "GIT_CURL_VERBOSE",
        ] {
            assert!(is_trace_var(OsStr::new(key)), "{key}");
        }
        for key in ["GIT_TERMINAL_PROMPT", "GIT_CONFIG_COUNT", "TRACE"] {
            assert!(!is_trace_var(OsStr::new(key)), "{key}");
        }
        let mut cmd = Command::new("git");
        remove_trace_vars(&mut cmd, ["GIT_TRACE_CURL".into(), "HOME".into()]);
        let envs: Vec<_> = cmd.as_std().get_envs().collect();
        assert_eq!(envs, [(OsStr::new("GIT_TRACE_CURL"), None)]);
    }

    #[tokio::test]
    async fn exhausted_budget_stops_before_spawning() {
        let mut cmd = Command::new("missing-git");
        let error = run_upstream(
            &mut cmd,
            "test",
            "test",
            None,
            Duration::ZERO,
            &CancellationToken::new(),
        )
        .await
        .unwrap_err();
        assert_eq!(failure_kind(&error), Some(FailureKind::Timeout));
    }

    #[tokio::test]
    async fn clone_parent_creation_failure_preserves_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let parent = dir.path().join("blocked");
        std::fs::write(&parent, b"keep this file").unwrap();
        let repo = RepoRef {
            name: "repo.git".into(),
            upstream_url: String::new(),
            cache_dir: parent.join("mirror.git"),
        };
        let cache = upload_pack_test_cache("git");
        let error = cache.clone_mirror(&repo, None).await.unwrap_err();
        assert_eq!(error.to_string(), "create cache parent for repo.git");
        assert!(
            error
                .root_cause()
                .downcast_ref::<std::io::Error>()
                .is_some()
        );
        assert_eq!(std::fs::read(&parent).unwrap(), b"keep this file");
        assert!(!repo.cache_dir.exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn eviction_rename_failure_preserves_mirror_and_releases_lock() {
        let (_dir, repo, _oids) = wanted_oids_fixture();
        let head = std::fs::read(repo.cache_dir.join("HEAD")).unwrap();
        let mut trash = repo.cache_dir.as_os_str().to_owned();
        trash.push(crate::repo::EVICTING_SUFFIX);
        let trash = std::path::PathBuf::from(trash);
        std::fs::write(&trash, b"keep this file").unwrap();
        let cache = upload_pack_test_cache("git");
        let error = cache.evict(&repo.name, &repo.cache_dir).await.unwrap_err();
        assert_eq!(error.to_string(), "rename mirror for eviction: repo.git");
        assert_eq!(std::fs::read(repo.cache_dir.join("HEAD")).unwrap(), head);
        assert_eq!(std::fs::read(&trash).unwrap(), b"keep this file");
        std::fs::remove_file(&trash).unwrap();
        tokio::time::timeout(
            Duration::from_secs(10),
            cache.evict(&repo.name, &repo.cache_dir),
        )
        .await
        .expect("failed eviction must release its repository lock")
        .unwrap();
        assert!(!repo.cache_dir.exists());
        assert!(!trash.exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn advertisement_failure_preserves_repository_and_git_diagnostic() {
        let dir = tempfile::tempdir().unwrap();
        let binary = write_failing_git(
            dir.path(),
            "printf 'advertisement refused\\n' >&2\nexit 7\n",
        );
        let cache = upload_pack_test_cache(binary.to_str().unwrap());
        let repo = RepoRef {
            name: "repo.git".into(),
            upstream_url: String::new(),
            cache_dir: dir.path().to_owned(),
        };
        for protocol in [None, Some("version=2")] {
            let error = cache.advertise_refs(&repo, protocol).await.unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("advertise-refs failed for repo.git")
            );
            assert!(error.to_string().contains("advertisement refused"));
        }
        assert!(!cache.metrics.gather().contains("kind=\"info_refs\""));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn prune_wants_command_failures_preserve_all_pins() {
        let (dir, repo, oids) = wanted_oids_fixture();
        for oid in &oids {
            git_test_command(&repo.cache_dir, &["fetch", "-q", "origin", oid]);
            git_test_command(
                &repo.cache_dir,
                &["update-ref", &format!("{PROXY_WANTS_NS}/{oid}"), oid],
            );
        }
        let before = git_test_command(&repo.cache_dir, &["show-ref"]);
        // Only `update-ref --stdin` is fed a pipe; draining any other command's
        // stdin would block on a terminal.
        for (command, drain, diagnostic) in [
            ("for-each-ref", "", "git for-each-ref failed"),
            (
                "update-ref",
                "cat >/dev/null\n  ",
                "git update-ref --stdin failed",
            ),
        ] {
            let binary = write_failing_git(
                dir.path(),
                &format!(
                    "if [ \"$1\" = '{command}' ]; then\n  {drain}exit 7\nfi\nexec git \"$@\"\n"
                ),
            );
            let mut cache = upload_pack_test_cache(binary.to_str().unwrap());
            cache.cfg.max_wants = 1;
            let error = cache.prune_wants(&repo).await.unwrap_err();
            assert!(error.to_string().contains(diagnostic), "{error}");
            assert!(error.to_string().contains(&repo.name), "{error}");
            assert_eq!(git_test_command(&repo.cache_dir, &["show-ref"]), before);
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn prune_wants_reports_exit_status_when_update_ref_stops_reading() {
        let (dir, repo, oids) = wanted_oids_fixture();
        // Loose refs written directly: the deletion list must exceed any pipe
        // buffer so the write blocks until the child exits and fails.
        git_test_command(&repo.cache_dir, &["fetch", "-q", "origin", &oids[0]]);
        let pins = repo.cache_dir.join(PROXY_WANTS_NS);
        std::fs::create_dir_all(&pins).unwrap();
        for i in 0..600 {
            std::fs::write(pins.join(format!("{i:0>200}")), format!("{}\n", oids[0])).unwrap();
        }
        let binary = write_failing_git(
            dir.path(),
            "if [ \"$1\" = update-ref ]; then exit 7; fi\nexec git \"$@\"\n",
        );
        let mut cache = upload_pack_test_cache(binary.to_str().unwrap());
        cache.cfg.max_wants = 1;
        let error = cache.prune_wants(&repo).await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("git update-ref --stdin failed for repo.git"),
            "{error}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn non_interactive_git_commands_do_not_inherit_stdin() {
        let (dir, repo, _oids) = wanted_oids_fixture();
        // A terminal or open pipe as stdin makes an inheriting child block; this
        // asserts the cause directly. It only fails when the test's own stdin is
        // not /dev/null, so also run the suite with a live stdin.
        let binary = write_failing_git(
            dir.path(),
            "[ /dev/stdin -ef /dev/null ] || { echo 'stdin inherited' >&2; exit 9; }\nexec git \"$@\"\n",
        );
        let cache = upload_pack_test_cache(binary.to_str().unwrap());
        cache.advertise_refs(&repo, None).await.unwrap();
        cache.prune_wants(&repo).await.unwrap();
        let mut cmd = Command::new("sh");
        cmd.args(["-c", "[ /dev/stdin -ef /dev/null ]"]);
        run_upstream(
            &mut cmd,
            "test",
            "repo.git",
            None,
            Duration::from_secs(60),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn wanted_fetch_survives_prune_failure_and_reports_the_cap() {
        use tracing::instrument::WithSubscriber;

        let (dir, repo, oids) = wanted_oids_fixture();
        let binary = write_failing_git(
            dir.path(),
            "if [ \"$1\" = for-each-ref ]; then exit 7; fi\nexec git \"$@\"\n",
        );
        let mut cache = upload_pack_test_cache(binary.to_str().unwrap());
        cache.cfg.max_wants = 1;
        let mut request = Vec::new();
        for oid in &oids {
            request.extend(pkt_line(&format!("want {oid}\n")));
        }
        request.extend_from_slice(b"0000");
        let wants = parse_wants(&request);
        let logs = tempfile::NamedTempFile::new().unwrap();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .without_time()
            .with_writer(logs.reopen().unwrap())
            .finish();
        cache
            .ensure_wanted_oids(&repo, &wants, None)
            .with_subscriber(subscriber)
            .await
            .unwrap();
        let refs = git_test_command(
            &repo.cache_dir,
            &["for-each-ref", "--format=%(objectname)", PROXY_WANTS_NS],
        );
        let pinned: Vec<_> = refs.lines().collect();
        assert_eq!(pinned.len(), 1);
        assert!(oids.iter().any(|oid| oid == pinned[0]));
        git_test_command(&repo.cache_dir, &["cat-file", "-e", pinned[0]]);
        let logs = std::fs::read_to_string(logs.path()).unwrap();
        assert!(logs.contains("capping want-by-sha fetch"), "{logs}");
        assert!(logs.contains("requested=2"), "{logs}");
        assert!(logs.contains("cap=1"), "{logs}");
        assert!(logs.contains("pruning want-by-sha pins failed"), "{logs}");
        assert!(logs.contains("git for-each-ref failed"), "{logs}");
        assert!(cache.metrics.gather().contains(
            r#"gitcacheproxy_upstream_ops_total{op="want_fetch",repo="repo.git",result="ok"} 1"#
        ));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_wanted_oids_failure_is_logged_by_ensure_fresh() {
        let (dir, repo, oids) = wanted_oids_fixture();
        let binary = write_failing_git(
            dir.path(),
            "if [ \"$1\" = cat-file ]; then cat >/dev/null; exit 1; fi\nexec git \"$@\"\n",
        );
        let cache = upload_pack_test_cache(binary.to_str().unwrap());
        let mut request = pkt_line(&format!("want {}\n", oids[0]));
        request.extend_from_slice(b"0000");
        let logs = tempfile::NamedTempFile::new().unwrap();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .without_time()
            .with_writer(logs.reopen().unwrap())
            .finish();
        // A global subscriber that wants every callsite: without one, a parallel
        // test thread with no subscriber can cache a callsite as disabled.
        let _ = tracing::subscriber::set_global_default(tracing_subscriber::registry());
        let _guard = tracing::subscriber::set_default(subscriber);
        cache
            .ensure_fresh(&repo, true, &request, None)
            .await
            .unwrap();
        let logs = std::fs::read_to_string(logs.path()).unwrap();
        assert_eq!(1, logs.matches(" WARN ").count(), "{logs}");
        assert!(logs.contains("ensure wanted oids failed"), "{logs}");
        assert!(logs.contains("git cat-file --batch-check"), "{logs}");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn prune_wants_retains_cap_and_other_refs_and_reports_deletion() {
        use tracing::instrument::WithSubscriber;

        let (_dir, repo, oids) = wanted_oids_fixture();
        for oid in &oids {
            git_test_command(&repo.cache_dir, &["fetch", "-q", "origin", oid]);
            git_test_command(
                &repo.cache_dir,
                &["update-ref", &format!("{PROXY_WANTS_NS}/{oid}"), oid],
            );
        }
        git_test_command(
            &repo.cache_dir,
            &["update-ref", "refs/heads/keep", &oids[0]],
        );
        let sorted = git_test_command(
            &repo.cache_dir,
            &[
                "for-each-ref",
                "--sort=-creatordate",
                "--format=%(refname)",
                PROXY_WANTS_NS,
            ],
        );
        assert_eq!(sorted.lines().count(), 2);
        let mut cache = upload_pack_test_cache("git");
        cache.cfg.max_wants = 1;
        let logs = tempfile::NamedTempFile::new().unwrap();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .without_time()
            .with_writer(logs.reopen().unwrap())
            .finish();
        cache
            .prune_wants(&repo)
            .with_subscriber(subscriber)
            .await
            .unwrap();
        let refs = git_test_command(
            &repo.cache_dir,
            &["for-each-ref", "--format=%(refname)", PROXY_WANTS_NS],
        );
        assert_eq!(refs.trim(), sorted.lines().next().unwrap());
        assert_eq!(
            git_test_command(&repo.cache_dir, &["rev-parse", "refs/heads/keep"]).trim(),
            oids[0]
        );
        let logs = std::fs::read_to_string(logs.path()).unwrap();
        assert!(
            logs.contains("pruned oldest want-by-sha pins over cap"),
            "{logs}"
        );
        assert!(logs.contains("pruned=1"), "{logs}");
        assert!(logs.contains("repo.git"), "{logs}");
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

        let cache = upload_pack_test_cache("git");
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
        let (read_progress_tx, read_progress_rx) = read_progress_channel(false);
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
            let (read_progress_tx, read_progress_rx) = read_progress_channel(false);
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
            let (read_progress_tx, read_progress_rx) = read_progress_channel(false);
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

    #[tokio::test]
    async fn upload_pack_spawn_failure_releases_the_permit() {
        let dir = tempfile::tempdir().unwrap();
        let binary = dir.path().join("missing-git");
        let cache = upload_pack_test_cache(binary.to_str().unwrap());
        let repo = RepoRef {
            name: "repo.git".into(),
            upstream_url: String::new(),
            cache_dir: dir.path().join("mirror.git"),
        };
        for _ in 0..2 {
            let result = tokio::time::timeout(
                Duration::from_secs(1),
                cache.upload_pack_rpc(&repo, None, Bytes::from_static(b"0000")),
            )
            .await
            .expect("a failed spawn must release the upload-pack permit");
            let error = result
                .err()
                .expect("a missing git binary must fail before streaming");
            assert_eq!(error.to_string(), "spawn git upload-pack");
            assert_eq!(
                error
                    .root_cause()
                    .downcast_ref::<std::io::Error>()
                    .unwrap()
                    .kind(),
                std::io::ErrorKind::NotFound
            );
            assert_eq!(cache.upload_packs.as_ref().unwrap().available_permits(), 1);
        }
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn upload_pack_exit_observer_reports_external_reaping() {
        use rustix::process::{Pid, WaitId, WaitIdOptions, waitid};

        let mut child = Command::new("true").spawn().unwrap();
        let pid = Pid::from_raw(child.id().unwrap() as i32).unwrap();
        assert!(
            waitid(WaitId::Pid(pid), WaitIdOptions::EXITED)
                .unwrap()
                .is_some()
        );
        let error = wait_upload_pack(&mut child).await.unwrap_err();
        assert_eq!(
            error.raw_os_error(),
            Some(rustix::io::Errno::CHILD.raw_os_error())
        );
        // Tokio still owns its bookkeeping after an external waiter reaps it.
        assert_eq!(
            child.wait().await.unwrap_err().raw_os_error(),
            error.raw_os_error()
        );
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
        let (read_progress_tx, read_progress_rx) = read_progress_channel(false);
        let (writer_alive, writer_dropped) = oneshot::channel::<()>();
        let writer = std::future::poll_fn(move |_| {
            let _owned = &writer_alive;
            Poll::Pending::<std::io::Result<()>>
        });
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
        let (read_progress_tx, read_progress_rx) = read_progress_channel(false);
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
        let (read_progress_tx, read_progress_rx) = read_progress_channel(false);
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
        let (read_progress_tx, read_progress_rx) = read_progress_channel(false);
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
        let (read_progress_tx, read_progress_rx) = read_progress_channel(true);
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
        let (read_progress_tx, read_progress_rx) = read_progress_channel(false);
        let owner = run_upload_pack(
            child,
            std::future::ready(Ok(())),
            read_progress_rx,
            UPLOAD_PACK_RESPONSE_IDLE_TIMEOUT,
        );
        tokio::pin!(owner);
        let result = finish_upload_pack_with_timeout(owner.as_mut(), read_progress_tx).await;
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
        let (read_progress_tx, read_progress_rx) = read_progress_channel(false);
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
        let result = finish_upload_pack_with_timeout(owner.as_mut(), read_progress_tx).await;
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
            let (read_progress_tx, read_progress_rx) = read_progress_channel(false);
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
        let (read_progress_tx, read_progress_rx) = read_progress_channel(true);
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
        write_test_script(&script, "#!/bin/sh\nexec sleep 300\n");
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
        write_test_script(&script, "#!/bin/sh\nexec 0<&-\nexec sleep 300\n");
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
        let (_read_progress_tx, read_progress_rx) = read_progress_channel(true);
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
        let (_read_progress_tx, read_progress_rx) = read_progress_channel(true);
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
    #[tokio::test(start_paused = true)]
    async fn upload_pack_test_timeout_cancels_and_reaps_its_owner() {
        let child = sleeping_child(true);
        let pid = rustix::process::Pid::from_raw(child.id().unwrap() as i32).unwrap();
        let (read_progress_tx, read_progress_rx) = read_progress_channel(false);
        let owner = run_upload_pack(
            child,
            std::future::ready(Ok(())),
            read_progress_rx,
            UPLOAD_PACK_RESPONSE_IDLE_TIMEOUT,
        );
        assert!(
            finish_upload_pack_with_timeout(owner, read_progress_tx)
                .await
                .is_err()
        );
        assert_eq!(
            rustix::process::waitpid(Some(pid), rustix::process::WaitOptions::NOHANG).unwrap_err(),
            rustix::io::Errno::CHILD
        );
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
    // Regression failures must still cancel and reap their process before panicking.
    async fn finish_upload_pack_with_timeout(
        owner: impl Future<Output = std::io::Result<()>>,
        read_progress_tx: watch::Sender<ReadProgress>,
    ) -> Result<std::io::Result<()>, tokio::time::error::Elapsed> {
        tokio::pin!(owner);
        let result = tokio::time::timeout(Duration::from_secs(1), &mut owner).await;
        if result.is_err() {
            drop(read_progress_tx);
            let _ = owner.await;
        }
        result
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

    #[cfg(unix)]
    fn write_failing_git(dir: &Path, body: &str) -> std::path::PathBuf {
        use std::os::unix::fs::PermissionsExt;

        let binary = dir.join("git-wrapper");
        write_test_script(&binary, &format!("#!/bin/sh\n{body}"));
        std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o755)).unwrap();
        binary
    }

    #[cfg(unix)]
    fn git_test_command(dir: &Path, args: &[&str]) -> String {
        let output = std::process::Command::new("git")
            .current_dir(dir)
            .args(["-c", "user.name=Test", "-c", "user.email=test@example.com"])
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap()
    }

    #[cfg(unix)]
    fn wanted_oids_fixture() -> (tempfile::TempDir, RepoRef, [String; 2]) {
        let dir = tempfile::tempdir().unwrap();
        git_test_command(dir.path(), &["init", "-q", "source"]);
        let source = dir.path().join("source");
        let oids = ["first", "second"].map(|message| {
            git_test_command(&source, &["commit", "-q", "--allow-empty", "-m", message]);
            git_test_command(&source, &["rev-parse", "HEAD"])
                .trim()
                .to_owned()
        });
        git_test_command(dir.path(), &["init", "-q", "--bare", "mirror.git"]);
        let repo = RepoRef {
            name: "repo.git".into(),
            upstream_url: source.to_str().unwrap().into(),
            cache_dir: dir.path().join("mirror.git"),
        };
        git_test_command(
            &repo.cache_dir,
            &["remote", "add", "origin", source.to_str().unwrap()],
        );
        (dir, repo, oids)
    }

    fn read_progress_channel(
        eof: bool,
    ) -> (watch::Sender<ReadProgress>, watch::Receiver<ReadProgress>) {
        watch::channel(ReadProgress {
            last_read_at: Instant::now(),
            eof,
        })
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

    /// `ps` state of `pid`: empty once reaped, `Z` while a zombie awaits reaping.
    /// `kill -0` reports zombies as alive, and /proc is Linux-only.
    async fn process_state(pid: &str) -> String {
        let out = Command::new("ps")
            .args(["-o", "stat=", "-p", pid])
            .output()
            .await
            .unwrap();
        String::from_utf8_lossy(&out.stdout).trim().to_owned()
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
                max_upload_packs: 1,
                ..test_git_config()
            },
            Arc::new(Metrics::new()),
            None,
        )
    }

    impl GitCache {
        /// `refresh_repo` with explicit settings, so a test can vary them per call.
        /// The clone shares slots, metrics and the shutdown token with `self`; only
        /// its private copy of the settings differs.
        async fn refresh_repo_with(
            &self,
            repo: &RepoRef,
            interval: Duration,
            active_window: Duration,
            deadline: Instant,
        ) {
            let mut cache = self.clone();
            cache.cfg.refresh = Some(RefreshConfig {
                interval,
                active_window,
                concurrency: 1,
            });
            cache.refresh_repo(repo, deadline).await;
        }
    }

    fn enable_refresh(cache: &mut GitCache, interval: u64, active_window: u64, concurrency: usize) {
        cache.cfg.refresh = Some(RefreshConfig {
            interval: Duration::from_secs(interval),
            active_window: Duration::from_secs(active_window),
            concurrency,
        });
    }

    /// Value of `refresh_total{result}`; 0 when the series was never recorded.
    fn refresh_count(cache: &GitCache, result: &str) -> u64 {
        let prefix = format!(r#"gitcacheproxy_refresh_total{{result="{result}"}} "#);
        cache
            .metrics
            .gather()
            .lines()
            .find_map(|line| line.strip_prefix(&prefix))
            .map_or(0, |value| value.parse().unwrap())
    }

    #[derive(Clone, Default)]
    struct RefreshLog(Arc<StdMutex<Vec<u8>>>);

    impl std::io::Write for RefreshLog {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn refresh_test_mirror(root: &Path, name: &str) -> RepoRef {
        let repo = crate::repo::resolve(name, "https://example.com", root).unwrap();
        std::fs::create_dir_all(&repo.cache_dir).unwrap();
        std::fs::write(repo.cache_dir.join("HEAD"), "ref: refs/heads/main").unwrap();
        repo
    }

    async fn refresh_spawn_failure_fixture(root: &Path) -> (GitCache, RepoRef) {
        let mut cfg = test_git_config();
        cfg.git_binary = root.join("missing-git").to_str().unwrap().into();
        let mut cache = GitCache::new(cfg, Arc::new(Metrics::new()), None);
        let repo = crate::repo::resolve("repo.git", "https://example.com", root).unwrap();
        std::fs::create_dir_all(&repo.cache_dir).unwrap();
        std::fs::write(repo.cache_dir.join("HEAD"), "ref: refs/heads/main").unwrap();
        enable_refresh(&mut cache, 3600, 604800, 1);
        cache.ensure_fresh(&repo, false, b"", None).await.unwrap();
        (cache, repo)
    }

    #[cfg(unix)]
    async fn refresh_fixture(root: &Path, script: &str) -> (GitCache, RepoRef) {
        use std::os::unix::fs::PermissionsExt;
        let binary = root.join("git-wrapper");
        write_test_script(&binary, &format!("#!/bin/sh\n{script}"));
        std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o755)).unwrap();
        let mut cfg = test_git_config();
        cfg.git_binary = binary.to_str().unwrap().into();
        cfg.upstream_auth_header = Some("Authorization: server".into());
        cfg.fetch_ttl = Duration::from_secs(60);
        let mut cache = GitCache::new(cfg, Arc::new(Metrics::new()), None);
        let repo = crate::repo::resolve("repo.git", "https://example.com", root).unwrap();
        std::fs::create_dir_all(&repo.cache_dir).unwrap();
        std::fs::write(repo.cache_dir.join("HEAD"), "ref: refs/heads/main").unwrap();
        enable_refresh(&mut cache, 3600, 604800, 1);
        cache.ensure_fresh(&repo, false, b"", None).await.unwrap();
        (cache, repo)
    }
}
