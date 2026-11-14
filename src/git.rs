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
//! proactively. A miss transparently pulls from upstream; optional stale serving
//! can return older refs while a refresh runs.
//!
//! Want-by-SHA: a client can `want` a commit that lives only under an unadvertised
//! upstream ref (e.g. a GitHub PR merge commit), which `clone --mirror` never
//! captured. Such wants are fetched from upstream on demand and pinned under
//! `PROXY_WANTS_NS` so the mirror can serve them.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::pin::Pin;
use std::process::Stdio;
use std::sync::{Arc, Mutex as StdMutex};
use std::task::Poll;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, ReadBuf};
use tokio::process::{Child, ChildStdout, Command};
use tokio::sync::{Mutex, OwnedRwLockReadGuard, OwnedSemaphorePermit, RwLock, Semaphore, oneshot};
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
const FRESHNESS_FILE: &str = "proxy-refresh.json";
const MAX_FRESHNESS_BYTES: u64 = 4096;

/// A bounded stale-serving admission lane has no capacity.
#[derive(Debug)]
pub struct CacheBusy;

impl std::fmt::Display for CacheBusy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("cache request capacity exhausted")
    }
}

impl std::error::Error for CacheBusy {}

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

/// Serialize mutations independently of readers and protect the mirror from eviction.
struct RepoSlot {
    fetch_lock: Arc<Mutex<Option<Instant>>>,
    lifecycle: Arc<RwLock<()>>,
    refresh: Mutex<RefreshState>,
}

#[derive(Default)]
struct RefreshState {
    last_success: Option<Instant>,
    next_attempt: Option<Instant>,
    in_flight: bool,
    record: Option<String>,
    identity: Option<String>,
}

/// Holds a request's mirror guard and setup permit.
pub struct RequestLease {
    guard: OwnedRwLockReadGuard<()>,
    permit: Option<OwnedSemaphorePermit>,
}

#[derive(Clone)]
pub struct GitCache {
    cfg: GitConfig,
    metrics: Arc<Metrics>,
    /// Map of repo name -> its serialization slot. A plain `Mutex` (not `RwLock`)
    /// because every access is a get-or-insert (`entry`), which needs a write lock
    /// anyway, and the critical section is a single O(1) map operation - far too
    /// short for reader/writer separation to pay off.
    slots: Arc<Mutex<HashMap<String, Arc<RepoSlot>>>>,
    /// Cache-size index for LRU eviction, or `None` when no cap is configured.
    /// When present it is `touch`ed on every request and `record`ed after each
    /// clone/fetch; when absent the eviction machinery is entirely inert.
    index: Option<Arc<crate::evict::CacheIndex>>,
    /// `None` when `max_upload_packs` is `0` (unlimited).
    upload_packs: Option<Arc<Semaphore>>,
    max_stale: Duration,
    background: Arc<Semaphore>,
    request_setups: Option<Arc<Semaphore>>,
    bodies: Option<Arc<Semaphore>>,
    preflights: Option<Arc<Semaphore>>,
    upstream: Arc<Semaphore>,
    tasks: Arc<StdMutex<Vec<tokio::task::JoinHandle<()>>>>,
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
            slots: Arc::new(Mutex::new(HashMap::new())),
            index,
            upload_packs,
            max_stale: Duration::ZERO,
            background: Arc::new(Semaphore::new(1)),
            request_setups: None,
            bodies: None,
            preflights: None,
            upstream: Arc::new(Semaphore::new(1)),
            tasks: Arc::new(StdMutex::new(Vec::new())),
        }
    }

    /// Enable bounded request-triggered background refreshes.
    pub fn with_stale(mut self, max_stale: Duration, max_background: usize) -> Self {
        self.max_stale = max_stale;
        self.background = Arc::new(Semaphore::new(max_background));
        if self.request_setups.is_none() {
            self.upstream = Arc::new(Semaphore::new(max_background));
        }
        self
    }

    /// Bound request setup tasks, including those whose clients disconnected.
    pub fn with_request_limit(mut self, limit: usize) -> Self {
        self.request_setups = (limit > 0).then(|| Arc::new(Semaphore::new(limit)));
        self.bodies = (limit > 0).then(|| Arc::new(Semaphore::new(limit)));
        self.preflights = (limit > 0).then(|| Arc::new(Semaphore::new(limit)));
        let upstream_limit = if limit == 0 {
            self.background.available_permits().max(1)
        } else {
            limit.saturating_sub(1).max(1)
        };
        self.upstream = Arc::new(Semaphore::new(upstream_limit));
        self
    }

    /// Wait for admitted background refreshes after the HTTP server drains.
    pub async fn shutdown(&self) {
        loop {
            let tasks = std::mem::take(&mut *self.tasks.lock().unwrap());
            if tasks.is_empty() {
                break;
            }
            for task in tasks {
                let _ = task.await;
            }
        }
    }

    /// Prepare a request and keep the mirror protected if its client disconnects.
    pub async fn prepare(
        &self,
        repo: RepoRef,
        want_fetch: bool,
        body: Bytes,
        cred: Option<&str>,
    ) -> Result<(CacheOutcome, RequestLease)> {
        let permit = if self.stale_enabled() {
            None
        } else {
            self.request_permit().await?
        };
        let preflight = self.preflight_permit()?;
        let cache = self.clone();
        let cred = cred.map(str::to_owned);
        self.spawn_tracked(async move {
            let guard = if cache.stale_enabled() {
                cache
                    .slot(&repo.name)
                    .await
                    .lifecycle
                    .clone()
                    .try_read_owned()
                    .map_err(|_| CacheBusy)?
            } else {
                cache.read_guard(&repo).await
            };
            let outcome = cache
                .ensure_fresh_inner(&repo, want_fetch, &body, cred.as_deref(), preflight)
                .await?;
            let permit = if cache.stale_enabled() {
                cache
                    .request_setups
                    .as_ref()
                    .map(|s| s.clone().try_acquire_owned())
                    .transpose()
                    .map_err(|_| CacheBusy)?
            } else {
                permit
            };
            Ok((outcome, RequestLease { guard, permit }))
        })
        .await
        .context("request preparation task")?
    }

    /// Whether request-triggered stale serving is enabled.
    pub fn stale_enabled(&self) -> bool {
        !self.max_stale.is_zero()
    }

    /// Bound request decoding independently of slow upstream preparation.
    pub fn body_permit(&self) -> Result<Option<OwnedSemaphorePermit>> {
        Ok(self
            .bodies
            .as_ref()
            .map(|s| s.clone().try_acquire_owned())
            .transpose()
            .map_err(|_| CacheBusy)?)
    }

    fn preflight_permit(&self) -> Result<Option<OwnedSemaphorePermit>> {
        if !self.stale_enabled() {
            return Ok(None);
        }
        Ok(self
            .preflights
            .as_ref()
            .map(|s| s.clone().try_acquire_owned())
            .transpose()
            .map_err(|_| CacheBusy)?)
    }

    async fn request_permit(&self) -> Result<Option<OwnedSemaphorePermit>> {
        let permit = match &self.request_setups {
            Some(limit) => Some(
                limit
                    .clone()
                    .acquire_owned()
                    .await
                    .context("request setup cap")?,
            ),
            None => None,
        };
        Ok(permit)
    }

    fn spawn_tracked<T: Send + 'static>(
        &self,
        task: impl std::future::Future<Output = T> + Send + 'static,
    ) -> oneshot::Receiver<T> {
        let (tx, rx) = oneshot::channel();
        let mut tasks = self.tasks.lock().unwrap();
        tasks.retain(|task| !task.is_finished());
        tasks.push(tokio::spawn(async move {
            let _ = tx.send(task.await);
        }));
        rx
    }

    async fn slot(&self, name: &str) -> Arc<RepoSlot> {
        self.slots
            .lock()
            .await
            .entry(name.to_string())
            .or_insert_with(|| {
                Arc::new(RepoSlot {
                    fetch_lock: Arc::new(Mutex::new(None)),
                    lifecycle: Arc::new(RwLock::new(())),
                    refresh: Mutex::new(RefreshState::default()),
                })
            })
            .clone()
    }

    /// Protect a mirror from eviction for the duration of a request.
    pub async fn read_guard(&self, repo: &RepoRef) -> OwnedRwLockReadGuard<()> {
        self.slot(&repo.name)
            .await
            .lifecycle
            .clone()
            .read_owned()
            .await
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
    /// re-clones. Active HTTP requests hold a read guard until Git exits.
    pub async fn evict(&self, name: &str, cache_dir: &Path) -> Result<()> {
        let slot = self.slot(name).await;
        let lifecycle = slot.lifecycle.write().await;
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
        drop(lifecycle);
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
        self.ensure_fresh_inner(repo, want_fetch, body, cred, self.preflight_permit()?)
            .await
    }

    async fn ensure_fresh_inner(
        &self,
        repo: &RepoRef,
        want_fetch: bool,
        body: &[u8],
        cred: Option<&str>,
        preflight: Option<OwnedSemaphorePermit>,
    ) -> Result<CacheOutcome> {
        let slot = self.slot(&repo.name).await;
        // Mark the repo used on every request - a served cache hit counts as much as
        // a fetch - so the eviction index keeps a truthful last-access ordering.
        if let Some(idx) = &self.index {
            idx.touch(&repo.name);
        }
        if !self.max_stale.is_zero() {
            return self
                .ensure_fresh_stale(repo, &slot, body, cred, preflight)
                .await;
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

    async fn ensure_fresh_stale(
        &self,
        repo: &RepoRef,
        slot: &Arc<RepoSlot>,
        body: &[u8],
        cred: Option<&str>,
        preflight: Option<OwnedSemaphorePermit>,
    ) -> Result<CacheOutcome> {
        self.restore_freshness(repo, slot).await;
        let exists = repo.cache_dir.join("HEAD").exists();
        let mut state = slot.refresh.lock().await;
        let usable = exists
            && state
                .last_success
                .is_some_and(|t| t.elapsed() <= self.max_stale);
        if usable {
            let stale = state
                .last_success
                .is_some_and(|t| self.cfg.fetch_ttl.is_zero() || t.elapsed() >= self.cfg.fetch_ttl);
            let ready = state.next_attempt.is_none_or(|t| Instant::now() >= t);
            if stale
                && ready
                && !state.in_flight
                && let Ok(permit) = self.background.clone().try_acquire_owned()
                && let Ok(lifecycle) = slot.lifecycle.clone().try_read_owned()
                && let Ok(fetch_guard) = slot.fetch_lock.clone().try_lock_owned()
            {
                state.in_flight = true;
                let cache = self.clone();
                let repo = repo.clone();
                let slot = slot.clone();
                let cred = cred.map(str::to_owned);
                let mut tasks = self.tasks.lock().unwrap();
                tasks.retain(|task| !task.is_finished());
                tasks.push(tokio::spawn(async move {
                    let _permit = permit;
                    let _fetch_guard = fetch_guard;
                    let _lifecycle = lifecycle;
                    let started = Instant::now();
                    let result = if repo.cache_dir.join("HEAD").exists() {
                        cache.fetch(&repo, cred.as_deref()).await
                    } else {
                        Err(anyhow::anyhow!("mirror was evicted before refresh"))
                    };
                    cache.restore_freshness(&repo, &slot).await;
                    let mut state = slot.refresh.lock().await;
                    if result.is_ok() {
                        state.last_success = Some(started);
                    } else {
                        tracing::warn!(repo = %repo.name, error = ?result, "background refresh failed");
                    }
                    state.next_attempt = Some(Instant::now() + cache.cooldown());
                    state.in_flight = false;
                }));
            }
            drop(state);
            self.ensure_wants_stale(repo, slot, body, cred, preflight)
                .await?;
            self.check_age(slot).await?;
            return Ok(CacheOutcome::Cached);
        }
        drop(state);

        let _upstream = self
            .upstream
            .clone()
            .try_acquire_owned()
            .map_err(|_| CacheBusy)?;
        drop(preflight);
        let mut last = slot.fetch_lock.try_lock().map_err(|_| CacheBusy)?;
        self.restore_freshness(repo, slot).await;
        let mut state = slot.refresh.lock().await;
        let exists = repo.cache_dir.join("HEAD").exists();
        if exists
            && state
                .last_success
                .is_some_and(|t| t.elapsed() <= self.max_stale)
        {
            drop(state);
            if let Err(e) = self.ensure_wanted_oids(repo, body, cred).await {
                tracing::warn!(repo = %repo.name, error = %e, "ensure wanted oids failed");
            }
            drop(last);
            self.check_age(slot).await?;
            return Ok(CacheOutcome::Cached);
        }
        if state.next_attempt.is_some_and(|t| Instant::now() < t) {
            bail!("upstream refresh is in cooldown");
        }
        drop(state);
        let started = Instant::now();
        let result = if exists {
            self.fetch(repo, cred).await
        } else {
            self.clone_mirror(repo, cred).await
        };
        self.restore_freshness(repo, slot).await;
        state = slot.refresh.lock().await;
        state.next_attempt = Some(Instant::now() + self.cooldown());
        if result.is_ok() {
            state.last_success = Some(started);
            *last = Some(started);
        }
        drop(state);
        result?;
        if started.elapsed() > self.max_stale {
            bail!("refreshed mirror exceeds maximum stale age");
        }
        if let Err(e) = self.ensure_wanted_oids(repo, body, cred).await {
            tracing::warn!(repo = %repo.name, error = %e, "ensure wanted oids failed");
        }
        drop(last);
        self.check_age(slot).await?;
        Ok(if exists {
            CacheOutcome::Fetched
        } else {
            CacheOutcome::Cloned
        })
    }

    async fn restore_freshness(&self, repo: &RepoRef, slot: &RepoSlot) {
        let record = read_freshness(repo).await.ok();
        let identity = format!(
            "{}:{}",
            repository_identity(repo),
            directory_identity(&repo.cache_dir)
                .await
                .unwrap_or_default()
        );
        let mut state = slot.refresh.lock().await;
        if state.identity.as_ref() != Some(&identity) {
            state.last_success = None;
            state.record = None;
            state.next_attempt = None;
            state.identity = Some(identity);
        }
        let text = record.as_ref().map(|(text, _)| text.clone());
        if state.record != text || text.is_none() {
            state.last_success =
                record.and_then(|(_, started)| Instant::now().checked_sub(started));
            state.record = text;
        }
    }

    async fn check_age(&self, slot: &RepoSlot) -> Result<()> {
        if slot
            .refresh
            .lock()
            .await
            .last_success
            .is_none_or(|t| t.elapsed() > self.max_stale)
        {
            bail!("mirror exceeds maximum stale age");
        }
        Ok(())
    }

    fn cooldown(&self) -> Duration {
        self.cfg.fetch_ttl.max(Duration::from_secs(1))
    }

    async fn ensure_wants_stale(
        &self,
        repo: &RepoRef,
        slot: &RepoSlot,
        body: &[u8],
        cred: Option<&str>,
        preflight: Option<OwnedSemaphorePermit>,
    ) -> Result<()> {
        let wants = parse_wants(body);
        if wants.is_empty() {
            return Ok(());
        }
        match self.missing_oids(repo, &wants).await {
            Ok(missing) if missing.is_empty() => return Ok(()),
            _ => {}
        }
        let _permit = self
            .upstream
            .clone()
            .try_acquire_owned()
            .map_err(|_| CacheBusy)?;
        drop(preflight);
        let _lock = slot.fetch_lock.try_lock().map_err(|_| CacheBusy)?;
        if let Err(e) = self.ensure_wanted_oids(repo, body, cred).await {
            tracing::warn!(repo = %repo.name, error = %e, "ensure wanted oids failed");
        }
        Ok(())
    }

    async fn check_serving_age(&self, repo: &RepoRef) -> Result<()> {
        if self.stale_enabled() {
            let slot = self.slot(&repo.name).await;
            self.restore_freshness(repo, &slot).await;
            self.check_age(&slot).await?;
        }
        Ok(())
    }

    /// `git upload-pack --advertise-refs`, wrapped with the smart-HTTP service
    /// header - i.e. the `GET .../info/refs?service=git-upload-pack` body.
    pub async fn advertise_refs(
        &self,
        repo: &RepoRef,
        git_protocol: Option<&str>,
    ) -> Result<Bytes> {
        let permit = self.request_permit().await?;
        let guard = self.read_guard(repo).await;
        self.advertise_refs_guarded(repo, git_protocol, RequestLease { guard, permit })
            .await
    }

    /// Advertise while retaining the request's eviction guard until Git exits.
    pub async fn advertise_refs_guarded(
        &self,
        repo: &RepoRef,
        git_protocol: Option<&str>,
        lease: RequestLease,
    ) -> Result<Bytes> {
        let mut cmd = self.local_cmd(git_protocol);
        cmd.arg("upload-pack")
            .arg("--stateless-rpc")
            .arg("--advertise-refs")
            .arg(&repo.cache_dir);
        let started = Instant::now();
        let cache = self.clone();
        let owned_repo = repo.clone();
        let out = self
            .spawn_tracked(async move {
                let _lease = lease;
                cache.check_serving_age(&owned_repo).await?;
                cmd.output().await.context("spawn git advertisement")
            })
            .await
            .context("advertise-refs task")?
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
        let guard = self.read_guard(repo).await;
        self.upload_pack_rpc_guarded(
            repo,
            git_protocol,
            body,
            RequestLease {
                guard,
                permit: None,
            },
        )
        .await
    }

    /// Start an upload-pack while retaining the request's eviction guard.
    pub async fn upload_pack_rpc_guarded(
        &self,
        repo: &RepoRef,
        git_protocol: Option<&str>,
        body: Bytes,
        lease: RequestLease,
    ) -> Result<ReaderStream<TimedReader<ChildStdout>>> {
        let RequestLease {
            guard,
            permit: mut setup_permit,
        } = lease;
        let _strict_setup = if self.stale_enabled() {
            None
        } else {
            setup_permit.take()
        };
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
            .stderr(Stdio::inherit());
        // Own process group, so a kill also reaches `pack-objects`, which writes
        // nothing while it counts and compresses and so never gets SIGPIPE.
        #[cfg(unix)]
        cmd.process_group(0);
        self.check_serving_age(repo).await?;
        let mut child = cmd.spawn().context("spawn git upload-pack")?;
        let mut stdin = child.stdin.take().context("upload-pack: no stdin")?;
        let stdout = child.stdout.take().context("upload-pack: no stdout")?;

        // Reap in a detached task: the caller streams stdout, so this method cannot
        // wait for exit without buffering the whole pack. Spawned before the stdin
        // write, so even a request dropped mid-write still kills and reaps the child.
        let (served_tx, served_rx) = oneshot::channel();
        drop(self.spawn_tracked(reap_upload_pack(
            child,
            served_rx,
            permit,
            repo.name.clone(),
            Some(RequestLease {
                guard,
                permit: setup_permit,
            }),
        )));

        stdin
            .write_all(&body)
            .await
            .context("write upload-pack request")?;
        drop(stdin); // EOF so upload-pack starts producing the pack

        let timed = TimedReader {
            inner: stdout,
            repo: repo.name.clone(),
            recorder: Some((self.metrics.clone(), started)),
            served: Some(served_tx),
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
        let observed = self.stale_enabled().then(unix_millis).transpose()?;
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
        if let Some(observed) = observed {
            write_freshness(repo, &tmp, observed).await?;
        }
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
        // Stale records bind the configured upstream, which may differ from a
        // reused mirror's origin. Strict mode keeps its existing origin behavior.
        let started = Instant::now();
        let observed = self.stale_enabled().then(unix_millis).transpose()?;
        let directory = if self.stale_enabled() {
            Some(directory_identity(&repo.cache_dir).await?)
        } else {
            None
        };
        let mut cmd = self.fetch_cmd(cred);
        cmd.current_dir(&repo.cache_dir).arg("fetch");
        if !self.max_stale.is_zero() {
            cmd.arg("--atomic");
        }
        cmd.arg("--prune")
            .arg("--quiet")
            .arg(if self.stale_enabled() {
                &repo.upstream_url
            } else {
                "origin"
            })
            // Mirror refspec, minus the local-only pin namespace: `--prune` would
            // otherwise drop those refs as "not on origin" (see `prune_wants`).
            .arg("+refs/*:refs/*")
            .arg(format!("^{PROXY_WANTS_NS}/*"));
        if let Err(error) = run_upstream(&mut cmd, "fetch", &repo.name, None).await {
            self.metrics
                .record_upstream(UpstreamOp::Fetch, Status::Error, "-");
            return Err(error);
        }
        if let Some(observed) = observed {
            anyhow::ensure!(
                Some(directory_identity(&repo.cache_dir).await?) == directory,
                "mirror replaced during refresh"
            );
            write_freshness(repo, &repo.cache_dir, observed).await?;
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
            .arg(if self.stale_enabled() {
                &repo.upstream_url
            } else {
                "origin"
            });
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
        if !self.max_stale.is_zero() {
            // Keep objects advertised before a refresh available to clients.
            c.arg("-c").arg("gc.auto=0");
            c.arg("-c").arg("maintenance.auto=false");
        }
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
        if !self.max_stale.is_zero() {
            // A client may request an object whose advertised ref was removed.
            c.arg("-c").arg("uploadpack.allowAnySHA1InWant=true");
        }
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
fn unix_millis() -> Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("clock predates Unix epoch")?
        .as_millis()
        .try_into()?)
}

fn repository_identity(repo: &RepoRef) -> String {
    let upstream = match reqwest::Url::parse(&repo.upstream_url) {
        Ok(mut url) => {
            let _ = url.set_username("");
            let _ = url.set_password(None);
            url.set_query(None);
            url.set_fragment(None);
            url.to_string()
        }
        Err(_) => repo.upstream_url.clone(),
    };
    Sha256::digest(format!("{}\n{upstream}", repo.name))
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

async fn directory_identity(path: &Path) -> Result<String> {
    let metadata = tokio::fs::metadata(path).await?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        Ok(format!("{}:{}", metadata.dev(), metadata.ino()))
    }
    #[cfg(not(unix))]
    {
        Ok(format!("{:?}", metadata.created()?))
    }
}

async fn write_freshness(repo: &RepoRef, directory: &Path, observed: u64) -> Result<()> {
    let record = serde_json::json!({
        "version": 1,
        "directory": directory_identity(directory).await?,
        "repository": repository_identity(repo),
        "observed_ms": observed,
    });
    let temporary = directory.join("proxy-refresh.tmp");
    let mut file = tokio::fs::File::create(&temporary).await?;
    file.write_all(record.to_string().as_bytes()).await?;
    file.sync_all().await?;
    drop(file);
    tokio::fs::rename(temporary, directory.join(FRESHNESS_FILE)).await?;
    #[cfg(unix)]
    tokio::fs::File::open(directory).await?.sync_all().await?;
    Ok(())
}

async fn read_freshness(repo: &RepoRef) -> Result<(String, Duration)> {
    let file = tokio::fs::File::open(repo.cache_dir.join(FRESHNESS_FILE)).await?;
    let mut text = String::new();
    file.take(MAX_FRESHNESS_BYTES + 1)
        .read_to_string(&mut text)
        .await?;
    anyhow::ensure!(
        text.len() <= MAX_FRESHNESS_BYTES as usize,
        "oversized freshness record"
    );
    let record: serde_json::Value = serde_json::from_str(&text)?;
    anyhow::ensure!(
        record["version"] == 1
            && record["directory"] == directory_identity(&repo.cache_dir).await?
            && record["repository"] == repository_identity(repo),
        "freshness identity mismatch"
    );
    let observed = record["observed_ms"]
        .as_u64()
        .context("invalid freshness time")?;
    let age = unix_millis()?
        .checked_sub(observed)
        .context("future freshness time")?;
    Ok((text, Duration::from_millis(age)))
}

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

/// Client gone before EOF (`served` closed unsent): kill the process group, so
/// `pack-objects` stops packing for nobody.
/// Holds `_permit` until the child is reaped, so the cap covers the whole pack.
async fn reap_upload_pack(
    mut child: Child,
    served: oneshot::Receiver<()>,
    _permit: Option<OwnedSemaphorePermit>,
    repo: String,
    _lease: Option<RequestLease>,
) {
    let status = tokio::select! {
        biased;
        status = child.wait() => status,
        result = served => {
            if result.is_err() {
                tracing::debug!(repo = %repo, "client gone before end of pack; killing upload-pack");
                if let Err(e) = kill_group(&child) {
                    // Still stop the leader, so the wait below cannot hang.
                    tracing::warn!(repo = %repo, "kill upload-pack process group failed: {e}");
                    let _ = child.start_kill();
                }
                let _ = child.wait().await;
                return;
            }
            child.wait().await
        }
    };
    match status {
        Ok(s) if !s.success() => tracing::warn!(repo = %repo, "git upload-pack exited: {s}"),
        Err(e) => tracing::warn!(repo = %repo, "git upload-pack wait failed: {e}"),
        _ => {}
    }
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
    /// Sent at EOF. Dropped unsent, it makes `reap_upload_pack` kill the child.
    served: Option<oneshot::Sender<()>>,
}

impl<R> TimedReader<R> {
    fn finish(&mut self) {
        if let Some(tx) = self.served.take() {
            let _ = tx.send(());
        }
        self.record();
    }

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
            this.finish();
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
    async fn request_setup_retains_permit_in_lease() {
        for stale in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mirror = dir.path().join("mirror.git");
            std::fs::create_dir(&mirror).unwrap();
            std::fs::write(mirror.join("HEAD"), "ref: refs/heads/main\n").unwrap();
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
            )
            .with_stale(Duration::from_secs(if stale { 60 } else { 0 }), 1)
            .with_request_limit(1);
            let repo = RepoRef {
                name: "repo.git".into(),
                upstream_url: String::new(),
                cache_dir: mirror,
            };
            let slot = cache.slot(&repo.name).await;
            *slot.fetch_lock.lock().await = Some(Instant::now());
            if stale {
                write_freshness(&repo, &repo.cache_dir, unix_millis().unwrap())
                    .await
                    .unwrap();
            }
            slot.refresh.lock().await.last_success = Some(Instant::now());
            let (_, lease) = cache.prepare(repo, true, Bytes::new(), None).await.unwrap();
            assert_eq!(
                cache.request_setups.as_ref().unwrap().available_permits(),
                0
            );
            drop(lease);
            assert_eq!(
                cache.request_setups.as_ref().unwrap().available_permits(),
                1
            );
            cache.shutdown().await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn request_setup_waits_for_a_permit_before_spawning() {
        let dir = tempfile::tempdir().unwrap();
        let cache = GitCache::new(
            GitConfig {
                git_binary: "false".into(),
                upstream_auth_header: None,
                big_file_threshold: "8m".into(),
                fetch_ttl: Duration::from_secs(10),
                max_wants: 100,
                max_upload_packs: 0,
                pack_threads: None,
                pack_window_memory: None,
            },
            Arc::new(Metrics::new()),
            None,
        )
        .with_request_limit(1);
        let permit = cache
            .request_setups
            .as_ref()
            .unwrap()
            .acquire()
            .await
            .unwrap();
        let repo = RepoRef {
            name: "repo.git".into(),
            upstream_url: String::new(),
            cache_dir: dir.path().join("mirror.git"),
        };
        let setup = cache.prepare(repo, true, Bytes::new(), None);
        tokio::pin!(setup);
        assert!(
            tokio::time::timeout(Duration::from_secs(1), &mut setup)
                .await
                .is_err()
        );
        assert!(cache.tasks.lock().unwrap().is_empty());
        drop(permit);
        assert!(setup.await.is_err());
        cache.shutdown().await;
    }

    #[cfg(unix)]
    #[tokio::test(start_paused = true)]
    async fn cancelled_advertisement_holds_mirror_until_child_exits() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let mirror = dir.path().join("mirror.git");
        std::fs::create_dir(&mirror).unwrap();
        std::fs::write(mirror.join("HEAD"), "ref: refs/heads/main\n").unwrap();
        let marker = dir.path().join("started");
        let gate = dir.path().join("gate");
        for fifo in [&marker, &gate] {
            assert!(
                std::process::Command::new("mkfifo")
                    .arg(fifo)
                    .status()
                    .unwrap()
                    .success()
            );
        }
        let wrapper = dir.path().join("git-wrapper");
        std::fs::write(
            &wrapper,
            format!(
                "#!/bin/sh\necho started > '{}'\nread gate < '{}'\n",
                marker.display(),
                gate.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();
        let cache = GitCache::new(
            GitConfig {
                git_binary: wrapper.to_string_lossy().into_owned(),
                upstream_auth_header: None,
                big_file_threshold: "8m".into(),
                fetch_ttl: Duration::from_secs(10),
                max_wants: 100,
                max_upload_packs: 0,
                pack_threads: None,
                pack_window_memory: None,
            },
            Arc::new(Metrics::new()),
            None,
        )
        .with_request_limit(1);
        let repo = RepoRef {
            name: "repo.git".into(),
            upstream_url: String::new(),
            cache_dir: mirror.clone(),
        };
        let started = tokio::spawn(async move { tokio::fs::read_to_string(marker).await.unwrap() });
        let request = tokio::spawn({
            let cache = cache.clone();
            let repo = repo.clone();
            async move { cache.advertise_refs(&repo, None).await }
        });
        assert_eq!(started.await.unwrap().trim(), "started");
        request.abort();
        let _ = request.await;
        assert_eq!(
            cache.request_setups.as_ref().unwrap().available_permits(),
            0
        );
        let eviction = cache.evict(&repo.name, &mirror);
        tokio::pin!(eviction);
        assert!(
            tokio::time::timeout(Duration::from_secs(1), &mut eviction)
                .await
                .is_err()
        );
        assert!(mirror.exists());
        let mut writer = tokio::fs::OpenOptions::new()
            .write(true)
            .open(gate)
            .await
            .unwrap();
        writer.write_all(b"go\n").await.unwrap();
        drop(writer);
        eviction.await.unwrap();
        cache.shutdown().await;
        assert_eq!(
            cache.request_setups.as_ref().unwrap().available_permits(),
            1
        );
        assert!(!mirror.exists());
    }

    #[cfg(unix)]
    #[tokio::test(start_paused = true)]
    async fn failed_refresh_cools_down_and_expired_mirror_fails() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let mirror = dir.path().join("mirror.git");
        std::fs::create_dir(&mirror).unwrap();
        std::fs::write(mirror.join("HEAD"), "ref: refs/heads/main\n").unwrap();
        let log = dir.path().join("attempts");
        let wrapper = dir.path().join("git-wrapper");
        std::fs::write(
            &wrapper,
            format!("#!/bin/sh\necho fetch >> '{}'\nexit 1\n", log.display()),
        )
        .unwrap();
        std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();
        let cache = GitCache::new(
            GitConfig {
                git_binary: wrapper.to_string_lossy().into_owned(),
                upstream_auth_header: None,
                big_file_threshold: "8m".into(),
                fetch_ttl: Duration::from_secs(10),
                max_wants: 100,
                max_upload_packs: 0,
                pack_threads: None,
                pack_window_memory: None,
            },
            Arc::new(Metrics::new()),
            None,
        )
        .with_stale(Duration::from_secs(12), 1);
        let repo = RepoRef {
            name: "repo.git".into(),
            upstream_url: String::new(),
            cache_dir: mirror,
        };
        write_freshness(&repo, &repo.cache_dir, unix_millis().unwrap())
            .await
            .unwrap();
        cache
            .restore_freshness(&repo, cache.slot(&repo.name).await.as_ref())
            .await;
        tokio::time::advance(Duration::from_secs(10)).await;
        assert!(matches!(
            cache.ensure_fresh(&repo, true, &[], None).await.unwrap(),
            CacheOutcome::Cached
        ));
        cache.shutdown().await;
        for _ in 0..3 {
            assert!(matches!(
                cache.ensure_fresh(&repo, true, &[], None).await.unwrap(),
                CacheOutcome::Cached
            ));
        }
        tokio::time::advance(Duration::from_secs(3)).await;
        assert!(cache.ensure_fresh(&repo, true, &[], None).await.is_err());
        assert_eq!(std::fs::read_to_string(&log).unwrap().lines().count(), 1);
        tokio::time::advance(Duration::from_secs(7)).await;
        assert!(cache.ensure_fresh(&repo, true, &[], None).await.is_err());
        assert_eq!(std::fs::read_to_string(&log).unwrap().lines().count(), 2);
        tokio::fs::remove_file(repo.cache_dir.join(FRESHNESS_FILE))
            .await
            .unwrap();
        let restarted = GitCache::new(cache.cfg.clone(), Arc::new(Metrics::new()), None)
            .with_stale(Duration::from_secs(12), 1);
        assert!(
            restarted
                .ensure_fresh(&repo, false, &[], None)
                .await
                .is_err()
        );
        assert_eq!(std::fs::read_to_string(&log).unwrap().lines().count(), 3);
    }

    #[cfg(unix)]
    #[tokio::test(start_paused = true)]
    async fn stale_reads_share_a_background_fetch() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let origin = dir.path().join("origin.git");
        let work = dir.path().join("work");
        std::fs::create_dir(&work).unwrap();
        assert!(
            std::process::Command::new("git")
                .args(["init", "--quiet", "-b", "main"])
                .arg(&work)
                .status()
                .unwrap()
                .success()
        );
        assert!(
            std::process::Command::new("git")
                .current_dir(&work)
                .args([
                    "-c",
                    "user.name=test",
                    "-c",
                    "user.email=test@example.invalid",
                    "commit",
                    "--quiet",
                    "--allow-empty",
                    "-m",
                    "initial"
                ])
                .status()
                .unwrap()
                .success()
        );
        let head = std::process::Command::new("git")
            .current_dir(&work)
            .args(["rev-parse", "HEAD"])
            .output()
            .unwrap();
        let head = String::from_utf8(head.stdout).unwrap();
        assert!(
            std::process::Command::new("git")
                .args(["clone", "--quiet", "--mirror"])
                .arg(&work)
                .arg(&origin)
                .status()
                .unwrap()
                .success()
        );
        let marker = dir.path().join("started");
        let gate = dir.path().join("gate");
        for fifo in [&marker, &gate] {
            assert!(
                std::process::Command::new("mkfifo")
                    .arg(fifo)
                    .status()
                    .unwrap()
                    .success()
            );
        }
        let wrapper = dir.path().join("git-wrapper");
        let script = format!(
            "#!/bin/sh\nfor arg do\n  if [ \"$arg\" = fetch ]; then\n    echo started > '{}'\n    read gate < '{}'\n    break\n  fi\ndone\nexec git \"$@\"\n",
            marker.display(),
            gate.display()
        );
        std::fs::write(&wrapper, script).unwrap();
        std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();
        let cache = GitCache::new(
            GitConfig {
                git_binary: wrapper.to_string_lossy().into_owned(),
                upstream_auth_header: None,
                big_file_threshold: "8m".into(),
                fetch_ttl: Duration::from_secs(10),
                max_wants: 100,
                max_upload_packs: 0,
                pack_threads: None,
                pack_window_memory: None,
            },
            Arc::new(Metrics::new()),
            None,
        )
        .with_stale(Duration::from_secs(100), 1);
        let repo = RepoRef {
            name: "repo.git".into(),
            upstream_url: origin.to_string_lossy().into_owned(),
            cache_dir: dir.path().join("mirror.git"),
        };
        assert!(matches!(
            cache.ensure_fresh(&repo, true, &[], None).await.unwrap(),
            CacheOutcome::Cloned
        ));
        let other = RepoRef {
            name: "other.git".into(),
            upstream_url: origin.to_string_lossy().into_owned(),
            cache_dir: dir.path().join("other.git"),
        };
        assert!(matches!(
            cache.ensure_fresh(&other, true, &[], None).await.unwrap(),
            CacheOutcome::Cloned
        ));
        tokio::time::advance(Duration::from_secs(11)).await;
        let started = tokio::spawn({
            let marker = marker.clone();
            async move { tokio::fs::read_to_string(marker).await.unwrap() }
        });
        let (ready_tx, ready_rx) = oneshot::channel();
        let trigger = tokio::spawn({
            let cache = cache.clone();
            let repo = repo.clone();
            async move {
                let (outcome, _guard) =
                    cache.prepare(repo, true, Bytes::new(), None).await.unwrap();
                assert!(matches!(outcome, CacheOutcome::Cached));
                let _ = ready_tx.send(());
                std::future::pending::<()>().await;
            }
        });
        ready_rx.await.unwrap();
        assert_eq!(started.await.unwrap().trim(), "started");
        trigger.abort();
        let _ = trigger.await;
        tokio::time::advance(Duration::from_secs(20)).await;
        assert!(matches!(
            cache.ensure_fresh(&other, true, &[], None).await.unwrap(),
            CacheOutcome::Cached
        ));
        assert!(!cache.slot(&other.name).await.refresh.lock().await.in_flight);
        assert!(matches!(
            cache.ensure_fresh(&repo, true, &[], None).await.unwrap(),
            CacheOutcome::Cached
        ));
        assert!(!cache.advertise_refs(&repo, None).await.unwrap().is_empty());
        let mut request = pkt_line(&format!("want {}\n", head.trim()));
        request.extend_from_slice(b"0000");
        request.extend(pkt_line("done\n"));
        let request = Bytes::from(request);
        let (_, guard) = cache
            .prepare(repo.clone(), false, request.clone(), None)
            .await
            .unwrap();
        let stream = cache
            .upload_pack_rpc_guarded(&repo, None, request, guard)
            .await
            .unwrap();
        let pack = axum::body::to_bytes(axum::body::Body::from_stream(stream), usize::MAX)
            .await
            .unwrap();
        assert!(pack.windows(4).any(|bytes| bytes == b"PACK"));
        let tree = std::process::Command::new("git")
            .current_dir(&work)
            .args(["rev-parse", "HEAD^{tree}"])
            .output()
            .unwrap();
        let tree = String::from_utf8(tree.stdout).unwrap();
        let replacement = std::process::Command::new("git")
            .current_dir(&work)
            .args([
                "-c",
                "user.name=test",
                "-c",
                "user.email=test@example.invalid",
                "commit-tree",
                tree.trim(),
                "-m",
                "replacement",
            ])
            .output()
            .unwrap();
        assert!(replacement.status.success());
        let replacement = String::from_utf8(replacement.stdout).unwrap();
        assert!(
            std::process::Command::new("git")
                .current_dir(&work)
                .args([
                    "push",
                    "--quiet",
                    "--force",
                    origin.to_str().unwrap(),
                    &format!("{}:refs/heads/main", replacement.trim())
                ])
                .status()
                .unwrap()
                .success()
        );
        tokio::time::advance(Duration::from_secs(70)).await;
        let expired = cache.prepare(repo.clone(), true, Bytes::new(), None).await;
        assert!(expired.err().unwrap().is::<CacheBusy>());
        let mut writer = tokio::fs::OpenOptions::new()
            .write(true)
            .open(&gate)
            .await
            .unwrap();
        writer.write_all(b"go\n").await.unwrap();
        drop(writer);
        cache.shutdown().await;
        let (outcome, guard) = cache
            .prepare(repo.clone(), true, Bytes::new(), None)
            .await
            .unwrap();
        assert!(matches!(outcome, CacheOutcome::Cached));
        drop(guard);
        assert!(!cache.slot(&repo.name).await.refresh.lock().await.in_flight);
        let current = std::process::Command::new("git")
            .arg("--git-dir")
            .arg(&repo.cache_dir)
            .args(["rev-parse", "refs/heads/main"])
            .output()
            .unwrap();
        assert_eq!(
            String::from_utf8(current.stdout).unwrap().trim(),
            replacement.trim()
        );
        assert!(matches!(
            cache.ensure_fresh(&repo, true, &[], None).await.unwrap(),
            CacheOutcome::Cached
        ));
        assert!(!cache.slot(&repo.name).await.refresh.lock().await.in_flight);
        let mut request = pkt_line(&format!("want {}\n", head.trim()));
        request.extend_from_slice(b"0000");
        request.extend(pkt_line("done\n"));
        let stream = cache
            .upload_pack_rpc(&repo, None, Bytes::from(request))
            .await
            .unwrap();
        let pack = axum::body::to_bytes(axum::body::Body::from_stream(stream), usize::MAX)
            .await
            .unwrap();
        assert!(pack.windows(4).any(|bytes| bytes == b"PACK"));
        assert!(
            std::process::Command::new("git")
                .arg("--git-dir")
                .arg(&origin)
                .args(["update-ref", "-d", "refs/heads/main"])
                .status()
                .unwrap()
                .success()
        );
        let started = tokio::spawn(async move { tokio::fs::read_to_string(marker).await.unwrap() });
        let fetch = tokio::spawn({
            let cache = cache.clone();
            let repo = repo.clone();
            async move { cache.fetch(&repo, None).await.unwrap() }
        });
        assert_eq!(started.await.unwrap().trim(), "started");
        let mut writer = tokio::fs::OpenOptions::new()
            .write(true)
            .open(&gate)
            .await
            .unwrap();
        writer.write_all(b"go\n").await.unwrap();
        drop(writer);
        fetch.await.unwrap();
        assert!(
            std::process::Command::new("git")
                .arg("--git-dir")
                .arg(&repo.cache_dir)
                .args(["cat-file", "-e", head.trim()])
                .status()
                .unwrap()
                .success()
        );
        let mut request = pkt_line(&format!("want {}\n", head.trim()));
        request.extend_from_slice(b"0000");
        request.extend(pkt_line("done\n"));
        let stream = cache
            .upload_pack_rpc(&repo, None, Bytes::from(request))
            .await
            .unwrap();
        let pack = axum::body::to_bytes(axum::body::Body::from_stream(stream), usize::MAX)
            .await
            .unwrap();
        assert!(pack.windows(4).any(|bytes| bytes == b"PACK"));
        let missing = "f".repeat(40);
        let mut request = pkt_line(&format!("want {missing}\n"));
        request.extend_from_slice(b"0000");
        request.extend(pkt_line("done\n"));
        let stream = cache
            .upload_pack_rpc(&repo, None, Bytes::from(request))
            .await
            .unwrap();
        let rejected = axum::body::to_bytes(axum::body::Body::from_stream(stream), usize::MAX)
            .await
            .unwrap();
        assert!(!rejected.windows(4).any(|bytes| bytes == b"PACK"));
    }

    #[tokio::test(start_paused = true)]
    async fn eviction_waits_for_active_reader() {
        let dir = tempfile::tempdir().unwrap();
        let mirror = dir.path().join("repo.git");
        tokio::fs::create_dir(&mirror).await.unwrap();
        tokio::fs::write(mirror.join("HEAD"), "ref: refs/heads/main\n")
            .await
            .unwrap();
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
            name: "repo.git".into(),
            upstream_url: String::new(),
            cache_dir: mirror.clone(),
        };
        let guard = cache.read_guard(&repo).await;
        let eviction = cache.evict(&repo.name, &mirror);
        tokio::pin!(eviction);
        assert!(
            tokio::time::timeout(Duration::from_secs(1), &mut eviction)
                .await
                .is_err()
        );
        assert!(mirror.join("HEAD").exists());
        drop(guard);
        eviction.await.unwrap();
        assert!(!mirror.exists());
    }

    #[tokio::test]
    async fn upstream_commands_disable_automatic_maintenance() {
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
        let args: Vec<_> = cache
            .local_cmd(None)
            .as_std()
            .get_args()
            .map(|arg| arg.to_owned())
            .collect();
        assert!(
            !args
                .iter()
                .any(|arg| arg == "uploadpack.allowAnySHA1InWant=true")
        );
        let args: Vec<_> = cache
            .fetch_cmd(None)
            .as_std()
            .get_args()
            .map(|arg| arg.to_owned())
            .collect();
        assert!(!args.iter().any(|arg| arg == "gc.auto=0"));
        let cache = cache.with_stale(Duration::from_secs(60), 1);
        let args: Vec<_> = cache
            .local_cmd(None)
            .as_std()
            .get_args()
            .map(|arg| arg.to_owned())
            .collect();
        assert!(
            args.iter()
                .any(|arg| arg == "uploadpack.allowAnySHA1InWant=true")
        );
        for (key, expected) in [("gc.auto", "0"), ("maintenance.auto", "false")] {
            let output = cache
                .fetch_cmd(None)
                .args(["config", "--get", key])
                .output()
                .await
                .unwrap();
            assert!(output.status.success());
            assert_eq!(String::from_utf8(output.stdout).unwrap().trim(), expected);
        }
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
            served: None,
        };
        // Reading to EOF drives the final zero-byte read, which records once.
        let mut sink = Vec::new();
        reader.read_to_end(&mut sink).await.unwrap();
        assert!(metrics.gather().contains(
            r#"gitcacheproxy_serve_duration_seconds_count{kind="upload_pack",repo="group/foo.git"} 1"#
        ));
    }

    #[tokio::test]
    async fn timed_reader_signals_served_only_at_eof() {
        use tokio::io::AsyncReadExt;

        let (tx, rx) = oneshot::channel();
        let mut reader = timed_reader(tx);
        reader.read_to_end(&mut Vec::new()).await.unwrap();
        drop(reader);
        assert!(rx.await.is_ok(), "EOF must tell the reaper not to kill");

        let (tx, rx) = oneshot::channel();
        let mut reader = timed_reader(tx);
        let mut first = [0u8; 4];
        reader.read_exact(&mut first).await.unwrap();
        drop(reader);
        assert!(
            rx.await.is_err(),
            "drop before EOF must tell the reaper to kill"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn reaper_kills_leader_when_group_kill_fails() {
        // Without `process_group(0)` no group has the child's ID, so `killpg`
        // fails and only the leader fallback can end the wait.
        let child = Command::new("sleep").arg("30").spawn().unwrap();
        let (tx, rx) = oneshot::channel();
        drop(tx);
        tokio::time::timeout(
            Duration::from_secs(10),
            reap_upload_pack(child, rx, None, "repo.git".into(), None),
        )
        .await
        .expect("reaper should kill the leader and return");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn reaper_leaves_a_fully_served_child_running() {
        // `read` blocks until stdin closes. The test holds stdin, because
        // `Child::wait` closes a stdin the child still owns.
        let mut child = Command::new("sh")
            .args(["-c", "read _"])
            .stdin(Stdio::piped())
            .spawn()
            .unwrap();
        let _stdin = child.stdin.take();
        let (tx, rx) = oneshot::channel();
        tx.send(()).unwrap();
        let reaped = tokio::time::timeout(
            Duration::from_millis(200),
            reap_upload_pack(child, rx, None, "repo.git".into(), None),
        )
        .await;
        assert!(reaped.is_err(), "EOF must not kill upload-pack");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn kill_group_refuses_a_reaped_child() {
        let mut child = Command::new("true").spawn().unwrap();
        child.wait().await.unwrap();
        assert!(kill_group(&child).is_err());
    }

    #[tokio::test]
    async fn freshness_survives_restart_and_rejects_invalid_or_replaced_mirrors() {
        let (dir, repo, cache) = freshness_fixture();
        cache.ensure_fresh(&repo, true, &[], None).await.unwrap();
        let marker = repo.cache_dir.join(FRESHNESS_FILE);
        let original = tokio::fs::read_to_string(&marker).await.unwrap();
        let mut offline = cache.cfg.clone();
        offline.git_binary = "false".into();
        let restarted = || {
            GitCache::new(offline.clone(), Arc::new(Metrics::new()), None)
                .with_stale(Duration::from_secs(60), 1)
        };
        assert!(matches!(
            restarted()
                .ensure_fresh(&repo, true, &[], None)
                .await
                .unwrap(),
            CacheOutcome::Cached
        ));
        let mut other = repo.clone();
        other.upstream_url.push_str("-different");
        assert!(
            restarted()
                .ensure_fresh(&other, true, &[], None)
                .await
                .is_err()
        );
        other = repo.clone();
        other.name.push_str("-different");
        assert!(
            restarted()
                .ensure_fresh(&other, true, &[], None)
                .await
                .is_err()
        );
        for (key, value) in [
            ("version", serde_json::json!(2)),
            ("directory", serde_json::json!("replacement")),
            ("repository", serde_json::json!("other")),
            (
                "observed_ms",
                serde_json::json!(unix_millis().unwrap() + 60_000),
            ),
            ("observed_ms", serde_json::json!("invalid")),
            (
                "observed_ms",
                serde_json::json!(unix_millis().unwrap() - 61_000),
            ),
        ] {
            let mut record: serde_json::Value = serde_json::from_str(&original).unwrap();
            record[key] = value;
            tokio::fs::write(&marker, record.to_string()).await.unwrap();
            assert!(
                restarted()
                    .ensure_fresh(&repo, false, &[], None)
                    .await
                    .is_err(),
                "{key}"
            );
        }
        for invalid in ["{".to_owned(), "x".repeat(MAX_FRESHNESS_BYTES as usize + 1)] {
            tokio::fs::write(&marker, invalid).await.unwrap();
            assert!(
                restarted()
                    .ensure_fresh(&repo, true, &[], None)
                    .await
                    .is_err()
            );
        }
        tokio::fs::remove_file(&marker).await.unwrap();
        tokio::fs::write(repo.cache_dir.join("unrelated"), "changed directory mtime")
            .await
            .unwrap();
        assert!(
            restarted()
                .ensure_fresh(&repo, true, &[], None)
                .await
                .is_err()
        );
        tokio::fs::rename(&repo.cache_dir, dir.path().join("old.git"))
            .await
            .unwrap();
        fixture_git(
            dir.path(),
            &[
                "clone",
                "--mirror",
                &repo.upstream_url,
                repo.cache_dir.to_str().unwrap(),
            ],
        );
        tokio::fs::write(&marker, original).await.unwrap();
        assert!(
            restarted()
                .ensure_fresh(&repo, true, &[], None)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn full_refs_renew_freshness_but_sha_and_failed_metadata_do_not() {
        let (dir, repo, cache) = freshness_fixture();
        cache.ensure_fresh(&repo, true, &[], None).await.unwrap();
        let marker = repo.cache_dir.join(FRESHNESS_FILE);
        let original = tokio::fs::read_to_string(&marker).await.unwrap();
        let origin = Path::new(&repo.upstream_url);
        let tree = fixture_git(origin, &["rev-parse", "HEAD^{tree}"]);
        let oid = fixture_git(
            origin,
            &[
                "commit-tree",
                tree.trim(),
                "-p",
                "HEAD",
                "-m",
                "unadvertised",
            ],
        );
        let body = pkt_line(&format!("want {}\n", oid.trim()));
        cache.ensure_wanted_oids(&repo, &body, None).await.unwrap();
        assert_eq!(tokio::fs::read_to_string(&marker).await.unwrap(), original);
        assert_eq!(
            fixture_git(
                &repo.cache_dir,
                &["rev-parse", &format!("{PROXY_WANTS_NS}/{}", oid.trim())]
            )
            .trim(),
            oid.trim()
        );
        fixture_git(origin, &["update-ref", "refs/heads/second", oid.trim()]);
        fixture_git(origin, &["update-ref", "refs/tags/new", oid.trim()]);
        fixture_git(origin, &["update-ref", "-d", "refs/heads/main"]);
        tokio::fs::create_dir(repo.cache_dir.join("proxy-refresh.tmp"))
            .await
            .unwrap();
        assert!(cache.fetch(&repo, None).await.is_err());
        assert_eq!(tokio::fs::read_to_string(&marker).await.unwrap(), original);
        tokio::fs::remove_dir(repo.cache_dir.join("proxy-refresh.tmp"))
            .await
            .unwrap();
        cache.fetch(&repo, None).await.unwrap();
        assert_eq!(
            fixture_git(&repo.cache_dir, &["rev-parse", "refs/heads/second"]).trim(),
            oid.trim()
        );
        assert_eq!(
            fixture_git(&repo.cache_dir, &["rev-parse", "refs/tags/new"]).trim(),
            oid.trim()
        );
        assert!(
            !std::process::Command::new("git")
                .current_dir(&repo.cache_dir)
                .args(["show-ref", "--verify", "--quiet", "refs/heads/main"])
                .status()
                .unwrap()
                .success()
        );
        let record = read_freshness(&repo).await.unwrap();
        assert!(record.1 < Duration::from_secs(60));
        let _ = dir;
    }

    #[cfg(unix)]
    #[tokio::test(start_paused = true)]
    async fn slow_successful_cold_clone_cannot_exceed_stale_bound() {
        tokio::time::resume();
        let (dir, repo, mut cache) = freshness_fixture();
        let ready = dir.path().join("ready");
        let gate = dir.path().join("gate");
        for path in [&ready, &gate] {
            assert!(
                std::process::Command::new("mkfifo")
                    .arg(path)
                    .status()
                    .unwrap()
                    .success()
            );
        }
        cache.cfg.git_binary = fixture_script(
            dir.path(),
            &format!(
                "#!/bin/sh\ngit \"$@\" || exit $?\necho ready > '{}'\nread gate < '{}'\n",
                ready.display(),
                gate.display()
            ),
        );
        let owner = tokio::spawn({
            let cache = cache.clone();
            let repo = repo.clone();
            async move { cache.ensure_fresh(&repo, true, &[], None).await }
        });
        tokio::fs::read_to_string(ready).await.unwrap();
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(61)).await;
        tokio::time::resume();
        let mut writer = tokio::fs::OpenOptions::new()
            .write(true)
            .open(gate)
            .await
            .unwrap();
        writer.write_all(b"go\n").await.unwrap();
        drop(writer);
        assert!(
            owner
                .await
                .unwrap()
                .unwrap_err()
                .to_string()
                .contains("maximum stale age")
        );
        assert!(repo.cache_dir.join(FRESHNESS_FILE).exists());
        cache.shutdown().await;
    }

    #[test]
    fn repository_identity_redacts_transport_secrets() {
        let mut repo = RepoRef {
            name: "repo.git".into(),
            upstream_url: "https://user:secret@example.invalid/repo.git?token=secret#fragment"
                .into(),
            cache_dir: "/unused".into(),
        };
        let identity = repository_identity(&repo);
        repo.upstream_url = "https://example.invalid/repo.git".into();
        assert_eq!(identity, repository_identity(&repo));
        assert!(!identity.contains("secret"));
    }

    #[tokio::test]
    async fn refresh_uses_configured_upstream_after_identity_changes() {
        let (dir, mut repo, cache) = freshness_fixture();
        cache.ensure_fresh(&repo, true, &[], None).await.unwrap();
        let origin = dir.path().join("new-origin.git");
        fixture_git(
            dir.path(),
            &[
                "clone",
                "--mirror",
                &repo.upstream_url,
                origin.to_str().unwrap(),
            ],
        );
        let tree = fixture_git(&origin, &["rev-parse", "HEAD^{tree}"]);
        let head = fixture_git(
            &origin,
            &[
                "commit-tree",
                tree.trim(),
                "-p",
                "HEAD",
                "-m",
                "new upstream",
            ],
        );
        fixture_git(&origin, &["update-ref", "refs/heads/main", head.trim()]);
        repo.upstream_url = origin.to_string_lossy().into_owned();
        assert!(matches!(
            cache.ensure_fresh(&repo, true, &[], None).await.unwrap(),
            CacheOutcome::Fetched
        ));
        assert_eq!(
            fixture_git(&repo.cache_dir, &["rev-parse", "HEAD"]).trim(),
            head.trim()
        );
        assert!(read_freshness(&repo).await.is_ok());
        let next = fixture_git(
            &origin,
            &[
                "commit-tree",
                tree.trim(),
                "-p",
                head.trim(),
                "-m",
                "sha upstream",
            ],
        );
        cache
            .ensure_wanted_oids(&repo, &pkt_line(&format!("want {}\n", next.trim())), None)
            .await
            .unwrap();
        assert_eq!(
            fixture_git(
                &repo.cache_dir,
                &["rev-parse", &format!("{PROXY_WANTS_NS}/{}", next.trim())]
            )
            .trim(),
            next.trim()
        );
    }

    #[tokio::test]
    async fn upload_admission_rejects_unknown_or_changed_identity_after_prepare() {
        for invalid in ["missing", "directory", "repository"] {
            let (_dir, repo, cache) = freshness_fixture();
            let cache = cache.with_request_limit(1);
            let (_, lease) = cache
                .prepare(repo.clone(), true, Bytes::new(), None)
                .await
                .unwrap();
            let marker = repo.cache_dir.join(FRESHNESS_FILE);
            if invalid == "missing" {
                tokio::fs::remove_file(marker).await.unwrap();
            } else {
                let mut record: serde_json::Value =
                    serde_json::from_slice(&tokio::fs::read(&marker).await.unwrap()).unwrap();
                record[invalid] = serde_json::json!("changed");
                tokio::fs::write(marker, record.to_string()).await.unwrap();
            }
            let result = cache
                .upload_pack_rpc_guarded(&repo, None, Bytes::new(), lease)
                .await;
            assert_eq!(
                result.err().unwrap().to_string(),
                "mirror exceeds maximum stale age"
            );
            assert_eq!(cache.upload_packs.as_ref().unwrap().available_permits(), 1);
            assert_eq!(
                cache.request_setups.as_ref().unwrap().available_permits(),
                1
            );
            assert!(cache.slot(&repo.name).await.lifecycle.try_write().is_ok());
            cache.shutdown().await;
        }
    }

    #[tokio::test]
    async fn queued_upload_rechecks_age_before_spawning_and_releases_leases() {
        let (_dir, repo, cache) = freshness_fixture();
        let mut cache = cache.with_request_limit(1);
        let (_, lease) = cache
            .prepare(repo.clone(), true, Bytes::new(), None)
            .await
            .unwrap();
        cache.cfg.git_binary = "missing-upload-pack-binary".into();
        let blocked = cache
            .upload_packs
            .as_ref()
            .unwrap()
            .clone()
            .acquire_owned()
            .await
            .unwrap();
        let mut upload = Box::pin(cache.upload_pack_rpc_guarded(&repo, None, Bytes::new(), lease));
        assert!(matches!(
            std::future::poll_fn(|cx| Poll::Ready(upload.as_mut().poll(cx))).await,
            Poll::Pending
        ));
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(61)).await;
        tokio::time::resume();
        drop(blocked);
        assert_eq!(
            upload.await.err().unwrap().to_string(),
            "mirror exceeds maximum stale age"
        );
        assert_eq!(cache.upload_packs.as_ref().unwrap().available_permits(), 1);
        assert_eq!(
            cache.request_setups.as_ref().unwrap().available_permits(),
            1
        );
        assert!(cache.slot(&repo.name).await.lifecycle.try_write().is_ok());
    }

    #[tokio::test]
    async fn admitted_pack_can_finish_after_its_refs_expire() {
        let (_dir, repo, cache) = freshness_fixture();
        cache.ensure_fresh(&repo, true, &[], None).await.unwrap();
        let oid = fixture_git(&repo.cache_dir, &["rev-parse", "HEAD"]);
        let mut request = pkt_line(&format!("want {}\n", oid.trim()));
        request.extend_from_slice(b"0000");
        request.extend(pkt_line("done\n"));
        let stream = cache
            .upload_pack_rpc(&repo, None, Bytes::from(request))
            .await
            .unwrap();
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(61)).await;
        tokio::time::resume();
        let pack = axum::body::to_bytes(axum::body::Body::from_stream(stream), usize::MAX)
            .await
            .unwrap();
        assert!(pack.windows(4).any(|bytes| bytes == b"PACK"));
        cache.shutdown().await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn slow_upstream_does_not_take_local_capacity_or_lose_client_credentials() {
        let (dir, repo, mut cache) = freshness_fixture();
        cache.ensure_fresh(&repo, true, &[], None).await.unwrap();
        let ready = dir.path().join("ready");
        let gate = dir.path().join("gate");
        let header = dir.path().join("header");
        for fifo in [&ready, &gate] {
            assert!(
                std::process::Command::new("mkfifo")
                    .arg(fifo)
                    .status()
                    .unwrap()
                    .success()
            );
        }
        cache.cfg.git_binary = fixture_script(
            dir.path(),
            &format!(
                "#!/bin/sh\nfor arg do\n if [ \"$arg\" = clone ]; then\n  printf '%s' \"$GIT_CONFIG_VALUE_2\" > '{}'\n  echo ready > '{}'\n  read gate < '{}'\n fi\ndone\nexec git \"$@\"\n",
                header.display(),
                ready.display(),
                gate.display()
            ),
        );
        let cache = cache.with_request_limit(1);
        let mut cold = repo.clone();
        cold.name = "cold.git".into();
        cold.cache_dir = dir.path().join("cold.git");
        let request = tokio::spawn({
            let cache = cache.clone();
            let cold = cold.clone();
            async move {
                cache
                    .prepare(
                        cold,
                        true,
                        Bytes::new(),
                        Some("Authorization: Bearer fixture-client"),
                    )
                    .await
            }
        });
        assert_eq!(
            tokio::fs::read_to_string(ready).await.unwrap().trim(),
            "ready"
        );
        request.abort();
        assert!(request.await.err().unwrap().is_cancelled());
        let busy = cache
            .prepare(cold.clone(), true, Bytes::new(), None)
            .await
            .err()
            .unwrap();
        assert!(busy.is::<CacheBusy>());
        let (_, lease) = cache
            .prepare(repo.clone(), true, Bytes::new(), None)
            .await
            .unwrap();
        assert!(
            !cache
                .advertise_refs_guarded(&repo, None, lease)
                .await
                .unwrap()
                .is_empty()
        );
        let metrics = Arc::new(Metrics::new());
        let cache_root = dir.path().join("cache");
        let upstream_base = dir.path().join("upstream").to_string_lossy().into_owned();
        let state = crate::server::AppState {
            cache: Arc::new(cache.clone()),
            lfs: Arc::new(crate::lfs::Lfs::new(
                crate::lfs::LfsConfig {
                    upstream_base: upstream_base.clone(),
                    cache_root: cache_root.clone(),
                    upstream_auth_header: None,
                },
                None,
            )),
            upstream_base,
            cache_root,
            auth: Arc::new(crate::auth::Auth::anonymous()),
            upstream_credential: crate::config::UpstreamCredential::Static,
            max_decoded_body: 1024,
            max_concurrent: 1,
            metrics,
        };
        use tower::ServiceExt;
        let blocked_body = cache.body_permit().unwrap();
        let app = crate::server::router(state);
        let healthy = app
            .clone()
            .oneshot(
                axum::http::Request::get("/repo.git/info/refs?service=git-upload-pack")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(healthy.status(), axum::http::StatusCode::OK);
        let overloaded = app
            .oneshot(
                axum::http::Request::post("/repo.git/git-upload-pack")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            overloaded.status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
        drop(blocked_body);
        assert_eq!(cache.upstream.available_permits(), 0);
        assert_eq!(
            tokio::fs::read_to_string(header).await.unwrap(),
            "Authorization: Bearer fixture-client"
        );
        let mut writer = tokio::fs::OpenOptions::new()
            .write(true)
            .open(gate)
            .await
            .unwrap();
        writer.write_all(b"go\n").await.unwrap();
        drop(writer);
        cache.shutdown().await;
        assert_eq!(cache.upstream.available_permits(), 1);
        assert!(cold.cache_dir.join("HEAD").exists());
        assert!(cache.slot(&cold.name).await.lifecycle.try_write().is_ok());
    }

    #[tokio::test]
    async fn strict_mode_does_not_write_freshness_records() {
        let (_dir, repo, cache) = freshness_fixture();
        let cache = cache.with_stale(Duration::ZERO, 1);
        cache.ensure_fresh(&repo, true, &[], None).await.unwrap();
        assert!(!repo.cache_dir.join(FRESHNESS_FILE).exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn background_refresh_keeps_the_triggering_client_credential() {
        let (dir, repo, mut cache) = freshness_fixture();
        cache.ensure_fresh(&repo, true, &[], None).await.unwrap();
        cache.cfg.fetch_ttl = Duration::ZERO;
        cache
            .slot(&repo.name)
            .await
            .refresh
            .lock()
            .await
            .next_attempt = None;
        let header = dir.path().join("header");
        cache.cfg.git_binary = fixture_script(
            dir.path(),
            &format!(
                "#!/bin/sh\nprintf '%s' \"$GIT_CONFIG_VALUE_2\" > '{}'\nexec git \"$@\"\n",
                header.display()
            ),
        );
        let credential = String::from("Authorization: Bearer background-fixture");
        assert!(matches!(
            cache
                .ensure_fresh(&repo, true, &[], Some(&credential))
                .await
                .unwrap(),
            CacheOutcome::Cached
        ));
        drop(credential);
        cache.shutdown().await;
        assert_eq!(
            tokio::fs::read_to_string(header).await.unwrap(),
            "Authorization: Bearer background-fixture"
        );
        let record = tokio::fs::read_to_string(repo.cache_dir.join(FRESHNESS_FILE))
            .await
            .unwrap();
        assert!(!record.contains("background-fixture"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn stale_preparation_preserves_typed_upstream_auth_failure() {
        let (dir, repo, mut cache) = freshness_fixture();
        cache.cfg.git_binary = fixture_script(
            dir.path(),
            "#!/bin/sh\necho 'fatal: Authentication failed' >&2\nexit 1\n",
        );
        let error = cache
            .prepare(
                repo,
                true,
                Bytes::new(),
                Some("Authorization: Bearer rejected"),
            )
            .await
            .err()
            .unwrap();
        assert!(is_auth_failure(&error));
        assert!(!error.is::<CacheBusy>());
        cache.shutdown().await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn stale_upload_holds_local_capacity_until_cancelled_child_is_reaped() {
        let (dir, repo, mut cache) = freshness_fixture();
        cache.ensure_fresh(&repo, true, &[], None).await.unwrap();
        cache.cfg.git_binary = fixture_script(dir.path(), "#!/bin/sh\nexec sleep 300\n");
        let cache = cache.with_request_limit(1);
        let (_, lease) = cache
            .prepare(repo.clone(), false, Bytes::new(), None)
            .await
            .unwrap();
        let stream = cache
            .upload_pack_rpc_guarded(&repo, None, Bytes::new(), lease)
            .await
            .unwrap();
        assert_eq!(
            cache.request_setups.as_ref().unwrap().available_permits(),
            0
        );
        assert!(cache.slot(&repo.name).await.lifecycle.try_write().is_err());
        drop(stream);
        tokio::time::timeout(Duration::from_secs(5), cache.shutdown())
            .await
            .unwrap();
        assert_eq!(
            cache.request_setups.as_ref().unwrap().available_permits(),
            1
        );
        assert_eq!(cache.upload_packs.as_ref().unwrap().available_permits(), 1);
        assert!(cache.slot(&repo.name).await.lifecycle.try_write().is_ok());
    }

    fn freshness_fixture() -> (tempfile::TempDir, RepoRef, GitCache) {
        let dir = tempfile::tempdir().unwrap();
        let upstream = dir.path().join("upstream");
        std::fs::create_dir(&upstream).unwrap();
        let origin = upstream.join("repo.git");
        fixture_git(
            dir.path(),
            &[
                "init",
                "--bare",
                "-q",
                "-b",
                "main",
                origin.to_str().unwrap(),
            ],
        );
        let tree = fixture_git(&origin, &["hash-object", "-t", "tree", "--stdin", "-w"]);
        let head = fixture_git(&origin, &["commit-tree", tree.trim(), "-m", "initial"]);
        fixture_git(&origin, &["update-ref", "refs/heads/main", head.trim()]);
        let repo = RepoRef {
            name: "repo.git".into(),
            upstream_url: origin.to_string_lossy().into_owned(),
            cache_dir: dir.path().join("cache/repo.git"),
        };
        let cache = GitCache::new(
            GitConfig {
                git_binary: "git".into(),
                upstream_auth_header: None,
                big_file_threshold: "8m".into(),
                fetch_ttl: Duration::from_secs(60),
                max_wants: 10,
                max_upload_packs: 1,
                pack_threads: None,
                pack_window_memory: None,
            },
            Arc::new(Metrics::new()),
            None,
        )
        .with_stale(Duration::from_secs(60), 1);
        (dir, repo, cache)
    }

    fn fixture_git(directory: &Path, args: &[&str]) -> String {
        let output = std::process::Command::new("git")
            .current_dir(directory)
            .args(args)
            .env("GIT_AUTHOR_NAME", "test")
            .env("GIT_AUTHOR_EMAIL", "test@example.invalid")
            .env("GIT_COMMITTER_NAME", "test")
            .env("GIT_COMMITTER_EMAIL", "test@example.invalid")
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap()
    }

    #[cfg(unix)]
    fn fixture_script(directory: &Path, script: &str) -> String {
        use std::os::unix::fs::PermissionsExt;
        let path = directory.join("git-wrapper");
        std::fs::write(&path, script).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        path.to_string_lossy().into_owned()
    }

    /// Collect an iterator of oids into a `HashSet` for order-insensitive assertions
    /// against `parse_wants`.
    fn want_set<I: IntoIterator<Item = String>>(oids: I) -> HashSet<String> {
        oids.into_iter().collect()
    }

    fn timed_reader(served: oneshot::Sender<()>) -> TimedReader<&'static [u8]> {
        TimedReader {
            inner: b"packfile bytes",
            repo: "group/foo.git".into(),
            recorder: Some((Arc::new(Metrics::new()), Instant::now())),
            served: Some(served),
        }
    }
}
