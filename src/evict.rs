// SPDX-License-Identifier: Apache-2.0
//! Bounding the on-disk cache with LRU eviction of idle mirrors.
//!
//! When a byte cap is configured (`--cache-max-mb`), a [`CacheIndex`] tracks every
//! mirror's size and access order in memory. The request path only ever does O(1)
//! bookkeeping against it - never disk IO:
//!   - `touch` on every request (a served cache hit counts as use),
//!   - `mark_changed` after a clone/fetch, flagging the mirror for (re)measurement.
//!
//! All disk work - measuring a changed mirror's size, and evicting mirrors - runs
//! in the background [`run`] task, off the critical path, so a client's clone/fetch
//! is never blocked by cache maintenance. Access order lives in an `lru::LruCache`
//! (a hashmap plus an intrusive list), so `touch` promotes in O(1) and eviction
//! pops the least-recently-used tail until back under the cap - no re-sorting. An
//! evicted mirror is transparently re-cloned on its next request, so eviction is a
//! cache-management concern only, never a correctness one. With no cap set, no index
//! is built and the default path keeps its current zero-overhead unbounded-growth
//! behaviour.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::SystemTime;

use lru::LruCache;
use tokio::sync::{Notify, watch};

use crate::git::GitCache;
use crate::metrics::Metrics;

struct Inner {
    /// Mirror name -> size, kept in access order (front = most-recently-used, back =
    /// least). The `lru` crate does the O(1) promote-on-use and tail eviction.
    cache: LruCache<String, u64>,
    /// Mirrors whose size changed and needs (re)measuring by the background task.
    dirty: HashSet<String>,
    /// Sum of the sizes in `cache`, maintained incrementally so the cap check is
    /// O(1). The byte total, not `LruCache`'s item count, is what bounds the cache.
    total: u64,
}

/// In-memory record of the on-disk mirror cache, plus the eviction trigger. Shared
/// (via `Arc`) between the `GitCache` that mutates it on clone/fetch/serve and the
/// background task in [`run`] that measures and evicts.
pub struct CacheIndex {
    cache_root: PathBuf,
    max_bytes: u64,
    metrics: Arc<Metrics>,
    /// Woken when a mirror changes or the cache may be over cap. `notify_one`
    /// coalesces a burst into a single maintenance pass and stores a permit if the
    /// task is mid-pass, so no wakeup is lost.
    work: Notify,
    state: Mutex<Inner>,
}

impl CacheIndex {
    /// Build the index by scanning `cache_root` once. Seeds access order from each
    /// mirror's newest file mtime (oldest first, so the oldest lands at the LRU
    /// tail), so recency roughly survives a restart. Blocking, but runs at startup
    /// before the server binds.
    pub fn new(cache_root: PathBuf, max_bytes: u64, metrics: Arc<Metrics>) -> Arc<Self> {
        let mut mirrors = find_mirrors(&cache_root);
        mirrors.sort_by_key(|m| m.mtime); // oldest first -> pushed to the LRU tail first
        // Unbounded: the cap is enforced by byte total, not `LruCache`'s item count.
        let mut cache: LruCache<String, u64> = LruCache::unbounded();
        let mut total = 0u64;
        for m in mirrors {
            total += m.size;
            cache.put(m.name, m.size);
        }
        let inner = Inner {
            cache,
            dirty: HashSet::new(),
            total,
        };
        metrics.set_cache_size(total, inner.cache.len());
        let over = total > max_bytes;
        let idx = Arc::new(Self {
            cache_root,
            max_bytes,
            metrics,
            work: Notify::new(),
            state: Mutex::new(inner),
        });
        if over {
            idx.work.notify_one(); // a previous run may have left the cache over-cap
        }
        idx
    }

    /// Mark a repo used. Serving does not grow the cache, so this never wakes the
    /// evictor; it only keeps the access order honest. No-op for a repo not yet
    /// tracked (its first clone tracks it via `mark_changed`).
    pub fn touch(&self, name: &str) {
        // `get` promotes to most-recently-used; the value itself is unused.
        let _ = self.lock().cache.get(name);
    }

    /// Flag a mirror as changed after a clone/fetch: promote it (it was just used),
    /// schedule it for measurement, and wake the background task. O(1) bookkeeping
    /// only - the size walk happens off the request path.
    pub fn mark_changed(&self, name: &str) {
        {
            let mut inner = self.lock();
            // `get` promotes an existing entry; otherwise track it with a placeholder
            // size until the background pass measures it.
            if inner.cache.get(name).is_none() {
                inner.cache.put(name.to_string(), 0);
            }
            inner.dirty.insert(name.to_string());
            self.set_gauges(&inner);
        }
        self.work.notify_one();
    }

    /// On-disk path of a tracked mirror.
    pub fn cache_dir(&self, name: &str) -> PathBuf {
        self.cache_root.join(name)
    }

    /// Current `(total_bytes, mirror_count)` - the values mirrored to the gauges.
    pub fn totals(&self) -> (u64, usize) {
        let inner = self.lock();
        (inner.total, inner.cache.len())
    }

    /// Take the set of mirrors needing (re)measurement, clearing it.
    fn take_dirty(&self) -> Vec<String> {
        self.lock().dirty.drain().collect()
    }

    /// Set a mirror's measured size, adjusting the running total. Called by the
    /// background task after walking the mirror. Uses `peek`/`peek_mut`, which leave
    /// recency untouched - a background measurement is not an access.
    fn set_size(&self, name: &str, size: u64) {
        let mut inner = self.lock();
        let Some(old) = inner.cache.peek(name).copied() else {
            return; // evicted between mark and measure
        };
        inner.total = inner.total - old + size;
        if let Some(v) = inner.cache.peek_mut(name) {
            *v = size;
        }
        self.set_gauges(&inner);
    }

    /// Pop least-recently-used mirrors off the tail until the total would be back
    /// under the cap, removing them from the index. Returns `(name, dir)` for each
    /// so the caller can delete it from disk. Empty when already under cap.
    fn take_victims(&self) -> Vec<(String, PathBuf)> {
        let mut inner = self.lock();
        let mut victims = Vec::new();
        while inner.total > self.max_bytes {
            let Some((name, size)) = inner.cache.pop_lru() else {
                break;
            };
            inner.total -= size;
            let dir = self.cache_root.join(&name);
            victims.push((name, dir));
        }
        self.set_gauges(&inner);
        victims
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.state.lock().expect("cache index lock")
    }

    fn set_gauges(&self, inner: &Inner) {
        self.metrics.set_cache_size(inner.total, inner.cache.len());
    }
}

/// Background maintenance task. Measures changed mirrors and evicts the LRU tail
/// whenever the index signals work, and exits cleanly when `shutdown` fires (or its
/// sender drops).
pub async fn run(
    cache: Arc<GitCache>,
    index: Arc<CacheIndex>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        // Run first: covers the startup-over-cap permit and any signal that arrived
        // while the previous pass ran (a stored `notify_one` permit makes the next
        // wait return immediately, so no work is missed).
        maintain(&cache, &index).await;
        tokio::select! {
            biased; // prefer shutdown over another pass when both are ready
            _ = shutdown.changed() => break,
            _ = index.work.notified() => {}
        }
    }
    tracing::debug!("cache evictor stopped");
}

/// One maintenance pass: (re)measure changed mirrors, then evict the LRU tail until
/// under the cap. All disk IO lives here, off the request path.
async fn maintain(cache: &GitCache, index: &CacheIndex) {
    let dirty = index.take_dirty();
    if !dirty.is_empty() {
        let dirs: Vec<(String, PathBuf)> = dirty
            .into_iter()
            .map(|n| {
                let dir = index.cache_dir(&n);
                (n, dir)
            })
            .collect();
        // The walk is blocking; keep it off the runtime.
        let measured = tokio::task::spawn_blocking(move || {
            dirs.into_iter()
                .map(|(name, dir)| (name, measure(&dir).0))
                .collect::<Vec<_>>()
        })
        .await
        .unwrap_or_default();
        for (name, size) in measured {
            index.set_size(&name, size);
        }
    }

    for (name, dir) in index.take_victims() {
        match cache.evict(&name, &dir).await {
            Ok(()) => {
                index.metrics.record_eviction();
                tracing::info!(repo = %name, "evicted idle mirror");
            }
            // The entry is already out of the index; a failed unlink just leaves an
            // untracked dir on disk, which the next startup scan picks back up.
            Err(e) => tracing::warn!(repo = %name, error = %e, "evict failed"),
        }
    }
}

/// A mirror found on disk during the startup scan.
struct Scanned {
    name: String,
    size: u64,
    mtime: SystemTime,
}

/// Discover every mirror under the cache root. Mirrors live at arbitrary depth
/// (`resolve` maps `group/team/foo.git` onto nested dirs), so this descends the
/// namespace dirs - via an explicit stack, not recursion - and stops at each mirror
/// root (a dir with a top-level `HEAD` file, the same "initialised mirror" marker
/// `ensure_fresh` uses). Reserved staging/trash dirs are skipped so they never
/// count toward the budget. Startup-only; steady state is the in-memory index. It
/// reads only the namespace dirs here; `measure` reads inside each mirror, so the
/// two never traverse the same directory twice.
fn find_mirrors(cache_root: &Path) -> Vec<Scanned> {
    let mut out = Vec::new();
    let mut stack = vec![cache_root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        if dir.join("HEAD").is_file() {
            let name = rel_name(cache_root, &dir);
            if name.is_empty() {
                continue; // the cache root itself is not a mirror
            }
            let (size, mtime) = measure(&dir);
            out.push(Scanned { name, size, mtime });
            continue; // a mirror's subdirs are not themselves mirrors
        }
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let Ok(ft) = entry.file_type() else { continue };
            if !ft.is_dir() {
                continue;
            }
            let fname = entry.file_name();
            let fname = fname.to_string_lossy();
            if fname.ends_with(crate::repo::INCOMING_SUFFIX)
                || fname.ends_with(crate::repo::EVICTING_SUFFIX)
            {
                continue;
            }
            stack.push(entry.path());
        }
    }
    out
}

/// A mirror's cache-key name: its path relative to the root, `/`-joined so it
/// matches the key `resolve` produces regardless of the platform separator.
fn rel_name(root: &Path, dir: &Path) -> String {
    dir.strip_prefix(root)
        .unwrap_or(dir)
        .components()
        .map(|c| c.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

/// Total byte size of a mirror and the newest mtime among its files. Reused for the
/// startup scan and the background size refresh. A bare mirror is a handful of
/// (mostly packed) files, so the walk cost tracks file count, not bytes.
pub(crate) fn measure(dir: &Path) -> (u64, SystemTime) {
    let mut size = 0u64;
    let mut mtime = SystemTime::UNIX_EPOCH;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&d) else {
            continue;
        };
        for entry in entries.flatten() {
            let Ok(md) = entry.metadata() else { continue };
            if md.is_dir() {
                stack.push(entry.path());
            } else {
                size += md.len();
                if let Ok(mt) = md.modified()
                    && mt > mtime
                {
                    mtime = mt;
                }
            }
        }
    }
    (size, mtime)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::time::Duration;

    use tokio::sync::watch;

    use crate::git::{GitCache, GitConfig};

    #[test]
    fn size_accounting_tracks_the_total() {
        let tmp = tempfile::tempdir().unwrap();
        let idx = CacheIndex::new(tmp.path().to_path_buf(), u64::MAX, Arc::new(Metrics::new()));
        idx.mark_changed("a"); // placeholder, size 0
        idx.set_size("a", 100);
        idx.mark_changed("b");
        idx.set_size("b", 50);
        assert_eq!(idx.totals(), (150, 2));
        idx.set_size("a", 200); // re-measure in place, not a new entry
        assert_eq!(idx.totals(), (250, 2));
    }

    #[test]
    fn victims_pop_oldest_first_until_under_cap() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let now = SystemTime::now();
        make_mirror(
            &root.join("old.git"),
            4096,
            Some(now - Duration::from_secs(120)),
        );
        make_mirror(
            &root.join("mid.git"),
            4096,
            Some(now - Duration::from_secs(60)),
        );
        make_mirror(&root.join("new.git"), 4096, Some(now));

        // Each mirror is ~4 KiB; a 6000-byte cap leaves room for one, so the two
        // oldest are popped, oldest first.
        let idx = CacheIndex::new(root.to_path_buf(), 6000, Arc::new(Metrics::new()));
        let names: Vec<String> = idx.take_victims().into_iter().map(|(n, _)| n).collect();
        assert_eq!(names, vec!["old.git".to_string(), "mid.git".to_string()]);
        assert_eq!(idx.totals().1, 1); // one mirror left in the index
    }

    #[test]
    fn touch_promotes_and_spares_from_eviction() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let now = SystemTime::now();
        make_mirror(
            &root.join("old.git"),
            4096,
            Some(now - Duration::from_secs(120)),
        );
        make_mirror(
            &root.join("mid.git"),
            4096,
            Some(now - Duration::from_secs(60)),
        );
        make_mirror(&root.join("new.git"), 4096, Some(now));

        let idx = CacheIndex::new(root.to_path_buf(), 6000, Arc::new(Metrics::new()));
        idx.touch("old.git"); // now the most-recently-used, must be spared
        let names: Vec<String> = idx.take_victims().into_iter().map(|(n, _)| n).collect();
        assert_eq!(names, vec!["mid.git".to_string(), "new.git".to_string()]);
    }

    #[tokio::test]
    async fn maintain_measures_then_evicts_on_disk() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        // Empty index, tiny cap; a single mirror appears on disk and is flagged.
        make_mirror(&root.join("big.git"), 8192, None);

        let metrics = Arc::new(Metrics::new());
        let idx = CacheIndex::new(root.to_path_buf(), 4096, metrics.clone());
        let cache = GitCache::new(dummy_cfg(), metrics.clone(), Some(idx.clone()));
        idx.mark_changed("big.git"); // request path would do this after a clone

        maintain(&cache, &idx).await; // measures big.git (>cap) then evicts it

        assert!(
            !root.join("big.git").exists(),
            "over-cap mirror should be evicted"
        );
        assert_eq!(idx.totals(), (0, 0));
        assert!(metrics.gather().contains("gitcacheproxy_evictions_total 1"));
    }

    #[tokio::test]
    async fn run_evicts_over_cap_then_stops_on_shutdown() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let now = SystemTime::now();
        make_mirror(
            &root.join("old.git"),
            4096,
            Some(now - Duration::from_secs(120)),
        );
        make_mirror(&root.join("new.git"), 4096, None);

        let metrics = Arc::new(Metrics::new());
        let idx = CacheIndex::new(root.to_path_buf(), 6000, metrics.clone()); // over cap
        let cache = Arc::new(GitCache::new(
            dummy_cfg(),
            metrics.clone(),
            Some(idx.clone()),
        ));

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(run(cache, idx.clone(), shutdown_rx));
        // `run` drains once before its first `select`, so the eviction completes
        // before the task can observe shutdown; awaiting the handle after signalling
        // guarantees the drain ran and the loop exited cleanly.
        shutdown_tx.send(true).unwrap();
        handle.await.unwrap();

        assert!(!root.join("old.git").exists(), "oldest mirror evicted");
        assert!(root.join("new.git").exists(), "newest mirror kept");
        assert!(metrics.gather().contains("gitcacheproxy_evictions_total 1"));
    }

    #[test]
    fn set_size_ignores_an_untracked_mirror() {
        let tmp = tempfile::tempdir().unwrap();
        let idx = CacheIndex::new(tmp.path().to_path_buf(), u64::MAX, Arc::new(Metrics::new()));
        // No entry for this name (e.g. evicted between mark and measure): no-op.
        idx.set_size("never-tracked", 999);
        assert_eq!(idx.totals(), (0, 0));
    }

    #[test]
    fn scan_skips_stray_files_and_reserved_dirs() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        make_mirror(&root.join("good.git"), 1024, None);
        // A non-directory entry at the root must be ignored by the walk.
        std::fs::write(root.join("stray.txt"), b"x").unwrap();
        // Crashed clone/eviction leftovers must not be scanned as mirrors.
        make_mirror(
            &root.join(format!("wip.git{}", crate::repo::INCOMING_SUFFIX)),
            1024,
            None,
        );
        make_mirror(
            &root.join(format!("gone.git{}", crate::repo::EVICTING_SUFFIX)),
            1024,
            None,
        );

        let idx = CacheIndex::new(root.to_path_buf(), u64::MAX, Arc::new(Metrics::new()));
        assert_eq!(idx.totals().1, 1, "only the real mirror is tracked");
    }

    #[tokio::test]
    async fn evict_is_a_noop_when_the_mirror_is_already_gone() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = GitCache::new(dummy_cfg(), Arc::new(Metrics::new()), None);
        // No `HEAD` at this path, so `evict` returns early without touching disk.
        let dir = tmp.path().join("absent.git");
        cache.evict("absent.git", &dir).await.unwrap();
        assert!(!dir.exists());
    }

    fn dummy_cfg() -> GitConfig {
        // Eviction never shells out to git, so the binary is irrelevant.
        GitConfig {
            git_binary: "git".into(),
            upstream_auth_header: None,
            fetch_ttl: Duration::from_secs(10),
        }
    }

    /// A fake bare mirror: `HEAD` plus a data file summing to at least `data_bytes`.
    /// When `mtime` is set, both files are stamped with it so the mirror's last-used
    /// signal is deterministic.
    fn make_mirror(dir: &Path, data_bytes: usize, mtime: Option<SystemTime>) {
        std::fs::create_dir_all(dir.join("objects")).unwrap();
        write_file(&dir.join("HEAD"), b"ref: refs/heads/main\n", mtime);
        write_file(
            &dir.join("objects/pack.data"),
            &vec![b'x'; data_bytes],
            mtime,
        );
    }

    fn write_file(path: &Path, bytes: &[u8], mtime: Option<SystemTime>) {
        let mut f = std::fs::File::create(path).unwrap();
        f.write_all(bytes).unwrap();
        if let Some(t) = mtime {
            f.set_modified(t).unwrap();
        }
    }
}
