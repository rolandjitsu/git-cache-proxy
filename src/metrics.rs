// SPDX-License-Identifier: Apache-2.0
//! Prometheus metrics.
//!
//! Cardinality note: both `requests_total` and `upstream_ops_total` carry a
//! `repo` label so operators can see per-repo traffic and totals
//! (`sum without (repo) (...)`). To keep the label set bounded, the real repo
//! name is emitted only for operations that *succeeded* (a served request, a
//! completed clone/fetch); every failure - failed auth, malformed path,
//! upstream error, a resolved-but-nonexistent repo - uses a `-` sentinel. The
//! set of successfully served repos is bounded (the repos the fleet actually
//! clones), so a flood of distinct but doomed repo paths cannot inflate the
//! series count.

use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, Opts, Registry,
    TextEncoder,
};

/// Histogram buckets, in seconds, for git operation latency: from a fast cached
/// advertisement (tens of ms) to a large clone over a slow WAN (minutes).
const DURATION_BUCKETS: &[f64] = &[
    0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0,
];

/// The `kind` label on `requests_total`: which client endpoint served the request.
#[derive(Debug, Clone, Copy)]
pub enum RequestKind {
    InfoRefs,
    UploadPack,
    Auth,
    ReceivePack,
    LfsBatch,
    LfsObject,
}

/// The `result` label on `requests_total` / `upstream_ops_total`.
#[derive(Debug, Clone, Copy)]
pub enum Status {
    Ok,
    Error,
    UpstreamError,
    Unauthorized,
    Rejected,
}

/// The `op` label on `upstream_ops_total` / `upstream_duration_seconds`.
#[derive(Debug, Clone, Copy)]
pub enum UpstreamOp {
    Clone,
    Fetch,
    /// On-demand fetch of a bare SHA the mirror lacks (e.g. a PR merge commit).
    WantFetch,
}

/// The `kind` label on `serve_duration_seconds`.
#[derive(Debug, Clone, Copy)]
pub enum ServeKind {
    InfoRefs,
    UploadPack,
}

/// The `result` label on `lfs_objects_total`.
#[derive(Debug, Clone, Copy)]
pub enum LfsResult {
    Hit,
    Miss,
    Error,
}

impl RequestKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::InfoRefs => "info_refs",
            Self::UploadPack => "upload_pack",
            Self::Auth => "auth",
            Self::ReceivePack => "receive_pack",
            Self::LfsBatch => "lfs_batch",
            Self::LfsObject => "lfs_object",
        }
    }
}

impl Status {
    fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Error => "error",
            Self::UpstreamError => "upstream_error",
            Self::Unauthorized => "unauthorized",
            Self::Rejected => "rejected",
        }
    }
}

impl UpstreamOp {
    fn as_str(self) -> &'static str {
        match self {
            Self::Clone => "clone",
            Self::Fetch => "fetch",
            Self::WantFetch => "want_fetch",
        }
    }
}

impl ServeKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::InfoRefs => "info_refs",
            Self::UploadPack => "upload_pack",
        }
    }
}

impl LfsResult {
    fn as_str(self) -> &'static str {
        match self {
            Self::Hit => "hit",
            Self::Miss => "miss",
            Self::Error => "error",
        }
    }
}

pub struct Metrics {
    pub registry: Registry,
    /// `requests_total{kind, result, repo}` - kind = info_refs | upload_pack |
    /// auth | receive_pack | lfs_batch | lfs_object; result = ok | error |
    /// upstream_error | unauthorized | rejected; repo = the served repo path when
    /// result = ok, else `-`.
    requests: IntCounterVec,
    /// `upstream_ops_total{op, result, repo}` - op = clone | fetch | want_fetch;
    /// result = ok | error; repo = the repo path when result = ok, else `-`.
    upstream: IntCounterVec,
    /// `cache_bytes` - total size of the on-disk mirror cache, maintained
    /// incrementally as mirrors are added, refreshed, and evicted. Populated only
    /// when a cap is configured (`--cache-max-mb`); with no cap it stays `0`.
    cache_bytes: IntGauge,
    /// `cache_mirrors` - number of cached mirrors (same caveat as `cache_bytes`).
    cache_mirrors: IntGauge,
    /// `evictions_total` - idle mirrors evicted to keep the cache under the cap.
    evictions: IntCounter,
    /// `lfs_objects_total{result}` - cached git-LFS object lookups; result = hit
    /// (served from disk) | miss (fetched from upstream, then cached) | error. No
    /// `repo` label: objects are content-addressed and shared across repos.
    lfs_objects: IntCounterVec,
    /// `upstream_duration_seconds{op, repo}` - clone/fetch wall-clock, observed
    /// only on success (same bounded-`repo` discipline as the counters).
    upstream_duration: HistogramVec,
    /// `serve_duration_seconds{kind, repo}` - kind = info_refs (the buffered
    /// advertisement) | upload_pack (the packfile stream, timed to EOF). Observed
    /// only on success.
    serve_duration: HistogramVec,
}

impl Metrics {
    pub fn new() -> Self {
        let registry = Registry::new();
        let requests = IntCounterVec::new(
            Opts::new("gitcacheproxy_requests_total", "Git requests served"),
            &["kind", "result", "repo"],
        )
        .expect("valid metric");
        let upstream = IntCounterVec::new(
            Opts::new(
                "gitcacheproxy_upstream_ops_total",
                "Upstream clone/fetch operations",
            ),
            &["op", "result", "repo"],
        )
        .expect("valid metric");
        let cache_bytes = IntGauge::new(
            "gitcacheproxy_cache_bytes",
            "Total size of the on-disk mirror cache in bytes",
        )
        .expect("valid metric");
        let cache_mirrors = IntGauge::new(
            "gitcacheproxy_cache_mirrors",
            "Number of cached mirrors on disk",
        )
        .expect("valid metric");
        let evictions = IntCounter::new(
            "gitcacheproxy_evictions_total",
            "Idle mirrors evicted to keep the cache under the configured cap",
        )
        .expect("valid metric");
        let lfs_objects = IntCounterVec::new(
            Opts::new(
                "gitcacheproxy_lfs_objects_total",
                "Cached git-LFS object lookups (hit/miss/error)",
            ),
            &["result"],
        )
        .expect("valid metric");
        let upstream_duration = HistogramVec::new(
            HistogramOpts::new(
                "gitcacheproxy_upstream_duration_seconds",
                "Upstream clone/fetch duration in seconds",
            )
            .buckets(DURATION_BUCKETS.to_vec()),
            &["op", "repo"],
        )
        .expect("valid metric");
        let serve_duration = HistogramVec::new(
            HistogramOpts::new(
                "gitcacheproxy_serve_duration_seconds",
                "Client serve duration in seconds (info/refs advertisement, upload-pack stream)",
            )
            .buckets(DURATION_BUCKETS.to_vec()),
            &["kind", "repo"],
        )
        .expect("valid metric");
        registry
            .register(Box::new(requests.clone()))
            .expect("register requests");
        registry
            .register(Box::new(upstream.clone()))
            .expect("register upstream");
        registry
            .register(Box::new(cache_bytes.clone()))
            .expect("register cache_bytes");
        registry
            .register(Box::new(cache_mirrors.clone()))
            .expect("register cache_mirrors");
        registry
            .register(Box::new(evictions.clone()))
            .expect("register evictions");
        registry
            .register(Box::new(lfs_objects.clone()))
            .expect("register lfs_objects");
        registry
            .register(Box::new(upstream_duration.clone()))
            .expect("register upstream_duration");
        registry
            .register(Box::new(serve_duration.clone()))
            .expect("register serve_duration");
        Self {
            registry,
            requests,
            upstream,
            cache_bytes,
            cache_mirrors,
            evictions,
            lfs_objects,
            upstream_duration,
            serve_duration,
        }
    }

    /// Record the outcome of a client request. `repo` is the served repo path on
    /// success and `-` on any failure, so unbounded client-supplied paths cannot
    /// inflate label cardinality.
    pub fn record_request(&self, kind: RequestKind, result: Status, repo: &str) {
        self.requests
            .with_label_values(&[kind.as_str(), result.as_str(), repo])
            .inc();
    }

    /// Record an upstream clone/fetch. Errors are recorded too (`Status::Error`);
    /// pass the repo path on success and `-` on error, matching `record_request`.
    pub fn record_upstream(&self, op: UpstreamOp, result: Status, repo: &str) {
        self.upstream
            .with_label_values(&[op.as_str(), result.as_str(), repo])
            .inc();
    }

    /// Refresh the cache-size gauges from the eviction index.
    pub fn set_cache_size(&self, bytes: u64, mirrors: usize) {
        self.cache_bytes.set(bytes as i64);
        self.cache_mirrors.set(mirrors as i64);
    }

    /// Record one evicted mirror.
    pub fn record_eviction(&self) {
        self.evictions.inc();
    }

    /// Record a cached LFS object lookup (hit / miss / error).
    pub fn record_lfs(&self, result: LfsResult) {
        self.lfs_objects.with_label_values(&[result.as_str()]).inc();
    }

    /// Observe an upstream op's duration. Call only on success with the real repo,
    /// matching the counters' bounded-`repo` cardinality discipline.
    pub fn observe_upstream(&self, op: UpstreamOp, repo: &str, seconds: f64) {
        self.upstream_duration
            .with_label_values(&[op.as_str(), repo])
            .observe(seconds);
    }

    /// Observe a client serve duration, same cardinality discipline as
    /// `observe_upstream`.
    pub fn observe_serve(&self, kind: ServeKind, repo: &str, seconds: f64) {
        self.serve_duration
            .with_label_values(&[kind.as_str(), repo])
            .observe(seconds);
    }

    pub fn gather(&self) -> String {
        let mut buf = Vec::new();
        let enc = TextEncoder::new();
        // encode never fails for the text format into a Vec.
        let _ = enc.encode(&self.registry.gather(), &mut buf);
        String::from_utf8_lossy(&buf).into_owned()
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gather_renders_recorded_series() {
        let m = Metrics::new();
        m.record_request(RequestKind::InfoRefs, Status::Ok, "group/foo.git");
        m.record_request(RequestKind::UploadPack, Status::Error, "group/bar.git");
        m.record_upstream(UpstreamOp::Fetch, Status::Ok, "group/foo.git");
        m.record_upstream(UpstreamOp::Clone, Status::Error, "group/bar.git");
        m.set_cache_size(2048, 3);
        m.record_eviction();
        m.record_eviction();
        m.record_lfs(LfsResult::Hit);
        m.record_lfs(LfsResult::Miss);
        m.observe_upstream(UpstreamOp::Clone, "group/foo.git", 1.5);
        m.observe_serve(ServeKind::UploadPack, "group/foo.git", 2.0);

        let out = m.gather();
        assert!(out.contains("gitcacheproxy_cache_bytes 2048"));
        assert!(out.contains("gitcacheproxy_cache_mirrors 3"));
        assert!(out.contains("gitcacheproxy_evictions_total 2"));
        assert!(out.contains(r#"gitcacheproxy_lfs_objects_total{result="hit"} 1"#));
        assert!(out.contains(r#"gitcacheproxy_lfs_objects_total{result="miss"} 1"#));
        assert!(out.contains(
            r#"gitcacheproxy_upstream_duration_seconds_count{op="clone",repo="group/foo.git"} 1"#
        ));
        assert!(out.contains(
            r#"gitcacheproxy_serve_duration_seconds_count{kind="upload_pack",repo="group/foo.git"} 1"#
        ));
        assert!(out.contains(
            r#"gitcacheproxy_requests_total{kind="info_refs",repo="group/foo.git",result="ok"} 1"#
        ));
        assert!(out.contains(
            r#"gitcacheproxy_requests_total{kind="upload_pack",repo="group/bar.git",result="error"} 1"#
        ));
        assert!(out.contains(r#"op="fetch",repo="group/foo.git",result="ok"#));
        assert!(out.contains(r#"op="clone",repo="group/bar.git",result="error"#));
    }
}
