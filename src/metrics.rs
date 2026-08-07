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

use prometheus::{Encoder, IntCounterVec, Opts, Registry, TextEncoder};

pub struct Metrics {
    pub registry: Registry,
    /// `requests_total{kind, result, repo}` - kind = info_refs | upload_pack |
    /// auth | receive_pack; result = ok | error | upstream_error | unauthorized |
    /// rejected; repo = the served repo path when result = ok, else `-`.
    requests: IntCounterVec,
    /// `upstream_ops_total{op, result, repo}` - op = clone | fetch; result = ok |
    /// error; repo = the repo path when result = ok, else `-`.
    upstream: IntCounterVec,
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
        registry
            .register(Box::new(requests.clone()))
            .expect("register requests");
        registry
            .register(Box::new(upstream.clone()))
            .expect("register upstream");
        Self {
            registry,
            requests,
            upstream,
        }
    }

    /// Record the outcome of a client request. `repo` is the served repo path on
    /// success and `-` on any failure, so unbounded client-supplied paths cannot
    /// inflate label cardinality. `kind` is `info_refs`, `upload_pack`, `auth` or
    /// `receive_pack`; `result` is `ok`, `error`, `upstream_error`,
    /// `unauthorized` or `rejected`.
    pub fn record_request(&self, kind: &str, result: &str, repo: &str) {
        self.requests.with_label_values(&[kind, result, repo]).inc();
    }

    /// Record an upstream clone/fetch. Errors are recorded too (`result =
    /// "error"`); pass the repo path on success and `-` on error, matching
    /// `record_request`.
    pub fn record_upstream(&self, op: &str, result: &str, repo: &str) {
        self.upstream.with_label_values(&[op, result, repo]).inc();
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
        m.record_request("info_refs", "ok", "group/foo.git");
        m.record_request("upload_pack", "error", "group/bar.git");
        m.record_upstream("fetch", "ok", "group/foo.git");
        m.record_upstream("clone", "error", "group/bar.git");

        let out = m.gather();
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
