// SPDX-License-Identifier: Apache-2.0
//! Prometheus metrics.

use prometheus::{Encoder, IntCounterVec, Opts, Registry, TextEncoder};

pub struct Metrics {
    pub registry: Registry,
    /// requests_total{kind, result} - kind = info_refs | upload_pack.
    pub requests: IntCounterVec,
    /// upstream_ops_total{op, result} - op = clone | fetch.
    pub upstream: IntCounterVec,
}

impl Metrics {
    pub fn new() -> Self {
        let registry = Registry::new();
        let requests = IntCounterVec::new(
            Opts::new("gitcacheproxy_requests_total", "Git requests served"),
            &["kind", "result"],
        )
        .expect("valid metric");
        let upstream = IntCounterVec::new(
            Opts::new(
                "gitcacheproxy_upstream_ops_total",
                "Upstream clone/fetch operations",
            ),
            &["op", "result"],
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
