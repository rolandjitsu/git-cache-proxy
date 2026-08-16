// SPDX-License-Identifier: Apache-2.0
//! Library surface for git-cache-proxy.
//!
//! The modules are exposed as a library (in addition to the `git-cache-proxy`
//! binary in `main.rs`) so that integration tests can drive the HTTP router and
//! the git cache in-process. See the binary crate for the runnable entry point.

pub mod config;
pub mod evict;
pub mod git;
pub mod metrics;
pub mod repo;
pub mod server;
