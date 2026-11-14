# git-cache-proxy

[![CI](https://img.shields.io/github/actions/workflow/status/rolandjitsu/git-cache-proxy/ci.yml?branch=main&style=flat-square&label=CI)](https://github.com/rolandjitsu/git-cache-proxy/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/codecov/c/github/rolandjitsu/git-cache-proxy/main?style=flat-square)](https://codecov.io/gh/rolandjitsu/git-cache-proxy)
[![crates.io](https://img.shields.io/crates/v/git-cache-proxy?style=flat-square)](https://crates.io/crates/git-cache-proxy)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue?style=flat-square)](./LICENSE)

A small, read-only **caching proxy for Git repositories**. It sits between many
clients (CI build/test machines) and an origin git server, serving clones and
fetches from a local bare mirror and pulling only the _delta_ from upstream on
each request.

It exists for the case where an elastic fleet of ephemeral machines (e.g.
autoscaled CI runners in another region/cloud) repeatedly clones large repos
over a slow or expensive link to an on-prem origin. Point the fleet at a
proxy running next to them and the bulk history is served locally; only new
commits cross the WAN, deduplicated across the whole fleet.

It is **lazy and pull-only**: it caches on first request, runs an incremental
`git fetch` before serving so a client always gets the ref it asked for, and
never pushes or replicates to the origin. All wire-protocol work is delegated to
the system `git` binary, so **v2**, shallow, and partial (filtered) clones are
git's responsibility, not something the proxy reimplements. git-LFS objects are
cached too.

See **[docs/architecture.md](./docs/architecture.md)** for why a plain mirror or
a generic HTTP cache doesn't fit, how it compares, the internals, and a benchmark.

## Install

```sh
cargo install git-cache-proxy
```

`cargo install` builds from source and needs a `git` binary on `PATH` at runtime
(all wire-protocol work is delegated to it). Or pull the container image, which
bundles `git`:

```sh
docker pull ghcr.io/rolandjitsu/git-cache-proxy
```

## Quick start

```sh
git-cache-proxy --upstream https://git.example.com --cache-root /var/cache/git-cache-proxy
# then, on a client:
git -c url."http://proxy:8080/".insteadOf="https://git.example.com/" clone https://git.example.com/group/repo.git
```

The `insteadOf` rewrite is how a client transparently routes through the proxy.
Pair it with `pushInsteadOf` back to the origin so pushes bypass the (read-only)
proxy:

```sh
git config url."http://proxy:8080/".insteadOf            "https://git.example.com/"
git config url."https://git.example.com/".pushInsteadOf  "https://git.example.com/"
```

Or try it against a public origin with no config - the image runs as-is:

```sh
docker run --rm -p 8080:8080 ghcr.io/rolandjitsu/git-cache-proxy --upstream https://github.com

# in another terminal - mirrored on the first request, served locally after:
git -c url."http://localhost:8080/".insteadOf="https://github.com/" \
  clone https://github.com/rolandjitsu/git-cache-proxy
```

## Configuration

Every flag has an environment-variable equivalent; the full reference is in
**[docs/configuration.md](./docs/configuration.md)**. The essentials:

- `--upstream` (required): origin git base URL; requested repo paths are appended.
- `--cache-root`: where the bare mirrors live.
- `--upstream-auth-header` / `--upstream-credential`: how the proxy authenticates
  to upstream.
- `--serve-token` / `--auth-provider`: how clients authenticate to the proxy.
- `--cache-max-mb`: cap on-disk cache size (LRU eviction; `0` = unlimited).

## Metrics

`GET /metrics` serves Prometheus text-format metrics - per-repo request and
upstream counters, cache-size gauges, and `*_duration_seconds` latency
histograms, all prefixed `gitcacheproxy_`. Full list in
**[docs/metrics.md](./docs/metrics.md)**.

## Authentication & security

By default the proxy serves anonymously (intended for a network-restricted
deployment) and fetches upstream with one shared credential, so **reachability is
the trust boundary**. It can instead require a shared serve token, or validate
each client's own credential per repo (`--auth-provider github|gitlab|custom`) -
and optionally fetch upstream with that same client credential so no long-lived
upstream secret is held (`--upstream-credential client`).

Read **[docs/security.md](./docs/security.md)** for the full auth and threat
model before exposing it on anything but a trusted network.

## Deploy

The container image is Alpine-with-`git` (the wire protocol is delegated to the
system `git`, a permanent runtime dependency - see [Status](#status--scope)). It
runs as UID/GID 10001, which must own the cache volume (`chown -R 10001:10001` a
cache written by an older root-running release, or set `fsGroup: 10001` on
Kubernetes - the chart's default).

On Kubernetes, a Helm chart lives in [`chart/`](./chart) (single-writer
Deployment, `/healthz`+`/readyz` probes, cache PVC, optional Ingress and
Prometheus `ServiceMonitor`):

```sh
helm install git-cache-proxy oci://ghcr.io/rolandjitsu/charts/git-cache-proxy \
  --set upstream=https://your-git-host.example.com
```

See the [chart README](./chart/README.md) for the full values reference.

## Status / scope

Working and end-to-end tested on both Git wire protocol versions - **v2**
(default since Git 2.26) and the legacy **v0/v1** advertisement - across full
clone, incremental fetch, and push rejection. **git-LFS** is cached too (batch
API proxied; objects stored content-addressed and integrity-checked).

Early, single-maintainer software: no independent review or wide deployment yet.
Pin a version and try it against your own setup before relying on it.

Not yet implemented:

- A background/scheduled refresh (today every `info/refs` triggers an on-demand,
  TTL-coalesced fetch).

**Non-goal: a git-free image.** The `git` binary is a permanent runtime
dependency: the proxy serves clients by running `git upload-pack`, and no
production-ready Rust library implements the upload-pack *server* side
(`gitoxide` is client-only; `libgit2` exposes only pack primitives). Moving it
in-process would mean hand-rolling the smart protocol - exactly what delegating
to `git` avoids.

Contributions welcome.

## License

[Apache-2.0](./LICENSE). Self-contained - no dependencies outside crates.io.

### Upstream operation lifetime

`--upstream-timeout-seconds` (`GITCACHEPROXY_UPSTREAM_TIMEOUT_SECONDS`, default
`3600`) bounds each clone, full-ref fetch and SHA backfill across attempts and
retry delays. The value must be positive and fit in a monotonic deadline.
Library users can set the same budget with `GitCache::with_upstream_timeout`.
The budget starts when the operation starts, after the repository lock is acquired.

SIGTERM and Ctrl-C reject new upstream work and cancel active commands, retry
backoff and repository lock waits before HTTP drain. Upstream owners kill process
groups on Unix and wait for the direct child before releasing repository locks.
Dropped requests also cancel their upstream owner. Library hosts call
`GitCache::shutdown` before draining HTTP and `wait_for_shutdown` after draining.
Active local upload-pack responses continue to drain. This does not impose a hard
shutdown bound on active uploads or blocking filesystem work. On non-Unix targets,
cancellation terminates only the direct child.
