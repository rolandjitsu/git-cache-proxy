# git-cache-proxy

[![CI](https://img.shields.io/github/actions/workflow/status/rolandjitsu/git-proxy-server/ci.yml?branch=main&style=flat-square&label=CI)](https://github.com/rolandjitsu/git-proxy-server/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/codecov/c/github/rolandjitsu/git-proxy-server/main?style=flat-square)](https://codecov.io/gh/rolandjitsu/git-proxy-server)
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

## Why not just a mirror or an HTTP cache?

- A **generic HTTP cache** doesn't work: `git fetch` is a _negotiated_ pack
  transfer (`POST /git-upload-pack` responses are computed per request), so
  there's nothing static to cache.
- A **scheduled full mirror** (Gitea pull-mirror, etc.) means enumerating and
  replicating every repo, and it can lag the exact commit CI just pushed.

This proxy is **lazy and pull-only**: it caches on first request, and on every
request it does an incremental `git fetch` from upstream _before_ serving - so
a client always gets the ref it asked for, and nothing is ever proactively
pushed or replicated to where the proxy runs. That last property tends to be
the difference between "sure" and "absolutely not" when the origin is sensitive.

## How it works

All git work is delegated to the system `git` binary, so protocol correctness -
**protocol v2**, shallow and partial (filtered) clones - comes for free.

```
GET  <repo>/info/refs?service=git-upload-pack
  1. resolve <repo> -> upstream URL + local bare mirror dir
  2. clone --mirror (first time) or fetch --prune (coalesced per repo)
  3. serve `git upload-pack --advertise-refs` from the mirror
POST <repo>/git-upload-pack
  -> stream `git upload-pack --stateless-rpc` from the mirror (packfile)
anything git-receive-pack  -> 403 (read-only)
```

Concurrent clients for the same repo are serialized so a burst triggers a single
upstream fetch; a short TTL coalesces repeated requests.

## Quick start

```sh
git-cache-proxy --upstream https://git.example.com --cache-root /var/cache/git-cache-proxy
# then, on a client:
git -c url."http://proxy:8080/".insteadOf="https://git.example.com/" clone https://git.example.com/group/repo.git
```

The `insteadOf` rewrite is how a client transparently routes through the proxy.
Pair it with `pushInsteadOf` back to the origin so pushes bypass the (read-only)
proxy:

```
git config url."http://proxy:8080/".insteadOf            "https://git.example.com/"
git config url."https://git.example.com/".pushInsteadOf  "https://git.example.com/"
```

## Configuration

Every flag has an environment-variable equivalent.

| Flag                     | Env                                  | Default                      | Purpose                                                                        |
| ------------------------ | ------------------------------------ | ---------------------------- | ------------------------------------------------------------------------------ |
| `--bind`                 | `GITCACHEPROXY_BIND`                 | `0.0.0.0:8080`               | Listen address                                                                 |
| `--cache-root`           | `GITCACHEPROXY_CACHE_ROOT`           | `/var/cache/git-cache-proxy` | Where bare mirrors live                                                        |
| `--upstream`             | `GITCACHEPROXY_UPSTREAM`             | - (required)                 | Origin git base URL                                                            |
| `--upstream-auth-header` | `GITCACHEPROXY_UPSTREAM_AUTH_HEADER` | -                            | e.g. `Authorization: Bearer <token>`; injected via env so it stays out of argv |
| `--serve-token`          | `GITCACHEPROXY_SERVE_TOKEN`          | -                            | If set, clients must send `Authorization: Bearer <token>`                      |
| `--fetch-ttl-seconds`    | `GITCACHEPROXY_FETCH_TTL_SECONDS`    | `10`                         | Skip upstream fetch if refreshed within this window (`0` = always fetch)       |
| `--git-binary`           | `GITCACHEPROXY_GIT_BINARY`           | `git`                        | Path to git                                                                    |

Endpoints: `/healthz`, `/readyz`, `/metrics` (Prometheus).

## Auth model

- **Upstream:** the proxy holds a single read-only credential (`--upstream-auth-header`)
  used only to `fetch`/`clone`. It never writes upstream.
- **Clients:** anonymous by default (intended for a network-restricted
  deployment); set `--serve-token` to require a bearer token.

## Deploy

The `Dockerfile` builds a statically linked (musl) binary and drops it onto a
minimal Alpine base. Because all wire-protocol work is delegated to the system
`git` binary, the runtime image must contain `git` - so it is Alpine-with-git
rather than a fully distroless/`FROM scratch` image. Removing that dependency
(and enabling a git-free image) means moving the git plumbing in-process to a
Rust library - see the roadmap below.

## Status / scope

Working and end-to-end tested against both Git wire protocol versions — the
modern **v2** (`git-protocol` header, the default since Git 2.26) and the legacy
**v0/v1** advertisement — covering full clone, incremental delta fetch, and
push rejection.

Not yet implemented, in rough priority order:

- LRU disk eviction of idle mirrors (the cache currently grows unbounded).
- Per-repo latency histograms (fetch/serve durations); per-repo counters exist.
- A background/scheduled refresh option (today every `info/refs` triggers an
  on-demand, TTL-coalesced fetch).
- No external `git` binary: move the plumbing in-process to a Rust library
  (`gitoxide`/`git2`). More robust (no subprocess/argv surface, structured
  errors) and unlocks a fully distroless, git-free image - a larger change,
  tracked as a possible v2.

Contributions welcome.

## License

[Apache-2.0](./LICENSE). Self-contained - no dependencies outside crates.io.
