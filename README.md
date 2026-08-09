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

## How it compares

| Approach                                     | Caches `fetch` [1] | Lazy [2] | Fresh ref [3] | Delta-only WAN [4] | Pull-only [5] | Any origin [6] |
| -------------------------------------------- | :----------------: | :------: | :-----------: | :----------------: | :-----------: | :------------: |
| **git-cache-proxy**                          |        yes         |   yes    |      yes      |        yes         |      yes      |      yes       |
| Generic HTTP cache (nginx / Varnish)         |         no         |   n/a    |      no       |         no         |      yes      |      yes       |
| Scheduled mirror (Gitea / GitLab pull-mirror)|        yes         |    no    |      lag      |         no         |      no       |      yes       |
| `git clone --reference` / alternates         |        n/a         |    no    |     seed      |        yes         |      yes      |      yes       |
| GitLab Geo / server geo-replication          |        yes         |    no    |      yes      |         no         |      no       |       no       |

1. Caches the _negotiated_ `git-upload-pack` response, not just static objects - a plain
   HTTP cache can't, because every fetch is computed per request.
2. Caches on first request; no repo enumeration or replication schedule to run.
3. Serves the exact ref asked for. The proxy fetches upstream _before_ serving; a scheduled
   mirror can lag its refresh interval (`lag`); `--reference` is only as fresh as its local
   seed (`seed`).
4. Only new objects cross the WAN per request; the bulk history is served from the local mirror.
5. Never writes to or proactively replicates from the origin - it only ever pulls what a
   client requested.
6. Works against any unmodified Git smart-HTTP origin. GitLab Geo needs GitLab on both ends.

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

## Benchmark

For a fleet of ephemeral clients cloning the same repo, only the first clone pays
the WAN cost; the rest are served from the local mirror. Cloning a 64 MB repo over
an emulated 20 Mbit/s, 60 ms-RTT link:

| Scenario                      | Clone time | WAN bytes |
| ----------------------------- | ---------: | --------: |
| Direct clone (today)          |     28.1 s |     64 MB |
| Via proxy, cold (runner 1)    |     28.5 s |     64 MB |
| Via proxy, warm (runner 2..N) |      0.6 s |     ~0 MB |

The first runner sees no penalty and every subsequent runner clones ~47x faster
while nothing crosses the WAN; the saving scales with fleet size and link cost.
Reproduce or retune (`TOTAL_MB`, `RATE_MBIT`, `RTT_MS`) with
[`bench/run.sh`](./bench/run.sh) - see [`bench/README.md`](./bench/README.md) for
the method and its caveats.

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

```
git config url."http://proxy:8080/".insteadOf            "https://git.example.com/"
git config url."https://git.example.com/".pushInsteadOf  "https://git.example.com/"
```

### Try it

The image on GHCR runs as-is against any public origin, no config file. Point it at
GitHub and clone through it:

```sh
docker run --rm -p 8080:8080 ghcr.io/rolandjitsu/git-cache-proxy \
  --upstream https://github.com

# in another terminal - the proxy mirrors the repo on the first request and
# serves it from the local mirror thereafter:
git -c url."http://localhost:8080/".insteadOf="https://github.com/" \
  clone https://github.com/rolandjitsu/git-cache-proxy
```

## Configuration

Every flag has an environment-variable equivalent.

| Flag                     | Env                                  | Default                      | Purpose                                                                        |
| ------------------------ | ------------------------------------ | ---------------------------- | ------------------------------------------------------------------------------ |
| `--bind`                 | `GITCACHEPROXY_BIND`                 | `0.0.0.0:8080`               | Listen address                                                                 |
| `--cache-root`           | `GITCACHEPROXY_CACHE_ROOT`           | `/var/cache/git-cache-proxy` | Where bare mirrors live                                                        |
| `--upstream`             | `GITCACHEPROXY_UPSTREAM`             | - (required)                 | Origin git base URL                                                            |
| `--upstream-auth-header` | `GITCACHEPROXY_UPSTREAM_AUTH_HEADER` | -                            | Full HTTP header injected on upstream clone/fetch, e.g. `Authorization: Basic <base64>` (see Auth model); injected via env so it stays out of argv |
| `--serve-token`          | `GITCACHEPROXY_SERVE_TOKEN`          | -                            | If set, clients must send `Authorization: Bearer <token>`                      |
| `--fetch-ttl-seconds`    | `GITCACHEPROXY_FETCH_TTL_SECONDS`    | `10`                         | Skip upstream fetch if refreshed within this window (`0` = always fetch)       |
| `--max-concurrent-requests` | `GITCACHEPROXY_MAX_CONCURRENT_REQUESTS` | `64`                    | Max concurrent in-flight requests; excess queue (`0` = unlimited)              |
| `--max-decoded-body-mb`  | `GITCACHEPROXY_MAX_DECODED_BODY_MB`  | `512`                        | Cap on a decoded upload-pack request body, in MiB (bounds memory / gzip bombs) |
| `--git-binary`           | `GITCACHEPROXY_GIT_BINARY`           | `git`                        | Path to git                                                                    |

Endpoints: `/healthz`, `/readyz`, `/metrics` (Prometheus).

## Auth model

- **Upstream:** the proxy holds a single read-only credential (`--upstream-auth-header`)
  used only to `fetch`/`clone`. It never writes upstream. The value is the full
  header line and is passed verbatim to git as `http.extraHeader`. If it is
  unset/blank the proxy warns at startup and contacts upstream anonymously.
- **Clients:** anonymous by default (intended for a network-restricted
  deployment); set `--serve-token` to require a bearer token.

### GitLab with a personal access token

GitLab's git smart-HTTP endpoint authenticates a PAT via **HTTP Basic** auth
(the PAT is the password; any non-empty username works) — not `Authorization:
Bearer` (OAuth2 access tokens only) and not the `PRIVATE-TOKEN` header (REST API
only). Build the header as Basic auth:

```bash
export GITCACHEPROXY_UPSTREAM_AUTH_HEADER="Authorization: Basic $(printf 'oauth2:%s' "$GITLAB_TOKEN" | base64 | tr -d '\n')"
```

Verify it against the git endpoint before wiring up the proxy — a `200` means
the header works, a `401` means the scheme is wrong:

```bash
curl -sS -o /dev/null -w '%{http_code}\n' \
  -H "$GITCACHEPROXY_UPSTREAM_AUTH_HEADER" \
  "https://gitlab.example.com/group/repo.git/info/refs?service=git-upload-pack"
```

When passing it into a container, prefer `-e GITCACHEPROXY_UPSTREAM_AUTH_HEADER`
(no `=`) so an unset variable is dropped with a warning rather than silently
forwarded as an empty string.

## Security model

The proxy is a shared, credentialed reader, so a few properties are worth making
explicit before you expose it:

- **Reachability is the trust boundary.** The proxy fetches from upstream with
  its own single credential and serves the result to whoever asked. There is no
  per-repo authorization: with `--serve-token` unset it serves anonymously, and
  when set the token is one shared secret that grants access to _everything the
  upstream credential can read_ - including repos a given client could not read
  directly. Restrict who can reach the proxy (private network, security group,
  mTLS at the ingress) and treat "can reach the port" as "can read every
  mirrored repo".
- **Plain HTTP.** The proxy speaks HTTP, so a `--serve-token` bearer travels in
  cleartext. Terminate TLS in front of it (reverse proxy / ingress) on any
  network you do not fully trust. Upstream fetches use whatever scheme the
  `--upstream` URL specifies - use `https://`.
- **Defaults are open.** It binds `0.0.0.0:8080` and serves anonymously unless
  `--serve-token` is set. That is deliberate for a locked-down CI network; do
  not place it on an untrusted one without a token and TLS.
- **DoS knobs.** `--max-concurrent-requests` caps concurrent upstream
  clone/fetch work and `--max-decoded-body-mb` bounds request-body memory
  (defusing a decompression bomb). The on-disk cache still grows unbounded (no
  eviction yet - see the roadmap), so isolate and monitor the cache volume.
- **Read-only.** Only `git-upload-pack` (clone/fetch) is served; `git-receive-pack`
  (push) is refused and upstream is only ever pulled from, never written.

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
