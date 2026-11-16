# Architecture

Why git-cache-proxy exists, how it compares to the alternatives, how it
works internally, and a throughput benchmark. For usage see the
[README](../README.md).

## Why not just a mirror or an HTTP cache?

- A **generic HTTP cache** doesn't work: `git fetch` is a _negotiated_ pack
  transfer (`POST /git-upload-pack` responses are computed per request), so
  there's nothing static to cache.
- A **scheduled full mirror** (Gitea pull-mirror, etc.) means enumerating and
  replicating every repo, and it can lag the exact commit CI just pushed.

This proxy is **lazy and pull-only**: it caches on first request, and on every
request runs an incremental `git fetch` before serving - so a client always gets
the ref it asked for, and nothing is ever pushed or replicated to the origin.
That pull-only property is what makes it viable against a sensitive origin.

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

All git work is delegated to the system `git` binary, so **protocol v2**,
shallow, and partial (filtered) clones are git's responsibility rather than a
reimplemented wire format.

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

Upstream clone/fetch retries recognized transient network failures up to three
times after the first attempt, waiting 1, 2, then 4 seconds. This includes TLS
EOF, connection resets/timeouts, temporary DNS failures, and HTTP 408, 429,
500, 502, 503, and 504. Authentication, missing repositories, certificate
validation, local filesystem errors, and unknown failures are not retried.
Classification uses Git's English stderr diagnostics; the child locale is fixed
to `C`. Retry logs contain the operation, attempt, delay, and error category,
without raw upstream stderr or credentials.

Retries stay inside the existing per-repository lock. Each failed clone's staging
directory is removed before another attempt; an existing fetch mirror is kept.
Only success updates the freshness timestamp. After retries are exhausted the
request still fails with 502; stale refs are not served as a fallback. The retry
budget limits attempts and adds at most seven seconds of backoff; it does not
impose a new transfer timeout on large repositories. LFS and local upload-pack
operations are outside this retry policy.

If a client `want`s a commit by SHA that the mirror never captured - typically a
GitHub pull-request merge commit, which lives only under the unadvertised
`refs/pull/<n>/merge` and so is what `actions/checkout` fetches on a PR build - the
proxy fetches that SHA from upstream on demand and serves it, instead of failing
with `not our ref`. This relies on the upstream serving arbitrary SHAs (GitHub's
`uploadpack.allowAnySHA1InWant`, which is on); an upstream that refuses simply
leaves the request to fail as it would without the proxy. Ordinary branch/tag
clones are unaffected - they pay only a cheap local object check, never an extra
upstream call. Each fetched SHA is pinned under a reserved ref so the mirror can
keep serving it; `--max-wants` bounds how many such pins a mirror retains, pruning
the oldest so they cannot accumulate without bound (like the mirror-level LRU, but
scoped to a single mirror's pins). If a single request needs more new SHAs than
`--max-wants`, only that many are fetched and the excess wants are left to fail with
`not our ref` (the proxy logs a `capping want-by-sha fetch` warning); such a request
would immediately have its pins pruned anyway, so raise `--max-wants` if you expect
that many unadvertised objects at once.

When serving client clone/fetch requests, the proxy writes negotiation input
to local git upload-pack while streaming its response to the client.
Writing the request has a five-minute deadline. The operation also stops
after 30 minutes without reading response data from Git, including during
pack generation. Each read resets this inactivity deadline. Once the request
is written, active transfers have no total duration limit.

These fixed limits are separate from upstream retries. Cancellation, pipe
errors and timeouts trigger subprocess cleanup. Failures after response
headers are sent terminate the response body. On Unix targets with
`waitid(NOWAIT)`, leader exit is observed independently of stdout EOF and
remaining group members are stopped before the leader is reaped. Other
targets wait for request completion and stdout EOF before observing exit.

### git-LFS

LFS objects use a different HTTP API from the git protocol, so they are cached
separately:

```
POST <repo>/info/lfs/objects/batch
  -> forward to upstream, then rewrite each object's download URL back to this
     proxy so the object fetch is cached here
GET  <repo>/info/lfs/objects/<oid>
  -> serve from the on-disk cache, or on a miss fetch it from upstream once
     (verify sha256 == oid, store), then serve
anything with operation=upload  -> 403 (read-only)
```

Objects are content-addressed and immutable, so a cached object is never stale and
is shared across every repo that references the same oid; the first fetch across the
fleet pays the WAN cost, the rest are served locally. The upstream LFS transfer is
in-process over HTTPS (reqwest + rustls, no OpenSSL), so the binary stays statically
linked. Cached objects share the `--cache-max-mb` budget and LRU eviction with the
git mirrors. No configuration is needed: LFS is served on the same endpoints the git
client already routes through the proxy.

### Cache management

The cache is self-managing. Mirrors are created on first request, and with
`--cache-max-mb` set, least-recently-used idle ones are evicted in the background
once the total exceeds the cap - that cap is the intended way to reclaim disk. There
is no endpoint to delete a specific mirror, and none is needed: an evicted or missing
mirror is transparently re-cloned on its next request, so eviction is a cache
concern, never a correctness one.

Deleting a mirror directory by hand works but is not the intended lever. The proxy
assumes it owns the cache root, so a manual deletion reaches the cache-size metrics
only once the background task reconciles it (within a minute) or the next restart
rescans. Prefer `--cache-max-mb`.

## Benchmark

For a fleet of ephemeral clients fetching the same content, only the first fetch
pays the WAN cost; the rest are served locally. Over an emulated 20 Mbit/s,
60 ms-RTT link - cloning a 64 MB repo, and fetching a 32 MB git-LFS object:

| Scenario                          |    Time | WAN bytes |
| --------------------------------- | ------: | --------: |
| Clone, direct (today)             |  28.1 s |     64 MB |
| Clone, via proxy cold (runner 1)  |  28.5 s |     64 MB |
| Clone, via proxy warm (2..N)      |   0.6 s |     ~0 MB |
| LFS object, cold (runner 1)       |  12.8 s |     32 MB |
| LFS object, warm (2..N)           |   0.2 s |     ~0 MB |

The first runner sees no penalty and every subsequent runner is served in-region
while nothing crosses the WAN; the saving scales with fleet size and link cost.
Reproduce or retune (`TOTAL_MB`, `LFS_MB`, `RATE_MBIT`, `RTT_MS`) with
[`bench/run.sh`](../bench/run.sh) - see [`bench/README.md`](../bench/README.md) for
the method and its caveats.

