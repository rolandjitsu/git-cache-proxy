# Authentication and security

How the proxy authenticates clients and reaches upstream, and the
properties to weigh before exposing it. For the flag reference see
[configuration](./configuration.md).

## Auth model

- **Upstream:** the proxy holds a single read-only credential (`--upstream-auth-header`)
  used only to `fetch`/`clone`. It never writes upstream. The value is the full
  header line and is passed verbatim to git as `http.extraHeader`. If it is
  unset/blank the proxy warns at startup and contacts upstream anonymously.
- **Clients:** anonymous by default (intended for a network-restricted
  deployment); set `--serve-token` to require one shared bearer token, or set
  `--auth-provider` to validate each request's own credential per repo
  (below). The shared token is read from `Authorization: Bearer <token>`
  (git's `http.extraHeader`). Provider mode also reads `Authorization: Basic
  <user:token>` (git's credential helpers and tokened clone URLs), and answers
  a 401 with a Basic challenge so git sends them.

### Per-request token validation (`--auth-provider github|gitlab|custom`)

With a shared serve token, anyone who can reach the proxy can read every repo
its upstream credential can. In `github`/`gitlab`/`custom` mode the proxy
instead checks each request's credential *for the repo that request names*,
before any upstream or mirror work - so a client only reaches what its own
credential could clone directly, and a CI fleet can run on short-lived per-job
tokens. The repo that is checked is exactly the repo that is served, so a
credential can never be checked against one repo while fetching another: repo
names must consist of letters, digits, `-`, `.`, `_`, `~`, and `/`, since a
`%` escape could decode to a different path upstream than at the check.

The validation endpoint receives every client credential, so it must be
`https://` unless `--auth-allow-http` is set (e.g. for an in-cluster endpoint
on a trusted network).

| Provider | Check | Notes |
| -------- | ----- | ----- |
| `github` | `GET {upstream}/{repo}/info/refs?service=git-upload-pack` (protocol v2) | Basic is forwarded as sent; a Bearer token goes as Basic `x-access-token:<token>` |
| `gitlab` | same | Basic is forwarded as sent (CI job tokens need the `gitlab-ci-token` username); a Bearer token goes as Basic `oauth2:<token>` |
| `custom` | `GET --auth-validate-url` with `--auth-token-header` | Any HTTP endpoint: 2xx allows, 401/403/404 rejects |

`github` and `gitlab` ask upstream's own git endpoint rather than a REST API: a
metadata API also answers credentials that cannot read the code (GitHub tokens
with only the Metadata permission, GitLab Guest members, `read_api`-only
tokens), while `info/refs` only advertises to a credential that can clone. Over
protocol v2 the answer is a few hundred bytes of capabilities, not the ref
list, and a 200 only counts if it is a git advertisement.

A 2xx allows the request; 401/403/404 rejects it with 401. Everything else -
5xx, 3xx (redirects are not followed, so a renamed repo needs its new path),
429, a throttled 403 (`x-ratelimit-remaining: 0` or `retry-after`) - is *not* a
verdict on the credential, so the request fails closed with 503: during an
outage the proxy serves nothing rather than something unvalidated. Concurrent
identical checks share one call.

`--auth-ttl-seconds` reuses a verdict per `(credential, repo)`. The default `0`
revalidates every request, so a protocol v2 clone costs at least three checks
(info/refs, then an upload-pack POST each for `ls-refs` and `fetch`), and a
checkout with submodules pays that per submodule on every fetch. For
submodule-heavy or frequently re-fetching workloads, set a short TTL (e.g.
`60`): that bounds how long a revoked credential keeps working.

Clients route through the proxy as usual and present their credential to it;
for GitHub Actions:

```bash
git config --global url."http://git-cache-proxy:8080/".insteadOf "https://github.com/"
git config --global http."http://git-cache-proxy:8080/".extraHeader \
  "Authorization: Bearer $GITHUB_TOKEN"
```

The per-run `GITHUB_TOKEN` is scoped to its repository and expires with the
job, so a compromised runner can only clone its own repo and cannot persist
access.

git-LFS does not need a TTL. The batch request is checked like any other, and
each rewritten download action carries a grant the proxy signs for exactly
that repo, object, and size, valid for 15 minutes. Object GETs verify the
grant locally instead of rechecking the credential, and a grant for one repo
cannot fetch another repo's object from the shared object cache. Every replica
behind one address must share `--auth-signing-key`, or a grant issued by one
is refused by the others; the caches themselves stay independent.

### Fetch upstream with the client's credential (`--upstream-credential client`)

In the default `static` mode the proxy clones and fetches upstream with the one
shared `--upstream-auth-header`, so that credential must be able to read every
repo the fleet mirrors - a leak of it exposes all of them. In `client` mode the
proxy instead uses the credential of the request that triggered the upstream
work, so no long-lived upstream secret is held anywhere. It builds on provider
validation: it requires `--auth-provider github` or `gitlab` (`custom` has no
upstream-shaped credential), rejects `--upstream-auth-header`, and the usual
"contacting upstream anonymously" startup warning does not apply.

Validation still runs first and unchanged - a cache hit or a coalesced waiter
never reaches upstream, and the mirror is shared - so the per-repo check, not
the fetch, is what gates access. The credential is reshaped exactly as the
validation probe reshapes it, so the check and the fetch cannot disagree. A
burst of clients for one repo coalesces into a single fetch under whichever
client won the lock; the rest, already validated for that repo, are served from
its result.

Because the credential is the client's own, an upstream auth failure on the
fetch is answered as `401` (with the Basic challenge), not `502`, and the cached
verdict for that `(credential, repo)` is dropped so the next request re-probes -
e.g. a token revoked within the `--auth-ttl-seconds` window. `static` mode keeps
the `502`.

For LFS the batch request forwards the client credential like git does; an
object GET carries only its grant, so the grant seals the credential (an AEAD
box, see the Security model) and the object fetch unseals it to re-batch
upstream. Grants stay valid for 15 minutes across replicas sharing
`--auth-signing-key`, exactly as in `static` mode.

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
  its own single credential and serves the result to whoever asked. With
  `--serve-token` unset it serves anonymously, and when set the token is one
  shared secret that grants access to _everything the upstream credential can
  read_ - including repos a given client could not read directly. Restrict who
  can reach the proxy (private network, security group, mTLS at the ingress).
- **Per-request validation bounds, not replaces, reachability.** In
  `--auth-provider github/gitlab/custom` mode each request must carry a
  credential that can clone *that repo*, so a leaked client token only exposes
  what that token already exposed; "can reach the port" becomes "can run an
  unauthenticated guessing attack against the validation endpoint". Validation
  fails closed: an outage means a 503 for clients, never an allow, and the
  proxy still fetches upstream with its own shared read credential - keep that
  one as narrowly scoped as the fleet allows.
- **Client upstream credential (`--upstream-credential client`).** This removes
  the shared upstream secret entirely: there is no long-lived upstream token in
  the secret store, the pod env, or the deployment. The trade is that the proxy
  process and its `git` children now hold many clients' credentials in memory
  while a fetch runs, and each LFS grant carries a sealed copy. Those credentials
  are short-lived and repo-scoped (a CI job token), but a compromised proxy, or a
  leaked `--auth-signing-key`, could use the ones it holds or can unseal for their
  remaining lifetime. An upstream auth failure is reported to the client as `401`,
  not `502`, since the credential is the client's own.
- **Plain HTTP.** The proxy speaks HTTP, so a client bearer travels in
  cleartext. Terminate TLS in front of it (reverse proxy / ingress) on any
  network you do not fully trust. Upstream fetches use whatever scheme the
  `--upstream` URL specifies - use `https://`.
- **Defaults are open.** It binds `0.0.0.0:8080` and serves anonymously unless
  `--serve-token` is set or a provider is configured. That is deliberate for a
  locked-down CI network; do not place it on an untrusted one without auth and
  TLS.
- **DoS knobs.** `--max-concurrent-requests` caps concurrent upstream
  clone/fetch work, `--max-concurrent-upload-packs` caps concurrent
  `pack-objects` memory, and `--max-decoded-body-mb` bounds request-body memory
  (defusing a decompression bomb). `--cache-max-mb` bounds on-disk growth by
  evicting least-recently-used idle mirrors; it defaults to `0` (unlimited), so
  set it - or isolate and monitor the cache volume - on an untrusted network.
  Provider validation adds one round-trip per request (`3` per v2 clone
  without a TTL), capped by `--auth-timeout-seconds`, and runs outside the
  concurrency limit so a hung or flooded check cannot starve serving.
  `--auth-max-concurrent` bounds the outbound calls in flight, so a flood of
  junk credentials turns into 503s rather than unbounded load on the git host.
  Each junk credential still costs one call, and failed logins count against
  the proxy's egress IP at the git host: rate-limit unauthenticated clients at
  the ingress.
- **Read-only.** Only `git-upload-pack` (clone/fetch) is served; `git-receive-pack`
  (push) is refused and upstream is only ever pulled from, never written.

