# Configuration

Every flag has an environment-variable equivalent.

| Flag                     | Env                                  | Default                      | Purpose                                                                        |
| ------------------------ | ------------------------------------ | ---------------------------- | ------------------------------------------------------------------------------ |
| `--bind`                 | `GITCACHEPROXY_BIND`                 | `0.0.0.0:8080`               | Listen address                                                                 |
| `--cache-root`           | `GITCACHEPROXY_CACHE_ROOT`           | `/var/cache/git-cache-proxy` | Where bare mirrors live                                                        |
| `--upstream`             | `GITCACHEPROXY_UPSTREAM`             | - (required)                 | Origin git base URL                                                            |
| `--upstream-auth-header` | `GITCACHEPROXY_UPSTREAM_AUTH_HEADER` | -                            | Full HTTP header injected on upstream clone/fetch, e.g. `Authorization: Basic <base64>` (see [Auth model](./security.md#auth-model)); injected via env so it stays out of argv |
| `--upstream-credential`  | `GITCACHEPROXY_UPSTREAM_CREDENTIAL`  | `static`                     | Which credential the proxy presents upstream: `static` (the shared `--upstream-auth-header`) or `client` (the requesting client's own, so no long-lived upstream secret). `client` requires `--auth-provider github`/`gitlab` and rejects `--upstream-auth-header` (see [Auth model](./security.md#auth-model)) |
| `--serve-token`          | `GITCACHEPROXY_SERVE_TOKEN`          | -                            | If set, clients must send `Authorization: Bearer <token>`                      |
| `--auth-provider`        | `GITCACHEPROXY_AUTH_PROVIDER`        | `none`                       | Client-auth mode: `none`, `shared`, `github`, `gitlab`, `custom` (see [Auth model](./security.md#auth-model)). Passing only `--serve-token` keeps the legacy shared-secret behavior |
| `--auth-signing-key`     | `GITCACHEPROXY_AUTH_SIGNING_KEY`     | random per process           | Provider modes: HMAC key (at least 32 bytes) signing LFS download grants; every replica behind one address needs the same key |
| `--auth-validate-url`    | `GITCACHEPROXY_AUTH_VALIDATE_URL`    | -                            | `custom` only: URL template of the validation endpoint, with `{repo}` interpolated (the token goes in `--auth-token-header`, never the URL) |
| `--auth-token-header`    | `GITCACHEPROXY_AUTH_TOKEN_HEADER`    | `Authorization: Bearer {token}` | `custom` only: header line presenting the client token to the endpoint, with `{token}` interpolated, e.g. `PRIVATE-TOKEN: {token}` |
| `--auth-repo-form`       | `GITCACHEPROXY_AUTH_REPO_FORM`       | `path`                       | `custom` only: repo encoding for `{repo}` - `path` (`owner/repo`) or `encoded-path` (`owner%2Frepo`, GitLab-style) |
| `--auth-timeout-seconds` | `GITCACHEPROXY_AUTH_TIMEOUT_SECONDS` | `10`                         | Budget for one token-validation request, shared by the `--auth-max-concurrent` slot wait and the HTTP call |
| `--auth-ttl-seconds`     | `GITCACHEPROXY_AUTH_TTL_SECONDS`     | `0`                          | Reuse a validation verdict per `(credential, repo)` for this many seconds, so the requests of one clone, or repeated submodule fetches, validate once (`0` = validate every request) |
| `--auth-max-concurrent`  | `GITCACHEPROXY_AUTH_MAX_CONCURRENT`  | `32`                         | Max token-validation requests in flight; a check waits up to `--auth-timeout-seconds` for a slot, then fails with 503 (`0` = unlimited) |
| `--auth-allow-http`      | `GITCACHEPROXY_AUTH_ALLOW_HTTP`      | `false`                      | Provider modes: accept a plain `http://` validation endpoint, which sends client credentials in cleartext |
| `--fetch-ttl-seconds`    | `GITCACHEPROXY_FETCH_TTL_SECONDS`    | `10`                         | Skip upstream fetch if the last one started within this window (`0` = always fetch) |
| `--max-stale-seconds` | `GITCACHEPROXY_MAX_STALE_SECONDS` | `0` | Maximum age of cached refs served during background refresh (`0` = disabled) |
| `--max-background-fetches` | `GITCACHEPROXY_MAX_BACKGROUND_FETCHES` | `4` | Maximum concurrent background refreshes (at least `1`) |
| `--max-concurrent-requests` | `GITCACHEPROXY_MAX_CONCURRENT_REQUESTS` | `64`                    | Request capacity: strict mode queues; stale mode isolates local and upstream work and rejects overload (see README for `0`)              |
| `--max-concurrent-upload-packs` | `GITCACHEPROXY_MAX_CONCURRENT_UPLOAD_PACKS` | `0`            | Max concurrent `git upload-pack` processes, held until each exits; bounds `pack-objects` memory. Clones waiting for a slot hold a `--max-concurrent-requests` slot (`0` = unlimited) |
| `--max-decoded-body-mb`  | `GITCACHEPROXY_MAX_DECODED_BODY_MB`  | `512`                        | Cap on a decoded upload-pack request body, in MiB (bounds memory / gzip bombs) |
| `--cache-max-mb`         | `GITCACHEPROXY_CACHE_MAX_MB`         | `0`                          | Cap on total on-disk mirror cache, in MiB; evicts least-recently-used idle mirrors when exceeded (`0` = unlimited, no eviction) |
| `--max-wants`            | `GITCACHEPROXY_MAX_WANTS`            | `100`                        | Cap on want-by-SHA pins retained per mirror; oldest pruned beyond it (`0` = unlimited) |
| `--git-binary`           | `GITCACHEPROXY_GIT_BINARY`           | `git`                        | Path to git                                                                    |
| `--big-file-threshold`   | `GITCACHEPROXY_BIG_FILE_THRESHOLD`   | `8m`                         | Stream blobs larger than this to disk during upstream clone/fetch instead of holding them in memory, bounding `index-pack` RSS so one very large repo can't OOM the proxy |
| `--pack-threads`         | `GITCACHEPROXY_PACK_THREADS`         | -                            | git `pack.threads` when serving: delta-search threads per `pack-objects` (unset = one per CPU) |
| `--pack-window-memory`   | `GITCACHEPROXY_PACK_WINDOW_MEMORY`   | -                            | git `pack.windowMemory` when serving: delta window memory per `pack-objects` thread, e.g. `64m` (unset = unlimited) |

Endpoints: `/healthz`, `/readyz`, `/metrics` (see [Metrics](./metrics.md)).

