# Metrics

`GET /metrics` serves Prometheus text-format metrics. All metrics are prefixed
`gitcacheproxy_`:

| Metric                      | Type      | Labels                 | Meaning                                                                       |
| --------------------------- | --------- | ---------------------- | ----------------------------------------------------------------------------- |
| `requests_total`            | counter   | `kind`, `result`, `repo` | Client requests served, by endpoint and outcome                              |
| `upstream_ops_total`        | counter   | `op`, `result`, `repo` | Upstream git operations (initial mirror clone, refresh fetch, want fetch)     |
| `cache_bytes`               | gauge     | -                      | Total size of the on-disk mirror cache, in bytes                              |
| `cache_mirrors`             | gauge     | -                      | Number of cached mirrors on disk                                              |
| `cache_max_bytes`           | gauge     | -                      | Configured on-disk cache cap in bytes (`0` = unlimited)                        |
| `fetch_ttl_seconds`         | gauge     | -                      | Configured upstream fetch TTL in seconds (`0` = always fetch)                  |
| `evictions_total`           | counter   | -                      | Idle mirrors evicted to keep the cache under `--cache-max-mb`                 |
| `lfs_objects_total`         | counter   | `result`               | Cached git-LFS object lookups                                                 |
| `cache_outcome_total`       | counter   | `outcome`              | info/refs operations by outcome: served locally vs. fetched from upstream     |
| `auth_validations_total`    | counter   | `result`               | Per-request token validation calls                                            |
| `auth_cache_hits_total`     | counter   | -                      | Token checks answered from the `--auth-ttl-seconds` verdict cache             |
| `upstream_duration_seconds` | histogram | `op`, `repo`           | Wall-clock duration of successful upstream clones/fetches                     |
| `serve_duration_seconds`    | histogram | `kind`, `repo`         | Time to serve a client, measured to the end of the response                   |

Label values:

- `kind` on `requests_total`: `info_refs`, `upload_pack`, `auth` (a request
  that failed the `--serve-token` check or token validation), `receive_pack` (a push attempt,
  always refused; the proxy is read-only), `lfs_batch`, `lfs_object`. On
  `serve_duration_seconds`: `info_refs` (the buffered ref advertisement) or
  `upload_pack` (the packfile stream, timed to EOF).
- `result` on `requests_total`: `ok`, `error`, `upstream_error`, `unauthorized`,
  `rejected`. On `upstream_ops_total`: `ok` or `error`. On `lfs_objects_total`:
  `hit` (served from disk), `miss` (fetched from upstream, then cached), `error`.
  On `auth_validations_total`: `allow`, `deny`, `unavailable` (the endpoint
  failed, throttled, or answered something else), `saturated` (no call made: no
  `--auth-max-concurrent` slot freed up in time).
- `op`: `clone` (first mirror of a repo), `fetch` (refresh of an existing
  mirror), `want_fetch` (on-demand fetch of a bare SHA the mirror lacks, e.g. a
  PR merge commit).
- `outcome` on `cache_outcome_total`: `cloned` (first mirror of a repo),
  `fetched` (refresh from upstream), `cached` (served from the local mirror with
  no upstream call). Recorded once per info/refs advertisement.
- `repo`: the served repo path (e.g. `group/project.git`). To keep cardinality
  bounded, the real path is emitted only on success; every failure (failed auth,
  malformed path, upstream error) uses a `-` sentinel, so a flood of distinct
  but doomed paths cannot inflate the series count. Aggregate across repos with
  `sum without (repo) (...)`.

Notes:

- `cache_bytes`/`cache_mirrors` are tracked only with a cache cap
  (`--cache-max-mb`); otherwise they stay `0`. Read utilization as
  `cache_bytes / cache_max_bytes`.
- `lfs_objects_total` and `auth_validations_total` carry no `repo` label: LFS
  objects are shared across repos, and junk credentials name arbitrary ones.
- Local-vs-upstream share: `cached / sum without (outcome) (cache_outcome_total)`.
- Histograms are observed on success only, with buckets from 50ms to 300s.

