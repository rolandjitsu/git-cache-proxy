# git-cache-proxy

A Helm chart that deploys [git-cache-proxy](https://github.com/rolandjitsu/git-cache-proxy),
a read-only caching proxy for Git. It sits between CI machines and an origin git server,
keeps a local bare mirror fresh (incremental pull from upstream), and serves clones/fetches
from that mirror - so the bulk history is served in-region and only the delta crosses the
WAN. It is strictly pull-only: it never pushes and never proactively replicates.

## Install

```sh
helm install git-cache-proxy oci://ghcr.io/rolandjitsu/charts/git-cache-proxy \
  --set upstream=https://your-git-host.example.com
```

Pin a version with `--version`, and override defaults with `-f my-values.yaml` or `--set`.
See [Configuration](#configuration).

Clients then clone through the Service as if it were the origin:

```sh
git clone http://git-cache-proxy.<namespace>.svc:8080/<owner>/<repo>.git
```

## Upstream and auth

`upstream` is the origin base URL (default `https://github.com`); requested repo paths are
appended to it. To cache private repos, give the proxy a read-only upstream credential as a
Secret holding the full HTTP `Authorization` header, and reference it:

```sh
kubectl create secret generic gcp-upstream \
  --from-literal=auth-header="Authorization: Bearer $TOKEN"

helm install git-cache-proxy ... \
  --set upstreamAuth.existingSecret=gcp-upstream
```

The header is injected via an env var, so the token never appears in the process argv. For
GitLab, a personal access token goes in as HTTP Basic: the header value is
`Authorization: Basic <base64 of oauth2:$PAT>`.

## Serving auth

By default the proxy serves anonymously, which is intended for a network-restricted
deployment. Anyone who can reach the port can read **every mirrored repo** (the proxy holds
one upstream credential). Restrict who can reach it (private network, NetworkPolicy, mTLS at
the ingress), and/or require a client bearer token via `serveToken.existingSecret`. The
proxy speaks plain HTTP, so terminate TLS in front of it on any untrusted network.

## Single writer (not HA)

The chart runs a single replica with the `Recreate` strategy. The bare mirrors live on one
`ReadWriteOnce` volume and concurrent fetches are already coalesced in-process, so a second
replica would only contend for the same PVC; `Recreate` ensures the PVC detaches from the
old pod before the new one attaches. `replicas` is therefore not exposed. An evicted or lost
mirror is transparently re-cloned on the next request, so an `emptyDir`
(`persistence.enabled=false`) is a valid choice for a pure accelerator.

## Persistence and eviction

With `persistence.enabled` (default), the chart creates a PVC of `persistence.size` on
`persistence.storageClassName` (empty = cluster default), or mounts
`persistence.existingClaim` if set. Bound the cache with `config.cacheMaxMb`: when the total
exceeds it, least-recently-used idle mirrors are evicted in the background until back under.
Leaving it `0` (unlimited) lets the volume grow until full, so set it whenever the volume is
bounded.

## Metrics

The proxy exposes Prometheus metrics at `/metrics` on the Service port: per-repo request and
upstream counters, cache-size gauges, and `*_duration_seconds` fetch/serve latency
histograms. Scrape it with pod annotations (`podAnnotations`) or, with the Prometheus
Operator, set `serviceMonitor.enabled=true`.

## Ingress

A `ClusterIP` Service is exposed by default; reach it in-cluster or port-forward it. Set
`ingress.enabled=true` for a standard `networking.k8s.io/v1` Ingress (configurable
`className`, `host`, `annotations`, `tls`). For a non-standard controller (e.g. a Traefik
`IngressRoute` CRD), leave the Ingress off and manage the route as a separate manifest
pointing at the Service.

## Configuration

| Key | Default | Description |
| --- | --- | --- |
| `image.repository` | `ghcr.io/rolandjitsu/git-cache-proxy` | Image; override for a fork/mirror |
| `image.tag` | `""` | Image tag; empty uses the chart `appVersion` |
| `image.pullPolicy` | `IfNotPresent` | |
| `imagePullSecrets` | `[]` | Pull secrets for a private registry |
| `nameOverride` / `fullnameOverride` | `""` | Override the generated names |
| `upstream` | `https://github.com` | Origin git base URL; repo paths are appended |
| `upstreamAuth.existingSecret` | `""` | Secret with the full upstream `Authorization` header (empty = anonymous) |
| `upstreamAuth.key` | `auth-header` | Key in that Secret |
| `serveToken.existingSecret` | `""` | Secret with a client bearer token to require (empty = anonymous) |
| `serveToken.key` | `token` | Key in that Secret |
| `config.fetchTtlSeconds` | `10` | Skip upstream fetch if refreshed within this window (`0` = always) |
| `config.cacheMaxMb` | `0` | Cap on on-disk cache, MiB; evicts LRU idle mirrors (`0` = unlimited) |
| `config.maxConcurrentRequests` | `64` | Max concurrent in-flight requests (`0` = unlimited) |
| `config.maxDecodedBodyMb` | `512` | Cap on a decoded upload-pack request body, MiB |
| `config.logLevel` | `info` | Log filter directive |
| `config.logFormat` | `text` | `text` or `json` |
| `persistence.enabled` | `true` | Mount a PVC for the cache; `false` uses an emptyDir |
| `persistence.existingClaim` | `""` | Use this PVC instead of creating one |
| `persistence.size` | `20Gi` | Created PVC size |
| `persistence.storageClassName` | `""` | StorageClass for the PVC; empty = cluster default |
| `persistence.accessModes` | `[ReadWriteOnce]` | PVC access modes |
| `persistence.emptyDirSizeLimit` | `20Gi` | emptyDir limit when persistence is off |
| `service.type` | `ClusterIP` | Service type |
| `service.port` | `8080` | Service port and the port the container binds |
| `resources` | `{}` | Pod resource requests/limits |
| `terminationGracePeriodSeconds` | `60` | Drain window for in-flight clones on shutdown |
| `podAnnotations` | `{}` | Extra pod annotations (e.g. a metrics scraper) |
| `podSecurityContext` / `securityContext` | `{}` | Pod/container security contexts (empty = image defaults) |
| `nodeSelector` / `tolerations` / `affinity` | `{}` / `[]` / `{}` | Scheduling |
| `caTrust.enabled` | `false` | Mount a private-CA bundle and point git at it |
| `caTrust.configMapName` / `caTrust.key` | `""` / `ca-certificates.crt` | Source ConfigMap and key |
| `serviceMonitor.enabled` | `false` | Render a Prometheus Operator ServiceMonitor |
| `serviceMonitor.interval` / `.scrapeTimeout` / `.labels` | `30s` / `10s` / `{}` | ServiceMonitor config |
| `ingress.enabled` | `false` | Render a standard Ingress |
| `ingress.className` / `.host` / `.path` / `.pathType` / `.annotations` / `.tls` | see `values.yaml` | Ingress config |

Full defaults and inline comments: [`values.yaml`](./values.yaml).
