# Changelog

All notable changes are documented here. This file is managed by
[knope](https://knope.tech/) from the Conventional Commits on `main`; do not edit it by
hand.
## 0.2.7 (2026-10-31)

### Fixes

- bump quinn-proto and chacha20

## 0.2.6 (2026-10-17)

### Features

- add pack.threads and pack.windowMemory options

## 0.2.5 (2026-10-17)

### Features

- cap concurrent upload-pack processes

## 0.2.4 (2026-10-17)

### Fixes

- start the fetch TTL when the fetch starts

## 0.2.3 (2026-10-17)

### Fixes

- bump rustls to 0.23.46

## 0.2.2 (2026-10-14)

### Fixes

- drain cat-file output while writing queries

## 0.2.1 (2026-10-14)

### Fixes

- kill upload-pack when the client disconnects

## 0.2.0 (2026-10-13)

### Breaking Changes

- a cache volume written by an older, root-running
release is not writable by UID 10001, and /readyz returns 503 until it
is chowned to 10001:10001. On Kubernetes the chart's fsGroup does this.

### Features

- run the image as a non-root user

## 0.1.15 (2026-10-10)

### Features

- serve want-by-sha for unadvertised refs

## 0.1.14 (2026-10-10)

### Fixes

- move the runtime base to alpine 3.24

## 0.1.13 (2026-09-16)

### Fixes

- retry transient upstream clone and fetch failures
- update rustls to fix RUSTSEC-2026-0285

## 0.1.12 (2026-09-11)

### Fixes

- ensure clients can get partial clones

## 0.1.11 (2026-09-01)

### Features

- bound upstream git memory to avoid OOM

## 0.1.10 (2026-08-24)

### Features

- cache git-LFS objects via batch proxy

## 0.1.9 (2026-08-21)

### Features

- add a Helm chart for Kubernetes deployment

## 0.1.8 (2026-08-20)

### Features

- add per-repo fetch/serve latency histograms

## 0.1.7 (2026-08-17)

### Features

- add LRU disk eviction of idle mirrors

## 0.1.6 (2026-08-11)

### Fixes

- exempt probes and metrics from request limit
- avoid staging-dir aliasing when cloning a mirror

## 0.1.5 (2026-08-10)

### Fixes

- enforce body cap on uncompressed requests

## 0.1.4 (2026-08-07)

### Features

- cap concurrent requests

### Fixes

- cap decoded upload-pack request body
- label metrics with repo only on success
- compare serve token in constant time

## 0.1.3 (2026-08-06)

### Features

- add repo label to request metrics

## 0.1.2 (2026-08-06)

### Fixes

- decode gzip-encoded upload-pack request bodies

## 0.1.1 (2026-08-06)

### Features

- add per-repo metrics, structured logging, and graceful shutdown

### Fixes

- read the published version from Cargo.toml, not git tag
