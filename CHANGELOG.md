# Changelog

All notable changes are documented here. This file is managed by
[knope](https://knope.tech/) from the Conventional Commits on `main`; do not edit it by
hand.
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
