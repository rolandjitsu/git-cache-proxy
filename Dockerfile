# syntax=docker/dockerfile:1
# Multi-stage build. The runtime image MUST contain `git`, since the proxy
# delegates all protocol work to the system git binary.

FROM rust:1-bookworm AS build
WORKDIR /src
# Build just this crate. When extracted to its own repo, copy the whole context.
COPY . .
RUN cargo build --release --bin git-cache-proxy

FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends git ca-certificates \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/git-cache-proxy /usr/local/bin/git-cache-proxy
# Bare mirrors live here; mount a volume for persistence across restarts.
ENV GITCACHEPROXY_CACHE_ROOT=/var/cache/git-cache-proxy
VOLUME /var/cache/git-cache-proxy
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/git-cache-proxy"]
