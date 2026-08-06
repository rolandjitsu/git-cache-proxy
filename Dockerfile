# syntax=docker/dockerfile:1
# Two-stage build producing a small, statically linked image.
#
# The proxy binary is built against musl with crt-static (the default for the
# musl target on `rust:alpine`), so it is fully static and carries no libc
# dependency of its own. The runtime layer is a bare Alpine that adds only
# `git` + CA certs.
#
# Why not a true "distroless" (gcr.io/distroless/static) image? The proxy
# delegates all wire-protocol work to the system `git` binary, so the runtime
# MUST contain git. distroless/static has no package manager and no git, so it
# cannot host this design as-is. Alpine is the smallest base that still ships a
# git package. A genuinely distroless (git-free) image only becomes possible if
# the git plumbing moves in-process to a Rust library (gitoxide/libgit2) - see
# the "no external git binary" item on the roadmap.

FROM rust:alpine AS build
RUN apk add --no-cache musl-dev
WORKDIR /src
COPY . .
# Default target on rust:alpine is x86_64-unknown-linux-musl (static).
RUN cargo build --release --locked --bin git-cache-proxy

FROM alpine:3.20
RUN apk add --no-cache git ca-certificates \
 && rm -rf /var/cache/apk/*
COPY --from=build /src/target/release/git-cache-proxy /usr/local/bin/git-cache-proxy
# Bare mirrors live here; mount a volume for persistence across restarts.
ENV GITCACHEPROXY_CACHE_ROOT=/var/cache/git-cache-proxy
VOLUME /var/cache/git-cache-proxy
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/git-cache-proxy"]
