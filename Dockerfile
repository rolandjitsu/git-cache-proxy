# syntax=docker/dockerfile:1
# Two-stage build producing a small, statically linked (musl) image. The runtime
# can't be distroless: the proxy delegates the git wire protocol to the system
# `git` binary, and Alpine is the smallest base that ships one.

FROM rust:alpine AS build
# musl-dev for the static libc; build-base gives the C toolchain `ring` (rustls'
# crypto provider) compiles its assembly with.
RUN apk add --no-cache build-base
WORKDIR /src
COPY . .
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
