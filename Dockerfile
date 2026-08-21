# syntax=docker/dockerfile:1
# Two-stage build producing a small, statically linked image.
#
# The proxy binary is built against musl with crt-static (the default for the
# musl target on `rust:alpine`), so it is fully static and carries no libc
# dependency of its own. The runtime layer is a bare Alpine that adds only
# `git` + CA certs.
#
# Why not a true "distroless" (gcr.io/distroless/static) image? The proxy delegates
# the git wire protocol to the system `git` binary, so the runtime MUST contain git.
# distroless/static has no package manager and no git, so it cannot host this design
# as-is. Alpine is the smallest base that still ships a git package. The LFS HTTPS
# transfer, by contrast, is in-process (reqwest + rustls), so it needs no runtime
# tool - only CA certs. A genuinely git-free image only becomes possible if the git
# plumbing also moves in-process - see the "no external git binary" roadmap item.

FROM rust:alpine AS build
# musl-dev for the static libc; build-base gives the C toolchain `ring` (rustls'
# crypto provider) compiles its assembly with.
RUN apk add --no-cache build-base
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
