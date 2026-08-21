# Benchmark

`run.sh` measures what the proxy saves a fleet of ephemeral clients that fetch
the same content over a slow link - both git clones and git-LFS objects.
Everything runs on localhost, so it needs no privileges and no Docker - just
`git`, `curl`, `python3`, and `cargo`.

```sh
./bench/run.sh
# tunables (env): TOTAL_MB=64 CHUNK_MB=8 LFS_MB=32 RATE_MBIT=20 RTT_MS=60
```

## What it does

1. Builds the release binary and creates a bare origin repo of incompressible
   random history (so the pack size is predictable), served by `git daemon`.
2. Runs `shim.py`, a userspace TCP proxy that emulates a slow WAN in front of the
   origin: a token-bucket bandwidth cap plus a fixed one-way latency. It counts
   the bytes that cross it.
3. Times three clones, all crossing that emulated WAN:
   - **A, direct** - client clones the origin through the WAN. Every runner pays this today.
   - **B, cold proxy** - client clones from the proxy, which fetches the origin through the WAN once (runner 1).
   - **C, warm proxy** - client clones from the proxy again; within the fetch TTL it serves from the local mirror, so ~0 bytes cross the WAN (runner 2..N).
4. Then, for git-LFS, points the shim at `lfs_origin.py` (a tiny batch-API + object
   server) and fetches one object through the proxy with `curl`:
   - **D, cold LFS** - the object is fetched from the origin through the WAN once (runner 1).
   - **E, warm LFS** - the object is served from the content-addressed cache, so ~0 bytes cross the WAN (runner 2..N).

## Caveats

This is a **bandwidth-bound approximation**, good for showing the shape of the
saving, not a precise WAN emulator:

- Transfer time is ~= `bytes / RATE_MBIT`, which is easy to sanity-check, but the
  shim applies latency once per connection rather than per round trip, so it
  under-models RTT-heavy negotiation. For faithful latency use `tc netem` (Linux).
- The **byte counts are exact** - those are the headline result and do not depend
  on the emulation fidelity.
- **Same WAN transport throughout** - the direct clone (A) and the proxy's upstream
  fetch (B) both cross the shim over `git://`; the client-to-proxy hop is local HTTP
  that never crosses the shim, so the byte comparison is like-for-like. For LFS, the
  batch response advertises the object href through the shim too, so the cold fetch
  (D) crosses the WAN and the warm fetch (E) does not.
