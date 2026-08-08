#!/usr/bin/env bash
# Benchmark: what does the proxy save a fleet of ephemeral clients?
#
# Everything runs on localhost. A userspace shim (shim.py) emulates a slow WAN
# in front of a local `git daemon` origin and counts bytes crossing it. We time
# three clones, all crossing that same emulated WAN:
#
#   A. direct     - client clones the origin through the WAN (today, no proxy).
#                   Every runner pays this.
#   B. cold proxy - client clones from the proxy; the proxy fetches the origin
#                   through the WAN once (runner #1).
#   C. warm proxy - client clones from the proxy again; within the fetch TTL the
#                   proxy serves from its local mirror, so ~0 bytes cross the WAN
#                   (runner #2..N).
#
# Tunables (env): TOTAL_MB (repo size), RATE_MBIT (WAN bandwidth), RTT_MS.
set -euo pipefail

cd "$(dirname "$0")/.."
ROOT="$(pwd)"

TOTAL_MB="${TOTAL_MB:-64}"
CHUNK_MB="${CHUNK_MB:-8}"
RATE_MBIT="${RATE_MBIT:-20}"
RTT_MS="${RTT_MS:-60}"
DAEMON_PORT="${DAEMON_PORT:-9419}"
SHIM_PORT="${SHIM_PORT:-9420}"
PROXY_PORT="${PROXY_PORT:-8899}"

WORK="$(mktemp -d "${TMPDIR:-/tmp}/git-cache-proxy-bench.XXXXXX")"
ORIGIN="$WORK/origin"
CACHE="$WORK/cache"
COUNTER="$WORK/counter"
BIN="$ROOT/target/release/git-cache-proxy"

DAEMON_PID="" PROXY_PID="" SHIM_PID=""
cleanup() {
  [ -n "$SHIM_PID" ] && kill "$SHIM_PID" 2>/dev/null || true
  [ -n "$PROXY_PID" ] && kill "$PROXY_PID" 2>/dev/null || true
  [ -n "$DAEMON_PID" ] && kill "$DAEMON_PID" 2>/dev/null || true
  wait 2>/dev/null || true
  rm -rf "$WORK"
}
trap cleanup EXIT

now() { python3 -c 'import time; print(time.time())'; }
elapsed() { python3 -c "print(f'{$2 - $1:.1f}')"; }

# --- build ---------------------------------------------------------------
echo ">> building release binary"
cargo build --release --bin git-cache-proxy >/dev/null 2>&1

# --- origin repo of incompressible history -------------------------------
echo ">> creating ${TOTAL_MB}MB origin repo"
mkdir -p "$ORIGIN/work"
git -C "$ORIGIN/work" init -q
git -C "$ORIGIN/work" config user.email bench@example.com
git -C "$ORIGIN/work" config user.name bench
commits=$(( TOTAL_MB / CHUNK_MB ))
for i in $(seq 1 "$commits"); do
  dd if=/dev/urandom of="$ORIGIN/work/blob_$i.bin" bs=1048576 count="$CHUNK_MB" status=none
  git -C "$ORIGIN/work" add -A
  git -C "$ORIGIN/work" commit -q -m "commit $i"
done
git clone -q --bare "$ORIGIN/work" "$ORIGIN/bench.git"
rm -rf "$ORIGIN/work"

# --- origin daemon (upload-pack only) ------------------------------------
git daemon --reuseaddr --listen=127.0.0.1 --port="$DAEMON_PORT" \
  --base-path="$ORIGIN" --export-all "$ORIGIN" &
DAEMON_PID=$!
sleep 1

start_shim() {
  : > "$COUNTER"
  python3 "$ROOT/bench/shim.py" --listen-port "$SHIM_PORT" \
    --origin-port "$DAEMON_PORT" --rate-mbit "$RATE_MBIT" --rtt-ms "$RTT_MS" \
    --counter-file "$COUNTER" &
  SHIM_PID=$!
  sleep 1
}
stop_shim() {
  kill "$SHIM_PID" 2>/dev/null || true
  wait "$SHIM_PID" 2>/dev/null || true
  SHIM_PID=""
}
wan_mb() {
  local d; d=$(cut -d' ' -f1 "$COUNTER" 2>/dev/null); [ -n "$d" ] || d=0
  python3 -c "print(f'{$d / 1048576:.1f}')"
}

# --- A: direct clone through the WAN -------------------------------------
echo ">> A: direct clone (through WAN)"
start_shim
t0=$(now); git clone -q "git://127.0.0.1:$SHIM_PORT/bench.git" "$WORK/a" ; t1=$(now)
stop_shim
A_TIME=$(elapsed "$t0" "$t1"); A_MB=$(wan_mb)
rm -rf "$WORK/a"

# --- proxy up ------------------------------------------------------------
"$BIN" --bind "127.0.0.1:$PROXY_PORT" --cache-root "$CACHE" \
  --upstream "git://127.0.0.1:$SHIM_PORT" --fetch-ttl-seconds 3600 >/dev/null 2>&1 &
PROXY_PID=$!
for _ in $(seq 1 30); do
  curl -fsS "http://127.0.0.1:$PROXY_PORT/readyz" >/dev/null 2>&1 && break || sleep 0.5
done

# --- B: cold clone via proxy (runner #1) ---------------------------------
echo ">> B: cold clone via proxy (runner #1)"
start_shim
t0=$(now); git clone -q "http://127.0.0.1:$PROXY_PORT/bench.git" "$WORK/b" ; t1=$(now)
stop_shim
B_TIME=$(elapsed "$t0" "$t1"); B_MB=$(wan_mb)
rm -rf "$WORK/b"

# --- C: warm clone via proxy (runner #2..N) ------------------------------
echo ">> C: warm clone via proxy (runner #2..N)"
start_shim
t0=$(now); git clone -q "http://127.0.0.1:$PROXY_PORT/bench.git" "$WORK/c" ; t1=$(now)
stop_shim
C_TIME=$(elapsed "$t0" "$t1"); C_MB=$(wan_mb)
rm -rf "$WORK/c"

# --- report --------------------------------------------------------------
echo
echo "repo=${TOTAL_MB}MB  WAN=${RATE_MBIT}Mbit/s  RTT=${RTT_MS}ms"
printf '%-28s %10s %12s\n' "scenario" "wall (s)" "WAN (MB)"
printf '%-28s %10s %12s\n' "A direct (per runner)" "$A_TIME" "$A_MB"
printf '%-28s %10s %12s\n' "B cold proxy (runner 1)" "$B_TIME" "$B_MB"
printf '%-28s %10s %12s\n' "C warm proxy (runner 2+)" "$C_TIME" "$C_MB"
