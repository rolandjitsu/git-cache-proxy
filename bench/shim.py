#!/usr/bin/env python3
"""A userspace TCP shim that emulates a slow WAN in front of the origin.

It forwards a single TCP port to another, applying a token-bucket bandwidth
cap and a fixed one-way latency, and counts the bytes that cross it in each
direction. It needs no privileges (unlike `tc netem`), so the benchmark runs
anywhere python3 and git are installed.

This models a bandwidth-bound link: transfer time is ~= bytes / rate, which is
easy to sanity-check. It does not faithfully model multi-round-trip latency the
way `tc netem` does (latency is applied once per connection, not per RTT), so
treat the absolute times as a ballpark. The byte counts are exact.

On SIGTERM it writes "<down_bytes> <up_bytes>\\n" to --counter-file and exits,
where "down" is origin -> client (the pack download).
"""

import argparse
import asyncio
import signal


class Bucket:
    """Token bucket pacing a byte stream to `rate` bytes/sec (0 = unlimited)."""

    def __init__(self, rate, loop):
        self.rate = rate
        self.loop = loop
        self.tokens = float(rate)
        self.last = loop.time()

    async def consume(self, n):
        if self.rate <= 0:
            return
        while n > 0:
            now = self.loop.time()
            self.tokens = min(self.rate, self.tokens + (now - self.last) * self.rate)
            self.last = now
            if self.tokens >= 1:
                take = min(int(self.tokens), n)
                self.tokens -= take
                n -= take
            else:
                await asyncio.sleep((1 - self.tokens) / self.rate)


async def pump(reader, writer, rate, oneway_s, counter, key, loop):
    bucket = Bucket(rate, loop)
    first = True
    try:
        while True:
            data = await reader.read(65536)
            if not data:
                break
            await bucket.consume(len(data))
            if first and oneway_s > 0:
                await asyncio.sleep(oneway_s)
                first = False
            writer.write(data)
            await writer.drain()
            counter[key] += len(data)
    except (ConnectionResetError, BrokenPipeError):
        pass
    finally:
        try:
            writer.close()
        except Exception:
            pass


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--listen-port", type=int, required=True)
    ap.add_argument("--origin-host", default="127.0.0.1")
    ap.add_argument("--origin-port", type=int, required=True)
    ap.add_argument("--rate-mbit", type=float, default=0.0, help="0 = unlimited")
    ap.add_argument("--rtt-ms", type=float, default=0.0, help="round trip; half applied each way")
    ap.add_argument("--counter-file", required=True)
    args = ap.parse_args()

    loop = asyncio.get_running_loop()
    rate = int(args.rate_mbit * 1_000_000 / 8) if args.rate_mbit > 0 else 0
    oneway_s = (args.rtt_ms / 1000.0) / 2.0
    counter = {"down": 0, "up": 0}

    async def handle(client_reader, client_writer):
        try:
            origin_reader, origin_writer = await asyncio.open_connection(
                args.origin_host, args.origin_port
            )
        except OSError:
            client_writer.close()
            return
        await asyncio.gather(
            pump(client_reader, origin_writer, rate, oneway_s, counter, "up", loop),
            pump(origin_reader, client_writer, rate, oneway_s, counter, "down", loop),
        )

    server = await asyncio.start_server(handle, "127.0.0.1", args.listen_port)

    stop = loop.create_future()
    loop.add_signal_handler(signal.SIGTERM, lambda: stop.set_result(None))
    loop.add_signal_handler(signal.SIGINT, lambda: stop.set_result(None))
    async with server:
        await stop
    with open(args.counter_file, "w") as f:
        f.write(f"{counter['down']} {counter['up']}\n")


if __name__ == "__main__":
    asyncio.run(main())
