#!/usr/bin/env python3
"""A minimal git-LFS origin for the benchmark: batch API plus object storage.

Serves two endpoints, enough to drive the proxy's LFS cache:

  POST <repo>/info/lfs/objects/batch -> a download action per requested object,
    with an href of <advertise-base>/lfs/<oid>. The advertise base is the shim,
    so the object download crosses the same emulated WAN as the batch (and is
    counted), exactly as a cold client fetch would.
  GET  /lfs/<oid>                     -> streams the object bytes.

Anonymous - the benchmark does not exercise auth. Single fixed object, since the
benchmark measures the cold-vs-warm transfer of one object, not fan-out.
"""

import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--advertise-base", required=True, help="e.g. http://127.0.0.1:<shim-port>")
    ap.add_argument("--object-file", required=True)
    args = ap.parse_args()

    with open(args.object_file, "rb") as f:
        blob = f.read()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):  # keep the benchmark output clean
            pass

        def do_POST(self):
            if not self.path.endswith("/info/lfs/objects/batch"):
                self.send_error(404)
                return
            n = int(self.headers.get("Content-Length", 0))
            req = json.loads(self.rfile.read(n) or b"{}")
            objects = [
                {
                    "oid": o["oid"],
                    "size": o["size"],
                    "actions": {"download": {"href": f"{args.advertise_base}/lfs/{o['oid']}"}},
                }
                for o in req.get("objects", [])
            ]
            body = json.dumps({"transfer": "basic", "objects": objects}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/vnd.git-lfs+json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            if "/lfs/" not in self.path:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(len(blob)))
            self.end_headers()
            self.wfile.write(blob)

    ThreadingHTTPServer(("127.0.0.1", args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
