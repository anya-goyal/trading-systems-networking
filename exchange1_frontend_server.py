"""
HTTP bridge + static UI for Exchange 1 order book snapshots.

Default UI: http://127.0.0.1:8765/
Requires exchange1.py running (snapshot port from config).

Usage:
  python exchange1_frontend_server.py
  python exchange1_frontend_server.py --port 9000
"""

from __future__ import annotations

import argparse
import json
import os
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import config
from exchange1.snapshot_client import SnapshotClientError, fetch_snapshot

# Directory containing this repo's static frontend (sibling of this file)
_FRONTEND_ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "frontend", "exchange1")

_DEFAULT_BIND = "127.0.0.1"
_DEFAULT_HTTP_PORT = 8765


def _snapshot_from_request(query: dict[str, list[str]]) -> dict:
    raw = (query.get("symbol") or ["ALL"])[0].strip()
    if not raw:
        raw = "ALL"
    return fetch_snapshot(
        config.EXCHANGE1_SNAPSHOT_HOST,
        config.EXCHANGE1_SNAPSHOT_PORT,
        raw,
    )


class Handler(BaseHTTPRequestHandler):
    server_version = "Exchange1Frontend/1.0"

    def log_message(self, format: str, *args) -> None:
        print("%s - %s" % (self.address_string(), format % args))

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/api/snapshot":
            try:
                q = parse_qs(parsed.query)
                data = _snapshot_from_request(q)
                payload = {"error": None, **data}
                body = json.dumps(payload).encode("utf-8")
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except SnapshotClientError as e:
                err = {"error": str(e), "snapshot_seq": None, "symbols": {}}
                body = json.dumps(err).encode("utf-8")
                self.send_response(HTTPStatus.BAD_GATEWAY)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            return

        path = parsed.path
        if path == "/" or path == "":
            path = "/index.html"

        rel = path.lstrip("/").replace("\\", "/")
        candidate = os.path.normpath(os.path.join(_FRONTEND_ROOT, rel))
        root_real = os.path.realpath(_FRONTEND_ROOT)
        cand_real = os.path.realpath(candidate)
        if cand_real != root_real and not cand_real.startswith(root_real + os.sep):
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        if not os.path.isfile(candidate):
            self.send_error(HTTPStatus.NOT_FOUND)
            return

        ctype = "application/octet-stream"
        if candidate.endswith(".html"):
            ctype = "text/html; charset=utf-8"
        elif candidate.endswith(".js"):
            ctype = "text/javascript; charset=utf-8"
        elif candidate.endswith(".css"):
            ctype = "text/css; charset=utf-8"

        try:
            with open(candidate, "rb") as f:
                body = f.read()
        except OSError:
            self.send_error(HTTPStatus.INTERNAL_SERVER_ERROR)
            return

        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main() -> None:
    ap = argparse.ArgumentParser(description="Exchange 1 snapshot UI + API")
    ap.add_argument("--host", default=_DEFAULT_BIND, help="bind address")
    ap.add_argument("--port", type=int, default=_DEFAULT_HTTP_PORT, help="HTTP port")
    args = ap.parse_args()

    if not os.path.isdir(_FRONTEND_ROOT):
        raise SystemExit(f"Frontend directory missing: {_FRONTEND_ROOT}")

    httpd = ThreadingHTTPServer((args.host, args.port), Handler)
    print(
        f"Serving Exchange 1 UI at http://{args.host}:{args.port}/ "
        f"(snapshot {config.EXCHANGE1_SNAPSHOT_HOST}:{config.EXCHANGE1_SNAPSHOT_PORT})"
    )
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("Shutting down.")
    finally:
        httpd.server_close()


if __name__ == "__main__":
    main()
