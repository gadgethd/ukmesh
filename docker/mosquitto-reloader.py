#!/usr/bin/env python3
"""Single-purpose authenticated Mosquitto SIGHUP helper."""

import hmac
import os
import signal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

TOKEN = os.environ.get("OWNER_ACL_RELOAD_TOKEN", "")
LOG_PATH = os.environ.get("MOSQUITTO_LOG_PATH", "/mosquitto/log/mosquitto.log")
if len(TOKEN) < 32:
    raise RuntimeError("OWNER_ACL_RELOAD_TOKEN must contain at least 32 characters")


def ensure_log_permissions() -> None:
    metadata = os.stat(LOG_PATH, follow_symlinks=False)
    if metadata.st_uid != os.getuid():
        raise RuntimeError("Mosquitto log is not owned by the broker runtime UID")
    os.chmod(LOG_PATH, 0o640, follow_symlinks=False)


class Handler(BaseHTTPRequestHandler):
    server_version = "meshcore-mosquitto-reloader/1"

    def _send(self, status: int, body: bytes = b"") -> None:
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body:
            self.wfile.write(body)

    def do_GET(self) -> None:
        if self.path == "/healthz":
            try:
                ensure_log_permissions()
            except (OSError, RuntimeError):
                self._send(503, b'{"status":"unhealthy"}')
                return
            self._send(200, b'{"status":"ok"}')
        else:
            self._send(404, b'{"error":"not found"}')

    def do_POST(self) -> None:
        supplied = self.headers.get("Authorization", "")
        expected = f"Bearer {TOKEN}"
        if self.path != "/reload":
            self._send(404, b'{"error":"not found"}')
            return
        if not hmac.compare_digest(supplied, expected):
            self._send(403, b'{"error":"forbidden"}')
            return
        length = int(self.headers.get("Content-Length", "0") or "0")
        if length > 16:
            self._send(413, b'{"error":"payload too large"}')
            return
        if length:
            self.rfile.read(length)
        os.kill(1, signal.SIGHUP)
        self._send(204)

    def log_message(self, fmt: str, *args: object) -> None:
        print(f"[mosquitto-reloader] {self.address_string()} {fmt % args}", flush=True)


ensure_log_permissions()
ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
