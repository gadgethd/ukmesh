#!/usr/bin/env python3
"""Single-purpose authenticated Mosquitto SIGHUP helper."""

import hmac
import os
import signal
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

TOKEN = os.environ.get("OWNER_ACL_RELOAD_TOKEN", "")
LOG_PATH = os.environ.get("MOSQUITTO_LOG_PATH", "/mosquitto/log/mosquitto.log")
RELOAD_ACK_TIMEOUT_SECONDS = float(os.environ.get("MOSQUITTO_RELOAD_ACK_TIMEOUT_SECONDS", "3"))
RELOAD_MARKER = b"Reloading config."
RELOAD_LOCK = threading.Lock()


def ensure_log_permissions() -> None:
    metadata = os.stat(LOG_PATH, follow_symlinks=False)
    if metadata.st_uid != os.getuid():
        raise RuntimeError("Mosquitto log is not owned by the broker runtime UID")
    os.chmod(LOG_PATH, 0o640, follow_symlinks=False)


def capture_log_cursor(log_path: str = LOG_PATH) -> tuple[int, int, int]:
    metadata = os.stat(log_path, follow_symlinks=False)
    return metadata.st_dev, metadata.st_ino, metadata.st_size


def wait_for_reload_ack(
    cursor: tuple[int, int, int],
    timeout_seconds: float = RELOAD_ACK_TIMEOUT_SECONDS,
    log_path: str = LOG_PATH,
) -> bool:
    """Wait for a reload marker appended after cursor, tolerating log rotation."""
    device, inode, offset = cursor
    trailing = b""
    deadline = time.monotonic() + timeout_seconds
    while True:
        try:
            with open(log_path, "rb", buffering=0) as broker_log:
                metadata = os.fstat(broker_log.fileno())
                if (metadata.st_dev, metadata.st_ino) != (device, inode) or metadata.st_size < offset:
                    device, inode, offset = metadata.st_dev, metadata.st_ino, 0
                    trailing = b""
                broker_log.seek(offset)
                appended = broker_log.read()
                if appended:
                    candidate = trailing + appended
                    if RELOAD_MARKER in candidate:
                        return True
                    trailing = candidate[-(len(RELOAD_MARKER) - 1):]
                    offset = broker_log.tell()
        except FileNotFoundError:
            # A rotating logger may briefly remove the path before recreating it.
            device, inode, offset = -1, -1, 0
            trailing = b""

        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        time.sleep(min(0.05, remaining))


def signal_and_wait_for_reload(
    log_path: str = LOG_PATH,
    timeout_seconds: float = RELOAD_ACK_TIMEOUT_SECONDS,
) -> bool:
    cursor = capture_log_cursor(log_path)
    os.kill(1, signal.SIGHUP)
    return wait_for_reload_ack(cursor, timeout_seconds, log_path)


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
        try:
            with RELOAD_LOCK:
                ensure_log_permissions()
                acknowledged = signal_and_wait_for_reload()
        except (OSError, RuntimeError) as error:
            print(f"[mosquitto-reloader] reload failed: {error}", flush=True)
            self._send(503, b'{"error":"reload failed"}')
            return
        if not acknowledged:
            print("[mosquitto-reloader] reload was not acknowledged by mosquitto", flush=True)
            self._send(504, b'{"error":"reload not acknowledged"}')
            return
        self._send(204)

    def log_message(self, fmt: str, *args: object) -> None:
        print(f"[mosquitto-reloader] {self.address_string()} {fmt % args}", flush=True)


def main() -> None:
    if len(TOKEN) < 32:
        raise RuntimeError("OWNER_ACL_RELOAD_TOKEN must contain at least 32 characters")
    if not 0 < RELOAD_ACK_TIMEOUT_SECONDS < 5:
        raise RuntimeError("MOSQUITTO_RELOAD_ACK_TIMEOUT_SECONDS must be between 0 and 5")
    ensure_log_permissions()
    ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()


if __name__ == "__main__":
    main()
