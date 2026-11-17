"""Real isolated loopback HTTP behavior; no application or database traffic."""
from __future__ import annotations

import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import socket
import ssl
import tempfile
import threading
import unittest
from unittest.mock import patch
import urllib.error
import urllib.request

import health
import loopback_http as transport


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_):
        pass

    def respond(self, code, body=b"", content_type="application/json", location=None):
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        if location:
            self.send_header("Location", location)
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_GET(self):
        self.server.events.append({"path": self.path, "peer": self.client_address,
                                   "connection": self.headers.get("Connection"),
                                   "agent": self.headers.get("User-Agent")})
        if self.path == "/redirect":
            self.respond(302, location="/ok")
        elif self.path.startswith("/redirect-"):
            scheme = self.path.removeprefix("/redirect-")
            target = "http://example.invalid/blocked" if scheme == "external" else scheme+"://127.0.0.1/blocked"
            self.respond(302, location=target)
        elif self.path == "/error":
            self.respond(503, b'{"error":"local fixture"}')
        elif self.path == "/slow":
            self.server.slow_release.wait(1)
            self.respond(200, b"{}")
        elif self.path == "/bytes":
            self.respond(200, b"x"*self.server.response_bytes, "application/octet-stream")
        else:
            self.respond(200, b'{"local":true}')

    def do_POST(self):
        body = self.rfile.read(int(self.headers["Content-Length"]))
        self.server.events.append({"path": self.path, "method": "POST", "body": body,
                                   "content_type": self.headers.get("Content-Type")})
        self.respond(200, self.server.post_body)


class LocalHTTP(unittest.TestCase):
    def setUp(self):
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.server.events = []
        self.server.slow_release = threading.Event()
        self.server.response_bytes = 0
        self.server.post_body = b'{"id":"owned-session","code":"OWNED123"}'
        self.thread = threading.Thread(target=lambda: self.server.serve_forever(poll_interval=.01))
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self):
        self.server.slow_release.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        self.assertFalse(self.thread.is_alive())

    def test_each_read_builds_fresh_http_transport_without_any_tls_factory(self):
        opener_type = urllib.request.OpenerDirector
        with patch.object(ssl, "create_default_context", side_effect=AssertionError("unused TLS")), \
             patch.object(ssl, "_create_default_https_context", side_effect=AssertionError("unused TLS")), \
             patch.object(urllib.request, "OpenerDirector", wraps=opener_type) as constructed:
            for _ in range(2):
                body, content_type = transport.http_get(self.url+"/ok")
                self.assertEqual(body, b'{"local":true}')
                self.assertEqual(content_type, "application/json")
        self.assertEqual(constructed.call_count, 2)
        self.assertEqual(len({item["peer"] for item in self.server.events}), 2)
        self.assertTrue(all(item["connection"] == "close" for item in self.server.events))
        self.assertTrue(all(item["agent"] == "UKMesh-owned-full-stack-qualification" for item in self.server.events))

    def test_relative_loopback_redirect_and_http_error_behavior_are_preserved(self):
        self.assertEqual(transport.http_get(self.url+"/redirect")[0], b'{"local":true}')
        self.assertEqual([item["path"] for item in self.server.events], ["/redirect", "/ok"])
        with self.assertRaises(urllib.error.HTTPError) as error:
            transport.http_get(self.url+"/error")
        self.assertEqual(error.exception.code, 503)
        error.exception.close()

    def test_remote_or_non_http_urls_and_redirects_never_open_a_remote_or_tls_connection(self):
        forbidden=[]
        original=http.client.HTTPConnection.connect
        def connect(connection):
            if connection.host not in ("127.0.0.1", "localhost", "::1"):
                forbidden.append(connection.host)
                raise AssertionError("nonloopback connection attempted")
            return original(connection)
        with patch.object(http.client.HTTPConnection, "connect", connect), \
             patch.object(ssl, "_create_default_https_context", side_effect=AssertionError("TLS constructed")):
            for url in ("http://example.invalid/blocked", "https://127.0.0.1/blocked", "ftp://127.0.0.1/file",
                        "file:///etc/passwd", "http://name:password@127.0.0.1/",
                        self.url.replace("http://", "http://@"), self.url.replace("http://", "http://:password@")):
                with self.subTest(url=url), self.assertRaises(ValueError):
                    transport.http_get(url)
            for kind in ("external", "https", "ftp"):
                with self.subTest(redirect=kind), self.assertRaises(ValueError):
                    transport.http_get(self.url+"/redirect-"+kind)
        self.assertEqual(forbidden, [])
        self.assertEqual(len(self.server.events), 3)

    def test_environment_proxy_settings_cannot_redirect_a_loopback_request(self):
        with patch.dict(os.environ, {"HTTP_PROXY":"http://127.0.0.1:9", "http_proxy":"http://127.0.0.1:9",
                                    "HTTPS_PROXY":"http://127.0.0.1:9", "ALL_PROXY":"http://127.0.0.1:9",
                                    "NO_PROXY":"", "no_proxy":""}):
            self.assertEqual(transport.http_get(self.url+"/ok")[0], b'{"local":true}')
        self.assertEqual(len(self.server.events), 1)

    def test_actual_eight_mib_boundary_is_accepted_and_next_byte_rejected(self):
        self.assertEqual(transport.MAX_GET_BYTES, 8*1024*1024)
        self.server.response_bytes = 8*1024*1024
        self.assertEqual(len(transport.http_get(self.url+"/bytes")[0]), self.server.response_bytes)
        self.server.response_bytes += 1
        with self.assertRaisesRegex(ValueError, "8 MiB"):
            transport.http_get(self.url+"/bytes")

    def test_timeout_is_forwarded_and_default_stays_fifteen_seconds(self):
        self.assertEqual(transport.DEFAULT_TIMEOUT_SECONDS, 15)
        with self.assertRaises((socket.timeout, TimeoutError)):
            transport.http_get(self.url+"/slow", timeout=.03)

    def test_health_post_uses_local_transport_with_unchanged_body_and_sixty_four_kib_limit(self):
        def local_post(request, *, timeout):
            self.assertEqual(request.full_url, "http://127.0.0.1:3090/api/sessions")
            self.assertEqual(timeout, 15)
            redirected=urllib.request.Request(self.url+"/sessions", data=request.data, method=request.get_method(),
                                              headers=dict(request.header_items()))
            return transport.open_local(redirected, timeout=timeout)
        with tempfile.TemporaryDirectory() as temporary, patch.object(health, "PROOF", Path(temporary)/"proof.json"), \
             patch.object(health, "open_local", side_effect=local_post):
            base={"id":"owned-session", "code":"OWNED123", "padding":""}
            base["padding"]="x"*(65536-len(json.dumps(base).encode()))
            self.server.post_body=json.dumps(base).encode()
            self.assertEqual(len(self.server.post_body), 65536)
            session=health.create_session(Path(temporary), {"token":"owned-fixture"})
            self.assertEqual(session["sessionId"], "owned-session")
            self.server.post_body += b" "
            with self.assertRaisesRegex(ValueError, "byte budget"):
                health.create_session(Path(temporary), {"token":"owned-fixture"})
        for item in self.server.events:
            self.assertEqual(item["method"], "POST")
            self.assertEqual(item["body"], b"{}")
            self.assertEqual(item["content_type"], "application/json")


if __name__ == "__main__":
    unittest.main()
