"""Fresh plain HTTP transport for loopback-only qualification endpoints."""
from __future__ import annotations

import urllib.request
from urllib.parse import urlsplit


DEFAULT_TIMEOUT_SECONDS = 15
MAX_GET_BYTES = 8 * 1024 * 1024


def local_url(url: str) -> None:
    parsed = urlsplit(url)
    if parsed.scheme != "http" or parsed.hostname not in ("127.0.0.1", "localhost", "::1") or parsed.username is not None:
        raise ValueError("qualification HTTP probes must use loopback without embedded credentials")


class LocalRedirects(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, response, code, message, headers, new_url):
        local_url(new_url)
        return super().redirect_request(request, response, code, message, headers, new_url)


def _fresh_opener() -> urllib.request.OpenerDirector:
    # build_opener adds unused HTTPS/FTP/proxy handlers; HTTPS construction
    # loads a TLS trust store even for the plain loopback requests allowed here.
    opener = urllib.request.OpenerDirector()
    for handler in (urllib.request.HTTPHandler(), urllib.request.HTTPDefaultErrorHandler(),
                    LocalRedirects(), urllib.request.HTTPErrorProcessor()):
        opener.add_handler(handler)
    return opener


def open_local(request: urllib.request.Request | str, *, timeout: float = DEFAULT_TIMEOUT_SECONDS):
    local_url(request.full_url if isinstance(request, urllib.request.Request) else request)
    return _fresh_opener().open(request, timeout=timeout)


def http_get(url: str, *, timeout: float = DEFAULT_TIMEOUT_SECONDS) -> tuple[bytes, str]:
    request = urllib.request.Request(url, headers={"User-Agent": "UKMesh-owned-full-stack-qualification"})
    with open_local(request, timeout=timeout) as response:
        body = response.read(MAX_GET_BYTES + 1)
        if len(body) > MAX_GET_BYTES:
            raise ValueError("probe response exceeded 8 MiB")
        return body, response.headers.get("Content-Type", "")
