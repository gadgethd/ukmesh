"""Lease fencing and Redis reconnection shared by durable queue protocols."""

from dataclasses import dataclass
import threading
import time


@dataclass(frozen=True)
class LeaseProtocol:
    name: str
    lease_ms: int
    renew_script: str
    states: str
    tokens: str
    leases: str


class LeaseGuard:
    def __init__(self, redis_factory, job_id: str, token: str, protocol: LeaseProtocol):
        self.redis_factory = redis_factory
        self.job_id = job_id
        self.token = token
        self.protocol = protocol
        self.stop_event = threading.Event()
        self.lease_lost = threading.Event()
        self.thread = threading.Thread(
            target=self._run,
            name=f"{protocol.name}-lease-{job_id[:8]}",
            daemon=True,
        )

    def _run(self):
        client = None
        last_success = time.monotonic()
        backoff = 0.25
        interval = max(0.25, self.protocol.lease_ms / 3000)
        while not self.stop_event.wait(min(backoff, interval)):
            try:
                if client is None:
                    client = self.redis_factory()
                renewed = client.eval(
                    self.protocol.renew_script,
                    3,
                    self.protocol.states,
                    self.protocol.tokens,
                    self.protocol.leases,
                    self.job_id,
                    self.token,
                    int(time.time() * 1000) + self.protocol.lease_ms,
                )
                if int(renewed) != 1:
                    self.lease_lost.set()
                    break
                last_success = time.monotonic()
                backoff = interval
            except Exception:
                if client is not None:
                    try:
                        client.close()
                    except Exception:
                        pass
                client = None
                if (time.monotonic() - last_success) * 1000 >= self.protocol.lease_ms:
                    self.lease_lost.set()
                    break
                backoff = min(5.0, backoff * 2)
        if client is not None:
            client.close()

    def assert_owned(self):
        if self.lease_lost.is_set():
            raise RuntimeError(f"{self.protocol.name.upper()}_LEASE_LOST:{self.job_id}")

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.stop_event.set()
        self.thread.join(timeout=5)
        self.assert_owned()
