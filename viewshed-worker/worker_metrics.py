import os
import time
from threading import Lock

from prometheus_client import Counter, Gauge, start_http_server

WORKER_JOBS = Counter(
    'meshcore_worker_jobs_total',
    'RF worker jobs by bounded worker, phase, and outcome.',
    ('worker', 'phase', 'outcome'),
)
WORKER_HEARTBEAT = Gauge(
    'meshcore_worker_heartbeat_timestamp_seconds',
    'Latest in-process RF worker heartbeat timestamp.',
    ('worker',),
)
SRTM_REQUESTS = Counter(
    'meshcore_srtm_requests_total',
    'SRTM tile acquisition outcomes.',
    ('outcome',),
)

_WORKERS = frozenset(('link', 'viewshed'))
_PHASES = frozenset(('job', 'terrain', 'rf_compute'))
_OUTCOMES = frozenset(('success', 'failure', 'retry', 'dead', 'cache_hit', 'not_found'))
_started = False
_start_lock = Lock()


def _bounded(value: str, allowed: frozenset[str]) -> str:
    normalized = str(value or '').strip().lower()
    return normalized if normalized in allowed else 'other'


def start_worker_metrics(worker: str) -> None:
    global _started
    with _start_lock:
        if _started:
            return
        port = max(1, min(65535, int(os.environ.get('METRICS_PORT', '9091'))))
        start_http_server(port, addr='0.0.0.0')
        _started = True
    heartbeat(worker)


def record_job(worker: str, phase: str, outcome: str) -> None:
    WORKER_JOBS.labels(
        _bounded(worker, _WORKERS),
        _bounded(phase, _PHASES),
        _bounded(outcome, _OUTCOMES),
    ).inc()


def record_srtm(outcome: str) -> None:
    SRTM_REQUESTS.labels(_bounded(outcome, _OUTCOMES)).inc()


def heartbeat(worker: str) -> None:
    WORKER_HEARTBEAT.labels(_bounded(worker, _WORKERS)).set(time.time())
