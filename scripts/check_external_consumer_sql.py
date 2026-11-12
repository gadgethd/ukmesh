#!/usr/bin/env python3
"""Repo-level entry point for the external-consumer SQL checker.

The implementation lives in ``viewshed-worker/`` because that is the tree the
worker image builds from: the worker's own test suite imports it inside the
container, so it has to be COPYed into the image.  This shim keeps the
documented ``python3 scripts/check_external_consumer_sql.py`` invocation (used
by ``backend/package.json``) working from a repo checkout.
"""
from __future__ import annotations

import sys
from pathlib import Path

_WORKER_ROOT = Path(__file__).resolve().parents[1] / "viewshed-worker"
if str(_WORKER_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKER_ROOT))

from check_external_consumer_sql import (  # noqa: E402
    MOVED_RELATIONS,
    main,
    scan_roots,
    scan_text,
)

__all__ = ["MOVED_RELATIONS", "main", "scan_roots", "scan_text"]

if __name__ == "__main__":
    raise SystemExit(main())
