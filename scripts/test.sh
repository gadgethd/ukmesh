#!/usr/bin/env bash
# The subset of this repository's tests that runs with no infrastructure.
#
# The viewshed-worker suite needs a GDAL base image, Postgres and Redis; the
# scripts/ suite includes a Postgres shared-memory test. Those cannot run on a
# bare checkout, so they are excluded here explicitly rather than silently
# skipped. Run them in the compose environment:
#
#   docker compose -f docker-compose.ci.yml run --rm viewshed-worker pytest
#
# What remains is stdlib-only and exercises real code paths: the Mosquitto
# reloader and the CI retry/exit-propagation helper.
set -euo pipefail
cd "$(dirname "$0")/.."

PY="${PYTHON:-python3}"
echo "test: using $PY (no-infrastructure subset)"
"$PY" -m unittest discover -s docker -p 'test_mosquitto_reloader.py' -v
"$PY" -m unittest discover -s scripts -p 'test_ci_retry.py' -v
