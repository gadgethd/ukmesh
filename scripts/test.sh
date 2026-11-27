#!/usr/bin/env bash
# The subset of this repository's tests that runs with no infrastructure.
#
# The Workers and Compose job runs the link-worker and PostgreSQL suites
# with GDAL, PostgreSQL and Redis fixtures.
#
# What remains is stdlib-only and exercises real code paths: the Mosquitto
# reloader and the CI retry/exit-propagation helper.
set -euo pipefail
cd "$(dirname "$0")/.."

PY="${PYTHON:-python3}"
echo "test: using $PY (no-infrastructure subset)"
"$PY" -m unittest discover -s docker -p 'test_mosquitto_reloader.py' -v
"$PY" -m unittest discover -s scripts -p 'test_ci_retry.py' -v
