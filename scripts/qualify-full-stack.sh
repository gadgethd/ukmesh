#!/usr/bin/env bash
# Qualification owns evidence and fixtures; service lifecycle stays in dev-all.
set -euo pipefail
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
exec env -i PATH="$PATH" HOME="$HOME" LANG=C.UTF-8 \
  python3 "$repo/scripts/full-dev/qualification/cli.py" "$@"
