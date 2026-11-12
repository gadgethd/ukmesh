#!/usr/bin/env bash
set -euo pipefail
root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
exec python3 "$root/scripts/check-ops-contracts.py" --release "${1:-$root/.github/workflows/release.yml}"
