#!/usr/bin/env bash
set -euo pipefail
case "${1:-all}" in
  corescope|beacon) exec bash "/dev-tools/build-$1.sh" ;;
  all)
    bash /dev-tools/build-corescope.sh
    exec bash /dev-tools/build-beacon.sh ;;
  *) echo 'Unknown companion build target' >&2; exit 2 ;;
esac
