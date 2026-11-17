#!/usr/bin/env bash
set -euo pipefail
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
if [[ ${UKMESH_CORESCOPE_PUBLIC_CLEAN:-} != 1 ]]; then
  exec env -i PATH="$PATH" HOME="$HOME" LANG=C.UTF-8 UKMESH_CORESCOPE_PUBLIC_CLEAN=1 bash "$repo/scripts/full-dev/corescope-public.sh" "$@"
fi
mkdir -p "$repo/plans/companion-dev"
if [[ ${UKMESH_COMPANION_LOCKED:-} != 1 ]]; then
  exec flock --nonblock --close --conflict-exit-code 75 "$repo/plans/companion-dev/control.lock" \
    env UKMESH_COMPANION_LOCKED=1 bash "$repo/scripts/full-dev/corescope-public.sh" "$@"
fi
exec python3 "$repo/scripts/full-dev/corescope-public.py" "$@"
