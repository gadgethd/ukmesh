#!/usr/bin/env bash
# Qualification owns evidence and fixtures; service lifecycle stays in dev-all.
set -euo pipefail
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)

# Acquire the target lease before the independent controller lock. The live
# owner frame and inherited FD are verified on every shell re-entry.
entry_env=(env -i PATH="$PATH" HOME="$HOME" LANG=C.UTF-8)
for entry_key in UKMESH_TARGET_LEASE_FD UKMESH_TARGET_ENTRY; do
  [[ ! -v $entry_key ]] || entry_env+=("$entry_key=${!entry_key}")
done
if "${entry_env[@]}" python3 "$repo/scripts/full-dev/qualification/target_entry.py" verify "$repo/scripts/qualify-full-stack.sh" "$@"; then :; else
  entry_status=$?
  [[ $entry_status == 1 ]] || exit "$entry_status"
  exec "${entry_env[@]}" python3 "$repo/scripts/full-dev/qualification/target_entry.py" run "$repo/scripts/qualify-full-stack.sh" "$@"
fi
exec python3 "$repo/scripts/full-dev/qualification/cli.py" "$@"
