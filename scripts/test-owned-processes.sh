#!/usr/bin/env bash
set -euo pipefail
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
source "$repo/scripts/lib/owned-processes.sh"
scratch=$(mktemp -d)
test_pids=()
cleanup() {
  local process_id
  for process_id in "${test_pids[@]}"; do kill -TERM "$process_id" 2>/dev/null || true; done
  for process_id in "${test_pids[@]}"; do wait "$process_id" 2>/dev/null || true; done
  rm -rf -- "$scratch"
}
trap cleanup EXIT

entrypoint="$scratch/path with spaces/entry.js"
bash -c 'trap "exit 0" TERM; while :; do sleep 0.02; done' _ "$entrypoint" --owned-fixture &
owned_pid=$!; test_pids+=("$owned_pid")
bash -c 'trap "exit 0" TERM; while :; do sleep 0.02; done' _ "$entrypoint" &
foreign_pid=$!; test_pids+=("$foreign_pid")
owned_wait_until 100 0.01 owned_process_matches "$owned_pid" "$entrypoint" --owned-fixture
printf '%s\n' "$foreign_pid" > "$scratch/foreign.pid"
owned_stop_process "$scratch/foreign.pid" "$entrypoint" 100 0.01 foreign --owned-fixture
kill -0 "$foreign_pid"
test ! -e "$scratch/foreign.pid"
printf '%s\n' "$owned_pid" > "$scratch/owned.pid"
owned_stop_process "$scratch/owned.pid" "$entrypoint" 100 0.01 owned --owned-fixture
! owned_process_alive "$owned_pid"
wait "$owned_pid"
test ! -e "$scratch/owned.pid"
printf 'not-a-pid\n' > "$scratch/stale.pid"
owned_stop_process "$scratch/stale.pid" "$entrypoint" 1 0 stale
test ! -e "$scratch/stale.pid"
probes=0
eventually_ready() { probes=$((probes+1)); (( probes == 3 )); }
owned_wait_until 3 0 eventually_ready
test "$probes" -eq 3
probes=0
! owned_wait_until 2 0 eventually_ready
test "$probes" -eq 2
echo 'Owned process matching, bounded waits and foreign-PID preservation passed.'
