#!/usr/bin/env bash
# Exercise startup ordering and fixture preservation without Docker or a server.
set -euo pipefail
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
test_root=$(mktemp -d)
source "$repo/scripts/lib/owned-processes.sh"
foreign_pids=()
cleanup() {
  local pid_file process_id fixture_repo service entrypoint
  for pid_file in "$test_root"/*/repo/plans/dev-stack/*.pid; do
    [[ -f $pid_file ]] || continue
    process_id=$(cat "$pid_file"); fixture_repo=${pid_file%/plans/dev-stack/*}
    service=${pid_file##*/}; service=${service%.pid}
    case "$service" in
      backend) entrypoint="$fixture_repo/backend/dist/index.js" ;;
      region-snapshot) entrypoint="$fixture_repo/backend/dist/workers/region-snapshot.js" ;;
      frontend) entrypoint="$fixture_repo/frontend/node_modules/vite/bin/vite.js" ;;
    esac
    if owned_process_matches "$process_id" "$entrypoint"; then kill -KILL "$process_id" 2>/dev/null || true; fi
  done
  for process_id in "${foreign_pids[@]}"; do kill -TERM "$process_id" 2>/dev/null || true; wait "$process_id" 2>/dev/null || true; done
  rm -rf -- "$test_root"
}
trap cleanup EXIT

init_fixture() {
  local name=$1
  fixture="$test_root/$name with spaces"
  mkdir -p "$fixture/bin" "$fixture/repo/scripts/lib" "$fixture/repo/backend/node_modules" \
    "$fixture/repo/backend/src/db" "$fixture/repo/frontend/node_modules" "$fixture/repo/plans/dev-stack" "$fixture/data"
  cp "$repo/scripts/dev-stack.sh" "$fixture/repo/scripts/"
  cp "$repo/scripts/lib/owned-processes.sh" "$fixture/repo/scripts/lib/"
  touch "$fixture/repo/scripts/bootstrap-backend-roles.sql" "$fixture/repo/scripts/bootstrap-reader-roles.sql"
  touch "$fixture/repo/backend/src/db/owner-auth.sql"
  printf 'snapshot\n' > "$fixture/repo/plans/dev-stack/fixture-ready"
  printf '%s\n' "$fixture/data" > "$fixture/repo/plans/dev-stack/data-dir"
  printf 'preserve this fixture\n' > "$fixture/data/marker"
  printf 'DOTENV_SENTINEL=must-not-be-read\n' > "$fixture/repo/.env"
  : > "$fixture/calls"
  cat > "$fixture/bin/docker" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
root=$(cd -- "$(dirname -- "$0")/.." && pwd)
[[ $1 != info ]] || exit 0
[[ $1 == compose && $2 == -f ]] || exit 90
case $4 in
  up) printf 'INFRA_UP\n' >> "$root/calls" ;;
  ps) ;;
  exec) cat >/dev/null; printf 'BOOTSTRAP_ROLE\n' >> "$root/calls" ;;
  *) echo 'Unexpected infrastructure mutation' >&2; exit 91 ;;
esac
MOCK
  cat > "$fixture/bin/node" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
root=$(cd -- "$(dirname -- "$0")/.." && pwd)
for variable in DATABASE_URL MESHCORE_HTTP_DATABASE_URL MESHCORE_INGEST_DATABASE_URL MESHCORE_WORKER_DATABASE_URL MESHCORE_MIGRATE_DATABASE_URL; do
  [[ ${!variable} == postgresql://meshcore:meshcore@127.0.0.1:5433/meshcore ]] || exit 92
done
[[ ${OWNER_DATABASE_URL:-} == postgresql://meshcore:meshcore@127.0.0.1:5433/meshcore_owner_auth ]] || exit 93
[[ ! ${DOTENV_SENTINEL+x} && ! ${UNTRUSTED_INHERITED_SETTING+x} ]] || exit 94
service=
case "$*" in
  *backend/src/tools/migrate.ts)
    printf 'MIGRATE %s\n' "${MIGRATION_BASE_SCHEMA_BOOTSTRAP_APPROVAL:-unset}" >> "$root/calls"
    [[ ! -f $root/fail-migration ]] || { echo 'fixture migration failed'; exit 42; } ;;
  *backend/dist/index.js) service=BACKEND ;;
  *backend/dist/workers/region-snapshot.js) service=REGION ;;
  *frontend/node_modules/vite/bin/vite.js*) service=FRONTEND ;;
  *backend/src/tools/loadSampleDataset.ts*) printf 'LOAD\n' >> "$root/calls" ;;
  *backend/src/tools/refreshSampleRecent.ts) printf 'REFRESH\n' >> "$root/calls" ;;
  *backend/src/tools/prepareSampleDataset.ts) printf 'PREPARE\n' >> "$root/calls" ;;
  *backend/src/tools/smokeSampleReplay.ts*)
    printf 'REPLAY\n' >> "$root/calls"
    [[ ! -f $root/replay-failure ]] || exit 55 ;;
  *backend/src/tools/backfillStatsRollups.ts*) printf 'ROLLUPS\n' >> "$root/calls" ;;
  *) echo 'Unexpected fixture load or replay' >&2; exit 95 ;;
esac
if [[ -n $service ]]; then
  printf '%s\n' "$service" >> "$root/calls"
  printf '%s %s\n' "$$" "$service" >> "$root/app-pids"
  [[ ! -f $root/$service-exit ]] || exit 73
  if [[ -f $root/$service-ignore-term ]]; then
    exec bash -c 'trap "" TERM; while :; do /bin/sleep 0.02; done' _ "$@"
  fi
  exec bash -c 'trap "exit 0" TERM; while :; do /bin/sleep 0.02; done' _ "$@"
fi
MOCK
  cat > "$fixture/bin/npm" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
root=$(cd -- "$(dirname -- "$0")/.." && pwd)
[[ $* == *' run build' ]] || exit 96
printf 'BUILD\n' >> "$root/calls"
MOCK
  cat > "$fixture/bin/curl" <<'MOCK'
#!/usr/bin/env bash
root=$(cd -- "$(dirname -- "$0")/.." && pwd)
case "$*" in
  *3100/healthz) grep -qx BACKEND "$root/calls" && [[ ! -f $root/BACKEND-unready ]] || exit 1 ;;
  *9092/metrics) grep -qx REGION "$root/calls" && [[ ! -f $root/REGION-unready && ! -f $root/REGION-exit ]] || exit 1 ;;
  *5173/) grep -qx FRONTEND "$root/calls" && [[ ! -f $root/FRONTEND-unready && ! -f $root/FRONTEND-exit ]] || exit 1 ;;
  *) exit 83 ;;
esac
printf 'meshcore_region_snapshot_healthy 1\n'
MOCK
  cat > "$fixture/bin/sleep" <<'MOCK'
#!/usr/bin/env bash
/bin/sleep 0.002
MOCK
  chmod +x "$fixture/bin/"*
}

run_fixture() {
  # The script's env -i must discard both external settings and remote DSNs.
  env PATH="$fixture/bin:$PATH" UNTRUSTED_INHERITED_SETTING=discard \
    DATABASE_URL=postgresql://untrusted.invalid/not-local \
    bash "$fixture/repo/scripts/dev-stack.sh" "$@" > "$fixture/output" 2>&1
}

init_fixture restart
run_fixture start
for ((attempt=0; attempt<100; attempt++)); do
  grep -qx FRONTEND "$fixture/calls" && break
  sleep 0.01
done
node --input-type=module - "$fixture" <<'JS'
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
const root = process.argv[2], read = name => readFileSync(join(root, name), 'utf8');
const calls = read('calls').trim().split('\n');
assert.equal(calls.filter(line => line === 'MIGRATE unset').length, 1);
assert.ok(calls.indexOf('MIGRATE unset') < calls.indexOf('BUILD'));
assert.ok(calls.indexOf('BUILD') < calls.indexOf('BACKEND'));
for (const stage of ['BACKEND', 'REGION', 'FRONTEND']) assert.ok(calls.includes(stage));
assert.ok(!calls.some(line => line.startsWith('BOOTSTRAP')));
assert.equal(read('data/marker'), 'preserve this fixture\n');
assert.equal(read('repo/plans/dev-stack/fixture-ready'), 'snapshot\n');
JS

init_fixture failed-restart
touch "$fixture/fail-migration"
if run_fixture start; then echo 'Migration failure did not stop startup' >&2; exit 1; fi
grep -qx 'MIGRATE unset' "$fixture/calls"
! grep -Eq 'BUILD|BACKEND|REGION|FRONTEND|BOOTSTRAP' "$fixture/calls"
grep -q 'fixture migration failed' "$fixture/output"
test -f "$fixture/data/marker"
test -f "$fixture/repo/plans/dev-stack/fixture-ready"

init_fixture fresh-bootstrap
rm "$fixture/repo/plans/dev-stack/fixture-ready"
touch "$fixture/fail-migration"
if run_fixture up; then echo 'Migration failure did not stop fresh startup' >&2; exit 1; fi
test "$(grep -c '^BOOTSTRAP_ROLE$' "$fixture/calls")" -eq 2
grep -qx 'MIGRATE bootstrap-ledger-from-base-schema' "$fixture/calls"
! grep -Eq 'BUILD|BACKEND|REGION|FRONTEND' "$fixture/calls"

init_fixture fresh-success
rm "$fixture/repo/plans/dev-stack/fixture-ready"
run_fixture up --data-dir "$fixture/data" --profile snapshot
node --input-type=module - "$fixture" <<'JS'
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
const root=process.argv[2], calls=readFileSync(root+'/calls','utf8').trim().split('\n');
const stages=['MIGRATE bootstrap-ledger-from-base-schema','LOAD','REFRESH','PREPARE','ROLLUPS','BUILD','BACKEND'];
for(const stage of stages) assert.equal(calls.filter(line=>line===stage).length,1);
for(let at=1;at<stages.length;at++) assert.ok(calls.indexOf(stages[at-1])<calls.indexOf(stages[at]));
assert.equal(readFileSync(root+'/repo/plans/dev-stack/fixture-ready','utf8'),'snapshot\n');
JS
assert_launch_unwound() {
  local process_id service
  while read -r process_id service; do ! owned_process_alive "$process_id"; done < "$fixture/app-pids"
  test ! -e "$fixture/repo/plans/dev-stack/backend.pid"
  test ! -e "$fixture/repo/plans/dev-stack/region-snapshot.pid"
  test ! -e "$fixture/repo/plans/dev-stack/frontend.pid"
  test -f "$fixture/data/marker"
  test -f "$fixture/repo/plans/dev-stack/fixture-ready"
  test -f "$fixture/repo/plans/dev-stack/backend.log"
}

for failed_stage in REGION FRONTEND; do
  init_fixture "$failed_stage-failure"
  touch "$fixture/$failed_stage-exit"
  if run_fixture start; then echo "$failed_stage failure did not stop startup" >&2; exit 1; fi
  assert_launch_unwound
done

init_fixture replay-failure
rm "$fixture/repo/plans/dev-stack/fixture-ready"
touch "$fixture/replay-failure"
if run_fixture up --data-dir "$fixture/data" --profile ingest; then echo 'Replay failure did not stop startup' >&2; exit 1; fi
assert_launch_unwound
grep -qx REPLAY "$fixture/calls"
! grep -qx REGION "$fixture/calls"

init_fixture foreign-pid
bash -c 'trap "exit 0" TERM; while :; do /bin/sleep 0.02; done' _ "$fixture/foreign-entry.js" &
foreign_pid=$!; foreign_pids+=("$foreign_pid")
printf '%s\n' "$foreign_pid" > "$fixture/repo/plans/dev-stack/backend.pid"
touch "$fixture/FRONTEND-unready"
if run_fixture start; then echo 'Frontend timeout did not stop startup' >&2; exit 1; fi
assert_launch_unwound
kill -0 "$foreign_pid"

init_fixture shutdown-failure
touch "$fixture/BACKEND-unready" "$fixture/BACKEND-ignore-term"
if run_fixture start; then echo 'Backend timeout did not stop startup' >&2; exit 1; fi
test -f "$fixture/repo/plans/dev-stack/backend.pid"
owned_process_alive "$(cat "$fixture/repo/plans/dev-stack/backend.pid")"
grep -q 'backend did not stop; inspect PID' "$fixture/output"
test -f "$fixture/data/marker"

init_fixture interrupted
touch "$fixture/REGION-unready"
env PATH="$fixture/bin:$PATH" bash "$fixture/repo/scripts/dev-stack.sh" start > "$fixture/output" 2>&1 &
launcher=$!
for ((attempt=0; attempt<500; attempt++)); do
  grep -qx REGION "$fixture/calls" && break
  /bin/sleep 0.002
done
grep -qx REGION "$fixture/calls"
kill -TERM "$launcher"
result=0; wait "$launcher" || result=$?
test "$result" -eq 143
assert_launch_unwound
echo 'DEV startup migration, fail-closed startup and fresh-bootstrap contracts passed.'
