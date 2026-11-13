#!/usr/bin/env bash
# Docker and service executables are mocks; no ports or databases are touched.
set -euo pipefail
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
scratch=$(mktemp -d)
fixture=
cleanup() {
  if [[ -n $fixture ]]; then PATH="$fixture/bin:$PATH" bash "$fixture/repo/scripts/lookup-stack-local.sh" stop >/dev/null 2>&1 || true; fi
  rm -rf -- "$scratch"
}
trap cleanup EXIT

init_fixture() {
  fixture="$scratch/$1 with spaces"
  mkdir -p "$fixture/bin" "$fixture/repo/scripts/lib" "$fixture/repo/scripts/local-sample" \
    "$fixture/repo/backend/dist/workers" "$fixture/repo/plans/database-lookups"
  cp "$repo/scripts/lookup-stack-local.sh" "$fixture/repo/scripts/"
  cp "$repo/scripts/lib/owned-processes.sh" "$fixture/repo/scripts/lib/"
  cp "$repo/scripts/local-sample/lookup-profile.sh" "$fixture/repo/scripts/local-sample/"
  touch "$fixture/repo/backend/dist/index.js" "$fixture/repo/backend/dist/workers/region-snapshot.js"
  : > "$fixture/calls"
  cat > "$fixture/bin/docker" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
root=$(cd -- "$(dirname -- "$0")/.." && pwd)
case "$1:$2" in
  start:ukmesh-lookup-*) printf 'START %s\n' "$2" >> "$root/calls" ;;
  inspect:ukmesh-lookup-*) ;;
  exec:ukmesh-lookup-db)
    case "$3" in
      pg_isready)
        count=$(cat "$root/probes" 2>/dev/null || printf 0); count=$((count+1)); printf '%s\n' "$count" > "$root/probes"
        printf 'PROBE\n' >> "$root/calls"
        [[ ! -f $root/db-unready && $count -ge 3 ]] ;;
      psql) printf 'OWNER_QUERY\n' >> "$root/calls"; printf '1\n' ;;
      *) exit 81 ;;
    esac ;;
  *) echo 'Unexpected Docker operation or foreign service' >&2; exit 82 ;;
esac
MOCK
  cat > "$fixture/bin/node" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
root=$(cd -- "$(dirname -- "$0")/.." && pwd)
if [[ $1 == -e ]]; then printf '%064d' 1; exit; fi
[[ ! ${UNTRUSTED_INHERITED_SETTING+x} ]]
[[ $DATABASE_URL == postgresql://meshcore:meshcore@127.0.0.1:5434/lookup_10x ]]
[[ $MESHCORE_HTTP_DATABASE_URL == "$DATABASE_URL" && $MESHCORE_INGEST_DATABASE_URL == "$DATABASE_URL" && $MESHCORE_WORKER_DATABASE_URL == "$DATABASE_URL" ]]
[[ $OWNER_DATABASE_URL == postgresql://meshcore:meshcore@127.0.0.1:5434/lookup_owner_auth ]]
[[ $PORT == 3101 && $REDIS_URL == redis://127.0.0.1:6381 && $MQTT_BROKER_URL == ws://127.0.0.1:9003 ]]
printf 'APP %s\n' "$1" >> "$root/calls"
printf '%s\n' "$$" >> "$root/app-pids"
[[ -f $1 ]] || exit 74
[[ $1 != *workers/region-snapshot.js || ! -f $root/region-exit ]] || exit 73
exec bash -c 'trap "exit 0" TERM; while :; do /bin/sleep 0.02; done' _ "$@"
MOCK
  cat > "$fixture/bin/curl" <<'MOCK'
#!/usr/bin/env bash
root=$(cd -- "$(dirname -- "$0")/.." && pwd)
case "$*" in
  *3101/healthz) [[ ! -f $root/backend-unready ]] ;;
  *9094/metrics)
    [[ ! -f $root/region-unready && ! -f $root/region-exit && ! -f $root/region-missing ]] || exit 1
    grep -q 'APP .*workers/region-snapshot.js' "$root/calls" || exit 1
    printf 'meshcore_region_snapshot_healthy 1\n' ;;
  *) exit 83 ;;
esac
MOCK
  cat > "$fixture/bin/sleep" <<'MOCK'
#!/usr/bin/env bash
/bin/sleep 0.002
MOCK
  chmod +x "$fixture/bin/"*
}

run_fixture() {
  env PATH="$fixture/bin:$PATH" UNTRUSTED_INHERITED_SETTING=discard \
    DATABASE_URL=postgresql://untrusted.invalid/not-local \
    bash "$fixture/repo/scripts/lookup-stack-local.sh" "$@" > "$fixture/output" 2>&1
}

init_fixture success
run_fixture start lookup_10x candidate
for ((attempt=0; attempt<100; attempt++)); do
  [[ $(grep -c '^APP ' "$fixture/calls") == 2 ]] && break
  /bin/sleep 0.01
done
node --input-type=module - "$fixture" <<'JS'
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
const calls=readFileSync(process.argv[2]+'/calls','utf8').trim().split('\n');
for(const service of ['db','redis','mqtt']) assert.equal(calls.filter(line=>line===`START ukmesh-lookup-${service}`).length,1);
assert.equal(calls.filter(line=>line==='PROBE').length,3);
assert.ok(calls.indexOf('OWNER_QUERY')>calls.lastIndexOf('PROBE'));
assert.equal(calls.filter(line=>line.startsWith('APP ')).length,2);
JS
run_fixture stop
test ! -e "$fixture/repo/plans/database-lookups/backend.pid"
test ! -e "$fixture/repo/plans/database-lookups/region-snapshot.pid"

init_fixture database-failure
touch "$fixture/db-unready"
if run_fixture start lookup_10x candidate; then echo 'Unready database did not stop startup' >&2; exit 1; fi
test "$(grep -c '^PROBE$' "$fixture/calls")" -eq 120
! grep -Eq 'OWNER_QUERY|APP |START ukmesh-lookup-(redis|mqtt)' "$fixture/calls"
grep -q 'database did not become ready' "$fixture/output"

init_fixture backend-failure
touch "$fixture/backend-unready"
if run_fixture start lookup_10x candidate; then echo 'Unready backend did not fail startup' >&2; exit 1; fi
test ! -e "$fixture/repo/plans/database-lookups/backend.pid"
test ! -e "$fixture/repo/plans/database-lookups/region-snapshot.pid"
test "$(grep -c '^APP ' "$fixture/calls")" -eq 1
while read -r process_id; do ! kill -0 "$process_id" 2>/dev/null || [[ $(ps -p "$process_id" -o stat= 2>/dev/null) == Z* ]]; done < "$fixture/app-pids"
for failure in region-exit region-unready region-missing; do
  init_fixture "$failure"
  touch "$fixture/$failure"
  if [[ $failure == region-missing ]]; then rm "$fixture/repo/backend/dist/workers/region-snapshot.js"; fi
  if run_fixture start lookup_10x candidate; then echo 'Region failure did not stop lookup startup' >&2; exit 1; fi
  test ! -e "$fixture/repo/plans/database-lookups/backend.pid"
  test ! -e "$fixture/repo/plans/database-lookups/region-snapshot.pid"
  while read -r process_id; do ! kill -0 "$process_id" 2>/dev/null || [[ $(ps -p "$process_id" -o stat= 2>/dev/null) == Z* ]]; done < "$fixture/app-pids"
done

init_fixture interrupted
touch "$fixture/region-unready"
env PATH="$fixture/bin:$PATH" bash "$fixture/repo/scripts/lookup-stack-local.sh" start lookup_10x candidate > "$fixture/output" 2>&1 &
launcher=$!
for ((attempt=0; attempt<500; attempt++)); do
  grep -q 'APP .*workers/region-snapshot.js' "$fixture/calls" && break
  /bin/sleep 0.002
done
grep -q 'APP .*workers/region-snapshot.js' "$fixture/calls"
kill -TERM "$launcher"
result=0; wait "$launcher" || result=$?
test "$result" -eq 143
test ! -e "$fixture/repo/plans/database-lookups/backend.pid"
test ! -e "$fixture/repo/plans/database-lookups/region-snapshot.pid"
while read -r process_id; do ! kill -0 "$process_id" 2>/dev/null || [[ $(ps -p "$process_id" -o stat= 2>/dev/null) == Z* ]]; done < "$fixture/app-pids"
echo 'Lookup service readiness, single starts, clean environment and failed-start unwind passed.'
