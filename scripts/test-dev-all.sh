#!/usr/bin/env bash
# Exercise orchestration, environment isolation, failure propagation and locking.
set -euo pipefail
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
fixture=$(mktemp -d)
trap 'rm -rf -- "$fixture"' EXIT
checkout="$fixture/repo with spaces"
qualification="$checkout/scripts/full-dev/qualification"
mkdir -p "$checkout/scripts/companion-dev" "$qualification" "$checkout/plans/dev-stack"
cp "$repo/scripts/dev-all.sh" "$checkout/scripts/"
cp "$repo/scripts/full-dev/qualification/"{common,target_entry,target_lease}.py "$qualification/"
cat > "$qualification/fixtures.py" <<'MOCK'
def selection_values():
    return {}
MOCK
cat > "$qualification/target_safety.py" <<'MOCK'
def inspect_snapshot():
    return {'consumers': {}}

def changing(*args):
    return set()

def target_values(values):
    return values

def check_start(*args, **kwargs):
    raise AssertionError('orchestration test must not apply real targets')

def mark_complete(values):
    pass
MOCK
printf 'retained\n' > "$checkout/plans/dev-stack/fixture-ready"
printf 'must not source $(touch dotenv-sourced)\n' > "$checkout/.env"
for subsystem in full-dev companion-dev; do
  cat > "$checkout/scripts/$subsystem/control.sh" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
repo=$(cd -- "$(dirname -- "$0")/../.." && pwd)
[[ ! ${DATABASE_URL+x} && ! ${UNTRUSTED_SETTING+x} ]] || exit 91
[[ $DOCKER_HOST == unix:///var/run/docker.sock ]] || exit 92
name=$(basename "$(dirname "$0")")
printf '%s %s\n' "$name" "$*" >> "$repo/calls"
if [[ $name == full-dev && $1 == build ]]; then
  if [[ -f $repo/fail ]]; then exit 42; fi
  if [[ -f $repo/pause ]]; then
    touch "$repo/entered"
    while [[ ! -f $repo/resume ]]; do sleep 0.01; done
  fi
fi
MOCK
 done
cat > "$checkout/scripts/qualify-full-stack.sh" <<'MOCK'
#!/usr/bin/env bash
repo=$(cd -- "$(dirname -- "$0")/.." && pwd)
printf 'qualify %s\n' "$*" >> "$repo/calls"
MOCK
run() { env DOCKER_HOST=ssh://untrusted.invalid DATABASE_URL=postgres://untrusted.invalid UNTRUSTED_SETTING=discard bash "$checkout/scripts/dev-all.sh" "$@" > "$fixture/output" 2>&1; }
run start
[[ $(cat "$checkout/calls") == $'full-dev start\ncompanion-dev start' ]]
[[ $(cat "$checkout/plans/dev-stack/fixture-ready") == retained ]]
[[ ! -f $checkout/dotenv-sourced ]]
: > "$checkout/calls"
run stop
[[ $(cat "$checkout/calls") == $'companion-dev stop\nfull-dev stop' ]]
: > "$checkout/calls"
run init
[[ $(cat "$checkout/calls") == 'companion-dev init' ]]
: > "$checkout/calls"
run qualify sample --seconds 15 --run '/tmp/evidence with spaces'
[[ $(cat "$checkout/calls") == 'qualify sample --seconds 15 --run /tmp/evidence with spaces' ]]
touch "$checkout/fail"
: > "$checkout/calls"
if run build; then echo 'Failed build accepted' >&2; exit 1; fi
[[ $(cat "$checkout/calls") == 'full-dev build' ]]
rm "$checkout/fail"
touch "$checkout/pause"
run build &
operation=$!
for attempt in {1..100}; do [[ ! -f $checkout/entered ]] || break; sleep 0.01; done
[[ -f $checkout/entered ]]
if run start; then echo 'Concurrent mutation accepted' >&2; exit 1; fi
grep -q 'Another DEV operation is running' "$fixture/output"
touch "$checkout/resume"
wait "$operation"
run init
if run erase-everything; then echo 'Unknown action accepted' >&2; exit 1; fi
echo 'Full DEV launcher contracts passed'
