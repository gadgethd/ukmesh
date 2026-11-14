#!/usr/bin/env bash
# Check fixture preservation, environment isolation and failure handling without services.
set -euo pipefail
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
test_root=$(mktemp -d)
trap 'rm -rf -- "$test_root"' EXIT
fixture="$test_root/fixture with spaces"
mkdir -p "$fixture/bin" "$fixture/repo/scripts" "$fixture/repo/plans/dev-stack"
cp "$repo/scripts/dev-all.sh" "$fixture/repo/scripts/"
for name in corescope beacon-server beacon-web; do mkdir -p "$fixture/repo/third_party/$name"; touch "$fixture/repo/third_party/$name/.git"; done
printf 'keep fixture\n' > "$fixture/repo/plans/dev-stack/fixture-ready"
printf 'must not be sourced $(touch dotenv-sourced)\n' > "$fixture/repo/.env"
cat > "$fixture/bin/docker" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
root=$(cd -- "$(dirname -- "$0")/.." && pwd)
[[ ! ${DATABASE_URL+x} && ! ${UNTRUSTED_INHERITED_SETTING+x} ]] || exit 91
printf 'docker %s\n' "$*" >> "$root/calls"
if [[ $1 == compose ]]; then
 [[ $2 == --env-file && $3 == /dev/null ]] || exit 92
 if [[ $* == *'run --rm --no-deps build' && -f $root/build-failure ]]; then exit 42; fi
 if [[ $* == *'run --rm --no-deps build' && -f $root/build-pause ]]; then
  touch "$root/build-entered"
  while [[ -f $root/build-pause && ! -f $root/build-resume ]]; do sleep 0.01; done
 fi
fi
MOCK
cat > "$fixture/bin/git" <<'MOCK'
#!/usr/bin/env bash
root=$(cd -- "$(dirname -- "$0")/.." && pwd)
printf 'git %s\n' "$*" >> "$root/calls"
if [[ $* == *'diff --name-only --diff-filter=U' && -f $root/merge-conflict ]]; then echo conflict.go; fi
if [[ $* == *'symbolic-ref --short -q HEAD' ]]; then echo ukmesh-dev; fi
MOCK
cat > "$fixture/bin/curl" <<'MOCK'
#!/usr/bin/env bash
root=$(cd -- "$(dirname -- "$0")/.." && pwd)
[[ ! -f $root/ukmesh-unhealthy ]]
MOCK
cat > "$fixture/repo/scripts/dev-stack.sh" <<'MOCK'
#!/usr/bin/env bash
root=$(cd -- "$(dirname -- "$0")/../.." && pwd)
printf 'ukmesh %s\n' "$*" >> "$root/calls"
MOCK
chmod +x "$fixture/bin/"*
run() { env PATH="$fixture/bin:$PATH" DATABASE_URL=postgresql://untrusted.invalid/db UNTRUSTED_INHERITED_SETTING=discard bash "$fixture/repo/scripts/dev-all.sh" "$@" > "$fixture/output" 2>&1; }
: > "$fixture/calls"
run start
! grep -q '^ukmesh ' "$fixture/calls"
grep -q 'run --rm --no-deps build' "$fixture/calls"
grep -q 'up -d --wait --wait-timeout 240' "$fixture/calls"
[[ $(cat "$fixture/repo/plans/dev-stack/fixture-ready") == 'keep fixture' ]]
[[ ! -e "$fixture/repo/dotenv-sourced" ]]
touch "$fixture/ukmesh-unhealthy"
run start
grep -q '^ukmesh start$' "$fixture/calls"
touch "$fixture/merge-conflict"
: > "$fixture/calls"
if run companions; then echo 'Unresolved merge was accepted' >&2; exit 1; fi
! grep -q 'docker compose' "$fixture/calls"
rm "$fixture/merge-conflict"
touch "$fixture/build-failure"
if run start; then echo 'Failed build was accepted' >&2; exit 1; fi
! grep -Eq '(^ukmesh reset|docker .* (down|stop))' "$fixture/calls"
rm "$fixture/build-failure"
: > "$fixture/calls"
if run restart unowned-service; then echo 'Unowned service was accepted' >&2; exit 1; fi
[[ ! -s "$fixture/calls" ]]
run init
! grep -Eq 'submodule update|switch' "$fixture/calls"
run stop
grep -q '^ukmesh stop$' "$fixture/calls"
[[ -f "$fixture/repo/plans/dev-stack/fixture-ready" ]]
touch "$fixture/build-pause"
run build &
first_operation=$!
for attempt in {1..100}; do [[ ! -f $fixture/build-entered ]] || break; sleep 0.01; done
[[ -f $fixture/build-entered ]]
if run companions; then echo 'Concurrent DEV mutation was accepted' >&2; exit 1; fi
grep -q 'Another DEV operation is running' "$fixture/output"
touch "$fixture/build-resume"
wait "$first_operation"
# The lock closes before child processes start, so later operations can proceed.
run init
echo 'DEV companion launcher contracts passed' 
