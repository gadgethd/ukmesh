#!/usr/bin/env bash

set -Eeuo pipefail

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly NODE_A='A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1'
readonly NODE_B='B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2'
readonly NODE_C="$(printf 'C3%.0s' {1..32})"

test_root="$(mktemp -d)"
cleanup() {
  local status=$?
  if (( status == 0 )); then
    rm -rf -- "$test_root"
  else
    printf 'newuser test artifacts retained at %s\n' "$test_root" >&2
  fi
}
trap cleanup EXIT
mkdir -p "$test_root/bin"

cat >"$test_root/bin/docker" <<'EOF'
#!/usr/bin/env bash
set -Eeuo pipefail

if [[ ${1:-} == inspect ]]; then
  if [[ $* == *State.Running* ]]; then
    printf 'true\n'
  elif [[ $* == *State.Status* ]]; then
    printf 'running\n'
  else
    printf 'healthy\n'
  fi
  exit 0
fi

if [[ ${1:-} == compose ]]; then
  case "$*" in
    *'ps -q mosquitto') printf 'infra-mosquitto-id\n' ;;
    *) printf 'unexpected docker compose invocation: %s\n' "$*" >&2; exit 1 ;;
  esac
  exit 0
fi

if [[ ${1:-} == logs ]]; then
  if [[ -n ${FAKE_DISCOVERY_WAIT_MARKER:-} ]]; then
    : >"$FAKE_DISCOVERY_WAIT_MARKER"
  fi
  if [[ -n ${FAKE_DISCOVERY_RELEASE:-} && -e $FAKE_DISCOVERY_RELEASE ]]; then
    printf "New client connected from 127.0.0.1:1234 as meshcore_%s_1 (p2, c1, k60, u'%s').\n" \
      "$FAKE_DISCOVERY_NODE" "$FAKE_DISCOVERY_USERNAME"
  fi
  exit 0
fi

[[ ${1:-} == exec ]] || { printf 'unexpected docker invocation: %s\n' "$*" >&2; exit 1; }

# Never put the generated broker password into test logs.
if [[ $* == *'NEWUSER_MQTT_PASSWORD='* ]]; then
  printf 'VERIFY_BROKER_LOGIN\n' >>"$FAKE_DOCKER_LOG"
  [[ ${FAKE_FAIL_STAGE:-} != login ]] || exit 1
  exit 0
fi

if [[ $* == *'mosquitto_passwd -b '* ]]; then
  username="${!#}"
  printf 'CREDENTIAL_CREATE %s\n' "$username" >>"$FAKE_DOCKER_LOG"
  : >"$FAKE_CREDENTIAL_FILE"
  [[ ${FAKE_FAIL_STAGE:-} != credential ]] || exit 1
  exit 0
fi

if [[ $* == *'mosquitto_passwd -D '* ]]; then
  username="${!#}"
  printf 'CREDENTIAL_DELETE %s\n' "$username" >>"$FAKE_DOCKER_LOG"
  rm -f -- "$FAKE_CREDENTIAL_FILE"
  exit 0
fi

if [[ $* == *'while IFS=: read'* ]]; then
  username="${!#}"
  if [[ ",${FAKE_EXISTING_USERS:-}," == *",${username},"* ]]; then
    exit 0
  fi
  exit 1
fi

if [[ $* == *'/mosquitto/config/acl'* ]]; then
  if [[ $* == *'node_id='* ]]; then
    printf 'ACL_READBACK\n' >>"$FAKE_DOCKER_LOG"
    [[ ${FAKE_FAIL_STAGE:-} != acl ]]
    exit $?
  fi
  printf 'ACL_PRECHECK\n' >>"$FAKE_DOCKER_LOG"
  exit 1
fi

if [[ ${4:-} == dist/tools/confirmOwnerGrants.js ]]; then
  action=$5
  mode=$6
  username=$7
  actor=$8
  shift 8
  case "$action" in
    inspect)
      active="$(awk -v name="$username" '$1 == name { print $2 }' "$FAKE_DB_STATE_FILE")"
      if [[ $mode == link && -z $active || $mode == create && -n $active ]]; then
        exit 1
      fi
      if [[ -n $active ]]; then printf '%s\n' "$active"; fi
      ;;
    record)
      printf 'APPROVAL_ACTOR %s %s\n' "$username" "$actor" >>"$FAKE_DOCKER_LOG"
      for key in "$@"; do
        if ! grep -Fqx -- "$username $key" "$FAKE_DB_STATE_FILE"; then
          printf '%s %s\n' "$username" "$key" >>"$FAKE_DB_STATE_FILE"
          printf 'DB_WRITE %s %s\n' "$username" "$key" >>"$FAKE_DOCKER_LOG"
        fi
      done
      [[ ${FAKE_FAIL_STAGE:-} != ambiguous-commit ]] || exit 1
      ;;
    verify)
      printf 'VERIFY_APPLIED_GENERATION\n' >>"$FAKE_DOCKER_LOG"
      [[ ${FAKE_FAIL_STAGE:-} != pending ]] || exit 75
      for key in "$@"; do grep -Fqx -- "$username $key" "$FAKE_DB_STATE_FILE"; done
      ;;
  esac
  exit 0
fi

if [[ $* == *' psql '* ]]; then
  printf 'newuser must access owner grants through the operator tool\n' >&2
  exit 1
fi

# SIGHUP, health probes, and all other fake exec operations succeed.
exit 0
EOF
chmod +x "$test_root/bin/docker"

init_sandbox() {
  local name=$1
  local initial_map=$2
  SANDBOX="$test_root/$name"
  mkdir -p "$SANDBOX/repo" "$SANDBOX/infra"
  touch "$SANDBOX/infra/docker-compose.yml"
  printf 'OWNER_MQTT_USERNAME_MAP=%s\n' "$initial_map" >"$SANDBOX/repo/.env"
  : >"$SANDBOX/docker.log"
  export FAKE_DOCKER_LOG="$SANDBOX/docker.log"
  export FAKE_CREDENTIAL_FILE="$SANDBOX/credential-state"
  export FAKE_DB_STATE_FILE="$SANDBOX/db-state"
  printf '%s\n' "$initial_map" | tr ',' '\n' | tr '=' ' ' >"$FAKE_DB_STATE_FILE"
}

run_newuser() {
  local sandbox=$1
  local output=$2
  local existing_users=$3
  local existing_db_users=$4
  local fail_stage=$5
  shift 5
  PATH="$test_root/bin:$PATH" \
  NEWUSER_REPO_DIR="$sandbox/repo" \
  MESHCORE_INFRA_PROJECT_DIR="$sandbox/infra" \
  TMPDIR="$sandbox" \
  NEWUSER_DISCOVERY_POLL_SECONDS=0.05 \
  NEWUSER_ACL_TIMEOUT_SECONDS=1 \
  FAKE_EXISTING_USERS="$existing_users" \
  FAKE_EXISTING_DB_USERS="$existing_db_users" \
  FAKE_FAIL_STAGE="$fail_stage" \
  FAKE_DISCOVERY_WAIT_MARKER="${sandbox}/discovery-waiting" \
  FAKE_DISCOVERY_RELEASE="${sandbox}/discovery-release" \
  FAKE_DISCOVERY_NODE="$NODE_B" \
  FAKE_DISCOVERY_USERNAME=charlie \
    bash "$SCRIPT_DIR/newuser.sh" "$@" >"$output" 2>&1
}

assert_file_contains() {
  local file=$1
  local text=$2
  grep -Fq -- "$text" "$file" \
    || { printf 'expected %s to contain: %s\n' "$file" "$text" >&2; exit 1; }
}

# Supplied fingerprints and passive broker logs are evidence, never a grant.
init_sandbox unconfirmed-explicit "alice=${NODE_A}"
run_newuser "$SANDBOX" "$SANDBOX/output.log" alice alice '' dave "$NODE_B"
[[ $(<"$SANDBOX/repo/.env") == "OWNER_MQTT_USERNAME_MAP=alice=${NODE_A}" ]]
assert_file_contains "$SANDBOX/output.log" 'No owner grant was written'
if grep -Eq '^(DB_WRITE|CREDENTIAL_CREATE|COMPOSE_BACKEND)' "$SANDBOX/docker.log"; then
  printf 'unconfirmed explicit key mutated owner or broker state\n' >&2
  exit 1
fi

init_sandbox unconfirmed-discovery "alice=${NODE_A}"
: >"$SANDBOX/discovery-release"
run_newuser "$SANDBOX" "$SANDBOX/output.log" 'alice,charlie' alice '' --watch --timeout 2 charlie
[[ $(<"$SANDBOX/repo/.env") == "OWNER_MQTT_USERNAME_MAP=alice=${NODE_A}" ]]
assert_file_contains "$SANDBOX/output.log" "$NODE_B"
assert_file_contains "$SANDBOX/output.log" 'No owner grant was written'
if grep -Eq '^(DB_WRITE|COMPOSE_BACKEND)' "$SANDBOX/docker.log"; then
  printf 'broker log discovery created an unconfirmed owner grant\n' >&2
  exit 1
fi

# Linking a node preserves the current map and existing broker password.
init_sandbox link "alice=${NODE_A},bob=${NODE_C}"
run_newuser "$SANDBOX" "$SANDBOX/output.log" 'alice' 'alice' '' --link --confirm-owner-grant "$NODE_B" alice "$NODE_B"
expected_map="OWNER_MQTT_USERNAME_MAP=alice=${NODE_A},bob=${NODE_C}"
[[ $(<"$SANDBOX/repo/.env") == "$expected_map" ]] \
  || { printf 'unexpected owner map: %s\n' "$(<"$SANDBOX/repo/.env")" >&2; exit 1; }
assert_file_contains "$SANDBOX/output.log" 'Password: unchanged (existing credential)'
assert_file_contains "$SANDBOX/output.log" 'set mqtt.1.topic.root meshcore/{IATA}/{PUBLIC_KEY}/packets'
assert_file_contains "$SANDBOX/output.log" 'The default root "meshcore" is denied'
assert_file_contains "$SANDBOX/docker.log" "DB_WRITE alice $NODE_B"
assert_file_contains "$SANDBOX/docker.log" 'VERIFY_APPLIED_GENERATION'
assert_file_contains "$SANDBOX/docker.log" 'ACL_READBACK'
assert_file_contains "$SANDBOX/db-state" "alice $NODE_A"
if grep -Fq 'CREDENTIAL_CREATE alice' "$SANDBOX/docker.log"; then
  printf 'link flow unexpectedly changed the existing credential\n' >&2
  exit 1
fi

# A second process adds a grant while discovery has released the lock. The
# discovery run must leave the other transaction and legacy configuration intact.
init_sandbox concurrent "alice=${NODE_A}"
PATH="$test_root/bin:$PATH" \
NEWUSER_REPO_DIR="$SANDBOX/repo" \
MESHCORE_INFRA_PROJECT_DIR="$SANDBOX/infra" \
TMPDIR="$SANDBOX" \
NEWUSER_DISCOVERY_POLL_SECONDS=0.05 \
FAKE_DOCKER_LOG="$SANDBOX/docker.log" \
FAKE_CREDENTIAL_FILE="$SANDBOX/credential-state" \
FAKE_DB_STATE_FILE="$SANDBOX/db-state" \
FAKE_EXISTING_USERS='alice,charlie' \
FAKE_EXISTING_DB_USERS=alice \
FAKE_DISCOVERY_WAIT_MARKER="$SANDBOX/discovery-waiting" \
FAKE_DISCOVERY_RELEASE="$SANDBOX/discovery-release" \
FAKE_DISCOVERY_NODE="$NODE_B" \
FAKE_DISCOVERY_USERNAME=charlie \
  bash "$SCRIPT_DIR/newuser.sh" --watch --timeout 10 charlie >"$SANDBOX/watcher.log" 2>&1 &
watcher_pid=$!
for _ in {1..100}; do
  [[ -e $SANDBOX/discovery-waiting ]] && break
  sleep 0.02
done
[[ -e $SANDBOX/discovery-waiting ]] \
  || { kill "$watcher_pid" 2>/dev/null || true; printf 'watcher did not enter discovery\n' >&2; exit 1; }
run_newuser "$SANDBOX" "$SANDBOX/second-run.log" 'alice,charlie' 'alice' '' --confirm-owner-grant "$NODE_C" bob "$NODE_C"
: >"$SANDBOX/discovery-release"
wait "$watcher_pid"
run_newuser "$SANDBOX" "$SANDBOX/charlie.log" 'alice,bob,charlie' 'alice,bob' '' --watch --confirm-owner-grant "$NODE_B" charlie
[[ $(<"$SANDBOX/repo/.env") == "OWNER_MQTT_USERNAME_MAP=alice=${NODE_A}" ]]
assert_file_contains "$SANDBOX/db-state" "alice $NODE_A"
assert_file_contains "$SANDBOX/db-state" "bob $NODE_C"
assert_file_contains "$SANDBOX/db-state" "charlie $NODE_B"

# Only confirmed keys are recorded and there is no mutation from a mismatch.
init_sandbox confirmation "alice=${NODE_A}"
if run_newuser "$SANDBOX" "$SANDBOX/output.log" alice alice '' --link --confirm-owner-grant "$NODE_C" alice "$NODE_B"; then
  printf 'mismatched explicit keys unexpectedly passed\n' >&2
  exit 1
fi
assert_file_contains "$SANDBOX/output.log" 'mismatch'
[[ $(<"$SANDBOX/db-state") == "alice $NODE_A" ]]

init_sandbox confirmation-batch "alice=${NODE_A}"
run_newuser "$SANDBOX" "$SANDBOX/output.log" alice alice '' \
  --confirm-owner-grant "$NODE_C" --confirm-owner-grant "$NODE_B" dave "$NODE_B,$NODE_C"
assert_file_contains "$SANDBOX/docker.log" "APPROVAL_ACTOR dave newuser:$(id -un)"
[[ $(<"$SANDBOX/db-state") == "$(printf 'alice %s\ndave %s\ndave %s' "$NODE_A" "$NODE_B" "$NODE_C")" ]]

# A credential failure happens before approval, so a partial credential rolls back.
for stage in credential login; do
  init_sandbox "rollback-$stage" "alice=${NODE_A}"
  if run_newuser "$SANDBOX" "$SANDBOX/output.log" alice alice "$stage" --confirm-owner-grant "$NODE_B" dave "$NODE_B"; then
    printf 'expected injected %s failure\n' "$stage" >&2
    exit 1
  fi
  [[ ! -e $SANDBOX/credential-state ]]
  [[ $(<"$SANDBOX/db-state") == "alice $NODE_A" ]]
  assert_file_contains "$SANDBOX/docker.log" 'CREDENTIAL_DELETE dave'
done

# After recording, a timeout, inconsistent broker readback or lost response
# must retain the exact approval and credential. A replay can verify it.
for stage in pending acl ambiguous-commit; do
  init_sandbox "pending-$stage" "alice=${NODE_A}"
  if run_newuser "$SANDBOX" "$SANDBOX/output.log" alice alice "$stage" --confirm-owner-grant "$NODE_B" dave "$NODE_B"; then
    printf 'expected incomplete verification in %s\n' "$stage" >&2
    exit 1
  fi
  [[ -e $SANDBOX/credential-state ]]
  assert_file_contains "$SANDBOX/db-state" "dave $NODE_B"
  assert_file_contains "$SANDBOX/output.log" 'credential remains available'
  if grep -Fq 'Provisioned MQTT owner account' "$SANDBOX/output.log"; then
    printf 'incomplete verification incorrectly reported provisioning success\n' >&2
    exit 1
  fi
  run_newuser "$SANDBOX" "$SANDBOX/retry.log" 'alice,dave' 'alice,dave' '' --watch --confirm-owner-grant "$NODE_B" dave
  assert_file_contains "$SANDBOX/retry.log" 'Provisioned MQTT owner account'
  [[ $(grep -Fc "DB_WRITE dave $NODE_B" "$SANDBOX/docker.log") == 1 ]]
done

printf 'newuser explicit approval, passive discovery, applied ACL readback and durable retry tests passed\n'
