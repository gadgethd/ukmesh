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
    *'ps -q timescaledb') printf 'infra-timescaledb-id\n' ;;
    *' up -d --no-deps backend') printf 'COMPOSE_BACKEND\n' >>"$FAKE_DOCKER_LOG" ;;
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

sql=''
if [[ " $* " == *' -i '* ]]; then
  sql="$(cat)"
  printf 'SQL %s\n' "$sql" >>"$FAKE_DOCKER_LOG"
fi

mqtt_username=''
for argument in "$@"; do
  case $argument in
    mqtt_username=*) mqtt_username=${argument#*=} ;;
  esac
done

if [[ $* == *' psql '* ]]; then
  if [[ $sql == *'SELECT COALESCE('* ]]; then
    if [[ ",${FAKE_EXISTING_DB_USERS:-}," == *",${mqtt_username},"* ]]; then
      printf 'active\n'
    else
      printf 'missing\n'
    fi
  elif [[ $sql == *'SELECT COUNT(*)'*'JOIN owner_accounts'* ]]; then
    if [[ ${FAKE_FAIL_STAGE:-} == database-readback ]]; then printf '0\n'; else printf '1\n'; fi
  elif [[ $sql == *'SELECT COUNT(*) FROM owner_account_nodes'* ]]; then
    printf '0\n'
  elif [[ $sql == *'INSERT INTO owner_account_nodes'* ]]; then
    printf 'DB_WRITE %s\n' "$mqtt_username" >>"$FAKE_DOCKER_LOG"
    [[ ${FAKE_FAIL_STAGE:-} != environment ]] || exit 1
    : >"$FAKE_DB_STATE_FILE"
    [[ ${FAKE_FAIL_STAGE:-} != db ]] || exit 1
  elif [[ $sql == *'node_identity_nodes'* ]]; then
    printf '0\n'
  elif [[ $sql == *'DELETE FROM owner_accounts'* || $sql == *'DELETE FROM owner_account_nodes'* ]]; then
    printf 'DB_ROLLBACK %s\n' "$mqtt_username" >>"$FAKE_DOCKER_LOG"
    rm -f -- "$FAKE_DB_STATE_FILE"
  fi
  exit 0
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

# Linking a node preserves the current map and existing broker password.
init_sandbox link "alice=${NODE_A},bob=${NODE_C}"
run_newuser "$SANDBOX" "$SANDBOX/output.log" 'alice' 'alice' '' --link alice "$NODE_B"
expected_map="OWNER_MQTT_USERNAME_MAP=alice=${NODE_A},bob=${NODE_C},alice=${NODE_B}"
[[ $(<"$SANDBOX/repo/.env") == "$expected_map" ]] \
  || { printf 'unexpected owner map: %s\n' "$(<"$SANDBOX/repo/.env")" >&2; exit 1; }
assert_file_contains "$SANDBOX/output.log" 'Password: unchanged (existing credential)'
assert_file_contains "$SANDBOX/docker.log" 'INSERT INTO owner_account_nodes'
assert_file_contains "$SANDBOX/docker.log" 'COMPOSE_BACKEND'
if grep -Fq 'CREDENTIAL_CREATE alice' "$SANDBOX/docker.log"; then
  printf 'link flow unexpectedly changed the existing credential\n' >&2
  exit 1
fi

# A second process adds a grant while discovery has released the lock. The
# discovery run must merge into the map it rereads after reacquiring the lock.
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
run_newuser "$SANDBOX" "$SANDBOX/second-run.log" 'alice,charlie' 'alice' '' bob "$NODE_C"
: >"$SANDBOX/discovery-release"
wait "$watcher_pid"
expected_map="OWNER_MQTT_USERNAME_MAP=alice=${NODE_A},bob=${NODE_C},charlie=${NODE_B}"
[[ $(<"$SANDBOX/repo/.env") == "$expected_map" ]] \
  || { printf 'concurrent owner map lost a grant: %s\n' "$(<"$SANDBOX/repo/.env")" >&2; exit 1; }

# Inject failures after each mutation class and check that EXIT rollback is
# idempotent and restores the mocked broker/config/database state.
for stage in credential environment db acl; do
  init_sandbox "rollback-${stage}" "alice=${NODE_A}"
  if run_newuser "$SANDBOX" "$SANDBOX/output.log" alice alice "$stage" dave "$NODE_B"; then
    printf 'expected injected %s failure\n' "$stage" >&2
    exit 1
  fi
  [[ $(<"$SANDBOX/repo/.env") == "OWNER_MQTT_USERNAME_MAP=alice=${NODE_A}" ]] \
    || { printf '%s failure did not restore the environment map\n' "$stage" >&2; exit 1; }
  [[ ! -e $SANDBOX/credential-state ]] \
    || { printf '%s failure left a broker credential behind\n' "$stage" >&2; exit 1; }
  [[ ! -e $SANDBOX/db-state ]] \
    || { printf '%s failure left owner database state behind\n' "$stage" >&2; exit 1; }
  if [[ $stage == credential ]]; then
    assert_file_contains "$SANDBOX/docker.log" 'CREDENTIAL_DELETE dave'
  fi
  if [[ $stage == db || $stage == acl ]]; then
    assert_file_contains "$SANDBOX/docker.log" 'DB_ROLLBACK dave'
  fi
  if [[ $stage == acl ]]; then
    compose_count="$(grep -c '^COMPOSE_BACKEND$' "$SANDBOX/docker.log")"
    [[ $compose_count -ge 2 ]] \
      || { printf 'ACL failure did not reapply restored backend config\n' >&2; exit 1; }
  fi
done

printf 'newuser concurrency and rollback regression tests passed\n'
