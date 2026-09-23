#!/usr/bin/env bash

set -Eeuo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly NODE_A='A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1'
readonly NODE_B='B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2'
readonly NODE_C='C3C3C3C3C3C3C3C3C3C3C3C3C3C3C3C3C3C3C3C3C3C3C3C3C3C3C3C3C3C3C3C3'

test_root="$(mktemp -d)"
trap 'rm -rf -- "$test_root"' EXIT
mkdir -p "$test_root/bin" "$test_root/repo" "$test_root/infra"
touch "$test_root/infra/docker-compose.yml"
printf 'OWNER_MQTT_USERNAME_MAP=alice=%s,bob=%s\n' "$NODE_A" "$NODE_C" \
  >"$test_root/repo/.env"

cat >"$test_root/bin/docker" <<'EOF'
#!/usr/bin/env bash
set -Eeuo pipefail

printf 'ARGS %s\n' "$*" >>"$FAKE_DOCKER_LOG"

case ${1:-} in
  inspect)
    if [[ $* == *State.Running* ]]; then
      printf 'true\n'
    else
      printf 'healthy\n'
    fi
    ;;
  compose)
    case "$*" in
      *'ps -q mosquitto') printf 'infra-mosquitto-id\n' ;;
      *'ps -q timescaledb') printf 'infra-timescaledb-id\n' ;;
      *' up -d --no-deps backend') ;;
      *) printf 'unexpected docker compose invocation: %s\n' "$*" >&2; exit 1 ;;
    esac
    ;;
  exec)
    sql=''
    if [[ " $* " == *' -i '* ]]; then
      sql="$(cat)"
      printf 'STDIN %s\n' "$sql" >>"$FAKE_DOCKER_LOG"
    fi
    if [[ $* == *' psql '* ]]; then
      if [[ $sql == *'SELECT COALESCE('* ]]; then
        printf 'active\n'
      elif [[ $sql == *'JOIN owner_accounts'* ]]; then
        printf '1\n'
      elif [[ $sql == *'node_identity_nodes'* ]]; then
        printf '1\n'
      fi
    fi
    ;;
  *)
    printf 'unexpected docker invocation: %s\n' "$*" >&2
    exit 1
    ;;
esac
EOF
chmod +x "$test_root/bin/docker"

export FAKE_DOCKER_LOG="$test_root/docker.log"
output="$test_root/output.log"
PATH="$test_root/bin:$PATH" \
NEWUSER_REPO_DIR="$test_root/repo" \
MESHCORE_INFRA_PROJECT_DIR="$test_root/infra" \
TMPDIR="$test_root" \
  bash "$SCRIPT_DIR/newuser.sh" --link alice "$NODE_B" >"$output" 2>&1

expected_map="OWNER_MQTT_USERNAME_MAP=alice=${NODE_A},bob=${NODE_C},alice=${NODE_B}"
[[ $(<"$test_root/repo/.env") == "$expected_map" ]] \
  || { printf 'unexpected owner map: %s\n' "$(<"$test_root/repo/.env")" >&2; exit 1; }
grep -Fq 'Password: unchanged (existing credential)' "$output"
grep -Fq 'INSERT INTO owner_account_nodes' "$FAKE_DOCKER_LOG"
grep -Fq 'compose -f docker-compose.yml -f docker-compose.live.yml up -d --no-deps backend' \
  "$FAKE_DOCKER_LOG"
grep -Fq "compose --project-directory $test_root/infra --project-name meshcore-infra -f $test_root/infra/docker-compose.yml ps -q mosquitto" \
  "$FAKE_DOCKER_LOG"
grep -Fq "compose --project-directory $test_root/infra --project-name meshcore-infra -f $test_root/infra/docker-compose.yml ps -q timescaledb" \
  "$FAKE_DOCKER_LOG"
if grep -Fq 'mosquitto_passwd -b' "$FAKE_DOCKER_LOG"; then
  printf 'link flow unexpectedly changed the existing credential\n' >&2
  exit 1
fi

printf 'newuser multi-node link test passed\n'
