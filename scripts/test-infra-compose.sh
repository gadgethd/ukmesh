#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
test_root="$(mktemp -d)"
trap 'rm -rf -- "$test_root"' EXIT

mkdir -p "$test_root/bin" "$test_root/external-infra"
cat >"$test_root/external-infra/compose.production.yml" <<'YAML'
services:
  timescaledb:
    image: timescale/timescaledb:fixture
  redis:
    image: redis:fixture
  mosquitto:
    image: eclipse-mosquitto:fixture
YAML
cat >"$test_root/bin/docker" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$MOCK_DOCKER_LOG"

command_name="${1:-}"
shift || true
case "$command_name" in
  compose)
    while [ "$#" -gt 0 ]; do
      case "$1" in
        --project-directory|--project-name|-f)
          shift 2
          ;;
        config)
          exit 0
          ;;
        ps)
          shift
          test "${1:-}" = '-q'
          service="${2:-}"
          if [ "${MOCK_PS_OUTPUT+x}" = x ]; then
            printf '%s\n' "$MOCK_PS_OUTPUT"
          else
            printf 'container-%s\n' "$service"
          fi
          exit 0
          ;;
        *)
          echo "unexpected mock Compose command: $*" >&2
          exit 90
          ;;
      esac
    done
    ;;
  inspect)
    if [ "${MOCK_STATE:-running}" = 'running' ]; then
      printf 'running\n'
    else
      printf '%s\n' "$MOCK_STATE"
    fi
    ;;
  *)
    echo "unexpected mock Docker command: $command_name $*" >&2
    exit 91
    ;;
esac
EOF
chmod 0755 "$test_root/bin/docker"

export PATH="$test_root/bin:$PATH"
export MOCK_DOCKER_LOG="$test_root/docker.log"
export MESHCORE_INFRA_PROJECT_DIR="$test_root/external-infra"
export MESHCORE_INFRA_COMPOSE_FILE="$test_root/external-infra/compose.production.yml"
export MESHCORE_INFRA_PROJECT_NAME='meshcore-infra-test'
source "$script_dir/lib/infra-compose.sh"

meshcore_infra_compose config -q
for service in timescaledb redis mosquitto; do
  resolved="$(resolve_infra_container "$service")"
  test "$resolved" = "container-$service"
  grep -Fq -- \
    "compose --project-directory $MESHCORE_INFRA_PROJECT_DIR --project-name $MESHCORE_INFRA_PROJECT_NAME -f $MESHCORE_INFRA_COMPOSE_FILE ps -q $service" \
    "$MOCK_DOCKER_LOG"
done

unset MESHCORE_INFRA_PROJECT_DIR MESHCORE_INFRA_DIR
test "$(resolve_infra_container timescaledb)" = 'container-timescaledb'
grep -Fq -- \
  "compose --project-directory $(dirname -- "$MESHCORE_INFRA_COMPOSE_FILE") --project-name $MESHCORE_INFRA_PROJECT_NAME -f $MESHCORE_INFRA_COMPOSE_FILE ps -q timescaledb" \
  "$MOCK_DOCKER_LOG"

if MOCK_PS_OUTPUT=$'container-one\ncontainer-two' resolve_infra_container timescaledb >/dev/null 2>&1; then
  echo 'ambiguous service resolution unexpectedly succeeded' >&2
  exit 1
fi
if MOCK_PS_OUTPUT='' resolve_infra_container redis >/dev/null 2>&1; then
  echo 'missing service resolution unexpectedly succeeded' >&2
  exit 1
fi
if MOCK_STATE=exited resolve_infra_container mosquitto >/dev/null 2>&1; then
  echo 'stopped service resolution unexpectedly succeeded' >&2
  exit 1
fi
if resolve_infra_container backend >/dev/null 2>&1; then
  echo 'unsupported service resolution unexpectedly succeeded' >&2
  exit 1
fi

printf 'split infrastructure resolver dry-run tests passed\n'
