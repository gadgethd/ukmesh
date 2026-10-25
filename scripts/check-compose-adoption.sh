#!/usr/bin/env bash
set -euo pipefail

# Read-only preflight. Never attempt automatic adoption or network recreation.
if [ "$#" -ne 1 ] || ! [[ "$1" =~ ^[a-zA-Z0-9][a-zA-Z0-9_.-]*$ ]]; then
  echo 'usage: check-compose-adoption.sh SERVICE' >&2
  exit 64
fi
service="$1"
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
project_dir="$(cd -- "${script_dir}/.." && pwd -P)"
project_name="${COMPOSE_PROJECT_NAME:-meshcore-analytics}"
container_id=""

unavailable() {
  echo "cannot verify Compose adoption for service ${service}: $1; no replacement is safe until this read-only check succeeds" >&2
  exit 69
}

reject() {
  echo "service ${service} is not Compose-adoptable: $1; operator reconciliation is required before replacement" >&2
  echo 'After verifying the image pins, environment, mounts and Compose files against the current service, run during an approved maintenance window (removes only the listed containers; leaves volumes and networks intact):' >&2
  # Print an exact, shell-quoted repair; never execute it here. Preserve an
  # explicit Compose file selection, including the live external-network overlay.
  printf '  (cd %q && export COMPOSE_PROJECT_NAME=%q' "$project_dir" "$project_name" >&2
  if [ -n "${COMPOSE_FILE:-}" ]; then
    printf ' COMPOSE_FILE=%q' "$COMPOSE_FILE" >&2
  fi
  if [ -n "${COMPOSE_PATH_SEPARATOR:-}" ]; then
    printf ' COMPOSE_PATH_SEPARATOR=%q' "$COMPOSE_PATH_SEPARATOR" >&2
  fi
  printf ' && ' >&2
  if [ -n "$container_id" ]; then
    local -a ids
    mapfile -t ids <<<"$container_id"
    printf 'docker rm -f --' >&2
    printf ' %q' "${ids[@]}" >&2
    printf ' && ' >&2
  fi
  printf 'docker compose --project-name %q up -d --no-deps --no-build %q' "$project_name" "$service" >&2
  printf ' && %q %q)\n' "${script_dir}/check-compose-adoption.sh" "$service" >&2
  exit 65
}

cd "$project_dir"
if ! container_id="$(docker compose --project-name "$project_name" ps -q "$service")"; then
  unavailable 'Compose container lookup failed; check Docker access and Compose configuration'
fi
if [ -z "$container_id" ]; then
  # A manual docker run can own the expected name without any Compose labels.
  # Resolve custom container_name as well as Compose's default name, so the
  # error can identify the actual conflict rather than suggest another failing up.
  if ! container_name="$(docker compose --project-name "$project_name" config --format json \
    | jq -er --arg project "$project_name" --arg service "$service" \
      '.services[$service].container_name // ($project + "-" + $service + "-1")')"; then
    unavailable 'cannot resolve the configured container name'
  fi
  # Listing by exact name distinguishes absence from an inspect/daemon failure.
  # Docker name filters are regular expressions; escape configured punctuation.
  name_pattern="$(printf '%s' "$container_name" | sed 's/[][\\.^$*+?(){}|]/\\&/g')"
  if ! container_id="$(docker ps --all --quiet --filter "name=^/${name_pattern}$")"; then
    unavailable 'Docker container-name lookup failed'
  fi
  reject "Compose did not select a current container (expected name: ${container_name})"
fi
if [[ "$container_id" == *$'\n'* ]]; then
  reject 'Compose must identify exactly one current container'
fi
if ! labels="$(docker inspect "$container_id" --format '{{json .Config.Labels}}')"; then
  unavailable 'container label inspection failed'
fi
if ! jq -e \
  --arg project "$project_name" \
  --arg service "$service" \
  --arg directory "$project_dir" \
  --arg base "${project_dir}/docker-compose.yml" '
    .["com.docker.compose.project"] == $project
    and .["com.docker.compose.service"] == $service
    and .["com.docker.compose.project.working_dir"] == $directory
    and ((.["com.docker.compose.project.config_files"] // "") | split(",") | index($base) != null)
  ' <<<"$labels" >/dev/null; then
  reject 'project, service, working directory or config-file labels are missing or differ from this checkout'
fi
printf 'Compose adoption metadata verified for %s\n' "$service"
