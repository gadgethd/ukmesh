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

reject() {
  echo "service ${service} is not Compose-adoptable: $1; operator reconciliation is required before replacement" >&2
  exit 65
}

cd "$project_dir"
container_id="$(docker compose --project-name "$project_name" ps -q "$service")"
if [ -z "$container_id" ] || [[ "$container_id" == *$'\n'* ]]; then
  reject 'Compose must identify exactly one current container'
fi
labels="$(docker inspect "$container_id" --format '{{json .Config.Labels}}')"
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
