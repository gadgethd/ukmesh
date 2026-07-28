#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <compose-service>" >&2
  exit 64
fi

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
project_dir="$(cd -- "${script_dir}/.." && pwd)"
service="$1"
project_name="${COMPOSE_PROJECT_NAME:-meshcore-analytics}"

cd "${project_dir}"
if ! docker compose --project-name "${project_name}" config --services | grep -Fxq -- "${service}"; then
  echo "Unknown Compose service: ${service}" >&2
  exit 65
fi

echo "Replacing ${service} in Compose project ${project_name}..."
docker compose --project-name "${project_name}" build "${service}"
docker compose --project-name "${project_name}" up \
  --detach \
  --no-deps \
  --force-recreate \
  "${service}"
docker compose --project-name "${project_name}" ps "${service}"
