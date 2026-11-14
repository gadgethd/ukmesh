# Shared local Compose binding. Public entrypoints clean the environment first.
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
source "$repo/scripts/full-dev/environment.sh"
export COMPOSE_PARALLEL_LIMIT=1
compose=(docker compose --project-name ukmesh-full --env-file /dev/null \
  -f "$repo/docker-compose.yml" -f "$repo/scripts/full-dev/compose.yml")

# Bind-mounted files keep their old inode after atomic replacement. Their
# content revisions make Compose recreate only the affected consumers.
refresh_wiring_revisions() {
  local name variable revision suffix
  for name in prometheus hopreach nginx.app nginx.website; do
    case "$name" in
      prometheus) variable=FULL_DEV_PROMETHEUS_REVISION; suffix=yml ;;
      hopreach) variable=FULL_DEV_HOPREACH_REVISION; suffix=yaml ;;
      nginx.app) variable=FULL_DEV_APP_PROXY_REVISION; suffix=conf ;;
      nginx.website) variable=FULL_DEV_WEBSITE_PROXY_REVISION; suffix=conf ;;
    esac
    revision=unprepared
    if [[ -f $repo/plans/lean-runtime/runtime/$name.$suffix ]]; then
      read -r revision _ < <(sha256sum "$repo/plans/lean-runtime/runtime/$name.$suffix")
    fi
    export "$variable=$revision"
  done
}
refresh_wiring_revisions
