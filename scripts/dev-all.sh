#!/usr/bin/env bash
# Serialized entrypoint; each subsystem owns its lifecycle and configuration.
set -euo pipefail
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
if [[ ${UKMESH_COMPANION_ENV_CLEAN:-} != 1 ]]; then
  exec env -i PATH="$PATH" HOME="$HOME" LANG=C.UTF-8 UKMESH_COMPANION_ENV_CLEAN=1 bash "$repo/scripts/dev-all.sh" "$@"
fi
# This launcher manages this VM, regardless of a saved Docker CLI context.
export DOCKER_HOST=unix:///var/run/docker.sock
action=${1:-start}
service=${2:-}
case "$action" in
  help|--help)
    echo 'scripts/dev-all.sh [start|companions|build|status|stop|logs [SERVICE]|restart SERVICE|init|qualify PHASE|hot ukmesh|hot beacon-web]'
    echo 'start runs the complete lean stack. stop retains stores. hot enables editing; start restores built assets.'
    exit 0 ;;
  start|companions|build|status|stop|logs|restart|init|qualify|hot) ;;
  *) echo "Unknown action: $action" >&2; exit 2 ;;
esac
state="$repo/plans/companion-dev"
mkdir -p "$state"
chmod 700 "$state"
if [[ $action != status && $action != logs && ${UKMESH_COMPANION_LOCKED:-} != 1 ]]; then
  if flock --nonblock --close --conflict-exit-code 75 "$state/control.lock" env UKMESH_COMPANION_LOCKED=1 bash "$repo/scripts/dev-all.sh" "$@"; then exit 0; else
    result=$?
    if [[ $result == 75 ]]; then echo 'Another DEV operation is running; let it finish before starting a new one.' >&2; fi
    exit "$result"
  fi
fi
case "$action" in
  init|companions) exec bash "$repo/scripts/companion-dev/control.sh" "$@" ;;
  qualify) shift; exec bash "$repo/scripts/qualify-full-stack.sh" "${@:-all}" ;;
  build|start|status)
    bash "$repo/scripts/full-dev/control.sh" "$action"
    exec bash "$repo/scripts/companion-dev/control.sh" "$action" ;;
  stop)
    bash "$repo/scripts/companion-dev/control.sh" stop
    exec bash "$repo/scripts/full-dev/control.sh" stop ;;
  hot|restart|logs)
    case "$service" in
      corescope|corescope-ingestor|beacon|beacon-db|beacon-redis|beacon-web|beacon-migrate)
        exec bash "$repo/scripts/companion-dev/control.sh" "$@" ;;
      *) exec bash "$repo/scripts/full-dev/control.sh" "$@" ;;
    esac ;;
esac
