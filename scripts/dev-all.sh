#!/usr/bin/env bash
# Serialized entrypoint; each subsystem owns its lifecycle and configuration.
set -euo pipefail
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)

# Acquire the target lease before the independent controller lock. The live
# owner frame and inherited FD are verified on every shell re-entry.
entry_env=(env -i PATH="$PATH" HOME="$HOME" LANG=C.UTF-8)
for entry_key in UKMESH_TARGET_LEASE_FD UKMESH_TARGET_ENTRY; do
  [[ ! -v $entry_key ]] || entry_env+=("$entry_key=${!entry_key}")
done
if "${entry_env[@]}" python3 "$repo/scripts/full-dev/qualification/target_entry.py" verify "$repo/scripts/dev-all.sh" "$@"; then :; else
  entry_status=$?
  [[ $entry_status == 1 ]] || exit "$entry_status"
  exec "${entry_env[@]}" python3 "$repo/scripts/full-dev/qualification/target_entry.py" run "$repo/scripts/dev-all.sh" "$@"
fi
if [[ ${UKMESH_COMPANION_ENV_CLEAN:-} != 1 ]]; then
  exec env -i PATH="$PATH" HOME="$HOME" LANG=C.UTF-8 UKMESH_COMPANION_ENV_CLEAN=1 bash "$repo/scripts/dev-all.sh" "$@"
fi
# This launcher manages this VM, regardless of a saved Docker CLI context.
export DOCKER_HOST=unix:///var/run/docker.sock
action=${1:-start}
service=${2:-}
case "$action" in
  help|--help)
    echo 'scripts/dev-all.sh [start|companions|build|status|stop|logs [SERVICE]|restart SERVICE|init|qualify PHASE|lan start [IP]|lan refresh|lan stop|lan status|hot ukmesh|hot beacon-web|ui ACTION|replay ACTION]'
    echo 'start runs the complete lean stack. stop retains stores. hot enables editing; start restores built assets.'
    exit 0 ;;
  start|companions|build|status|stop|logs|restart|init|qualify|lan|hot|ui|replay) ;;
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
  replay) shift; exec python3 "$repo/scripts/full-dev/replay/control.py" "$@" ;;
  ui)
    source "$repo/scripts/full-dev/runtime.sh"
    source "$repo/scripts/companion-dev/revisions.sh"
    load_companion_revisions
    shift
    exec python3 "$repo/scripts/full-dev/ui/control.py" "$@" ;;
  init|companions) exec bash "$repo/scripts/companion-dev/control.sh" "$@" ;;
  qualify) shift; exec bash "$repo/scripts/qualify-full-stack.sh" "${@:-all}" ;;
  lan)
    control="$repo/scripts/full-dev/lan/control.py"
    refresh_lan_origins() {
      local component
      for component in backend mesh-health-check; do
        bash "$repo/scripts/full-dev/control.sh" restart "$component" || return
      done
    }
    case "$service" in
      start)
        python3 "$control" prepare "${3:-127.0.0.1}"
        if refresh_lan_origins && python3 "$control" up; then exit 0; fi
        if python3 "$control" stop; then refresh_lan_origins; fi
        exit 1 ;;
      stop)
        python3 "$control" stop
        refresh_lan_origins ;;
      refresh|status) exec python3 "$control" "$service" ;;
      *) echo 'lan supports start [IP], refresh, stop or status' >&2; exit 2 ;;
    esac ;;

  start)
    bash "$repo/scripts/full-dev/control.sh" start
    bash "$repo/scripts/companion-dev/control.sh" start
    exec python3 "$repo/scripts/full-dev/replay/control.py" resume ;;
  build|status)
    bash "$repo/scripts/full-dev/control.sh" "$action"
    exec bash "$repo/scripts/companion-dev/control.sh" "$action" ;;
  stop)
    python3 "$repo/scripts/full-dev/replay/control.py" suspend
    bash "$repo/scripts/companion-dev/control.sh" stop
    exec bash "$repo/scripts/full-dev/control.sh" stop ;;
  hot|restart|logs)
    case "$service" in
      corescope|corescope-ingestor|beacon|beacon-db|beacon-redis|beacon-web|beacon-migrate)
        exec bash "$repo/scripts/companion-dev/control.sh" "$@" ;;
      *) exec bash "$repo/scripts/full-dev/control.sh" "$@" ;;
    esac ;;
esac
