#!/usr/bin/env bash
# Published consumer identities only; source compilation belongs to the builder.
companion_published_source() {
  local value=unknown
  if [[ -f $1 ]]; then value=$(cat -- "$1") || return; fi
  [[ $value =~ ^[a-f0-9]{64}$ ]] || value=unknown
  printf '%s\n' "$value"
}

companion_config_revision() {
  local revision
  revision=$(sha256sum < "$1") || return
  printf '%s\n' "${revision%% *}"
}

load_companion_revisions() {
  local component=${1:-all} core_config=${2:-} tools root state value
  case "$component" in all|corescope) ;; *) echo 'Unknown companion revision component' >&2; return 2 ;; esac
  tools=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd) || return
  root=$(cd -- "$tools/../.." && pwd) || return
  state="$root/plans/companion-dev/runtime"
  value=$(companion_published_source "$state/corescope-source") || return
  export "COMPANION_CORESCOPE_SOURCE=$value"
  value=$(companion_config_revision "${core_config:-$tools/corescope.json}") || return
  export "COMPANION_CORESCOPE_CONFIG=$value"
  [[ $component == all ]] || return 0

  value=$(companion_published_source "$state/beacon-source") || return
  export "COMPANION_BEACON_SOURCE=$value"
  value=$(python3 "$tools/artifacts.py" web-revision) || return
  export "COMPANION_WEB_SOURCE=$value"
  value=$(python3 "$tools/artifacts.py" web-publication) || return
  export "COMPANION_WEB_PUBLICATION=$value"
  value=$(companion_config_revision "$tools/beacon.yaml") || return
  export "COMPANION_BEACON_CONFIG=$value"
  value=$(companion_config_revision "$tools/nginx.conf") || return
  export "COMPANION_WEB_CONFIG=$value"
}

if [[ ${BASH_SOURCE[0]} == "$0" ]]; then
  set -euo pipefail
  revision_component=all
  revision_core_config=
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --component) [[ $# -ge 2 ]] || exit 2; revision_component=$2; shift 2 ;;
      --core-config) [[ $# -ge 2 && -n $2 ]] || exit 2; revision_core_config=$2; shift 2 ;;
      --) shift; break ;;
      *) break ;;
    esac
  done
  [[ $# -gt 0 ]] || { echo 'A companion consumer command is required' >&2; exit 2; }
  load_companion_revisions "$revision_component" "$revision_core_config"
  exec "$@"
fi
