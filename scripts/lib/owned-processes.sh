#!/usr/bin/env bash

owned_process_alive() {
  local process_id=$1
  [[ $process_id =~ ^[0-9]+$ ]] && kill -0 "$process_id" 2>/dev/null \
    && [[ $(ps -p "$process_id" -o stat= 2>/dev/null) != Z* ]]
}

owned_process_matches() {
  local process_id=$1 entrypoint=$2 marker=${3:-} argument entry_found=false marker_found=false
  [[ $process_id =~ ^[0-9]+$ && -r /proc/$process_id/cmdline ]] || return 1
  while IFS= read -r -d '' argument; do
    [[ $argument != "$entrypoint" ]] || entry_found=true
    [[ -z $marker || $argument != "$marker" ]] || marker_found=true
  done < "/proc/$process_id/cmdline"
  [[ $entry_found == true && ( -z $marker || $marker_found == true ) ]]
}

owned_wait_until() {
  local attempts=$1 interval=$2 attempt
  shift 2
  for ((attempt=0; attempt<attempts; attempt++)); do
    if "$@"; then return 0; fi
    sleep "$interval"
  done
  return 1
}

owned_process_stopped() { ! owned_process_alive "$1"; }

owned_stop_pid() {
  local process_id=$1 entrypoint=$2 attempts=$3 interval=$4 service=$5 marker=${6:-}
  if owned_process_matches "$process_id" "$entrypoint" "$marker"; then
    kill -TERM "$process_id" 2>/dev/null || true
    if ! owned_wait_until "$attempts" "$interval" owned_process_stopped "$process_id"; then
      echo "$service did not stop; inspect PID $process_id" >&2
      return 1
    fi
  fi
}

owned_stop_process() {
  local pid_file=$1 entrypoint=$2 attempts=$3 interval=$4 service=$5 marker=${6:-} process_id
  [[ -f $pid_file ]] || return 0
  process_id=$(cat "$pid_file")
  owned_stop_pid "$process_id" "$entrypoint" "$attempts" "$interval" "$service" "$marker" || return
  rm -f -- "$pid_file"
}
