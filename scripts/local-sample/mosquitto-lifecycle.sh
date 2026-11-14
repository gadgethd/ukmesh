#!/bin/sh
# The broker owns its signal endpoint; consumers keep their own PID namespace.
set -eu
fifo=${MOSQUITTO_RELOAD_FIFO:-/mosquitto/control/reload}
directory=${fifo%/*}
owner="$(id -u):$(id -g)"
case "$fifo" in /*) ;; *) echo 'Broker control FIFO must be absolute' >&2; exit 1 ;; esac
[ "$(readlink -f "$directory")" = "$directory" ] &&
  [ "$(stat -c '%u:%g:%a' "$directory")" = "$owner:700" ] || {
    echo 'Broker control directory ownership differs' >&2; exit 1;
  }
if [ ! -e "$fifo" ] && [ ! -L "$fifo" ]; then
  (umask 077; mkfifo "$fifo")
fi
[ ! -L "$fifo" ] && [ -p "$fifo" ] &&
  [ "$(stat -c '%u:%g:%a' "$fifo")" = "$owner:600" ] || {
    echo 'Broker control endpoint must be its owned private FIFO' >&2; exit 1;
  }

# RDWR keeps the FIFO open across individual authenticated writer requests.
exec 3<> "$fifo"
"$@" 3>&- &
broker=$!
interrupted=0
forward() { interrupted=1; kill -"$1" "$broker" 2>/dev/null || :; }
trap 'forward TERM' TERM
trap 'forward INT' INT
(
  trap 'exit 0' TERM INT
  while IFS= read -r command <&3; do
    case "$command" in reload) kill -HUP "$broker" 2>/dev/null || : ;; esac
  done
) &
reader=$!
cleanup() {
  kill -TERM "$reader" 2>/dev/null || :
  wait "$reader" 2>/dev/null || :
  exec 3>&-
}
trap cleanup EXIT
previous=0
while :; do
  interrupted=0
  status=0
  wait "$broker" || status=$?
  # A trapped signal interrupts wait before the broker's own exit is reaped.
  if [ "$interrupted" = 1 ] && [ "$status" -gt 128 ]; then previous=$status; continue; fi
  if [ "$status" = 127 ] && [ "$previous" != 0 ]; then status=$previous; fi
  exit "$status"
done
