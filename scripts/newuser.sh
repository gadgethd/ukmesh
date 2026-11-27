#!/usr/bin/env bash

set -Eeuo pipefail
IFS=$'\n\t'

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly REPO_DIR="${NEWUSER_REPO_DIR:-/home/ben/ukmesh/meshcore-analytics}"
if [ -n "${MESHCORE_INFRA_COMPOSE_FILE:-}" ]; then
  infra_compose_file="$MESHCORE_INFRA_COMPOSE_FILE"
  infra_project_dir="${MESHCORE_INFRA_PROJECT_DIR:-${MESHCORE_INFRA_DIR:-$(dirname -- "$infra_compose_file")}}"
else
  infra_project_dir="${MESHCORE_INFRA_PROJECT_DIR:-${MESHCORE_INFRA_DIR:-$(dirname -- "$REPO_DIR")/meshcore-infra}}"
  infra_compose_file="$infra_project_dir/docker-compose.yml"
fi
export MESHCORE_INFRA_PROJECT_DIR="$infra_project_dir"
export MESHCORE_INFRA_COMPOSE_FILE="$infra_compose_file"
source "$SCRIPT_DIR/lib/infra-compose.sh"
readonly MOSQUITTO_CONTAINER="$(resolve_infra_container mosquitto)"
readonly BACKEND_CONTAINER="${NEWUSER_BACKEND_CONTAINER:-meshcore-analytics-backend-1}"
readonly BROKER_URL='wss://mqtt.ukmesh.com:443'
readonly DEFAULT_DISCOVERY_TIMEOUT_SECONDS=900
readonly DISCOVERY_POLL_SECONDS="${NEWUSER_DISCOVERY_POLL_SECONDS:-5}"
readonly ACL_TIMEOUT_SECONDS="${NEWUSER_ACL_TIMEOUT_SECONDS:-90}"

usage() {
  cat <<'EOF'
Usage: newuser [--confirm-owner-grant <NODE_ID>] <username> [key1,key2,...]
       newuser --watch [--confirm-owner-grant <NODE_ID>] <username>
       newuser --link [--confirm-owner-grant <NODE_ID>] <username> [key1,key2,...]

Creates one MQTT/owner-dashboard account. A missing username is prompted for.
Each key must be a 64-character hexadecimal MeshCore public key.

When keys are omitted, the credential is printed immediately and newuser watches
fresh Mosquitto logs for the user's first device contact. The default discovery
timeout is 15 minutes; durations accept seconds or an s, m, or h suffix.

--watch resumes discovery for an existing credential-only user.
--link adds one or more nodes to an existing owner without changing its password.
When link keys are omitted, the next node observed for that user is added.

Owner-portal authority is never created automatically. Without an explicit
--confirm-owner-grant <NODE_ID>, newuser prints the detected fingerprint (the
64-character public key), writes no owner grant and exits 0. Pass the flag — once
per node — with the key(s) being granted. Approved grants are recorded and
wait for verified ACL readback from the running service. The confirmed key
also allows --watch to resume provisioning without waiting for discovery.
EOF
}

die() {
  printf 'newuser: ERROR: %s\n' "$*" >&2
  exit 1
}

log() {
  printf 'newuser: %s\n' "$*" >&2
}

trim() {
  local value=$1
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "$value"
}

# Record one explicit operator confirmation. The value must be a full MeshCore
# public key; it is the only thing that can authorise the owner-grant write.
add_confirm_owner_grant() {
  local raw=$1
  local key
  key="$(trim "$raw")"
  key=${key^^}
  [[ $key =~ ^[0-9A-F]{64}$ ]] \
    || die "invalid --confirm-owner-grant public key: ${raw}"
  if [[ -z ${confirmed_keys[$key]+set} ]]; then
    confirmed_keys[$key]=1
    confirm_owner_grants+=("$key")
  fi
}

print_owner_grant_fingerprint() {
  local key
  printf '\nDetected node fingerprint (MeshCore public key):\n' >&2
  for key in "$@"; do
    printf '  %s\n' "$key" >&2
  done
  printf 'No owner grant was written. Broker write scope is not owner-portal authority.\n' >&2
}

# The confirmed set must equal the set of nodes about to be granted: every key
# confirmed, nothing confirmed that is not being written.
assert_owner_grants_confirmed() {
  local -a required=("$@")
  local -A required_set=()
  local key
  for key in "${required[@]}"; do
    required_set[$key]=1
  done
  (( ${#confirm_owner_grants[@]} == ${#required[@]} )) \
    || die "--confirm-owner-grant mismatch: expected ${#required[@]} key(s), got ${#confirm_owner_grants[@]}"
  for key in "${confirm_owner_grants[@]}"; do
    [[ -n ${required_set[$key]+set} ]] \
      || die "--confirm-owner-grant mismatch: ${key} is not a node being granted"
  done
}

join_keys() {
  local separator=$1
  shift
  local IFS=$separator
  printf '%s' "$*"
}

duration_seconds() {
  local duration=${1,,}
  local amount
  local multiplier
  if [[ $duration =~ ^([1-9][0-9]*)([smh]?)$ ]]; then
    amount=${BASH_REMATCH[1]}
    case ${BASH_REMATCH[2]} in
      ''|s) multiplier=1 ;;
      m) multiplier=60 ;;
      h) multiplier=3600 ;;
    esac
    printf '%s' "$(( amount * multiplier ))"
    return 0
  fi
  return 1
}

# Read Mosquitto log lines from stdin and print the first public key attributable
# to mqtt_username. A denied topic is trusted only after that exact client ID was
# observed connecting with the requested username in the same fresh log window.
extract_node_key_from_logs() {
  local mqtt_username=$1
  awk -v username="$mqtt_username" '
    function is_public_key(value) {
      return length(value) == 64 && value !~ /[^0-9A-Fa-f]/
    }
    function connected_client(line, marker, remainder, end_at) {
      marker = " as "
      if ((start_at = index(line, marker)) == 0) return ""
      remainder = substr(line, start_at + length(marker))
      if ((end_at = index(remainder, " (")) == 0) return ""
      return substr(remainder, 1, end_at - 1)
    }
    function key_from_client(client, remainder, candidate, suffix) {
      if (index(client, "meshcore_") == 1) {
        remainder = substr(client, 10)
        candidate = substr(remainder, 1, 64)
        suffix = substr(remainder, 65)
        if (is_public_key(candidate) && suffix ~ /^_[0-9]+$/) {
          return toupper(candidate)
        }
      }
      if (index(client, "auto-") == 1) {
        candidate = substr(client, 6)
        if (is_public_key(candidate)) return toupper(candidate)
      }
      return ""
    }
    BEGIN {
      quote = sprintf("%c", 39)
      user_marker = "u" quote username quote
    }
    found_key != "" { next }
    index($0, "New client connected") {
      client = connected_client($0)
      if (client == "") next
      options_at = index($0, " (")
      if (options_at == 0 || !index(substr($0, options_at), user_marker)) {
        delete user_clients[client]
        next
      }
      user_clients[client] = 1
      key = key_from_client(client)
      if (key != "") {
        found_key = key
      }
      next
    }
    index($0, "Denied PUBLISH from ") {
      denied_marker = "Denied PUBLISH from "
      remainder = substr($0, index($0, denied_marker) + length(denied_marker))
      end_at = index(remainder, " (")
      if (end_at == 0) next
      client = substr(remainder, 1, end_at - 1)
      if (!(client in user_clients)) next

      quoted_count = split($0, quoted, quote)
      for (i = 2; i <= quoted_count; i += 2) {
        part_count = split(quoted[i], topic, "/")
        if (part_count < 4) continue
        if (topic[1] != "meshcore" && topic[1] != "meshcore-test" && topic[1] != "ukmesh") continue
        if (is_public_key(topic[3])) {
          found_key = toupper(topic[3])
          next
        }
      }
    }
    END {
      if (found_key != "") print found_key
    }
  '
}

read_discovery_logs() {
  local since_timestamp=$1
  local log_file=${2:-}
  local line
  if [[ -n $log_file ]]; then
    [[ -r $log_file ]] || return 1
    while IFS= read -r line || [[ -n $line ]]; do
      printf '%s\n' "$line"
    done <"$log_file"
    return 0
  fi
  docker logs "$MOSQUITTO_CONTAINER" --since "$since_timestamp" 2>&1
}

discover_node_key() {
  local mqtt_username=$1
  local since_timestamp=$2
  local timeout_seconds=$3
  local log_file=${4:-}
  local deadline=$(( $(date +%s) + timeout_seconds ))
  local logs
  local key
  local warned=0

  while :; do
    if logs="$(read_discovery_logs "$since_timestamp" "$log_file")"; then
      warned=0
      key="$(extract_node_key_from_logs "$mqtt_username" <<<"$logs")"
      if [[ -n $key ]]; then
        printf '%s' "$key"
        return 0
      fi
    elif (( ! warned )); then
      log 'could not read Mosquitto logs; discovery will retry'
      warned=1
    fi

    (( $(date +%s) < deadline )) || return 1
    sleep "$DISCOVERY_POLL_SECONDS"
  done
}

print_mctomqtt_settings() {
  local mqtt_username=$1
  local mqtt_password=$2
  cat <<EOF

MQTT credential created — configure mctomqtt now
Server hostname/IP: mqtt.ukmesh.com
Port: 443
Use WebSockets transport?: y
Use TLS/SSL encryption?: y
Verify TLS certificates?: y
Authentication method: username/password
Username: ${mqtt_username}
Password: ${mqtt_password}

For built-in MQTT firmware, set IATA to your location code and Topic root to:
  meshcore/{IATA}/{PUBLIC_KEY}/packets
Keep the braces; the firmware expands them. The default root "meshcore" is
denied by this broker even when login succeeds. Apply and read back the settings.
EOF
}

run_owner_grants() {
  local action=$1
  shift
  docker exec "$BACKEND_CONTAINER" node dist/tools/confirmOwnerGrants.js \
    "$action" "$provisioning_mode" "$username" "$actor" "$@"
}

wait_for_owner_grants() {
  local end=$((SECONDS + ACL_TIMEOUT_SECONDS))
  local result node ready
  while (( SECONDS < end )); do
    if run_owner_grants verify "${keys[@]}"; then
      ready=1
      for node in "${keys[@]}"; do
        if ! acl_has_grant "$username" "$node"; then ready=0; break; fi
      done
      if (( ready )); then return 0; fi
    else
      result=$?
      if (( result != 75 )); then return "$result"; fi
    fi
    sleep 1
  done
  return 1
}

acl_has_grant() {
  local mqtt_username=$1
  local node_id=$2
  docker exec "$MOSQUITTO_CONTAINER" awk -v username="$mqtt_username" -v node_id="$node_id" '
    $1 == "user" {
      active = ($2 == username)
      next
    }
    active && $1 == "topic" && $2 == "write" { seen[$3] = 1 }
    END {
      suffixes[1] = "packets"
      suffixes[2] = "status"
      suffixes[3] = "neighbors"
      suffixes[4] = "neighbours"
      for (i = 1; i <= 4; i++) {
        required = "meshcore/+" "/" node_id "/" suffixes[i]
        if (!seen[required]) exit 1
      }
    }
  ' /mosquitto/config/acl
}

verify_broker_login() {
  local mqtt_username=$1
  local mqtt_password=$2
  docker exec \
    -e "NEWUSER_MQTT_USERNAME=${mqtt_username}" \
    -e "NEWUSER_MQTT_PASSWORD=${mqtt_password}" \
    "$BACKEND_CONTAINER" \
    node --input-type=module -e '
      import mqtt from "mqtt";
      const finish = (code) => {
        clearTimeout(timer);
        client.removeAllListeners();
        client.end(true, {}, () => process.exit(code));
      };
      const client = mqtt.connect("ws://mosquitto:9001", {
        username: process.env.NEWUSER_MQTT_USERNAME,
        password: process.env.NEWUSER_MQTT_PASSWORD,
        reconnectPeriod: 0,
        connectTimeout: 5000,
        clean: true,
        clientId: `newuser-check-${process.pid}`,
      });
      const timer = setTimeout(() => finish(1), 6000);
      client.once("connect", () => finish(0));
      client.once("error", () => finish(1));
      client.once("close", () => finish(1));
    ' >/dev/null
}

wait_for_broker_login() {
  local attempt
  for attempt in 1 2 3; do
    if verify_broker_login "$1" "$2"; then return 0; fi
    if (( attempt < 3 )); then sleep 1; fi
  done
  return 1
}

main() {
watch_mode=0
link_mode=0
timeout_value=$DEFAULT_DISCOVERY_TIMEOUT_SECONDS
declare -a confirm_owner_grants=()
declare -A confirmed_keys=()
while (( $# > 0 )); do
  case $1 in
    --confirm-owner-grant)
      (( $# >= 2 )) || die '--confirm-owner-grant requires a 64-character node public key'
      add_confirm_owner_grant "$2"
      shift 2
      ;;
    --confirm-owner-grant=*)
      add_confirm_owner_grant "${1#*=}"
      shift
      ;;
    --watch)
      (( ! link_mode )) || die '--watch and --link are mutually exclusive'
      watch_mode=1
      shift
      ;;
    --link)
      (( ! watch_mode )) || die '--watch and --link are mutually exclusive'
      link_mode=1
      shift
      ;;
    --timeout)
      (( $# >= 2 )) || die '--timeout requires a duration'
      timeout_value=$2
      shift 2
      ;;
    --timeout=*)
      timeout_value=${1#*=}
      shift
      ;;
    -h|--help)
      usage
      return 0
      ;;
    --)
      shift
      break
      ;;
    -*)
      usage >&2
      die "unknown option: $1"
      ;;
    *)
      break
      ;;
  esac
done

if (( watch_mode )); then
  (( $# == 1 )) || { usage >&2; return 2; }
elif (( link_mode )); then
  (( $# >= 1 && $# <= 2 )) || { usage >&2; return 2; }
else
  (( $# <= 2 )) || { usage >&2; return 2; }
fi

timeout_seconds="$(duration_seconds "$timeout_value")" \
  || die "invalid discovery timeout: ${timeout_value}"
[[ $DISCOVERY_POLL_SECONDS =~ ^[0-9]+([.][0-9]+)?$ ]] \
  || die 'NEWUSER_DISCOVERY_POLL_SECONDS must be a non-negative number'

username=${1:-}
raw_key_list=${2:-}
if [[ -z $username ]]; then
  read -r -p 'MQTT username: ' username || die 'unable to read username'
fi

username="$(trim "$username")"
[[ $username =~ ^[A-Za-z0-9_.@-]{1,128}$ ]] \
  || die 'username must match [A-Za-z0-9_.@-] and be 1-128 characters long'

declare -a keys=()
declare -A seen_keys=()
if [[ -n $raw_key_list ]]; then
  IFS=',' read -r -a raw_keys <<<"$raw_key_list"
  for raw_key in "${raw_keys[@]}"; do
    key="$(trim "$raw_key")"
    key=${key^^}
    [[ $key =~ ^[0-9A-F]{64}$ ]] \
      || die "invalid MeshCore public key: ${raw_key}"
    if [[ -z ${seen_keys[$key]+set} ]]; then
      seen_keys[$key]=1
      keys+=("$key")
    fi
  done
fi
# An explicit --confirm-owner-grant is itself an explicit key assertion: it lets
# an operator grant a credential-only or --watch user without replaying broker
# log discovery.
if (( ${#keys[@]} == 0 && ${#confirm_owner_grants[@]} > 0 )); then
  keys=("${confirm_owner_grants[@]}")
  for key in "${keys[@]}"; do seen_keys[$key]=1; done
fi
discovery_mode=0
if (( ${#keys[@]} == 0 )); then
  discovery_mode=1
fi
existing_credential_mode=0
if (( watch_mode || link_mode )); then
  existing_credential_mode=1
fi

[[ $ACL_TIMEOUT_SECONDS =~ ^[1-9][0-9]{0,2}$ ]] && (( ACL_TIMEOUT_SECONDS <= 300 )) \
  || die 'NEWUSER_ACL_TIMEOUT_SECONDS must be in 1-300'

for command_name in docker openssl awk flock id date sleep; do
  command -v "$command_name" >/dev/null || die "required command not found: ${command_name}"
done
[[ -d $REPO_DIR ]] || die "canonical stack not found at ${REPO_DIR}"

for container in "$MOSQUITTO_CONTAINER" "$BACKEND_CONTAINER"; do
  [[ $(docker inspect -f '{{.State.Running}}' "$container" 2>/dev/null || true) == true ]] \
    || die "required container is not running: ${container}"
done

provisioning_mode=create
if (( watch_mode )); then provisioning_mode=watch; fi
if (( link_mode )); then provisioning_mode=link; fi
actor="newuser:$(id -un)"

# Serialize broker credential mutations. The transaction also coordinates with
# grant writers outside this provisioning command.
exec 9>>"${TMPDIR:-/tmp}/meshcore-analytics-newuser.lock"
flock -x 9
run_owner_grants inspect >/dev/null

passwd_exists=0
if docker exec "$MOSQUITTO_CONTAINER" sh -c '
    wanted=$1
    while IFS=: read -r name ignored; do
      [ "$name" = "$wanted" ] && exit 0
    done < /mosquitto/config/passwd
    exit 1
  ' sh "$username" >/dev/null; then
  passwd_exists=1
else
  passwd_check_status=$?
  (( passwd_check_status == 1 )) \
    || die 'could not inspect Mosquitto passwd'
fi
if (( existing_credential_mode )); then
  (( passwd_exists )) || die "username does not exist in Mosquitto passwd: ${username}"
else
  (( ! passwd_exists )) || die "username already exists in Mosquitto passwd: ${username}"
fi

if (( ! existing_credential_mode )); then
  if docker exec "$MOSQUITTO_CONTAINER" awk -v username="$username" '
      $1 == "user" && $2 == username { found = 1 }
      END { exit found ? 0 : 1 }
    ' /mosquitto/config/acl >/dev/null; then
    die "username already exists in Mosquitto ACL: ${username}"
  else
    acl_check_status=$?
    (( acl_check_status == 1 )) \
      || die 'could not inspect Mosquitto ACL'
  fi
fi

credential_created=0
grant_recording_started=0
grant_recorded=0
rollback_done=0
rollback() {
  local status=${1:-1}
  (( rollback_done )) && exit "$status"
  rollback_done=1
  trap - ERR INT TERM EXIT
  set +e
  if (( grant_recording_started )); then
    if (( grant_recorded )); then
      log 'Owner grant recorded; verification is pending. Retry the same approval after checking the broker ACL.'
    else
      log 'Owner grant record could not be confirmed. Check the database before retrying the same approval.'
    fi
    if (( credential_created )); then
      print_mctomqtt_settings "$username" "$password"
    fi
    log 'The broker credential remains available for a retry.'
    if (( ! existing_credential_mode )); then
      printf 'Retry for this existing credential using newuser --watch with the same --confirm-owner-grant flags.\n' >&2
    fi
  elif (( credential_created )); then
    printf 'newuser: provisioning failed; rolling back credential for %s\n' "$username" >&2
    docker exec --user 1883:1883 "$MOSQUITTO_CONTAINER" \
      mosquitto_passwd -D /mosquitto/config/passwd "$username" >/dev/null 2>&1 \
      || printf 'newuser: WARNING: failed to remove Mosquitto credential\n' >&2
    docker exec "$MOSQUITTO_CONTAINER" chmod 640 /mosquitto/config/passwd >/dev/null 2>&1 || true
    docker exec "$MOSQUITTO_CONTAINER" sh -c 'kill -HUP 1' >/dev/null 2>&1 || true
  fi
  exit "$status"
}
# EXIT trap guarantees rollback even when die() exits explicitly (ERR does not
# fire on `exit 1`). rollback is idempotent and no-ops before any mutation flag.
trap 'rollback $?' ERR
trap 'rollback 130' INT
trap 'rollback 143' TERM
trap 'rollback 1' EXIT

# Grant gate 1 — a brand-new user with explicit keys. Nothing has been mutated
# yet, so an unconfirmed run creates no credential or owner grant.
# The existing-credential modes check approvals after link filtering.
if (( ! discovery_mode && ! existing_credential_mode )); then
  if (( ${#confirm_owner_grants[@]} == 0 )); then
    print_owner_grant_fingerprint "${keys[@]}"
    printf 'Grant it with:\n  newuser --confirm-owner-grant %s %s %s\n' \
      "${keys[0]}" "$username" "$(join_keys ',' "${keys[@]}")" >&2
    printf 'Repeat --confirm-owner-grant once per key when granting several nodes.\n' >&2
    trap - ERR INT TERM EXIT
    exit 0
  fi
fi

password=''
discovery_since=''
if (( existing_credential_mode )); then
  if (( discovery_mode )); then
    discovery_since="$(date -u +'%Y-%m-%dT%H:%M:%SZ')"
    if (( link_mode )); then
      log "discovering an additional node for ${username}; reconnect it using the existing credential"
    else
      log "resuming discovery for ${username}; reconnect the device using its existing credential"
    fi
  fi
else
  if (( discovery_mode )); then
    discovery_since="$(date -u +'%Y-%m-%dT%H:%M:%SZ')"
  fi
  password="$(openssl rand -hex 24)"

  log 'creating persistent Mosquitto credential'
  # Mark the transaction before mutating so a partial credential is rolled back.
  credential_created=1
  # Match the broker UID when atomically replacing its password file.
  docker exec --user 1883:1883 "$MOSQUITTO_CONTAINER" \
    mosquitto_passwd -b /mosquitto/config/passwd "$username" "$password"
  docker exec "$MOSQUITTO_CONTAINER" chmod 640 /mosquitto/config/passwd
  # Mosquitto reads password_file during SIGHUP. Reload before probe/discovery.
  docker exec "$MOSQUITTO_CONTAINER" sh -c 'kill -HUP 1' >/dev/null 2>&1 \
    || die 'could not reload the broker credential file'

  if (( discovery_mode )); then
    # Once shown to the operator, this credential must survive timeout or an
    # interrupted/failed finalization so --watch can resume without a reset.
    credential_created=0
    print_mctomqtt_settings "$username" "$password"
  fi
fi

if (( discovery_mode )); then
  log "watching fresh Mosquitto logs for up to ${timeout_seconds}s"
  # The watch is passive (log polling only); holding the lock through it would
  # block every other newuser run for the whole window. Release, then
  # re-acquire before the mutation phase.
  flock -u 9 2>/dev/null || true
  if discovered_key="$(discover_node_key "$username" "$discovery_since" "$timeout_seconds")"; then
    keys=("$discovered_key")
    log "discovered node public key ${discovered_key}"
  else
    discovery_status=$?
    trap - ERR INT TERM EXIT
    if (( watch_mode )); then
      log "discovery timed out; reconnect the device and retry: newuser --watch --timeout ${timeout_value} ${username}"
    elif (( link_mode )); then
      log "discovery timed out; reconnect the additional device and retry: newuser --link --timeout ${timeout_value} ${username}"
    else
      log "discovery timed out; the broker credential remains valid"
      log "resume after reconnecting the device: newuser --watch --timeout ${timeout_value} ${username}"
    fi
    return "$discovery_status"
  fi
fi

flock -x 9  # re-acquire for the mutation phase

# Discovery released the shell lock. Use the latest active grants before
# selecting the keys for explicit approval. Confirmed existing keys are
# retained so a retry can verify an already recorded grant.
current_keys="$(run_owner_grants inspect)"
if (( link_mode )); then
  declare -A owned_keys=()
  while IFS= read -r key; do
    if [[ $key =~ ^[0-9A-F]{64}$ ]]; then owned_keys[$key]=1; fi
  done <<<"$current_keys"
  declare -a approved_keys=()
  for key in "${keys[@]}"; do
    if [[ -z ${owned_keys[$key]+set} || -n ${confirmed_keys[$key]+set} ]]; then
      approved_keys+=("$key")
    fi
  done
  (( ${#approved_keys[@]} > 0 )) || die "all requested nodes are already linked to username: ${username}"
  keys=("${approved_keys[@]}")
fi

# Grant gate 2 — final key set (discovery, --watch and --link). A fingerprint
# from broker log output is candidate evidence only: it must never create a
# grant by itself. Print it, leave the broker credential intact, write nothing
# and exit 0.
if (( ${#confirm_owner_grants[@]} == 0 )); then
  print_owner_grant_fingerprint "${keys[@]}"
  printf 'Grant it by re-running with the fingerprint above:\n' >&2
  if (( link_mode )); then
    printf '  newuser --link --confirm-owner-grant %s %s %s\n' \
      "${keys[0]}" "$username" "$(join_keys ',' "${keys[@]}")" >&2
  else
    printf '  newuser --watch --confirm-owner-grant %s %s\n' "${keys[0]}" "$username" >&2
  fi
  printf 'The broker credential remains valid; no owner grant was written.\n' >&2
  trap - ERR INT TERM EXIT
  exit 0
fi
assert_owner_grants_confirmed "${keys[@]}"

if (( ! existing_credential_mode )); then
  wait_for_broker_login "$username" "$password" \
    || die 'broker rejected the new credentials'
fi

log 'recording confirmed owner grants for the running ACL reconciler'
grant_recording_started=1
run_owner_grants record "${keys[@]}"
grant_recorded=1
flock -u 9
wait_for_owner_grants || die 'could not verify that the running broker applied the confirmed grants'
log 'Verified the confirmed owner rows and broker ACL readback.'
log 'A new node appears on the dashboard after its first valid status or packet publish.'

trap - ERR INT TERM EXIT
credential_created=0

printf '\nProvisioned MQTT owner account\n'
printf 'Broker: %s\n' "$BROKER_URL"
printf 'Username: %s\n' "$username"
if (( existing_credential_mode )); then
  printf 'Password: unchanged (existing credential)\n'
else
  printf 'Password: %s\n' "$password"
fi
printf 'Topic root(s):\n'
for key in "${keys[@]}"; do
  printf '  meshcore/<IATA>/%s/\n' "$key"
done

cat <<'EOF'

Current MeshCore MQTT firmware template (credentials are the values above):
  set mqtt.wifi.ssid <wifi-ssid>
  set mqtt.wifi.pass <wifi-password>
  set mqtt.1.uri wss://mqtt.ukmesh.com:443/
  set mqtt.1.username <username-above>
  set mqtt.1.password <password-above>
  set mqtt.1.iata <IATA>
  set mqtt.1.topic.root meshcore/{IATA}/{PUBLIC_KEY}/packets
  set mqtt.1.retain.status 1
  set mqtt.1.enabled 1

Use the slot number for your UK Mesh broker. Replace <IATA> with your location
code and keep {IATA}/{PUBLIC_KEY} in Topic root. Apply and read back the settings.
The default root "meshcore" is denied even when MQTT login succeeds.
If a full topic is still denied, verify the owner grant for that full public key.
EOF
}

if [[ ${BASH_SOURCE[0]} == "$0" ]]; then
  main "$@"
fi
