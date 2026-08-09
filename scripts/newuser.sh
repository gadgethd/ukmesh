#!/usr/bin/env bash

set -Eeuo pipefail
IFS=$'\n\t'

readonly REPO_DIR='/home/ben/ukmesh/meshcore-analytics'
readonly ENV_FILE="${REPO_DIR}/.env"
readonly MOSQUITTO_CONTAINER='meshcore-analytics-mosquitto-1'
readonly BACKEND_CONTAINER='meshcore-analytics-backend-1'
readonly TIMESCALEDB_CONTAINER='meshcore-analytics-timescaledb-1'
readonly OWNER_DATABASE='meshcore_owner_auth'
readonly POSTGRES_USER='meshcore'
readonly BROKER_URL='wss://mqtt.ukmesh.com:443'
readonly DEFAULT_DISCOVERY_TIMEOUT_SECONDS=900
readonly DISCOVERY_POLL_SECONDS="${NEWUSER_DISCOVERY_POLL_SECONDS:-5}"

usage() {
  cat <<'EOF'
Usage: newuser [--timeout <duration>] <username> [key1,key2,...]
       newuser --watch [--timeout <duration>] <username>

Creates one MQTT/owner-dashboard account. A missing username is prompted for.
Each key must be a 64-character hexadecimal MeshCore public key.

When keys are omitted, the credential is printed immediately and newuser watches
fresh Mosquitto logs for the user's first device contact. The default discovery
timeout is 15 minutes; durations accept seconds or an s, m, or h suffix.

--watch resumes discovery for an existing credential-only user.
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
EOF
}

replace_owner_map() {
  local value=$1
  local temporary
  temporary="$(mktemp "${ENV_FILE}.newuser.XXXXXX")"
  if ! awk -v replacement="OWNER_MQTT_USERNAME_MAP=${value}" '
      BEGIN { matches = 0 }
      /^OWNER_MQTT_USERNAME_MAP=/ {
        print replacement
        matches++
        next
      }
      { print }
      END { if (matches != 1) exit 42 }
    ' "$ENV_FILE" >"$temporary"; then
    rm -f -- "$temporary"
    return 1
  fi
  chmod --reference="$ENV_FILE" "$temporary"
  mv -f -- "$temporary" "$ENV_FILE"
}

delete_owner_account() {
  local mqtt_username=$1
  docker exec -i "$TIMESCALEDB_CONTAINER" \
    psql -X -q -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$OWNER_DATABASE" \
      -v "mqtt_username=${mqtt_username}" >/dev/null <<'SQL'
BEGIN;
DELETE FROM owner_grant_audit WHERE mqtt_username = :'mqtt_username';
DELETE FROM mqtt_node_logins WHERE mqtt_username = :'mqtt_username';
DELETE FROM owner_accounts WHERE mqtt_username = :'mqtt_username';
COMMIT;
SQL
}

restore_keyless_owner_account() {
  local mqtt_username=$1
  local node_ids=$2
  local restore_active=$3
  docker exec -i "$TIMESCALEDB_CONTAINER" \
    psql -X -q -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$OWNER_DATABASE" \
      -v "mqtt_username=${mqtt_username}" \
      -v "node_ids=${node_ids}" \
      -v "restore_active=${restore_active}" >/dev/null <<'SQL'
BEGIN;
DELETE FROM mqtt_node_logins
WHERE mqtt_username = :'mqtt_username'
  AND node_id = ANY(string_to_array(:'node_ids', ','));
DELETE FROM owner_account_nodes
WHERE mqtt_username = :'mqtt_username'
  AND node_id = ANY(string_to_array(:'node_ids', ','));
UPDATE owner_accounts
SET is_active = :'restore_active'::boolean, updated_at = NOW()
WHERE mqtt_username = :'mqtt_username';
COMMIT;
SQL
}

apply_backend_config() {
  (
    cd "$REPO_DIR"
    docker compose \
      -f docker-compose.yml \
      -f docker-compose.live.yml \
      up -d --no-deps backend
  ) >&2
}

wait_for_backend() {
  local state=''
  local attempt
  # Startup refreshes the canonical node-identity view before the healthcheck
  # turns green. On the production dataset that can legitimately take well over
  # one minute, so allow five minutes without weakening the readback checks.
  for attempt in {1..150}; do
    state="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' \
      "$BACKEND_CONTAINER" 2>/dev/null || true)"
    case "$state" in
      healthy|running) return 0 ;;
      exited|dead) return 1 ;;
    esac
    sleep 2
  done
  printf 'newuser: backend did not become healthy (last state: %s)\n' "${state:-missing}" >&2
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

main() {
watch_mode=0
timeout_value=$DEFAULT_DISCOVERY_TIMEOUT_SECONDS
while (( $# > 0 )); do
  case $1 in
    --watch)
      watch_mode=1
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
discovery_mode=0
if (( watch_mode )) || (( ${#keys[@]} == 0 )); then
  discovery_mode=1
fi

for command_name in docker openssl awk flock mktemp chmod mv date sleep; do
  command -v "$command_name" >/dev/null || die "required command not found: ${command_name}"
done
[[ -d $REPO_DIR && -f $ENV_FILE ]] || die "canonical stack not found at ${REPO_DIR}"
[[ -O $ENV_FILE && -w $ENV_FILE ]] || die "${ENV_FILE} must be owned and writable by the current user"

for container in "$MOSQUITTO_CONTAINER" "$BACKEND_CONTAINER" "$TIMESCALEDB_CONTAINER"; do
  [[ $(docker inspect -f '{{.State.Running}}' "$container" 2>/dev/null || true) == true ]] \
    || die "required container is not running: ${container}"
done

# Serialize operators using this script. The lock lives outside the repository,
# so OWNER_MQTT_USERNAME_MAP remains the only repository configuration touched.
exec 9>"${TMPDIR:-/tmp}/meshcore-analytics-newuser.lock"
flock -x 9

mapfile -t owner_map_lines < <(awk '/^OWNER_MQTT_USERNAME_MAP=/{ print }' "$ENV_FILE")
(( ${#owner_map_lines[@]} == 1 )) \
  || die "expected exactly one OWNER_MQTT_USERNAME_MAP line in ${ENV_FILE}"
old_owner_map=${owner_map_lines[0]#OWNER_MQTT_USERNAME_MAP=}

IFS=',' read -r -a existing_entries <<<"$old_owner_map"
for existing_entry in "${existing_entries[@]}"; do
  existing_entry="$(trim "$existing_entry")"
  [[ -z $existing_entry ]] && continue
  [[ $existing_entry == *=* ]] || die 'existing OWNER_MQTT_USERNAME_MAP is malformed'
  existing_username="$(trim "${existing_entry%%=*}")"
  [[ $existing_username != "$username" ]] \
    || die "username already exists in OWNER_MQTT_USERNAME_MAP: ${username}"
done

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
if (( watch_mode )); then
  (( passwd_exists )) || die "username does not exist in Mosquitto passwd: ${username}"
else
  (( ! passwd_exists )) || die "username already exists in Mosquitto passwd: ${username}"
fi

if (( ! watch_mode )); then
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

owner_db_state="$(
  docker exec -i "$TIMESCALEDB_CONTAINER" \
    psql -X -q -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$OWNER_DATABASE" \
      -v "mqtt_username=${username}" -At <<'SQL'
SELECT COALESCE(
  (SELECT CASE WHEN is_active THEN 'active' ELSE 'inactive' END
   FROM owner_accounts WHERE mqtt_username = :'mqtt_username'),
  'missing'
);
SQL
)"
owner_db_preexisting=0
owner_db_restore_active=false
if (( watch_mode )); then
  if [[ $owner_db_state != missing ]]; then
    owner_db_preexisting=1
    [[ $owner_db_state == active ]] && owner_db_restore_active=true
    existing_owner_node_count="$(
      docker exec -i "$TIMESCALEDB_CONTAINER" \
        psql -X -q -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$OWNER_DATABASE" \
          -v "mqtt_username=${username}" -At <<'SQL'
SELECT COUNT(*) FROM owner_account_nodes WHERE mqtt_username = :'mqtt_username';
SQL
    )"
    [[ $existing_owner_node_count == 0 ]] \
      || die "owner database already has node grants for username: ${username}"
  fi
else
  [[ $owner_db_state == missing ]] \
    || die "username already exists in owner database: ${username}"
fi

credential_created=0
env_changed=0
db_changed=0
preserve_credential=0
rollback() {
  local status=${1:-1}
  trap - ERR INT TERM
  set +e
  if (( preserve_credential )); then
    printf 'newuser: provisioning failed; rolling back owner grant for %s (broker credential remains)\n' "$username" >&2
  else
    printf 'newuser: provisioning failed; rolling back %s\n' "$username" >&2
  fi
  if (( env_changed )); then
    replace_owner_map "$old_owner_map" \
      || printf 'newuser: WARNING: failed to restore OWNER_MQTT_USERNAME_MAP\n' >&2
  fi
  if (( db_changed )); then
    if (( owner_db_preexisting )); then
      restore_keyless_owner_account "$username" "$key_csv" "$owner_db_restore_active" \
        || printf 'newuser: WARNING: failed to restore key-less owner database row\n' >&2
    else
      delete_owner_account "$username" \
        || printf 'newuser: WARNING: failed to remove owner database rows\n' >&2
    fi
  fi
  if (( credential_created )); then
    docker exec "$MOSQUITTO_CONTAINER" \
      mosquitto_passwd -D /mosquitto/config/passwd "$username" >/dev/null 2>&1 \
      || printf 'newuser: WARNING: failed to remove Mosquitto credential\n' >&2
  fi
  if (( env_changed )); then
    apply_backend_config \
      || printf 'newuser: WARNING: failed to re-apply backend config after rollback\n' >&2
  fi
  exit "$status"
}
trap 'rollback $?' ERR
trap 'rollback 130' INT
trap 'rollback 143' TERM

password=''
discovery_since=''
if (( watch_mode )); then
  preserve_credential=1
  discovery_since="$(date -u +'%Y-%m-%dT%H:%M:%SZ')"
  log "resuming discovery for ${username}; reconnect the device using its existing credential"
else
  if (( discovery_mode )); then
    discovery_since="$(date -u +'%Y-%m-%dT%H:%M:%SZ')"
  fi
  password="$(openssl rand -hex 24)"

  log 'creating persistent Mosquitto credential'
  docker exec "$MOSQUITTO_CONTAINER" \
    mosquitto_passwd -b /mosquitto/config/passwd "$username" "$password"
  credential_created=1

  if (( discovery_mode )); then
    # Once shown to the operator, this credential must survive timeout or an
    # interrupted/failed finalization so --watch can resume without a reset.
    preserve_credential=1
    credential_created=0
    print_mctomqtt_settings "$username" "$password"
  fi
fi

if (( discovery_mode )); then
  log "watching fresh Mosquitto logs for up to ${timeout_seconds}s"
  if discovered_key="$(discover_node_key "$username" "$discovery_since" "$timeout_seconds")"; then
    keys=("$discovered_key")
    log "discovered node public key ${discovered_key}"
  else
    discovery_status=$?
    trap - ERR INT TERM
    if (( watch_mode )); then
      log "discovery timed out; reconnect the device and retry: newuser --watch --timeout ${timeout_value} ${username}"
    else
      log "discovery timed out; the broker credential remains valid"
      log "resume after reconnecting the device: newuser --watch --timeout ${timeout_value} ${username}"
    fi
    return "$discovery_status"
  fi
fi

key_pipe="$(IFS='|'; printf '%s' "${keys[*]}")"
key_csv="$(IFS=','; printf '%s' "${keys[*]}")"
new_entry="${username}=${key_pipe}"
if [[ -n $old_owner_map ]]; then
  new_owner_map="${old_owner_map},${new_entry}"
else
  new_owner_map=$new_entry
fi

log 'adding deduplicated owner grant to OWNER_MQTT_USERNAME_MAP'
replace_owner_map "$new_owner_map"
env_changed=1

log 'inserting verified owner account and node grant rows'
docker exec -i "$TIMESCALEDB_CONTAINER" \
  psql -X -q -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$OWNER_DATABASE" \
    -v "mqtt_username=${username}" -v "node_ids=${key_csv}" >/dev/null <<'SQL'
BEGIN;
INSERT INTO owner_accounts (mqtt_username, is_active, updated_at)
VALUES (:'mqtt_username', TRUE, NOW())
ON CONFLICT (mqtt_username) DO UPDATE
SET is_active = TRUE, updated_at = NOW();

INSERT INTO owner_account_nodes (
  mqtt_username,
  node_id,
  verification_method,
  verified_at,
  grant_id,
  revoked_at,
  revocation_reason,
  grant_generation,
  updated_at
)
SELECT
  :'mqtt_username',
  node_id,
  'operator-config',
  NOW(),
  md5(random()::text || clock_timestamp()::text || node_id),
  NULL,
  NULL,
  NULL,
  NOW()
FROM unnest(string_to_array(:'node_ids', ',')) AS configured(node_id)
ON CONFLICT (mqtt_username, node_id) DO UPDATE
SET verification_method = 'operator-config',
    verified_at = NOW(),
    grant_id = EXCLUDED.grant_id,
    revoked_at = NULL,
    revocation_reason = NULL,
    updated_at = NOW();
COMMIT;
SQL
db_changed=1

log 'recreating only the backend to reconcile owner grants and reload the ACL'
apply_backend_config
wait_for_backend

verified_grant_count="$(
  docker exec -i "$TIMESCALEDB_CONTAINER" \
    psql -X -q -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$OWNER_DATABASE" \
      -v "mqtt_username=${username}" -v "node_ids=${key_csv}" -At <<'SQL'
SELECT COUNT(*)
FROM owner_account_nodes oan
JOIN owner_accounts oa ON oa.mqtt_username = oan.mqtt_username
WHERE oa.mqtt_username = :'mqtt_username'
  AND oa.is_active = TRUE
  AND oan.node_id = ANY(string_to_array(:'node_ids', ','))
  AND oan.node_id ~ '^[0-9A-F]{64}$'
  AND oan.verified_at IS NOT NULL
  AND oan.verification_method IN ('operator-config', 'operator-database')
  AND oan.revoked_at IS NULL;
SQL
)"
[[ $verified_grant_count == "${#keys[@]}" ]] \
  || die "owner database verification returned ${verified_grant_count}/${#keys[@]} grants"

for key in "${keys[@]}"; do
  acl_has_grant "$username" "$key" \
    || die "ACL readback is incomplete for ${username}=${key}"
done

if (( watch_mode )); then
  log 'existing broker password is unchanged; ACL and database readbacks verified'
else
  # ownerAccess.verifyMqttCredentials() performs a clean MQTT CONNECT over this
  # same internal WebSocket endpoint. Reproduce that check without publishing.
  verify_broker_login "$username" "$password" \
    || die 'broker rejected the new credentials'
fi

dashboard_node_count="$(
  docker exec -i "$TIMESCALEDB_CONTAINER" \
    psql -X -q -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d meshcore \
      -v "node_ids=${key_csv}" -At <<'SQL'
SELECT COUNT(DISTINCT n.node_id)
FROM node_identity_nodes n
WHERE n.node_id IN (
  SELECT meshcore_canonical_node_id(source_node_id)
  FROM unnest(string_to_array(:'node_ids', ',')) AS source(source_node_id)
);
SQL
)"
if [[ $dashboard_node_count == 0 ]]; then
  log 'grant is active; the dashboard will show the node after its first valid status/packet publish'
else
  log "dashboard source contains ${dashboard_node_count} matching node(s)"
fi

trap - ERR INT TERM
credential_created=0
env_changed=0
db_changed=0

printf '\nProvisioned MQTT owner account\n'
printf 'Broker: %s\n' "$BROKER_URL"
printf 'Username: %s\n' "$username"
if (( watch_mode )); then
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
  set mqtt.1.retain.status 1
  set mqtt.1.enabled 1
EOF
for key in "${keys[@]}"; do
  printf '  set mqtt.1.topic.root meshcore/<IATA>/%s/packets\n' "$key"
done
}

if [[ ${BASH_SOURCE[0]} == "$0" ]]; then
  main "$@"
fi
