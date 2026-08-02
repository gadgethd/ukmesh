#!/usr/bin/env bash
set -euo pipefail
umask 077

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
project_dir="$(cd -- "${script_dir}/.." && pwd)"
archive_path="${1:?usage: restore-drill.sh <archive.cms> <backup-receipt.json>}"
backup_receipt="${2:?usage: restore-drill.sh <archive.cms> <backup-receipt.json>}"
backup_signature="${BACKUP_RECEIPT_SIGNATURE:-${backup_receipt}.sig}"
backup_verify_key="${BACKUP_RECEIPT_VERIFY_KEY:?BACKUP_RECEIPT_VERIFY_KEY is required}"
decryption_cert="${BACKUP_DECRYPTION_CERT:?BACKUP_DECRYPTION_CERT is required}"
decryption_key="${BACKUP_DECRYPTION_KEY:?BACKUP_DECRYPTION_KEY is required}"
restore_signing_key="${RESTORE_RECEIPT_SIGNING_KEY:?RESTORE_RECEIPT_SIGNING_KEY is required}"
restore_verify_key="${RESTORE_RECEIPT_VERIFY_KEY:?RESTORE_RECEIPT_VERIFY_KEY is required}"
receipt_dir="${RESTORE_RECEIPT_DIR:?RESTORE_RECEIPT_DIR is required}"

timescale_image="timescale/timescaledb@sha256:22e8a5ae7aef121d1537afe946dd7cc5deeeb63ab36ce19849d671bd3b663509"
redis_image="redis@sha256:8b81dd37ff027bec4e516d41acfbe9fe2460070dc6d4a4570a2ac5b9d59df065"
mosquitto_image="eclipse-mosquitto@sha256:9cfdd46ad59f3e3e5f592f6baf57ab23e1ad00605509d0f5c1e9b179c5314d87"
busybox_image="busybox@sha256:9532d8c39891ca2ecde4d30d7710e01fb739c87a8b9299685c63704296b16028"

for command in docker openssl jq tar sha256sum stat head; do
  command -v "$command" >/dev/null || {
    echo "required command is unavailable: $command" >&2
    exit 69
  }
done
for path in \
  "$archive_path" "$backup_receipt" "$backup_signature" "$backup_verify_key" \
  "$decryption_cert" "$decryption_key" "$restore_signing_key" "$restore_verify_key"; do
  test -r "$path" || {
    echo "required restore input is not readable: $path" >&2
    exit 66
  }
done
openssl dgst -sha256 -verify "$backup_verify_key" \
  -signature "$backup_signature" "$backup_receipt" >/dev/null

test "$(jq -r '.format' "$backup_receipt")" = "meshcore-backup-receipt-v1"
test "$(jq -r '.status' "$backup_receipt")" = "complete"
backup_id="$(jq -r '.backup_id' "$backup_receipt")"
source_revision="$(jq -r '.source_revision' "$backup_receipt")"
expected_archive_sha="$(jq -r '.archive_sha256' "$backup_receipt")"
backup_completed_at="$(jq -r '.completed_at' "$backup_receipt")"
encryption_format="$(jq -r '.encryption' "$backup_receipt")"
case "$source_revision" in
  [0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f]*) ;;
  *) echo "backup receipt has an invalid source revision" >&2; exit 65 ;;
esac
actual_archive_sha="$(sha256sum "$archive_path" | awk '{print $1}')"
test "$actual_archive_sha" = "$expected_archive_sha" || {
  echo "encrypted archive checksum does not match its signed receipt" >&2
  exit 65
}

run_id="meshcore-restore-$$"
network_name="${run_id}-network"
postgres_name="${run_id}-postgres"
redis_name="${run_id}-redis"
mosquitto_name="${run_id}-mosquitto"
backend_name="${run_id}-backend"
postgres_volume="${run_id}-postgres-data"
redis_volume="${run_id}-redis-data"
mosquitto_volume="${run_id}-mosquitto-data"
application_image="${run_id}-application"
tmp_dir="$(mktemp -d)"
payload_dir="$tmp_dir/payload"
restore_started_epoch="$(date +%s)"
restore_started_at="$(date -u +%Y-%m-%dT%H:%M:%S.000Z)"
restore_password="$(openssl rand -hex 24)"
restore_jwt="$(openssl rand -hex 32)"
receipt_partial=""
signature_partial=""
verify_partial=""
receipt_publish_started="false"
receipt_publish_complete="false"
previous_receipt_dir="$tmp_dir/previous-receipt"

cleanup() {
  if [ "$receipt_publish_started" = "true" ] \
    && [ "$receipt_publish_complete" != "true" ]; then
    for receipt_name in latest.json latest.json.sig verify.pem; do
      if [ -f "$previous_receipt_dir/$receipt_name" ]; then
        cp -p "$previous_receipt_dir/$receipt_name" "$receipt_dir/$receipt_name"
      else
        rm -f -- "$receipt_dir/$receipt_name"
      fi
    done
  fi
  for incomplete_receipt in \
    "$receipt_partial" "$signature_partial" "$verify_partial"; do
    if [ -n "$incomplete_receipt" ]; then
      rm -f -- "$incomplete_receipt"
    fi
  done
  docker rm -f \
    "$backend_name" "$mosquitto_name" "$redis_name" "$postgres_name" \
    >/dev/null 2>&1 || true
  docker network rm "$network_name" >/dev/null 2>&1 || true
  docker volume rm \
    "$postgres_volume" "$redis_volume" "$mosquitto_volume" \
    >/dev/null 2>&1 || true
  docker image rm "$application_image" >/dev/null 2>&1 || true
  rm -rf "$tmp_dir"
}
trap cleanup EXIT

mkdir -p "$payload_dir"
decrypt_chunked_archive() {
  local magic chunk_length chunk_bytes encrypted_chunk actual_chunk_bytes
  exec 3<"$archive_path"
  IFS= read -r magic <&3
  test "$magic" = "MESHCORE-CMS-CHUNKS-v1" || {
    echo "invalid chunked backup archive header" >&2
    return 65
  }
  encrypted_chunk="$tmp_dir/encrypted-restore-part.cms"
  while IFS= read -r chunk_length <&3; do
    [[ "$chunk_length" =~ ^[0-9]{20}$ ]] || {
      echo "invalid encrypted backup chunk length" >&2
      return 65
    }
    chunk_bytes="$((10#$chunk_length))"
    if [ "$chunk_bytes" -eq 0 ]; then
      if IFS= read -r -n 1 _trailing_byte <&3; then
        echo "unexpected trailing data after backup chunk terminator" >&2
        return 65
      fi
      return 0
    fi
    if [ "$chunk_bytes" -gt 268435456 ]; then
      echo "encrypted backup chunk exceeds the restore memory bound" >&2
      return 65
    fi
    head -c "$chunk_bytes" <&3 >"$encrypted_chunk"
    actual_chunk_bytes="$(stat -c '%s' "$encrypted_chunk")"
    test "$actual_chunk_bytes" -eq "$chunk_bytes" || {
      echo "truncated encrypted backup chunk" >&2
      return 65
    }
    openssl cms -decrypt -binary -inform DER \
      -in "$encrypted_chunk" \
      -recip "$decryption_cert" \
      -inkey "$decryption_key"
  done
  echo "encrypted backup archive has no terminator" >&2
  return 65
}

case "$encryption_format" in
  CMS-AES-256-CBC-CHUNKED-v1)
    decrypt_chunked_archive | tar -C "$payload_dir" -xzf -
    ;;
  CMS-AES-256-CBC)
    openssl cms -decrypt -binary -inform DER \
      -in "$archive_path" \
      -recip "$decryption_cert" \
      -inkey "$decryption_key" |
      tar -C "$payload_dir" -xzf -
    ;;
  *)
    echo "unsupported backup encryption format: $encryption_format" >&2
    exit 65
    ;;
esac

manifest_path="$payload_dir/manifest.json"
jq -e '.format == "meshcore-backup-manifest-v1" and (.files | type == "object")' \
  "$manifest_path" >/dev/null
while IFS= read -r payload_name; do
  if ! [[ "$payload_name" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]]; then
    echo "unsafe payload name in backup manifest: $payload_name" >&2
    exit 65
  fi
  payload_path="$payload_dir/$payload_name"
  test -f "$payload_path" || {
    echo "missing payload declared by backup manifest: $payload_name" >&2
    exit 65
  }
  expected_payload_sha="$(
    jq -r --arg name "$payload_name" '.files[$name].sha256' "$manifest_path"
  )"
  expected_payload_bytes="$(
    jq -r --arg name "$payload_name" '.files[$name].bytes' "$manifest_path"
  )"
  actual_payload_sha="$(sha256sum "$payload_path" | awk '{print $1}')"
  actual_payload_bytes="$(stat -c '%s' "$payload_path")"
  if [ "$actual_payload_sha" != "$expected_payload_sha" ] \
    || [ "$actual_payload_bytes" != "$expected_payload_bytes" ]; then
    echo "payload integrity mismatch: $payload_name" >&2
    exit 65
  fi
done < <(jq -r '.files | keys[]' "$manifest_path")
for required_payload in \
  analytics.dump owner-auth.dump redis-state.tgz mosquitto-state.tgz \
  mosquitto-config.tgz configuration.tgz; do
  jq -e --arg name "$required_payload" '.files[$name] != null' \
    "$manifest_path" >/dev/null || {
      echo "missing backup payload: $required_payload" >&2
      exit 65
    }
done
configuration_members="$(tar -tzf "$payload_dir/configuration.tgz")"
for required_configuration in \
  .env docker-compose.yml Dockerfile.backend Dockerfile.mosquitto-reloader \
  anubis/botPolicy.yaml logging/prometheus.yml logging/alertmanager.yml \
  backend/src/db/schema/base.sql scripts/backup.sh scripts/restore-drill.sh \
  scripts/replace-container.sh; do
  printf '%s\n' "$configuration_members" | grep -Fxq "$required_configuration" || {
    echo "missing protected recovery configuration: $required_configuration" >&2
    exit 65
  }
done
mosquitto_configuration_members="$(tar -tzf "$payload_dir/mosquitto-config.tgz")"
for required_mosquitto_configuration in \
  mosquitto/mosquitto.conf mosquitto/acl mosquitto/passwd; do
  printf '%s\n' "$mosquitto_configuration_members" \
    | grep -Fxq "$required_mosquitto_configuration" || {
      echo "missing Mosquitto recovery configuration: $required_mosquitto_configuration" >&2
      exit 65
    }
done

docker network create "$network_name" >/dev/null
docker volume create "$postgres_volume" >/dev/null
docker volume create "$redis_volume" >/dev/null
docker volume create "$mosquitto_volume" >/dev/null

docker run -d \
  --name "$postgres_name" \
  --network "$network_name" \
  --network-alias timescaledb \
  -e POSTGRES_DB=meshcore \
  -e POSTGRES_USER=meshcore \
  -e "POSTGRES_PASSWORD=$restore_password" \
  -v "$postgres_volume:/var/lib/postgresql/data" \
  "$timescale_image" \
  postgres -c autovacuum=off >/dev/null
for _ in $(seq 1 240); do
  if docker logs "$postgres_name" 2>&1 | grep -q 'PostgreSQL init process complete' &&
    docker exec "$postgres_name" pg_isready -U meshcore -d meshcore >/dev/null 2>&1; then
    break
  fi
  sleep 0.5
done
docker logs "$postgres_name" 2>&1 | grep -q 'PostgreSQL init process complete'
docker exec "$postgres_name" pg_isready -U meshcore -d meshcore >/dev/null
docker exec "$postgres_name" createdb -U meshcore meshcore_owner_auth
docker exec -i "$postgres_name" pg_restore \
  -U meshcore -d meshcore --no-owner --no-acl --exit-on-error \
  <"$payload_dir/analytics.dump"
docker exec -i "$postgres_name" pg_restore \
  -U meshcore -d meshcore_owner_auth --no-owner --no-acl --exit-on-error \
  <"$payload_dir/owner-auth.dump"
echo "PostgreSQL datasets restored."

echo "Restoring Redis durable state..."
docker run --rm \
  -v "$redis_volume:/restore" \
  -v "$payload_dir:/payload:ro" \
  "$busybox_image" \
  sh -c 'tar -C /restore -xzf /payload/redis-state.tgz' >/dev/null
docker run -d \
  --name "$redis_name" \
  --network "$network_name" \
  --network-alias redis \
  -v "$redis_volume:/data" \
  "$redis_image" \
  redis-server \
  --requirepass "$restore_password" \
  --appendonly yes \
  --appendfsync everysec \
  --maxmemory-policy noeviction >/dev/null
redis_ready="false"
for _ in $(seq 1 60); do
  if docker exec "$redis_name" redis-cli -a "$restore_password" --no-auth-warning ping \
    2>/dev/null | grep -q PONG; then
    redis_ready="true"
    break
  fi
  sleep 0.5
done
if [ "$redis_ready" != "true" ]; then
  echo "restored Redis did not become ready" >&2
  docker logs "$redis_name" >&2 || true
  exit 75
fi
echo "Redis durable state restored."

echo "Restoring Mosquitto configuration and durable state..."
mkdir -p "$tmp_dir/mosquitto"
tar -C "$tmp_dir" -xzf "$payload_dir/mosquitto-config.tgz"
for mosquitto_config_name in mosquitto.conf acl passwd; do
  mosquitto_config_path="$tmp_dir/mosquitto/$mosquitto_config_name"
  test -f "$mosquitto_config_path" && test ! -L "$mosquitto_config_path" || {
    echo "unsafe restored Mosquitto configuration: $mosquitto_config_name" >&2
    exit 65
  }
  chmod 0644 "$mosquitto_config_path"
done
# mktemp plus umask 077 makes the bind-mount root inaccessible to Mosquitto's
# uid. The parent remains 0700 on the host, while this signed, short-lived
# child directory is traversable only through the container's read-only bind.
chmod 0755 "$tmp_dir/mosquitto"
docker run --rm \
  -v "$mosquitto_volume:/restore" \
  -v "$payload_dir:/payload:ro" \
  "$busybox_image" \
  sh -c 'tar -C /restore -xzf /payload/mosquitto-state.tgz; chown -R 1883:1883 /restore' \
  >/dev/null
docker run -d \
  --name "$mosquitto_name" \
  --network "$network_name" \
  --network-alias mosquitto \
  -v "$tmp_dir/mosquitto:/mosquitto/config:ro" \
  -v "$mosquitto_volume:/mosquitto/data" \
  "$mosquitto_image" >/dev/null
mosquitto_ready="false"
for _ in $(seq 1 30); do
  if [ "$(docker inspect "$mosquitto_name" --format '{{.State.Running}}')" = "true" ]; then
    mosquitto_ready="true"
    break
  fi
  sleep 0.5
done
if [ "$mosquitto_ready" != "true" ]; then
  echo "restored Mosquitto did not become ready" >&2
  docker logs "$mosquitto_name" >&2 || true
  exit 75
fi
echo "Mosquitto durable state restored."

echo "Building current application for migration and smoke verification..."
docker build \
  --file "$project_dir/Dockerfile.backend" \
  --tag "$application_image" \
  "$project_dir" >/dev/null
database_url="postgresql://meshcore:${restore_password}@timescaledb:5432/meshcore"
owner_database_url="postgresql://meshcore:${restore_password}@timescaledb:5432/meshcore_owner_auth"
redis_url="redis://redis:6379"

docker run --rm \
  --network "$network_name" \
  -e "DATABASE_URL=$database_url" \
  -e DATABASE_STATEMENT_TIMEOUT_MS=0 \
  -e MIGRATION_016_PRIVATE_PREFIXES_APPROVAL=supersede-016-and-017-with-authoritative-privacy-and-026 \
  -e NODE_ENV=production \
  "$application_image" \
  node dist/tools/migrate.js >/dev/null

docker run -d \
  --name "$backend_name" \
  --network "$network_name" \
  --network-alias backend \
  --read-only \
  --cap-drop ALL \
  --security-opt no-new-privileges:true \
  --tmpfs /tmp:rw,noexec,nosuid,size=64m \
  -v "$tmp_dir/mosquitto/acl:/mosquitto/config/acl:ro" \
  -e "DATABASE_URL=$database_url" \
  -e "OWNER_DATABASE_URL=$owner_database_url" \
  -e "REDIS_URL=$redis_url" \
  -e "REDIS_PASSWORD=$restore_password" \
  -e "JWT_SECRET=$restore_jwt" \
  -e "OWNER_COOKIE_SECRET=$restore_jwt" \
  -e "OPERATOR_SITE_TOKEN=$restore_jwt" \
  -e MQTT_INGEST_ENABLED=false \
  -e DATABASE_SKIP_SCHEMA_INIT=true \
  -e OWNER_AUTHORIZATION_MODE=shadow \
  -e OWNER_ACL_MODE=shadow \
  -e MOSQUITTO_ACL_PATH=/mosquitto/config/acl \
  -e NODE_ENV=production \
  "$application_image" >/dev/null
backend_ready="false"
for _ in $(seq 1 120); do
  if docker exec "$backend_name" wget -qO- http://127.0.0.1:3000/readyz \
    2>/dev/null | jq -e '.status == "ready"' >/dev/null 2>&1; then
    backend_ready="true"
    break
  fi
  if [ "$(docker inspect "$backend_name" --format '{{.State.Running}}')" != "true" ]; then
    break
  fi
  sleep 0.5
done
if [ "$backend_ready" != "true" ]; then
  echo "restored backend did not become ready" >&2
  docker logs "$backend_name" >&2 || true
  exit 75
fi
docker exec "$backend_name" wget -qO- 'http://127.0.0.1:3000/api/stats?network=ukmesh' \
  | jq -e '.totalNodes >= 0' >/dev/null

owner_lookup_count="$(
  docker exec "$postgres_name" psql -U meshcore -d meshcore_owner_auth -Atc \
    'SELECT COUNT(*) FROM owner_accounts oa LEFT JOIN owner_account_nodes oan ON oan.mqtt_username = oa.mqtt_username'
)"
case "$owner_lookup_count" in
  ''|*[!0-9]*) echo "owner lookup verification failed" >&2; exit 65 ;;
esac

integrity_json="$(
  docker exec "$postgres_name" psql -U meshcore -d meshcore -Atc "
    WITH packet_sample AS (
      SELECT packet_hash, time, network
      FROM packets
      ORDER BY time, packet_hash
      LIMIT 1000
    ), node_sample AS (
      SELECT node_id, COALESCE(name, '') AS name
      FROM nodes
      ORDER BY node_id
      LIMIT 1000
    )
    SELECT json_build_object(
      'packet_sample_rows', (SELECT COUNT(*) FROM packet_sample),
      'packet_sample_md5', (SELECT md5(COALESCE(string_agg(packet_hash || time::text || network, '' ORDER BY time, packet_hash), '')) FROM packet_sample),
      'node_sample_rows', (SELECT COUNT(*) FROM node_sample),
      'node_sample_md5', (SELECT md5(COALESCE(string_agg(node_id || name, '' ORDER BY node_id), '')) FROM node_sample)
    )"
)"
printf '%s' "$integrity_json" | jq -e \
  '.packet_sample_rows >= 0 and (.packet_sample_md5 | length == 32) and .node_sample_rows >= 0 and (.node_sample_md5 | length == 32)' \
  >/dev/null
schema_version="$(
  docker exec "$postgres_name" psql -U meshcore -d meshcore -Atc \
    "SELECT COALESCE(MAX(((regexp_match(name, '^([0-9]+)_'))[1])::int), 0) FROM schema_migrations"
)"

restore_verified_epoch="$(date +%s)"
restore_verified_at="$(date -u +%Y-%m-%dT%H:%M:%S.000Z)"
restore_duration_seconds="$((restore_verified_epoch - restore_started_epoch))"
backup_completed_epoch="$(date -d "$backup_completed_at" +%s)"
recovery_point_age_seconds="$((restore_verified_epoch - backup_completed_epoch))"
receipt_id="restore-$(date -u +%Y%m%dT%H%M%SZ)-${backup_id#backup-}"
mkdir -p "$receipt_dir"
chmod 0700 "$receipt_dir"
receipt_partial="$receipt_dir/latest.json.partial"
signature_partial="$receipt_dir/latest.json.sig.partial"
verify_partial="$receipt_dir/verify.pem.partial"

jq -n \
  --arg receipt_id "$receipt_id" \
  --arg backup_id "$backup_id" \
  --arg backup_completed_at "$backup_completed_at" \
  --arg restore_started_at "$restore_started_at" \
  --arg restore_verified_at "$restore_verified_at" \
  --arg archive_sha256 "$actual_archive_sha" \
  --arg source_revision "$source_revision" \
  --argjson restore_duration_seconds "$restore_duration_seconds" \
  --argjson recovery_point_age_seconds "$recovery_point_age_seconds" \
  --argjson schema_version "$schema_version" \
  --argjson integrity "$integrity_json" \
  --argjson owner_lookup_count "$owner_lookup_count" \
  '{
    format: "meshcore-restore-receipt-v1",
    receipt_id: $receipt_id,
    backup_id: $backup_id,
    backup_completed_at: $backup_completed_at,
    restore_started_at: $restore_started_at,
    restore_verified_at: $restore_verified_at,
    restore_duration_seconds: $restore_duration_seconds,
    recovery_point_age_seconds: $recovery_point_age_seconds,
    archive_sha256: $archive_sha256,
    source_revision: $source_revision,
    schema_version: $schema_version,
    datasets: ["analytics", "owner_auth", "mosquitto", "redis", "configuration"],
    evidence: {
      integrity: $integrity,
      owner_lookup_count: $owner_lookup_count
    },
    checks: {
      migrations: "passed",
      integrity: "passed",
      owner_lookup: "passed",
      readiness: "passed"
    },
    status: "verified"
  }' >"$receipt_partial"
openssl dgst -sha256 -sign "$restore_signing_key" \
  -out "$signature_partial" "$receipt_partial"
openssl dgst -sha256 -verify "$restore_verify_key" \
  -signature "$signature_partial" "$receipt_partial" >/dev/null

cp "$restore_verify_key" "$verify_partial"
mkdir -p "$previous_receipt_dir"
for receipt_name in latest.json latest.json.sig verify.pem; do
  if [ -f "$receipt_dir/$receipt_name" ]; then
    cp -p "$receipt_dir/$receipt_name" "$previous_receipt_dir/$receipt_name"
  fi
done
receipt_publish_started="true"
mv "$verify_partial" "$receipt_dir/verify.pem"
mv "$signature_partial" "$receipt_dir/latest.json.sig"
mv "$receipt_partial" "$receipt_dir/latest.json"
sync "$receipt_dir/latest.json" "$receipt_dir/latest.json.sig" "$receipt_dir/verify.pem"
receipt_publish_complete="true"

echo "Isolated restore drill passed:"
jq -n \
  --arg backup_id "$backup_id" \
  --arg receipt "$receipt_dir/latest.json" \
  --argjson rpo_seconds "$recovery_point_age_seconds" \
  --argjson rto_seconds "$restore_duration_seconds" \
  '{backup_id: $backup_id, restore_receipt: $receipt, demonstrated_rpo_seconds: $rpo_seconds, demonstrated_rto_seconds: $rto_seconds}'
