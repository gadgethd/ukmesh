#!/usr/bin/env bash
set -euo pipefail
umask 077

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
project_dir="$(cd -- "${script_dir}/.." && pwd)"
project_name="${COMPOSE_PROJECT_NAME:-meshcore-analytics}"
output_dir="${BACKUP_OUTPUT_DIR:?BACKUP_OUTPUT_DIR must name an encrypted backup target}"
encryption_cert="${BACKUP_ENCRYPTION_CERT:?BACKUP_ENCRYPTION_CERT must name a public X.509 certificate}"
signing_key="${BACKUP_RECEIPT_SIGNING_KEY:?BACKUP_RECEIPT_SIGNING_KEY must name the offline receipt signing key}"
allow_local="${BACKUP_ALLOW_LOCAL_STAGING:-false}"
minimum_free_bytes="${BACKUP_MIN_FREE_BYTES:-10737418240}"
db_user="${POSTGRES_USER:-meshcore}"
analytics_db="${POSTGRES_DB:-meshcore}"
owner_db="${OWNER_POSTGRES_DB:-meshcore_owner_auth}"
busybox_image="busybox@sha256:9532d8c39891ca2ecde4d30d7710e01fb739c87a8b9299685c63704296b16028"

for command in docker openssl jq tar sha256sum stat findmnt df find sort split; do
  command -v "$command" >/dev/null || {
    echo "required command is unavailable: $command" >&2
    exit 69
  }
done
for path in "$encryption_cert" "$signing_key"; do
  test -r "$path" || {
    echo "required key material is not readable: $path" >&2
    exit 66
  }
done
openssl x509 -in "$encryption_cert" -noout >/dev/null
openssl pkey -in "$signing_key" -noout >/dev/null

mkdir -p "$output_dir"
output_dir="$(cd "$output_dir" && pwd)"
project_device="$(findmnt -T "$project_dir" -n -o SOURCE)"
output_device="$(findmnt -T "$output_dir" -n -o SOURCE)"
if [ "$allow_local" != "true" ]; then
  test -f "$output_dir/.meshcore-offsite-target" || {
    echo "backup target is not attested as off-host (.meshcore-offsite-target missing)" >&2
    exit 65
  }
  if [ "$project_device" = "$output_device" ]; then
    echo "backup target shares the application filesystem; use a mounted off-host target" >&2
    exit 65
  fi
fi
free_bytes="$(df -PB1 "$output_dir" | awk 'NR==2 {print $4}')"
if [ "$free_bytes" -lt "$minimum_free_bytes" ]; then
  echo "backup target has insufficient free space" >&2
  exit 75
fi

cd "$project_dir"
if [ ! -f "$project_dir/.env" ] || [ -L "$project_dir/.env" ]; then
  echo "protected runtime configuration .env is missing or unsafe" >&2
  exit 66
fi
env_mode="$(stat -c '%a' "$project_dir/.env")"
if (( (8#$env_mode & 8#077) != 0 )); then
  echo "protected runtime configuration .env must not be group/world accessible" >&2
  exit 66
fi
docker compose --project-name "$project_name" config -q
for service in timescaledb redis mosquitto; do
  container_id="$(docker compose --project-name "$project_name" ps -q "$service")"
  test -n "$container_id" || {
    echo "required service is not running: $service" >&2
    exit 69
  }
  if [ "$(docker inspect "$container_id" --format '{{.State.Status}}')" != "running" ]; then
    echo "required service is not running: $service" >&2
    exit 69
  fi
done

started_at="$(date -u +%Y-%m-%dT%H:%M:%S.000Z)"
backup_id="backup-$(date -u +%Y%m%dT%H%M%SZ)-$(git rev-parse --short=12 HEAD)"
source_revision="$(git rev-parse HEAD)"
tmp_dir="$(mktemp -d)"
payload_dir="$tmp_dir/payload"
mkdir -p "$payload_dir"
archive_partial=""
archive_path=""
receipt_partial=""
receipt_path=""
signature_partial=""
backup_complete="false"
cleanup() {
  rm -rf -- "$tmp_dir"
  if [ "$backup_complete" != "true" ]; then
    for incomplete_path in \
      "$archive_partial" \
      "$archive_path" \
      "$receipt_partial" \
      "$receipt_path" \
      "$signature_partial" \
      "${receipt_path:+${receipt_path}.sig}"; do
      if [ -n "$incomplete_path" ]; then
        rm -f -- "$incomplete_path"
      fi
    done
  fi
}
trap cleanup EXIT

echo "Creating consistent PostgreSQL custom-format dumps..."
docker compose --project-name "$project_name" exec -T timescaledb \
  pg_dump -U "$db_user" -d "$analytics_db" \
  --format=custom --compress=6 --no-owner --no-acl \
  >"$payload_dir/analytics.dump"
docker compose --project-name "$project_name" exec -T timescaledb \
  pg_dump -U "$db_user" -d "$owner_db" \
  --format=custom --compress=6 --no-owner --no-acl \
  >"$payload_dir/owner-auth.dump"

schema_version="$(
  docker compose --project-name "$project_name" exec -T timescaledb \
    psql -U "$db_user" -d "$analytics_db" -Atc \
    "SELECT COALESCE(MAX(((regexp_match(name, '^([0-9]+)_'))[1])::int), 0) FROM schema_migrations"
)"
schema_version="${schema_version//$'\r'/}"

echo "Checkpointing Redis durable queues and state..."
docker compose --project-name "$project_name" exec -T redis sh -c \
  'redis-cli -a "$REDIS_PASSWORD" --no-auth-warning BGSAVE >/dev/null || true'
redis_ready="false"
for _ in $(seq 1 120); do
  persistence="$(
    docker compose --project-name "$project_name" exec -T redis sh -c \
      'redis-cli -a "$REDIS_PASSWORD" --no-auth-warning INFO persistence'
  )"
  if printf '%s' "$persistence" | tr -d '\r' | grep -q '^rdb_bgsave_in_progress:0$' \
    && printf '%s' "$persistence" | tr -d '\r' | grep -q '^rdb_last_bgsave_status:ok$'; then
    redis_ready="true"
    break
  fi
  sleep 0.5
done
test "$redis_ready" = "true" || {
  echo "Redis persistence checkpoint did not complete successfully" >&2
  exit 75
}
docker compose --project-name "$project_name" exec -T redis \
  tar -C /data -czf - . >"$payload_dir/redis-state.tgz"

echo "Flushing and capturing Mosquitto credentials, ACL, and persistence..."
docker compose --project-name "$project_name" exec -T mosquitto sh -c \
  'kill -USR1 1; sleep 1; tar -C /mosquitto/data -czf - .' \
  >"$payload_dir/mosquitto-state.tgz"
tar -C "$project_dir" -czf "$payload_dir/mosquitto-config.tgz" \
  mosquitto/mosquitto.conf mosquitto/acl mosquitto/passwd
tar -C "$project_dir" -czf "$payload_dir/configuration.tgz" \
  .env .env.example .trivyignore.yaml \
  docker-compose.yml \
  Dockerfile Dockerfile.app Dockerfile.backend Dockerfile.website \
  Dockerfile.mesh-health-check Dockerfile.mosquitto-reloader \
  viewshed-worker/Dockerfile ml-path-learner/Dockerfile \
  nginx.app.conf nginx.website.conf nginx.security-headers.conf \
  anubis/botPolicy.yaml docker/mosquitto-reloader.py \
  logging \
  backend/src/db/migrations backend/src/db/schema backend/src/db/owner-auth.sql \
  scripts/backup.sh scripts/restore-drill.sh scripts/replace-container.sh \
  scripts/bootstrap-mosquitto.sh vacuum-compressed-chunks.sh

completed_at="$(date -u +%Y-%m-%dT%H:%M:%S.000Z)"
manifest_path="$payload_dir/manifest.json"
manifest_files='{}'
while IFS= read -r -d '' payload_file; do
  payload_name="${payload_file##*/}"
  payload_sha256="$(sha256sum "$payload_file" | awk '{print $1}')"
  payload_bytes="$(stat -c '%s' "$payload_file")"
  manifest_files="$(
    jq \
      --arg name "$payload_name" \
      --arg sha256 "$payload_sha256" \
      --argjson bytes "$payload_bytes" \
      '. + {($name): {sha256: $sha256, bytes: $bytes}}' \
      <<<"$manifest_files"
  )"
done < <(
  find "$payload_dir" -maxdepth 1 -type f ! -name manifest.json -print0 \
    | sort -z
)
jq -n \
  --arg backup_id "$backup_id" \
  --arg source_revision "$source_revision" \
  --arg started_at "$started_at" \
  --arg completed_at "$completed_at" \
  --argjson schema_version "$schema_version" \
  --argjson files "$manifest_files" \
  '{
    format: "meshcore-backup-manifest-v1",
    backup_id: $backup_id,
    source_revision: $source_revision,
    started_at: $started_at,
    completed_at: $completed_at,
    schema_version: $schema_version,
    datasets: ["analytics", "owner_auth", "mosquitto", "redis", "configuration"],
    files: $files
  }' >"$manifest_path"

archive_partial="$output_dir/${backup_id}.tar.gz.cms.partial"
archive_path="$output_dir/${backup_id}.tar.gz.cms"
encrypted_parts_dir="$tmp_dir/encrypted-parts"
mkdir -p "$encrypted_parts_dir"
export MESHCORE_BACKUP_ENCRYPTION_CERT="$encryption_cert"
tar -C "$payload_dir" -czf - . \
  | split \
      --bytes=134217728 \
      --numeric-suffixes=0 \
      --suffix-length=4 \
      --filter='openssl cms -encrypt -binary -aes-256-cbc -in - -outform DER -out "${FILE}.cms" "$MESHCORE_BACKUP_ENCRYPTION_CERT"' \
      - "$encrypted_parts_dir/part-"
unset MESHCORE_BACKUP_ENCRYPTION_CERT

printf 'MESHCORE-CMS-CHUNKS-v1\n' >"$archive_partial"
encrypted_part_count=0
for encrypted_part in "$encrypted_parts_dir"/part-*.cms; do
  test -f "$encrypted_part" || {
    echo "backup encryption did not produce any chunks" >&2
    exit 75
  }
  encrypted_part_bytes="$(stat -c '%s' "$encrypted_part")"
  printf '%020d\n' "$encrypted_part_bytes" >>"$archive_partial"
  cat "$encrypted_part" >>"$archive_partial"
  rm -f -- "$encrypted_part"
  encrypted_part_count="$((encrypted_part_count + 1))"
done
test "$encrypted_part_count" -gt 0
printf '%020d\n' 0 >>"$archive_partial"
archive_sha256="$(sha256sum "$archive_partial" | awk '{print $1}')"
archive_bytes="$(stat -c '%s' "$archive_partial")"

receipt_path="$output_dir/${backup_id}.receipt.json"
receipt_partial="${receipt_path}.partial"
jq -n \
  --arg backup_id "$backup_id" \
  --arg source_revision "$source_revision" \
  --arg started_at "$started_at" \
  --arg completed_at "$completed_at" \
  --arg archive "$(basename "$archive_path")" \
  --arg archive_sha256 "$archive_sha256" \
  --argjson archive_bytes "$archive_bytes" \
  --argjson schema_version "$schema_version" \
  '{
    format: "meshcore-backup-receipt-v1",
    backup_id: $backup_id,
    source_revision: $source_revision,
    started_at: $started_at,
    completed_at: $completed_at,
    schema_version: $schema_version,
    archive: $archive,
    archive_sha256: $archive_sha256,
    archive_bytes: $archive_bytes,
    datasets: ["analytics", "owner_auth", "mosquitto", "redis", "configuration"],
    encryption: "CMS-AES-256-CBC-CHUNKED-v1",
    status: "complete"
  }' >"$receipt_partial"
signature_partial="${receipt_path}.sig.partial"
openssl dgst -sha256 -sign "$signing_key" \
  -out "$signature_partial" "$receipt_partial"

mv "$archive_partial" "$archive_path"
mv "$receipt_partial" "$receipt_path"
mv "$signature_partial" "${receipt_path}.sig"
sync "$archive_path" "$receipt_path" "${receipt_path}.sig"
backup_complete="true"

echo "Encrypted backup complete:"
jq -n \
  --arg backup_id "$backup_id" \
  --arg archive "$archive_path" \
  --arg receipt "$receipt_path" \
  --argjson bytes "$archive_bytes" \
  '{backup_id: $backup_id, archive: $archive, receipt: $receipt, encrypted_bytes: $bytes}'
