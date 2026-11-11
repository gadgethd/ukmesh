#!/usr/bin/env bash
# Run after building the backend image, with the CI compose environment set.
set -euo pipefail

repo_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
mkdir -p "$repo_dir/node_modules"
scratch="$(mktemp -d "$repo_dir/node_modules/ci-db-readiness.XXXXXX")"
project="meshcore-ci-db-readiness-${GITHUB_RUN_ID:-local}-$$"
compose=(docker compose -f "$repo_dir/docker-compose.yml"
  -f "$repo_dir/docker-compose.ci.yml" -f "$scratch/compose.yml"
  --project-name "$project")

cleanup() {
  local result=$?
  if (( result != 0 )); then
    "${compose[@]}" logs --no-color --tail=100 timescaledb db-migrate || true
  fi
  "${compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
  rm -rf -- "$scratch"
}
trap cleanup EXIT

# Hold the entrypoint's socket-only server open past the first health probe.
# A Unix-socket pg_isready falsely passes here; the migration client uses TCP.
cat >"$scratch/999_ci_readiness.sql" <<'SQL'
SELECT pg_sleep(15);
CREATE TABLE ci_init_completed (id integer);
SQL
cat >"$scratch/compose.yml" <<YAML
services:
  timescaledb:
    volumes:
      - "$scratch/999_ci_readiness.sql:/docker-entrypoint-initdb.d/999_ci_readiness.sql:ro"
    healthcheck:
      start_interval: 1s
YAML

"${compose[@]}" up --detach --no-build --wait --wait-timeout 120 timescaledb db-migrate
migrator="$("${compose[@]}" ps --all --quiet db-migrate)"
test -n "$migrator"
test "$(docker wait "$migrator")" = 0
# The runner records one row per applied file, but a file can be superseded by a
# later one (for example 011 -> 044), so the row count is deliberately lower than
# the file count. The invariant that matters is that the highest-sorted migration
# is recorded, and that a replay finds nothing pending (checked below).
latest="$(find "$repo_dir/backend/src/db/migrations" -maxdepth 1 -name '*.sql' -printf '%f\n' | sort | tail -1)"
test -n "$latest"
rows="$("${compose[@]}" exec -T timescaledb psql \
  -U "${POSTGRES_USER:-meshcore}" -d "${POSTGRES_DB:-meshcore}" -Atc \
  "SELECT count(*) FROM schema_migrations")"
latest_applied="$("${compose[@]}" exec -T timescaledb psql \
  -U "${POSTGRES_USER:-meshcore}" -d "${POSTGRES_DB:-meshcore}" -Atc \
  "SELECT count(*) FROM schema_migrations WHERE name = '$latest'")"
test "$latest_applied" = "1"
printf '[ci-db-readiness] ledger rows=%s head=%s applied=%s\n' "$rows" "$latest" "$latest_applied"
replay="$("${compose[@]}" run --rm --no-deps db-migrate)"
grep -q '\[db-migrate\] no pending migrations' <<<"$replay"
printf '[ci-db-readiness] PASS: delayed init, %s migrations (head %s), idempotent replay\n' "$rows" "$latest"
