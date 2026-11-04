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
expected="$(find "$repo_dir/backend/src/db/migrations" -name '*.sql' | wc -l | tr -d ' ')"
actual="$("${compose[@]}" exec -T timescaledb psql \
  -U "${POSTGRES_USER:-meshcore}" -d "${POSTGRES_DB:-meshcore}" -Atc \
  "SELECT count(*) FROM schema_migrations WHERE to_regclass('ci_init_completed') IS NOT NULL")"
test "$actual" = "$expected"
replay="$("${compose[@]}" run --rm --no-deps db-migrate)"
grep -q '\[db-migrate\] no pending migrations' <<<"$replay"
printf '[ci-db-readiness] PASS: delayed init, %s migrations, idempotent replay\n' "$actual"
