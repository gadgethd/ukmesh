#!/usr/bin/env bash
set -euo pipefail

ROOT=/home/ben/ukmesh/meshcore-analytics
OUT=/home/ben/ukmesh/backups/p2-10-check-$(date -u +%Y%m%dT%H%M%SZ)
mkdir -p "$OUT"
cd "$ROOT/backend"

fail() {
  printf 'FAILED|%s|%s\n' "$1" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >"$OUT/FAILED"
  echo "p2-10 check failed: $1" >&2
  exit 1
}

DATABASE_URL=$(
  docker inspect meshcore-analytics-backend-1 \
    --format '{{range .Config.Env}}{{println .}}{{end}}' \
    | grep '^DATABASE_URL=' \
    | cut -d= -f2- \
    | sed 's/@timescaledb:5432/@127.0.0.1:5432/'
)
export DATABASE_URL

if ! node --import tsx src/tools/compareRecentReaders.ts >"$OUT/route-diff.json" 2>&1; then
  fail 'route differential returned a non-zero status'
fi

docker exec meshcore-infra-timescaledb-1 psql -U meshcore -d meshcore -Atc "
  select 'uid_coverage|' || count(*) || '|' || count(*) filter (where packet_uid is not null) || '|' || count(*) filter (where packet_uid is null)
    from mesh.packets where time > now() - interval '24 hours';
  select 'coverage_age_h|' || coalesce(extract(epoch from (now() - min(time))) / 3600, 0)::numeric(10,1)
    from mesh.packets where time > now() - interval '24 hours';
  select 'covered_hours|' || count(distinct date_trunc('hour', time))
    from mesh.packets where time > now() - interval '24 hours';
  select 'recent_missing_uid|' || count(*)
    from mesh.packets where time > now() - interval '24 hours' and packet_uid is null;
  select 'owner_acl|' || desired_generation || '|' || rendered_generation || '|' || applied_generation || '|' || last_verified_at
    from core.owner_acl_state;
  select 'owner_accounts|' || count(*) from core.owner_accounts;
  select 'owner_nodes|' || count(*) from core.owner_account_nodes;
" >"$OUT/coverage.txt" 2>&1

missing_uid=$(awk -F'|' '$1 == "recent_missing_uid" { print $2 }' "$OUT/coverage.txt")
covered_hours=$(awk -F'|' '$1 == "covered_hours" { print $2 }' "$OUT/coverage.txt")
if [[ -z "$missing_uid" || "$missing_uid" -ne 0 ]]; then
  fail "24h rows still missing packet_uid: ${missing_uid:-unknown}"
fi
if [[ -z "$covered_hours" || "$covered_hours" -lt 24 ]]; then
  fail "24h coverage has only ${covered_hours:-unknown} covered hours"
fi

docker inspect meshcore-analytics-backend-1 \
  --format '{{.Config.Image}}|{{.State.Status}}|{{.State.StartedAt}}' >"$OUT/backend-state.txt" 2>&1
if ! curl -fsS -m 10 http://127.0.0.1:3000/readyz >"$OUT/readyz.json" 2>&1; then
  fail 'readyz probe failed'
fi
if ! grep -q '"status":"ready"' "$OUT/readyz.json"; then
  fail 'readyz did not report ready'
fi
if ! curl -fsS -m 10 http://127.0.0.1:3000/api/health >"$OUT/health.json" 2>&1; then
  fail 'health probe failed'
fi
if ! grep -q '"status":"healthy"' "$OUT/health.json"; then
  fail 'health did not report healthy'
fi

printf 'completed|%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >"$OUT/COMPLETE"
echo "p2-10 check passed: $OUT"
