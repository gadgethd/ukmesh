#!/usr/bin/env bash
set -euo pipefail

ROOT=/home/ben/ukmesh/meshcore-analytics
OUT=/home/ben/ukmesh/backups/p2-10-check-$(date -u +%Y%m%dT%H%M%SZ)
mkdir -p "$OUT"
cd "$ROOT/backend"

DATABASE_URL=$(
  docker inspect meshcore-analytics-backend-1 \
    --format '{{range .Config.Env}}{{println .}}{{end}}' \
    | grep '^DATABASE_URL=' \
    | cut -d= -f2- \
    | sed 's/@timescaledb:5432/@127.0.0.1:5432/'
)
export DATABASE_URL
node --import tsx src/tools/compareRecentReaders.ts >"$OUT/route-diff.json" 2>&1

docker exec meshcore-infra-timescaledb-1 psql -U meshcore -d meshcore -Atc "
  select 'uid_coverage|' || count(*) || '|' || count(*) filter (where packet_uid is not null) || '|' || count(*) filter (where packet_uid is null)
    from mesh.packets where time > now() - interval '24 hours';
  select 'decoded_age_h|' || coalesce(extract(epoch from (now() - min(time))) / 3600, 0)::numeric(10,1)
    from mesh.packets where identity_provenance = 'meshcore-decoded-wire';
  select 'recent_missing_uid|' || count(*)
    from mesh.packets where identity_provenance = 'meshcore-decoded-wire' and packet_uid is null;
  select 'owner_acl|' || desired_generation || '|' || rendered_generation || '|' || applied_generation || '|' || last_verified_at
    from core.owner_acl_state;
  select 'owner_accounts|' || count(*) from core.owner_accounts;
  select 'owner_nodes|' || count(*) from core.owner_account_nodes;
" >"$OUT/coverage.txt" 2>&1

docker inspect meshcore-analytics-backend-1 \
  --format '{{.Config.Image}}|{{.State.Status}}|{{.State.StartedAt}}' >"$OUT/backend-state.txt" 2>&1
curl -sS -m 10 http://127.0.0.1:3000/readyz >"$OUT/readyz.json" 2>&1 || true
curl -sS -m 10 http://127.0.0.1:3000/api/health >"$OUT/health.json" 2>&1 || true
printf 'completed|%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >"$OUT/COMPLETE"
