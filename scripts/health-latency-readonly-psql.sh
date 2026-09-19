#!/usr/bin/env bash
# Read-only evidence queries on stdin; never migrations. No credentials emitted.
set -euo pipefail
docker exec -i meshcore-infra-timescaledb-1 sh -c '
  PGAPPNAME=health-latency-audit \
  PGOPTIONS="-c default_transaction_read_only=on -c statement_timeout=30000 -c idle_in_transaction_session_timeout=15000" \
    psql -X -v ON_ERROR_STOP=1 -P pager=off -U "$POSTGRES_USER" -d "$POSTGRES_DB"
'
