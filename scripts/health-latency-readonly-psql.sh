#!/usr/bin/env bash
# Read-only evidence queries on stdin; never migrations. No credentials emitted.
set -euo pipefail
docker exec -i meshcore-infra-timescaledb-1 sh -c '
  PGOPTIONS="-c default_transaction_read_only=on -c statement_timeout=30000 -c application_name=health-latency-audit" \
    psql -X -v ON_ERROR_STOP=1 -P pager=off -U "$POSTGRES_USER" -d "$POSTGRES_DB"
'
