#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

# Find all compressed packet chunks and VACUUM FULL them.
# Autovacuum skips these because they report 0 live/dead tuples after compression,
# causing index and heap bloat to accumulate over time.

CHUNKS=$(docker compose exec -T timescaledb psql -U meshcore -d meshcore -t -A -c "
  SELECT chunk_schema||'.'||chunk_name
  FROM timescaledb_information.chunks
  WHERE is_compressed = true
    AND hypertable_name = 'packets'
")

if [ -z "$CHUNKS" ]; then
  echo "$(date -u +%FT%TZ) no compressed chunks found"
  exit 0
fi

for chunk in $CHUNKS; do
  echo "$(date -u +%FT%TZ) VACUUM FULL $chunk"
  docker compose exec -T timescaledb psql -U meshcore -d meshcore -c "VACUUM FULL $chunk;"
done

echo "$(date -u +%FT%TZ) done"
