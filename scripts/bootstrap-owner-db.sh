#!/bin/sh
# Create the owner-auth database the backend expects.
#
# The backend derives OWNER_DATABASE_URL from DATABASE_URL by swapping the
# database name (default `meshcore_owner_auth`), and `backend/src/db/owner-auth.sql`
# is the only thing that defines its tables. A fresh CI volume seeds the main
# database from base.sql but never creates this second database, so the backend
# dies at startup with `database "meshcore_owner_auth" does not exist`.
#
# Production creates it out of band (scripts/newuser.sh, restore-drill.sh), so
# this mirrors that step for the CI stack instead of pointing CI at a different
# shape than production.
set -eu

owner_db="${OWNER_POSTGRES_DB:-meshcore_owner_auth}"

exists="$(psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d postgres -tAc \
  "SELECT 1 FROM pg_database WHERE datname = '${owner_db}'")"
if [ "$exists" != "1" ]; then
  psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d postgres \
    -c "CREATE DATABASE \"${owner_db}\""
fi

psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$owner_db" \
  -f /opt/bootstrap/owner-auth.sql
