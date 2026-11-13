#!/usr/bin/env bash
# This profile belongs only to the isolated lookup fixtures.
LOOKUP_BASELINE_REF=5c780e66ae2f079f15cf0007c4979dabd22d0d50
LOOKUP_DB_CONTAINER=ukmesh-lookup-db
LOOKUP_REDIS_CONTAINER=ukmesh-lookup-redis
LOOKUP_MQTT_CONTAINER=ukmesh-lookup-mqtt
LOOKUP_DB_PORT=5434
LOOKUP_REDIS_PORT=6381
LOOKUP_MQTT_TCP_PORT=1885
LOOKUP_MQTT_WS_PORT=9003
LOOKUP_HTTP_PORT=3101
LOOKUP_METRICS_PORT=9093
LOOKUP_REGION_METRICS_PORT=9094
LOOKUP_DATABASE_BASE="postgresql://meshcore:meshcore@127.0.0.1:$LOOKUP_DB_PORT"
LOOKUP_REDIS_URL="redis://127.0.0.1:$LOOKUP_REDIS_PORT"
LOOKUP_BROKER_URL="ws://127.0.0.1:$LOOKUP_MQTT_WS_PORT"
LOOKUP_HTTP_URL="http://127.0.0.1:$LOOKUP_HTTP_PORT"
LOOKUP_METRICS_URL="http://127.0.0.1:$LOOKUP_METRICS_PORT/metrics"

lookup_database_name() { [[ $1 =~ ^lookup_(1x|10x|soak|ingest_baseline|ingest_candidate)$ ]]; }
lookup_database_url() { lookup_database_name "$1" && printf '%s/%s\n' "$LOOKUP_DATABASE_BASE" "$1"; }
