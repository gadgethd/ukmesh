# RAM-first MQTT ingestion (experimental)

RAM mode is opt-in. Durable mode remains the default and acknowledges QoS 1
deliveries after PostgreSQL persistence. RAM mode acknowledges after a bounded
Redis Stream admission; the writer commits each observation later. A Redis loss
before PostgreSQL commit can therefore lose already acknowledged telemetry.
Keep the Redis instance dedicated to this queue, persistent (AOF), and on a
`noeviction` policy. Do not expose the raw stream to public API credentials;
only backend and the explicitly configured private companion reader should
have stream access.

The pending live overlay applies to recent packet lists and the initial
WebSocket packet/message lists. Packet detail and long channel history remain
PostgreSQL views and become available after commit; they do not advertise
pending entries. Recent packets use a five-minute window, while the initial
message list uses 24 hours. The overlay reads at most the configured 10,000
stream entries per request; a larger legacy stream must be drained before
turning on RAM mode.

## Enable and monitor

Set `MQTT_INGEST_MODE=ram` in the Compose `.env`. The standard
`docker-compose.yml` passes through `MQTT_RAM_BATCH_SIZE`,
`MQTT_RAM_MAX_AGE_MS`, `MQTT_RAM_QUEUE_MAX_PACKETS`,
`MQTT_RAM_QUEUE_MAX_BYTES`, `MQTT_RAM_STREAM_KEY`,
`MQTT_RAM_CONSUMER_GROUP`, and `MQTT_RAM_CONSUMER_NAME`. Check the rendered
configuration before a maintenance change:

```sh
docker compose config --format json | jq '.services.backend.environment | with_entries(select(.key | startswith("MQTT_RAM_") or . == "MQTT_INGEST_MODE"))'
```

Use a canary or isolated fixture first. Monitor `/readyz`,
`ram_pending_depth`, `ram_retained_depth`, `ram_pel_depth`, the oldest pending
age, admission refusals, commit failures, PostgreSQL disk/WAL and Redis
memory/AOF. A full queue refuses admission and must leave MQTT QoS 1 delivery
unacknowledged. A PostgreSQL outage makes the public RAM merge unavailable
when current privacy authority cannot be established. Queue age is a healthy
writer flush trigger, not a completion guarantee during an outage.

The queue has a hard count bound and conservative byte reservation, including
retained entries and ambiguous admission delivery records. Size Redis
`maxmemory` above the queue budget plus AOF, PEL, quarantine and other key
headroom. Do not use Redis eviction to make room for acknowledged packets.

## Drain and return to durable mode

Plan a maintenance window. Stop the **backend** container first so no further
MQTT deliveries can enter the RAM stream; the broker will retain unacknowledged
QoS 1 deliveries for reconnect. Keep PostgreSQL and Redis running. Use the
same image and environment as the backend to run the standalone drain:

```sh
docker compose stop backend
docker compose run --rm --no-deps backend node dist/ram/drain.js
```

The command starts one writer, waits for the stream to become empty after the
120-second committed-entry reader overlap, joins the writer, confirms an empty
PEL and quarantine, and reconciles the byte counter to zero. Review any
quarantined raw evidence before retrying the drain. The default deadline is ten
minutes; set `MQTT_RAM_DRAIN_DEADLINE_MS` between 130000 and 3600000 for a
larger backlog. Exit code 0 and `RAM_DRAIN_COMPLETE` are required before
changing mode. On timeout, PostgreSQL failure or a nonempty PEL, it exits
nonzero and reports `RAM_DRAIN_INCOMPLETE`; keep RAM mode and rerun the drain
after repairing the dependency. The drain intentionally does not initialize
schema or run migrations. Apply migration 058 through the normal release
process before enabling RAM mode.

Once the drain has succeeded, set `MQTT_INGEST_MODE=durable`, check
`docker compose config`, and start the backend with the approved image. Check
`/readyz` and the broker session backlog. Never change the mode while the RAM
stream still contains uncommitted observations: durable mode does not run the
RAM writer. Keep the old Redis data and logs until integrity counts are
verified; the drain deletes each committed stream entry only after retention.

## Verification scope

The CI integration suite exercises real Redis admission, replay, quarantine
and PostgreSQL packet mutation. Before a production rollout, the isolated
bench final-head campaign must also compare stable observation identities,
packet/path/hourly/region counts, PEL/unread reconciliation, privacy flips,
and whole-stack memory, AOF and WAL behavior. RAM mode remains a draft until
that evidence and the companion completeness contract are complete.
