# RAM-first MQTT ingestion (experimental)

RAM mode is opt-in. Durable mode remains the default and acknowledges QoS 1
deliveries after PostgreSQL persistence. RAM mode acknowledges after a bounded
Redis Stream admission; the writer commits each observation later. A Redis loss
before PostgreSQL commit can therefore lose already acknowledged telemetry.
Keep the Redis queue private, persistent (AOF), and on a bounded
`noeviction` instance. Do not expose the raw stream to public API credentials;
only the backend and a separate quarantine operator should have raw stream
access. Public companions use the credentialed filtered HTTP source below.

The pending live overlay applies to recent packet lists and the initial
WebSocket packet/message lists. Packet detail and long channel history remain
PostgreSQL views and become available after commit; they do not advertise
pending entries. Recent packets use a five-minute window, while the initial
message list uses 24 hours. The overlay reads at most the configured 10,000
stream entries per request. SQL also returns aggregates for captured RAM
hashes, and the merged distinct-hash page is limited after representative
selection. A larger legacy stream must be drained before
turning on RAM mode.

## Public companion authorization

Set `MQTT_RAM_PUBLIC_READ_TOKEN` to a random secret of at least 32 characters;
Compose passes it to the backend. The endpoint is disabled when unset. Give
the companion this token, not a Redis credential. Restrict the endpoint's
network reachability to the companion:

- `GET /internal/ingest/public/policy` returns the current completed public
  policy generation, or 503 while authority is unavailable or rematerializing.
- `GET /internal/ingest/public/entries?after=0-0&limit=500` returns currently
  public observations with stable IDs and a `nextCursor`. The limit is 1–500.
  Send `Authorization: Bearer <token>` to both endpoints. Private observations
  advance the opaque stream cursor without exposing their payloads.

Authorization uses a fresh policy snapshot after each captured read. REST
merges reject SQL aggregates from an older generation. Initial WebSocket
caches carry a generation and recheck it before delivery. Live MQTT and
WebSocket delivery use one authority query per bounded batch, with explicit
overflow/unavailable counters in `meshcore_public_live_outcomes_total`.
This contract authorizes a captured delivery batch at that final check;
already delivered data and network bytes in flight cannot be recalled.

The stream is a bounded live overlap. Pair it with PostgreSQL's committed-order
ledger and coverage fence for completeness across retention or cursor loss.
The repaired CoreScope public mode additionally requires an empty dedicated
SQLite measurement database, an ingestor-owned provenance journal and a server
revocation fence. Its ingestor rebuilds copied and derived measurements on a
generation change; the read-only server returns 503 until that generation is
complete and matches current authority. Preserve an old untracked database
and backfill a new dedicated database rather than purging mixed-source data.

## Enable and monitor

Set `MQTT_INGEST_MODE=ram` in the Compose `.env`. The standard
`docker-compose.yml` passes through `MQTT_RAM_BATCH_SIZE`,
`MQTT_RAM_MAX_AGE_MS`, `MQTT_RAM_QUEUE_MAX_PACKETS`,
`MQTT_RAM_QUEUE_MAX_BYTES`, `MQTT_RAM_STREAM_KEY`,
`MQTT_RAM_CONSUMER_GROUP`, and `MQTT_RAM_CONSUMER_NAME`. Check the rendered
configuration before a maintenance change:

```sh
export COMPOSE_FILE=docker-compose.yml:docker-compose.live.yml
docker compose config --format json | jq '.services.backend.environment | with_entries(select(.key | (startswith("MQTT_RAM_") and . != "MQTT_RAM_PUBLIC_READ_TOKEN") or . == "MQTT_INGEST_MODE"))'
```

Keep the deployment's established Compose file selection. The example uses
the split-host live overlay; the base file alone does not define its external
data-plane services. CI renders the base plus `docker-compose.ci.yml` as its
self-contained test stack. The drain commands below use the same selected
deployment files. Neither render command changes a running service.

Use a canary or isolated fixture first. Monitor `/readyz`,
`ram_pending_depth`, `ram_retained_depth`, `ram_pel_depth`, the oldest pending
age, admission refusals, commit failures, PostgreSQL disk/WAL and Redis
memory/AOF. A full queue refuses admission and must leave MQTT QoS 1 delivery
unacknowledged. A PostgreSQL outage makes the public RAM merge unavailable
when current privacy authority cannot be established. Queue age is a healthy
writer flush trigger, not a completion guarantee during an outage.

The queue has a hard count bound and conservative byte reservation, including
retained entries and ambiguous admission delivery records. RAM startup and
each new admission require a finite Redis `maxmemory`, `noeviction`, and at
least **32 MiB** beyond the configured queue budget. Admission also checks
current `used_memory` atomically with reservation, leaving that 32 MiB free.
Unsafe startup is retried before MQTT admission starts; unsafe new admission
reports `redis_headroom` and receives no PUBACK. A replay returns its original
stored identity even if capacity is now unavailable. Quarantine is exactly
capped at 500 records and 16 KiB per encoded record, including JSON escaping;
oversized raw values retain their byte length and SHA-256 instead of content.
The separate headroom covers quarantine, PEL, commit metadata and allocator
slack. Application reservations are not a measurement of process RSS, AOF
buffers, other keys or the container's physical memory. Size the container and
storage above those additional costs and measure them in the completed window.
Do not use Redis eviction to make room for acknowledged packets.

For a dedicated stream principal, the required Redis permissions are
`+@connection +@stream +@string +@hash +del +eval +evalsha +info +memory`,
restricted to the configured stream and its `:*` bookkeeping keys. Add only
the normal backend's separately required queue/cache permissions if sharing
its existing Redis instance. The quarantine operator needs `XRANGE`, `XLEN`,
`DEL` and `MEMORY` on `<stream>:quarantine`, without access to the raw stream
or delivery ledger. Disable the default user or remove its unauthenticated
access; keep credentials in private ACL files. The public CoreScope companion
uses only the filtered HTTP token and its restricted PostgreSQL reader, with
no raw Redis credential. These principals are exercised against real Redis
by `npm run test:ram-operational`; the test owns its container and credentials.

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
schema or run migrations. Apply migrations 058 and 059 through the normal
release process before enabling RAM mode or a paired CoreScope reader.
Migration 059 takes the packet writer lock, assigns missing historical UUIDs
(including compressed packets), and backfills retained history into the
committed-order ledger. Its coverage fence commits atomically with the
backfill. Schedule this one-time maintenance transaction for the retained
history size; ingestion retries while the lock is held. It does not change
packet, path or statistics totals. CoreScope requires the coverage fence;
an empty or lost sequence cursor replays all retained ledger history with
stable-ID deduplication. History already removed by retention cannot be
recovered by either source.

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
