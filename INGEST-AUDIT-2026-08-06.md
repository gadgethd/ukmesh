# MQTT ingest audit — 2026-08-06

## Result

The backend had a real lossy failure path: MQTT messages were accepted into the
ingest workers, but a TimescaleDB connection/recovery error rejected the packet
batch and the MQTT handler swallowed the error. There was no retry, durable
handoff, or MQTT persistent session. This can explain packets lost during the
database/container instability around the VPS cutover.

The permanent backend fix deployed on this host is:

- transient packet-batch database errors retry for a bounded ~8-second window;
- retry attempts use an idempotent `new_rows` CTE, so a lost database response
  cannot duplicate packets or rollups;
- the normal path keeps the original fast SQL and does not run the idempotency
  lookup on every batch;
- the MQTT client now uses the stable client ID
  `meshcore-analytics-ingest`, `clean=false`, and QoS 1 subscriptions;
- bounded ingest outcome metrics now identify invalid JSON, empty packets,
  duplicate packets, persistence success/failure, and status failures.

The live deploy used backend image
`sha256:7e085b1286d5159be24d12492aa8603baf995a86d33c5ac76b612d1d7fbfb733`,
pinned in `.env`. Only the backend was recreated. Mosquitto and all other
services were left running.

## Full ingest path

```text
Mosquitto listeners 1883/9001
  -> backend MQTT.js WebSocket client
  -> subscriptions meshcore/#, ukmesh/#, meshcore-test/#
  -> bounded in-process queue (concurrency 8, max 1000, max payload 64 KiB)
  -> topic validation and JSON parsing
  -> status persistence OR packet decode/metadata extraction
  -> per-observer duplicate filter (hash + observer + hop count, 120 s)
  -> insertPacket()
  -> packetBatch (50 rows or 50 ms)
  -> one PostgreSQL CTE statement:
       packets, nodes, network sightings, daily/hourly rollups,
       region packet sightings, region observer sightings
```

Relevant implementation files:

- `backend/src/mqtt/client.ts` owns connection/reconnect, subscription, payload
  limits, queueing, topic handling, decoding, dedupe, and the `insertPacket`
  handoff.
- `backend/src/mqtt/topic.ts` accepts exactly four topic segments, a configured
  prefix, an alphanumeric IATA, a 64-hex observer key, and only `packets` or
  `status` as the suffix. `TST` is blocked.
- `backend/src/db/index.ts` normalizes the packet and calls
  `enqueuePacket()`; it does not insert directly.
- `backend/src/db/packetBatch.ts` writes batches atomically. There is no unique
  constraint on `packets.packet_hash`; rows represent observer receptions.
- `DISTINCT ON(packet_hash)` occurs in `backfillHistoricalLinks()` as a
  read/backfill selection. It is not part of the live packet insert.

## Drop and reject paths

Before the fix, the following could remove a broker message from the ingest
result without replay:

1. MQTT disconnects/reconnects with the default random client ID and clean
   session; the old subscription was QoS 0.
2. Payload/topic over the configured bounds.
3. Queue full or process draining.
4. Invalid topic shape/prefix/suffix, invalid JSON, status identity mismatch,
   empty packet envelope, or missing packet type.
5. The deliberate in-memory duplicate filter: the same final hash at the same
   observer and hop count within 120 seconds.
6. Any `insertPacket()`/packet-batch database error. The old catch logged
   `[mqtt] db insert failed` and returned; the batch rejected all its promises.

Decoder failures generally fall back to envelope fields. Impossible multibyte
path metadata is cleared rather than dropping the packet row. The packet batch
has no `ON CONFLICT` protection, so blindly retrying it would have been unsafe;
the deployed retry path first uses the original fast SQL, then switches to an
idempotent existence check only after a retryable database failure.

## Historical counts and observer evidence

The raw public `packets` counts at audit time were:

| UTC day | rows |
|---|---:|
| 2026-07-30 | 89,140 (the supplied snapshot was 87,895) |
| 2026-07-31 | 110,305 |
| 2026-08-01 | 139,684 |
| 2026-08-02 | 131,330 |
| 2026-08-03 | 154,764 |
| 2026-08-04 | 121,540 |
| 2026-08-05 | 100,154 |
| 2026-08-06 | 6,906 at approximately 01:42Z (partial) |

The fall starts before the migration cutover. Per-IATA rows show the same
pattern, not an isolated database-only change:

| day | MME | NCL | EMA | LTN | LBA | BHX | BOH | NWI |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Aug 3 | 67,614 | 25,640 | 14,410 | 15,681 | 11,908 | 9,588 | 8,345 | 1,578 |
| Aug 4 | 44,730 | 18,392 | 14,613 | 14,240 | 11,076 | 9,376 | 7,848 | 1,265 |
| Aug 5 | 35,752 | 14,795 | 11,319 | 11,818 | 9,473 | 9,722 | 6,882 | 393 |

Notable observer changes include:

- MME observer `65821B...A104F8`: 11,978 rows on Aug 3, only 393 on Aug 4,
  then zero on Aug 5/6.
- NCL observer `E1C1F5...923D4A`: 8,121 on Aug 3, 3,761 on Aug 4, 2,120
  on Aug 5.
- NCL observer `CFECDA...215B7`: 5,586 on Aug 2, absent on Aug 3/4, then
  1,267 late on Aug 5.
- NWI fell from 1,578 on Aug 3 to 1,265 on Aug 4 and 393 on Aug 5.

The migration cutover itself does not show a packet cliff. Raw rows around the
reported 22:46Z cutover were:

| window | rows | approximate rate |
|---|---:|---:|
| Aug 4 18:00–22:46 | 21,225 | 4,452/hour |
| Aug 5 18:00–22:46 | 21,787 | 4,571/hour |
| Aug 5 22:46–00:00 | 5,855 | 4,747/hour |

The Aug 4 decline is therefore upstream/source-observer traffic loss and/or
feed availability, beginning before the VPS cutover. The cutover and database
instability added a separate backend loss risk.

## Dedupe and `DISTINCT ON` quantification

The live table contains observer reception rows, while the in-memory filter
deliberately removes relay copies for the same observer within 120 seconds.
Historical raw rows versus distinct values were:

| day | raw rows | distinct packet hashes | distinct hash + observer |
|---|---:|---:|---:|
| Jul 30 | 89,140 | 11,083 | 49,639 |
| Jul 31 | 110,305 | 12,076 | 60,738 |
| Aug 1 | 139,684 | 14,162 | 78,289 |
| Aug 2 | 131,330 | 12,907 | 75,627 |
| Aug 3 | 154,764 | 13,174 | 87,772 |
| Aug 4 | 121,540 | 12,002 | 72,940 |
| Aug 5 | 100,154 | 11,876 | 61,397 |

This duplicate fraction is broadly stable or slightly lower after Aug 4; the
dedupe rule was introduced in March and is not the cause of the Aug 4 step
down. It does, however, explain why a service counting every MQTT envelope can
report more packets than this table.

## Broker-side and backend-side measurements

Historical `$SYS/broker/messages/received` was not available: the command was
rejected by Mosquitto ACLs because the backend user has no `$SYS/#` permission.
The broker log is connection-only (`log_type error/warning/notice`) and begins
at Aug 5 02:05Z, so it cannot reconstruct Aug 4 message totals. No broker
restart or ACL broadening was performed.

An authenticated, read-only `mosquitto_sub` on the broker was used for live
packet-envelope counts. The final synchronized window was 2026-08-06
02:07:12–02:08:12Z:

- broker packet envelopes: 112;
- backend valid `ukmesh` MQTT messages: 135 (includes status messages);
- packet rows persisted: 46;
- duplicate rejects: 65;
- queue-full/oversize/draining drops: 0;
- packet-batch retries: 0;
- database rows in the same time window: 46.

Per-observer broker envelopes versus database rows:

| IATA | observer prefix | broker | database | dedupe/other |
|---|---|---:|---:|---:|
| MME | `1155AB...` | 2 | 2 | 0 |
| MME | `227744...` | 14 | 4 | 10 |
| LTN | `59A1DD...` | 2 | 2 | 0 |
| EMA | `68DE80...` | 7 | 4 | 3 |
| NCL | `7B3F9F...` | 18 | 5 | 13 |
| MME | `A86A0C...` | 3 | 2 | 1 |
| NCL | `CFECDA...` | 6 | 2 | 4 |
| BOH | `D0B527...` | 4 | 3 | 1 |
| MME | `D7696E...` | 16 | 4 | 12 |
| LBA | `DC9199...` | 14 | 4 | 10 |
| LTN | `E0218F...` | 4 | 3 | 1 |
| NCL | `E1C1F5...` | 12 | 4 | 8 |
| MME | `F51858...` | 4 | 3 | 1 |
| BHX | `FD636A...` | 6 | 4 | 2 |
| **total** |  | **112** | **46** | **66** |

The 66-envelope difference is explained by 65 duplicate outcomes and one
non-persistable packet envelope in the synchronized metric/sample window. The
backend queue was empty at the end of the test, and the persisted delta matched
the table count.

## Database failure evidence

The live TimescaleDB container had `OOMKilled=true`, with its previous process
finishing at 22:34:16Z and the current container starting at 22:38:39Z. This is
strong evidence of a resource event around the migration period, although the
available logs do not prove that event was the sole cause of the Aug 4 decline.

During this audit, a broad historical join used for an initial cutover query
also caused a PostgreSQL server process to be killed at 01:44:14Z. PostgreSQL
automatically recovered and was accepting connections at 01:44:20Z; no
TimescaleDB container restart was performed by this audit. The backend logs
during recovery show the exact old loss path: packet-batch failures and
`[mqtt] db insert failed` messages, with no retry. This incident is recorded
explicitly because it validates the failure mode, but it must not be confused
with evidence that the audit query caused the original Aug 4 decline.

The database was also under sustained write pressure: checkpoints were commonly
8–11 seconds apart before the recovery event, with roughly 0.5 GB WAL distance
between checkpoints. The ingest retry fix protects packets across a transient
restart; the database resource pressure remains an operational item to monitor.

## Topic coverage

The backend subscribes to all three configured prefix wildcards:

```text
meshcore/#
ukmesh/#
meshcore-test/#
```

The live broker sample contained `meshcore` packet/status traffic,
`meshcore-test` status traffic, and a `neighbours` suffix. The latter is
received by the wildcard but intentionally rejected by the strict topic parser;
it is not a packet topic. The repository contains no other backend packet
subscriber. The health-check service’s narrower filter includes packets,
status, and internal topics, but that is not the ingest client.

Therefore no public packet prefix was missing from the backend subscription.
The only historical broker measurement limitation is the lack of `$SYS` ACL and
publish-level broker logging.

## Verification and deployment

Executed:

```text
BACKEND_IMAGE=meshcore-analytics-backend:ingest-fix \
  docker compose -f docker-compose.yml -f docker-compose.live.yml build backend

docker compose -f docker-compose.yml -f docker-compose.live.yml \
  up -d --no-deps --force-recreate backend
```

The image was rebuilt once more after removing the normal-path idempotency
lookup, then pinned in `.env` to the digest listed at the top of this report.

Required endpoint checks passed:

```text
GET http://127.0.0.1:3000/api/health
{"status":"healthy", ... "components":{"ingest":{"status":"ok"}, ...}}

GET http://127.0.0.1:3000/hopreach/api/nodes
HTTP 200
```

Post-deploy ingest metrics showed queue depth 0, no queue-full/oversize/draining
drops, zero packet-batch retries, and 533 successful batches. Cumulative
backend metrics at the final scrape showed 585 persisted packet outcomes and
688 duplicate outcomes; the synchronized test window is the authoritative
per-observer comparison above. Normal packet-batch duration was about 13.2 ms
on average after the corrected deploy.

Focused retry test passed. The production image TypeScript build passed. The
full unit suite had 263/264 passing; the sole failure is the existing
`policyRegistry.test.ts` expectation for the unrelated unregistered
`src/mqtt/channelRegistry.ts#channelCache`. The ingest integration test was
skipped because `TEST_INGEST_DATABASE_URL` is not configured.

No Mosquitto rebuild/restart was needed.
