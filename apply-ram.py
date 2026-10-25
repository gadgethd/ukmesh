#!/usr/bin/env python3
"""Apply RAM-first ingestion integration onto the ram-ingest worktree.

Run from the worktree root: python3 apply-ram.py
Every edit is asserted before writing; a failed assert aborts that file only.
"""
import os
import sys

W = os.getcwd()
BE = os.path.join(W, "backend")
SRC = os.path.join(BE, "src")

def read(path):
    with open(path) as f:
        return f.read()

def write(path, content):
    with open(path, "w") as f:
        f.write(content)

def replace_once(s, old, new, label):
    count = s.count(old)
    assert count == 1, f"{label}: expected 1 occurrence, found {count}"
    return s.replace(old, new, 1)

def insert_import(path, text):
    """Insert import lines after the last import statement."""
    s = read(path)
    lines = s.split("\n")
    last = -1
    in_block = False
    for i, line in enumerate(lines[:120]):
        if in_block:
            if line.rstrip().endswith(";"):
                in_block = False
                last = i
            continue
        stripped = line.strip()
        if stripped.startswith("import "):
            if stripped.endswith(";"):
                last = i
            else:
                in_block = True
    assert last >= 0, f"{path}: no import block found"
    lines.insert(last + 1, text)
    write(path, "\n".join(lines))

# ============================================================ packetBatch.ts
p = os.path.join(SRC, "db/packetBatch.ts")
s = read(p)

s = replace_once(
    s,
    "import {\n  PrivatePrefixCache,\n  type PrivatePrefixRow,\n} from '../privacy/privatePrefixCache.js';",
    "import {\n  PrivatePrefixCache,\n  type PacketPrivacyClassification,\n  type PacketPrivacyInput,\n  type PrivatePrefixRow,\n} from '../privacy/privatePrefixCache.js';",
    "PB import",
)

s = replace_once(
    s,
    "function isRetryableDatabaseError(error: unknown): boolean {",
    "export function isRetryableDatabaseError(error: unknown): boolean {",
    "PB export isRetryable",
)

s = replace_once(
    s,
    "function retryDelay(attempt: number): number {",
    "export function retryDelay(attempt: number): number {",
    "PB export retryDelay",
)

s = replace_once(
    s,
    "export type PacketBatchResult = {\n  isPrivate: boolean;\n  visibilityOk: boolean;\n};",
    """export type PacketBatchResult = {
  isPrivate: boolean;
  visibilityOk: boolean;
};

/** One packet for persistEntriesCore: batch input + optional stable observation identity. */
export type PersistEntry = {
  input: PacketBatchInput;
  observationId: string | null;
};

export type PersistRow = {
  row_id: number;
  is_private: boolean;
  visibility_ok: boolean;
  prefix_cache_fresh: boolean;
};""",
    "PB result types",
)

s = replace_once(
    s,
    """export function configurePacketBatch(query: QueryFn): void {
  queryFn = query;
  privatePrefixCache = new PrivatePrefixCache();
  privatePrefixRefresh = null;
}""",
    """export function configurePacketBatch(query: QueryFn): void {
  queryFn = query;
  privatePrefixCache = new PrivatePrefixCache();
  privatePrefixRefresh = null;
}

/**
 * Admission-time classification for RAM-mode ingest: loads the cache once if
 * it has never been populated, then classifies against the shared cache
 * instance. The database re-classifies authoritatively at commit (fail-closed
 * fence), so a stale cache can only ever delay visibility, never leak.
 */
export async function classifyForAdmission(
  input: PacketPrivacyInput,
): Promise<PacketPrivacyClassification> {
  if (privatePrefixCache.currentGeneration < 1 && queryFn) {
    try {
      await refreshPrivatePrefixCache(queryFn);
    } catch (error) {
      console.error('[db] private prefix cache refresh failed', (error as Error).message);
    }
  }
  return privatePrefixCache.classify(input);
}""",
    "PB classifyForAdmission",
)

# --- writeBatch -> persistEntriesCore + wrapper
s = replace_once(
    s,
    """async function writeBatch(batch: PendingPacket[], idempotent: boolean): Promise<void> {
  const query = queryFn;
  if (!query) throw new Error('PACKET_BATCH_NOT_CONFIGURED');

  if (privatePrefixCache.currentGeneration < 1) {""",
    """export async function persistEntriesCore(
  entries: PersistEntry[],
  query: QueryFn,
  idempotent: boolean,
): Promise<PersistRow[]> {
  if (privatePrefixCache.currentGeneration < 1) {""",
    "PB core head",
)

s = replace_once(
    s,
    """  const columnsPerRow = 25;
  const casts = [
    'int', 'timestamptz', 'text', 'text', 'text', 'text', 'text', 'text',
    'int', 'int', 'int', 'double precision', 'double precision', 'text', 'text',
    'text', 'int', 'text[]', 'int', 'text', 'text', 'text', 'bigint', 'boolean', 'boolean',
  ];
  const params: unknown[] = [];
  const values = batch.map(({ packet }, rowIndex) => {
    const offset = rowIndex * columnsPerRow;
    const privacy = privatePrefixCache.classify({
      network: packet.network,
      rxNodeId: packet.rxNodeId,
      srcNodeId: packet.srcNodeId,
      pathHashes: packet.pathHashes,
      pathHashSizeBytes: packet.pathHashSizeBytes,
    });
    params.push(
      rowIndex,
      packet.time,""",
    """  const columnsPerRow = 26;
  const casts = [
    'int', 'uuid', 'timestamptz', 'text', 'text', 'text', 'text', 'text', 'text',
    'int', 'int', 'int', 'double precision', 'double precision', 'text', 'text',
    'text', 'int', 'text[]', 'int', 'text', 'text', 'text', 'bigint', 'boolean', 'boolean',
  ];
  const params: unknown[] = [];
  const values = entries.map(({ input: packet, observationId }, rowIndex) => {
    const offset = rowIndex * columnsPerRow;
    const privacy = privatePrefixCache.classify({
      network: packet.network,
      rxNodeId: packet.rxNodeId,
      srcNodeId: packet.srcNodeId,
      pathHashes: packet.pathHashes,
      pathHashSizeBytes: packet.pathHashSizeBytes,
    });
    params.push(
      rowIndex,
      observationId,
      packet.time,""",
    "PB values head",
)

s = replace_once(
    s,
    """       incoming (
         row_id, time, packet_hash, rx_node_id, src_node_id, topic, topic_prefix,""",
    """       incoming (
         row_id, observation_id, time, packet_hash, rx_node_id, src_node_id, topic, topic_prefix,""",
    "PB incoming cols",
)

s = replace_once(
    s,
    """       classified AS MATERIALIZED (
         SELECT i.*,
           gen_random_uuid() AS observation_id,""",
    """       classified AS MATERIALIZED (
         SELECT i.*,
           COALESCE(i.observation_id, gen_random_uuid()) AS observation_id,""",
    "PB classified obsid",
)

s = replace_once(
    s,
    """  const result = await query<{
    row_id: number;
    is_private: boolean;
    visibility_ok: boolean;
    prefix_cache_fresh: boolean;
  }>(""",
    """  const result = await query<PersistRow>(""",
    "PB result type",
)

s = replace_once(
    s,
    """  if (result.rows.length !== batch.length) throw new Error('PACKET_BATCH_RESULT_MISMATCH');
  if (result.rows.some((row) => !row.prefix_cache_fresh)) {
    void refreshPrivatePrefixCache(query).catch((error: unknown) => {
      console.error('[db] private prefix cache refresh failed', (error as Error).message);
    });
  }
  const settledAt = process.hrtime.bigint();
  for (const row of result.rows) {
    const item = batch[row.row_id];
    if (item) {
      packetBatchQueueWait.observe(Number(settledAt - item.enqueuedNs) / 1e9);
    }
    privacyFilterTotal.inc({
      operation: 'packet_ingest',
      outcome: row.visibility_ok ? 'public' : (row.is_private ? 'private' : 'invalid'),
    });
    batch[row.row_id]?.resolve({
      isPrivate: Boolean(row.is_private),
      visibilityOk: Boolean(row.visibility_ok),
    });
  }
}""",
    """  if (result.rows.length !== entries.length) throw new Error('PACKET_BATCH_RESULT_MISMATCH');
  if (result.rows.some((row) => !row.prefix_cache_fresh)) {
    void refreshPrivatePrefixCache(query).catch((error: unknown) => {
      console.error('[db] private prefix cache refresh failed', (error as Error).message);
    });
  }
  return result.rows;
}

async function writeBatch(batch: PendingPacket[], idempotent: boolean): Promise<void> {
  const query = queryFn;
  if (!query) throw new Error('PACKET_BATCH_NOT_CONFIGURED');
  const rows = await persistEntriesCore(
    batch.map(({ packet }) => ({ input: packet, observationId: null })),
    query,
    idempotent,
  );
  const settledAt = process.hrtime.bigint();
  for (const row of rows) {
    const item = batch[row.row_id];
    if (item) {
      packetBatchQueueWait.observe(Number(settledAt - item.enqueuedNs) / 1e9);
    }
    privacyFilterTotal.inc({
      operation: 'packet_ingest',
      outcome: row.visibility_ok ? 'public' : (row.is_private ? 'private' : 'invalid'),
    });
    batch[row.row_id]?.resolve({
      isPrivate: Boolean(row.is_private),
      visibilityOk: Boolean(row.visibility_ok),
    });
  }
}""",
    "PB tail",
)

write(p, s)
print("packetBatch.ts OK")

# ============================================================ db/packetInput.ts + db/index.ts
p_idx = os.path.join(SRC, "db/index.ts")
s_idx = read(p_idx)

start = s_idx.find("export async function insertPacket(p: {")
assert start != -1, "insertPacket not found"
end_marker = "  });\n}\n\nexport async function insertNodeStatusSample("
end = s_idx.find(end_marker, start)
assert end != -1, "insertPacket end not found"
block = s_idx[start : end + len("  });\n}")]
assert "return enqueuePacket({" in block, "unexpected insertPacket body"
assert "observerRegionFromTopic(p.topic)" in block, "unexpected insertPacket body 2"

# --- extract observerRegionFromTopic (local fn) via brace scan
fn_start = s_idx.find("function observerRegionFromTopic(topic: string): string | null {")
assert fn_start != -1, "observerRegionFromTopic not found"
depth = 0
fn_end = None
for i in range(fn_start, len(s_idx)):
    c = s_idx[i]
    if c == "{":
        depth += 1
    elif c == "}":
        depth -= 1
        if depth == 0:
            fn_end = i + 1
            break
assert fn_end, "observer fn end not found"
fn_text = s_idx[fn_start:fn_end]
assert fn_text.startswith("function observerRegionFromTopic")
# ensure no external importers rely on the db/index definition silently:
external_users = []
for root, dirs, files in os.walk(SRC):
    dirs[:] = [d for d in dirs if d not in ("node_modules",)]
    for f in files:
        if not f.endswith(".ts") or "db/index.ts" in os.path.join(root, f):
            continue
        fp = os.path.join(root, f)
        if "observerRegionFromTopic" in read(fp):
            external_users.append(fp)
print("observerRegionFromTopic external users:", external_users)

# --- build packetInput.ts
body = block
body = body.replace("export async function insertPacket(p: {", "export type InsertPacketInput = {", 1)
body = body.replace(
    "}): Promise<PacketBatchResult> {\n  const inferredPathHashSizeBytes",
    "};\n\nexport function buildPacketBatchInput(p: InsertPacketInput): PacketBatchInput {\n  const inferredPathHashSizeBytes",
    1,
)
body = body.replace("  return enqueuePacket({", "  return {", 1)
tail = "  });\n}"
ti = body.rfind(tail)
assert ti != -1, "insertPacket tail not found"
body = body[:ti] + "  };\n}" + body[ti + len(tail):]
assert "enqueuePacket" not in body, "enqueuePacket still referenced"

fn_exported = fn_text.replace("function observerRegionFromTopic", "export function observerRegionFromTopic", 1)

packet_input = (
    "import type { PacketBatchInput } from './packetBatch.js';\n\n"
    + fn_exported
    + "\n\n"
    + body
    + "\n"
)
write(os.path.join(SRC, "db/packetInput.ts"), packet_input)

# --- rewrite db/index.ts
replacement = (
    "export async function insertPacket(p: InsertPacketInput): Promise<PacketBatchResult> {\n"
    "  return enqueuePacket(buildPacketBatchInput(p));\n"
    "}\n"
)
s_idx = read(p_idx)
s_idx = s_idx.replace(block, replacement, 1)
s_idx = s_idx.replace(fn_text + "\n\n", "", 1)
write(p_idx, s_idx)
insert_import(
    p_idx,
    "import { buildPacketBatchInput, type InsertPacketInput } from './packetInput.js';",
)
s_idx = read(p_idx)
s_idx = s_idx.replace(
    replacement,
    replacement + "\nexport { observerRegionFromTopic, buildPacketBatchInput } from './packetInput.js';\n",
    1,
)
write(p_idx, s_idx)
print("db/packetInput.ts + db/index.ts OK")

# ============================================================ metrics.ts
p = os.path.join(SRC, "metrics.ts")
s = read(p)
assert "ramAdmissionsTotal" not in s, "metrics already patched?"
s += """
// ---------------------------------------------------------------- RAM ingest
export const ramQueueDepth = new Gauge({
  name: 'meshcore_ram_ingest_queue_depth',
  help: 'Pending packets in the RAM ingest stream.',
  registers: [metricsRegistry],
});

export const ramQueueBytes = new Gauge({
  name: 'meshcore_ram_ingest_queue_bytes',
  help: 'Redis memory used by the RAM ingest stream (MEMORY USAGE estimate).',
  registers: [metricsRegistry],
});

export const ramEarliestPendingAge = new Gauge({
  name: 'meshcore_ram_ingest_oldest_pending_age_seconds',
  help: 'Age of the oldest packet still pending in the RAM ingest stream.',
  registers: [metricsRegistry],
});

export const ramAdmissionsTotal = new Counter({
  name: 'meshcore_ram_ingest_admissions_total',
  help: 'RAM ingest admissions by outcome (admitted | queue_full | queue_bytes | redis_error).',
  labelNames: ['outcome'] as const,
  registers: [metricsRegistry],
});

export const ramAdmissionDuration = new Histogram({
  name: 'meshcore_ram_ingest_admission_duration_seconds',
  help: 'RAM ingest admission (classify + XADD) duration.',
  buckets: [0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5],
  registers: [metricsRegistry],
});

export const ramCommitTotal = new Counter({
  name: 'meshcore_ram_ingest_commit_total',
  help: 'RAM ingest batch commits by outcome.',
  labelNames: ['outcome'] as const,
  registers: [metricsRegistry],
});

export const ramCommitDuration = new Histogram({
  name: 'meshcore_ram_ingest_commit_duration_seconds',
  help: 'RAM ingest batch commit duration.',
  labelNames: ['outcome'] as const,
  buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 15],
  registers: [metricsRegistry],
});

export const ramCommitBatchSize = new Histogram({
  name: 'meshcore_ram_ingest_commit_batch_size',
  help: 'Packets committed per RAM ingest batch.',
  buckets: [1, 2, 5, 10, 20, 35, 50, 100, 250, 500],
  registers: [metricsRegistry],
});

export const ramVisibilityLatency = new Histogram({
  name: 'meshcore_ram_ingest_visibility_latency_seconds',
  help: 'RAM ingest admission to durable commit latency.',
  buckets: [0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300],
  registers: [metricsRegistry],
});

export const ramRetryTotal = new Counter({
  name: 'meshcore_ram_ingest_retries_total',
  help: 'Transient database retry attempts for RAM ingest batches.',
  registers: [metricsRegistry],
});

export const ramReclaimedTotal = new Counter({
  name: 'meshcore_ram_ingest_reclaimed_total',
  help: 'Un-acked stream entries reclaimed by a (re)started writer.',
  registers: [metricsRegistry],
});
"""
write(p, s)
print("metrics.ts OK")

# ============================================================ client.ts
p = os.path.join(SRC, "mqtt/client.ts")
s = read(p)

s = replace_once(
    s,
    "import { waitForInFlightPacket } from './dedupeRegistry.js';",
    "import { waitForInFlightPacket } from './dedupeRegistry.js';\nimport { admitPacketToRam } from '../ram/liveStore.js';\nimport { getRamIngestMode } from '../ram/protocol.js';",
    "CL import",
)

s = replace_once(
    s,
    "function markMqttPersistenceHealthy(): void {",
    "export function markMqttPersistenceHealthy(): void {",
    "CL export marker",
)

s = replace_once(
    s,
    "let mqttStopping = false;",
    "let mqttStopping = false;\nconst MQTT_INGEST_MODE = getRamIngestMode();",
    "CL mode const",
)

s = replace_once(
    s,
    """  const persistOnce = async (): Promise<void> => {
    const visibility = await insertPacket({""",
    """  const persistOnce = async (): Promise<void> => {
    const persistInput = {""",
    "CL persist head",
)

s = replace_once(
    s,
    """      regionScope:    decodedRegionScope,
    });
    rememberPacket(finalHash, observerKey, decodedHops);
    markMqttPersistenceHealthy();""",
    """      regionScope:    decodedRegionScope,
    };
    const visibility = MQTT_INGEST_MODE === 'ram'
      ? await admitPacketToRam(persistInput)
      : await insertPacket(persistInput);
    rememberPacket(finalHash, observerKey, decodedHops);
    if (MQTT_INGEST_MODE !== 'ram') markMqttPersistenceHealthy();""",
    "CL persist tail",
)

s = replace_once(
    s,
    """    mqttIngestOutcomesTotal.inc({ outcome: 'packet_persisted' });
  };""",
    """    mqttIngestOutcomesTotal.inc({
      outcome: MQTT_INGEST_MODE === 'ram' ? 'packet_admitted' : 'packet_persisted',
    });
  };""",
    "CL outcome label",
)

s = replace_once(
    s,
    "      mqttIngestOutcomesTotal.inc({ outcome: 'packet_persist_failure' });",
    "      mqttIngestOutcomesTotal.inc({\n        outcome: MQTT_INGEST_MODE === 'ram' ? 'packet_admission_failure' : 'packet_persist_failure',\n      });",
    "CL failure label",
)

s = replace_once(
    s,
    "        '[mqtt] db insert failed',",
    "        MQTT_INGEST_MODE === 'ram' ? '[mqtt] ram admission failed' : '[mqtt] db insert failed',",
    "CL failure log",
)

write(p, s)
print("client.ts OK")

# ============================================================ index.ts
p = os.path.join(SRC, "index.ts")
s = read(p)

s = replace_once(
    s,
    "import { getMqttRuntimeStatus, startMqttClient, stopMqttClient, onPacket, onNodeSeen, onNodeUpsert } from './mqtt/client.js';",
    "import { getMqttRuntimeStatus, startMqttClient, stopMqttClient, onPacket, onNodeSeen, onNodeUpsert } from './mqtt/client.js';\nimport { startRamWriter, closeRamWriter } from './ram/writer.js';\nimport { getRamIngestMode } from './ram/protocol.js';",
    "ID import",
)

s = replace_once(
    s,
    """lifecycle.register({
  name: 'mqtt-ingest',
  stage: 10,
  close: () => stopMqttClient(Math.max(1_000, SHUTDOWN_DEADLINE_MS - 5_000)),
});""",
    """lifecycle.register({
  name: 'mqtt-ingest',
  stage: 10,
  close: () => stopMqttClient(Math.max(1_000, SHUTDOWN_DEADLINE_MS - 5_000)),
});
lifecycle.register({
  name: 'ram-ingest-writer',
  stage: 30,
  close: () => closeRamWriter(Math.max(1_000, SHUTDOWN_DEADLINE_MS - 10_000)),
});""",
    "ID lifecycle",
)

s = replace_once(
    s,
    """  packetShareForwarder.start();
  if (MQTT_INGEST_ENABLED) {
    void startMqttClient().catch((err: unknown) => {
      console.error('[mqtt] failed to start client:', err instanceof Error ? err.message : err);
    });
  }""",
    """  packetShareForwarder.start();
  if (MQTT_INGEST_ENABLED) {
    void startMqttClient().catch((err: unknown) => {
      console.error('[mqtt] failed to start client:', err instanceof Error ? err.message : err);
    });
  }
  if (getRamIngestMode() === 'ram') {
    void startRamWriter().catch((err: unknown) => {
      console.error('[ram] failed to start writer:', err instanceof Error ? err.message : err);
    });
  }""",
    "ID start writer",
)

write(p, s)
print("index.ts OK")

# ============================================================ routes/misc.ts
p = os.path.join(SRC, "api/routes/misc.ts")
s = read(p)
s = replace_once(
    s,
    """      const packets = raw
        ? await getRecentPacketEvents(limit, network, observer)
        : await getRecentPackets(limit, network, observer, fields);
      res.json(packets);""",
    """      const packets = raw
        ? await getRecentPacketEvents(limit, network, observer)
        : await mergePendingPacketsIntoRecentRows(
            await getRecentPackets(limit, network, observer, fields),
            { limit, network, observer },
          );
      res.json(packets);""",
    "RT merge",
)
write(p, s)
insert_import(p, "import { mergePendingPacketsIntoRecentRows } from '../../ram/liveRead.js';")
print("routes/misc.ts OK")

# ============================================================ ws/server.ts
p = os.path.join(SRC, "ws/server.ts")
s = read(p)
s = replace_once(
    s,
    """      const [rawNodes, packets, messages, rawViableLinks] = await Promise.all([
        getNodes(network, observer, 'slim', signal),
        getRecentPackets(7, network, observer, 'full', signal),
        getRecentMessages(200, network, observer, signal),
        getCachedViableLinks(network, observer, signal),
      ]);""",
    """      const [rawNodes, packets, messages, rawViableLinks] = await Promise.all([
        getNodes(network, observer, 'slim', signal),
        mergePendingPacketsIntoRecentRows(
          await getRecentPackets(7, network, observer, 'full', signal),
          { limit: 7, network, observer },
        ),
        mergePendingPacketsIntoRecentRows(
          await getRecentMessages(200, network, observer, signal),
          { limit: 200, network, observer, includePayload: true },
        ),
        getCachedViableLinks(network, observer, signal),
      ]);""",
    "WS merge",
)
write(p, s)
insert_import(p, "import { mergePendingPacketsIntoRecentRows } from '../ram/liveRead.js';")
print("ws/server.ts OK")

# ============================================================ .env.example
env_path = None
for candidate in [os.path.join(BE, ".env.example"), os.path.join(W, ".env.example")]:
    if os.path.exists(candidate):
        env_path = candidate
        break
assert env_path, ".env.example not found"
s = read(env_path)
assert "MQTT_INGEST_MODE" not in s, "env already patched?"
s += """
# ------------------------------------------------------------------------------
# RAM-first ingestion (experimental prototype; see docs/ram-first-ingestion).
# Default "durable" keeps the existing acknowledge-after-commit pipeline.
# "ram" admits validated packets to a bounded Redis stream (PUBACK after RAM
# admission, batched persistence in the background): recent telemetry can be
# lost on crash in exchange for lower database load. Privacy-critical state
# stays durable; the database remains the authoritative historical store.
# MQTT_INGEST_MODE=durable
# MQTT_RAM_BATCH_SIZE=100
# MQTT_RAM_MAX_AGE_MS=120000
# MQTT_RAM_QUEUE_MAX_PACKETS=5000
# MQTT_RAM_QUEUE_MAX_BYTES=33554432
# MQTT_RAM_STREAM_KEY=meshcore:ingest:pending
# MQTT_RAM_CONSUMER_GROUP=ingest-writers
"""
write(env_path, s)
print(f".env.example OK ({env_path})")

print("ALL RAM INTEGRATION EDITS APPLIED")
