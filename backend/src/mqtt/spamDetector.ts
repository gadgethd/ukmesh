import { createHash } from 'node:crypto';
import { haversineKm } from '../platform/geo.js';
import { query, type DatabaseQueryFn } from '../db/index.js';
import { readSpamSnapshot } from './spamSnapshot.js';
import { spamDetectorSnapshotEntries, spamDetectorSnapshotUpdatedSeconds, spamDetectorRefreshHealthy, spamDetectorRefreshesTotal, setSpamDetectorRuntimeCollector } from '../metrics.js';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type SignalName =
  | 'NAME_KEY_FLOOD_HIGH'
  | 'NAME_KEY_FLOOD_LOW'
  | 'NAME_MULTI_KEY_CONFIRMED'
  | 'NAME_MULTI_KEY_BURST'
  | 'LOCATION_JUMP_LARGE'
  | 'LOCATION_JUMP_MEDIUM'
  | 'COORD_OUTSIDE_UK'
  | 'COORD_NULL_ISLAND'
  | 'SUPPORTING_CLOCK_ANOMALY'
  | 'SUPPORTING_OUTSIDE_UK'
  | 'SUPPORTING_NULL_COORDS'
  | 'BURST_NEW_NODES'
  | 'HIGH_FREQ_ADVERT'
  | 'EXTREME_ADVERT_FLOOD'
  | 'TIMESTAMP_ANOMALY'
  | 'NAME_ROTATION'
  | 'PUBLIC_KEY_NAME_ROTATION'
  | 'HOP_ANOMALY'
  | 'SLOW_BURN_CLONE';

export interface Signal {
  name: SignalName;
  score: number;
  detail: string;
}

export type Verdict = 'clean' | 'suspect' | 'spam';

export interface EvalResult {
  signals: Signal[];
  totalScore: number;
  verdict: Verdict;
  canonicalKey?: string;
}

// ---------------------------------------------------------------------------
// Thresholds (all tunable here, no other code changes needed)
// ---------------------------------------------------------------------------

const BLOCK_THRESHOLD = 100;
const FLAG_THRESHOLD  = 60;

const SIGNAL_SCORES: Record<SignalName, number> = {
  NAME_KEY_FLOOD_HIGH:  90,
  NAME_KEY_FLOOD_LOW:   45,
  NAME_MULTI_KEY_CONFIRMED: 85,
  NAME_MULTI_KEY_BURST:     80,
  LOCATION_JUMP_LARGE:  55,
  LOCATION_JUMP_MEDIUM: 20,
  COORD_OUTSIDE_UK:     30,
  COORD_NULL_ISLAND:    25,
  SUPPORTING_CLOCK_ANOMALY: 10,
  SUPPORTING_OUTSIDE_UK:    10,
  SUPPORTING_NULL_COORDS:    5,
  BURST_NEW_NODES:      80,
  HIGH_FREQ_ADVERT:     20,
  EXTREME_ADVERT_FLOOD: 45,
  TIMESTAMP_ANOMALY:    10,
  NAME_ROTATION:        65,
  PUBLIC_KEY_NAME_ROTATION: 65,
  HOP_ANOMALY:          15,
  SLOW_BURN_CLONE:      55,
};

// Window/threshold config
const HIGH_FREQ_WINDOW_MS      = 60 * 1000;            // 1 min
const EXTREME_HIGH_FREQ_COUNT  = 30;                   // high advert rate is supporting only
const NAME_ROTATION_WINDOW_MS  = 30 * 60 * 1000;       // 30 min
const NAME_ROTATION_THRESHOLD  = 3;                    // >N different names from same key
const ESTABLISHED_MIN_ADVERTS  = 10;
const LOCATION_JUMP_LARGE_KM   = 300;
const LOCATION_JUMP_MEDIUM_KM  = 100;
const HOP_ANOMALY_ESTABLISHED  = 5;   // established avg hops ≤ this
const HOP_ANOMALY_THRESHOLD    = 15;  // sudden hop count ≥ this
const TIMESTAMP_DRIFT_S        = 86400; // 24h
const IDENTITY_WINDOW_DAYS     = 7;
const EVIDENCED_KEY_MIN_ADVERTS = 2;
const EVIDENCED_KEY_MIN_OBSERVERS = 2;
const STRONG_KEY_MIN_ADVERTS = 10;
const STRONG_KEY_MIN_OBSERVERS = 5;
const MULTI_KEY_CONFIRMED_THRESHOLD = 4;
const MULTI_KEY_BURST_THRESHOLD = 4;
const SEVERE_KEY_ROTATION_NAMES = 4;
const SEVERE_KEY_ROTATION_ADVERTS = 20;

// UK bounding box (generous)
const UK_LAT_MIN = 49.0, UK_LAT_MAX = 61.0;
const UK_LON_MIN = -9.0, UK_LON_MAX = 3.0;

// ---------------------------------------------------------------------------
// In-memory state
// ---------------------------------------------------------------------------

// srcNodeId → { times: number[], namesSeen: Map<name, number> }
const nodeFreqTracker = new Map<string, {
  times: number[];
  namesSeen: Map<string, number>;
}>();

// DB-backed established node locations (refreshed every 15 min at startup)
// srcNodeId → { lat, lon, avgHops, advertCount }
interface EstablishedNode {
  lat: number;
  lon: number;
  avgHops: number;
  advertCount: number;
}
let establishedNodes = new Map<string, EstablishedNode>();

// Known src_node_ids from the most recent complete database snapshot
let knownNodeIds = new Set<string>();

interface IdentityKeyStats {
  publicKey: string;
  advertCount: number;
  observerCount: number;
  firstSeen: Date;
  lastSeen: Date;
  maxAdvertsPerMinute: number;
}

interface IdentityNameStats {
  name: string;
  canonicalKey: string;
  keyCount: number;
  evidencedKeys: number;
  strongKeys: number;
  totalAdverts: number;
  maxEvidencedKeysPerHour: number;
  keys: Map<string, IdentityKeyStats>;
}

interface KeyRotationStats {
  publicKey: string;
  nameCount: number;
  advertCount: number;
  sampleNames: string[];
}

let identityByName = new Map<string, IdentityNameStats>();
let keyRotationByPublicKey = new Map<string, KeyRotationStats>();

const SPAM_RUNTIME_TRACKER_MAX = 10_000;
const SPAM_NAMES_PER_NODE_MAX = 64;

// Retain fixed-size digests rather than arbitrary input strings in runtime maps.
function trackerKey(value: string): string {
  return createHash('sha256').update(value).digest('base64');
}

setSpamDetectorRuntimeCollector(() => {
  let arrivals = 0, names = 0;
  for (const entry of nodeFreqTracker.values()) {
    arrivals += entry.times.length;
    names += entry.namesSeen.size;
  }
  return { trackers: nodeFreqTracker.size, arrivals, names };
});

function evictOldest<K, V>(map: Map<K, V>, capacity: number): void {
  while (map.size >= capacity) {
    const oldest = map.keys().next().value as K | undefined;
    if (oldest === undefined) break;
    map.delete(oldest);
  }
}

// ---------------------------------------------------------------------------
// Startup & refresh
// ---------------------------------------------------------------------------

let refreshInFlight: Promise<void> | undefined;
let initialized = false;

export async function initSpamDetector(): Promise<void> {
  await refreshSpamDetectorCaches();
  if (initialized) return;
  initialized = true;
  setInterval(() => { void refreshSpamDetectorCaches(); }, 15 * 60 * 1000).unref();
  setInterval(cleanupMemory, 5 * 60 * 1000).unref();
}

export function refreshSpamDetectorCaches(databaseQuery: DatabaseQueryFn = query): Promise<void> {
  if (refreshInFlight) return refreshInFlight;
  refreshInFlight = Promise.all([
    refreshEstablishedNodes(databaseQuery),
    refreshIdentityEvidence(databaseQuery),
  ]).then(() => undefined).finally(() => { refreshInFlight = undefined; });
  return refreshInFlight;
}

function refreshSucceeded(dataset: 'established' | 'identity', entries: number): void {
  spamDetectorSnapshotEntries.set({ dataset }, entries);
  spamDetectorSnapshotUpdatedSeconds.set({ dataset }, Date.now() / 1000);
  spamDetectorRefreshHealthy.set({ dataset }, 1);
  spamDetectorRefreshesTotal.inc({ dataset, outcome: 'success' });
}

function refreshFailed(dataset: 'established' | 'identity'): void {
  spamDetectorRefreshHealthy.set({ dataset }, 0);
  spamDetectorRefreshesTotal.inc({ dataset, outcome: 'failure' });
}

async function refreshEstablishedNodes(databaseQuery: DatabaseQueryFn): Promise<void> {
  try {
    const rows = await readSpamSnapshot<{
      src_node_id: string;
      avg_lat: number;
      avg_lon: number;
      avg_hops: number;
      advert_count: number;
    }>(databaseQuery, `
      SELECT
        n.node_id AS src_node_id,
        AVG(NULLIF(p.payload->'appData'->'location'->>'latitude', '')::double precision)  AS avg_lat,
        AVG(NULLIF(p.payload->'appData'->'location'->>'longitude', '')::double precision) AS avg_lon,
        AVG(p.hop_count)   AS avg_hops,
        n.advert_count
      FROM core.nodes n
      JOIN mesh.packets p ON p.src_node_id = n.node_id
      WHERE n.advert_count >= $1
        AND p.packet_type = 4
        AND p.time > NOW() - INTERVAL '30 days'
        AND p.payload->'appData'->'location'->>'latitude' IS NOT NULL
      GROUP BY n.node_id, n.advert_count
      HAVING
        STDDEV(NULLIF(p.payload->'appData'->'location'->>'latitude', '')::double precision) < 0.3
        AND COUNT(DISTINCT p.packet_hash) >= $1
    `, [ESTABLISHED_MIN_ADVERTS]);

    const nextEstablished = new Map<string, EstablishedNode>();
    const nextKnown = new Set<string>();

    for (const row of rows) {
      if (row.avg_lat == null || row.avg_lon == null) continue;
      nextEstablished.set(row.src_node_id, {
        lat: Number(row.avg_lat),
        lon: Number(row.avg_lon),
        avgHops: Number(row.avg_hops ?? 5),
        advertCount: Number(row.advert_count),
      });
      nextKnown.add(row.src_node_id);
    }

    // Also load all known node IDs (even without stable location) for slow-burn detection
    const allIds = await readSpamSnapshot<{ node_id: string }>(databaseQuery,
      `SELECT node_id
         FROM core.nodes
        WHERE last_seen > NOW() - INTERVAL '180 days'
        ORDER BY last_seen DESC`
    );
    for (const row of allIds) nextKnown.add(row.node_id);
    establishedNodes = nextEstablished;
    knownNodeIds = nextKnown;
    refreshSucceeded('established', establishedNodes.size + knownNodeIds.size);

    console.log(`[spam-detect] established nodes: ${establishedNodes.size}, known ids: ${knownNodeIds.size}`);
  } catch (err: unknown) {
    refreshFailed('established');
    console.error('[spam-detect] refreshEstablishedNodes error:', (err as Error).message);
  }
}

async function refreshIdentityEvidence(databaseQuery: DatabaseQueryFn): Promise<void> {
  try {
    const keyRows = await readSpamSnapshot<{
      name: string;
      public_key: string;
      advert_count: string;
      observer_count: string;
      first_seen: Date;
      last_seen: Date;
      max_adverts_per_minute: string;
    }>(databaseQuery, `
      WITH base AS (
        SELECT
          payload->>'_summary' AS name,
          upper(payload->>'publicKey') AS public_key,
          packet_hash,
          rx_node_id,
          time
        FROM mesh.packets
        WHERE packet_type = 4
          AND time > NOW() - ($1 * INTERVAL '1 day')
          AND payload ? 'publicKey'
          AND payload->>'_summary' IS NOT NULL
          AND payload->>'publicKey' ~* '^[0-9a-f]{64}$'
      ),
      key_stats AS (
        SELECT
          name,
          public_key,
          COUNT(DISTINCT packet_hash) AS advert_count,
          COUNT(DISTINCT rx_node_id) AS observer_count,
          MIN(time) AS first_seen,
          MAX(time) AS last_seen
        FROM base
        GROUP BY name, public_key
      ),
      minute_stats AS (
        SELECT name, public_key, date_trunc('minute', time) AS minute, COUNT(DISTINCT packet_hash) AS adverts
        FROM base
        GROUP BY name, public_key, date_trunc('minute', time)
      ),
      max_minute AS (
        SELECT name, public_key, MAX(adverts) AS max_adverts_per_minute
        FROM minute_stats
        GROUP BY name, public_key
      )
      SELECT
        ks.name,
        ks.public_key,
        ks.advert_count,
        ks.observer_count,
        ks.first_seen,
        ks.last_seen,
        COALESCE(mm.max_adverts_per_minute, 0) AS max_adverts_per_minute
      FROM key_stats ks
      LEFT JOIN max_minute mm ON mm.name = ks.name AND mm.public_key = ks.public_key
    `, [IDENTITY_WINDOW_DAYS]);

    const burstRows = await readSpamSnapshot<{ name: string; max_evidenced_keys_per_hour: string }>(databaseQuery, `
      WITH base AS (
        SELECT
          payload->>'_summary' AS name,
          upper(payload->>'publicKey') AS public_key,
          packet_hash,
          rx_node_id,
          date_trunc('hour', time) AS hour_bucket
        FROM mesh.packets
        WHERE packet_type = 4
          AND time > NOW() - ($1 * INTERVAL '1 day')
          AND payload ? 'publicKey'
          AND payload->>'_summary' IS NOT NULL
          AND payload->>'publicKey' ~* '^[0-9a-f]{64}$'
      ),
      key_hour AS (
        SELECT
          name,
          public_key,
          hour_bucket,
          COUNT(DISTINCT packet_hash) AS adverts,
          COUNT(DISTINCT rx_node_id) AS observers
        FROM base
        GROUP BY name, public_key, hour_bucket
      ),
      evidenced_hour AS (
        SELECT
          name,
          hour_bucket,
          COUNT(*) FILTER (WHERE adverts >= $2 AND observers >= $3) AS evidenced_keys
        FROM key_hour
        GROUP BY name, hour_bucket
      )
      SELECT name, MAX(evidenced_keys) AS max_evidenced_keys_per_hour
      FROM evidenced_hour
      GROUP BY name
    `, [IDENTITY_WINDOW_DAYS, EVIDENCED_KEY_MIN_ADVERTS, EVIDENCED_KEY_MIN_OBSERVERS]);

    const rotationRows = await readSpamSnapshot<{
      public_key: string;
      name_count: string;
      advert_count: string;
      sample_names: string[];
    }>(databaseQuery, `
      SELECT
        upper(payload->>'publicKey') AS public_key,
        COUNT(DISTINCT payload->>'_summary') AS name_count,
        COUNT(DISTINCT packet_hash) AS advert_count,
        (array_agg(DISTINCT left(payload->>'_summary', 32) ORDER BY left(payload->>'_summary', 32)))[1:4] AS sample_names
      FROM mesh.packets
      WHERE packet_type = 4
        AND time > NOW() - ($1 * INTERVAL '1 day')
        AND payload ? 'publicKey'
        AND payload->>'_summary' IS NOT NULL
        AND payload->>'publicKey' ~* '^[0-9a-f]{64}$'
      GROUP BY upper(payload->>'publicKey')
      HAVING COUNT(DISTINCT payload->>'_summary') >= 2
    `, [IDENTITY_WINDOW_DAYS]);

    const burstByName = new Map<string, number>();
    for (const row of burstRows) {
      burstByName.set(row.name, Number(row.max_evidenced_keys_per_hour ?? 0));
    }

    const nextIdentityByName = new Map<string, IdentityNameStats>();
    for (const row of keyRows) {
      const advertCount = Number(row.advert_count ?? 0);
      const observerCount = Number(row.observer_count ?? 0);
      const keyStats: IdentityKeyStats = {
        publicKey: row.public_key,
        advertCount,
        observerCount,
        firstSeen: new Date(row.first_seen),
        lastSeen: new Date(row.last_seen),
        maxAdvertsPerMinute: Number(row.max_adverts_per_minute ?? 0),
      };

      let nameStats = nextIdentityByName.get(row.name);
      if (!nameStats) {
        nameStats = {
          name: row.name,
          canonicalKey: row.public_key,
          keyCount: 0,
          evidencedKeys: 0,
          strongKeys: 0,
          totalAdverts: 0,
          maxEvidencedKeysPerHour: burstByName.get(row.name) ?? 0,
          keys: new Map(),
        };
        nextIdentityByName.set(row.name, nameStats);
      }

      nameStats.keys.set(row.public_key, keyStats);
      nameStats.keyCount += 1;
      nameStats.totalAdverts += advertCount;
      if (isEvidencedKey(keyStats)) nameStats.evidencedKeys += 1;
      if (isStrongKey(keyStats)) nameStats.strongKeys += 1;

      const currentCanonical = nameStats.keys.get(nameStats.canonicalKey);
      if (!currentCanonical || advertCount > currentCanonical.advertCount) {
        nameStats.canonicalKey = row.public_key;
      }
    }

    const nextKeyRotation = new Map<string, KeyRotationStats>();
    for (const row of rotationRows) {
      nextKeyRotation.set(row.public_key, {
        publicKey: row.public_key,
        nameCount: Number(row.name_count ?? 0),
        advertCount: Number(row.advert_count ?? 0),
        sampleNames: row.sample_names ?? [],
      });
    }

    identityByName = nextIdentityByName;
    keyRotationByPublicKey = nextKeyRotation;
    refreshSucceeded('identity', keyRows.length + identityByName.size + keyRotationByPublicKey.size);

    const suspiciousNames = Array.from(identityByName.values()).filter((stats) => stats.evidencedKeys >= MULTI_KEY_CONFIRMED_THRESHOLD).length;
    console.log(`[spam-detect] identity evidence names: ${identityByName.size}, suspicious names: ${suspiciousNames}, rotating keys: ${keyRotationByPublicKey.size}`);
  } catch (err: unknown) {
    refreshFailed('identity');
    console.error('[spam-detect] refreshIdentityEvidence error:', (err as Error).message);
  }
}

function cleanupMemory(): void {
  const now = performance.now();

  for (const [id, entry] of nodeFreqTracker) {
    entry.times = entry.times.filter(t => now - t < HIGH_FREQ_WINDOW_MS);
    for (const [n, ts] of entry.namesSeen) {
      if (now - ts > NAME_ROTATION_WINDOW_MS) entry.namesSeen.delete(n);
    }
    if (entry.times.length === 0 && entry.namesSeen.size === 0) nodeFreqTracker.delete(id);
  }

}


// ---------------------------------------------------------------------------
// Signal checks
// ---------------------------------------------------------------------------

function sig(name: SignalName, detail: string): Signal {
  return { name, score: SIGNAL_SCORES[name], detail };
}

function isFullPublicKey(publicKey: string): boolean {
  return /^[0-9a-f]{64}$/i.test(publicKey);
}

function isEvidencedKey(stats: IdentityKeyStats): boolean {
  return stats.advertCount >= EVIDENCED_KEY_MIN_ADVERTS
    && stats.observerCount >= EVIDENCED_KEY_MIN_OBSERVERS;
}

function isStrongKey(stats: IdentityKeyStats): boolean {
  return stats.advertCount >= STRONG_KEY_MIN_ADVERTS
    && stats.observerCount >= STRONG_KEY_MIN_OBSERVERS;
}

function checkLocationJump(srcNodeId: string, lat?: number, lon?: number): Signal[] {
  if (lat == null || lon == null) return [];
  const established = establishedNodes.get(srcNodeId);
  if (!established) return [];

  const dist = haversineKm(established.lat, established.lon, lat, lon);
  if (dist >= LOCATION_JUMP_LARGE_KM) {
    return [sig('LOCATION_JUMP_LARGE',
      `Established node jumped ${dist.toFixed(0)} km (was ${established.lat.toFixed(3)},${established.lon.toFixed(3)}; now ${lat.toFixed(3)},${lon.toFixed(3)})`
    )];
  }
  if (dist >= LOCATION_JUMP_MEDIUM_KM) {
    return [sig('LOCATION_JUMP_MEDIUM',
      `Established node jumped ${dist.toFixed(0)} km`
    )];
  }
  return [];
}

function checkCoords(name: string, lat?: number, lon?: number): Signal[] {
  if (lat == null || lon == null) return [];
  const signals: Signal[] = [];

  if (lat === 0 && lon === 0) {
    if (identityByName.has(name)) {
      signals.push(sig('SUPPORTING_NULL_COORDS', `Zero coordinates for "${name}"`));
    }
  } else if (lat < UK_LAT_MIN || lat > UK_LAT_MAX || lon < UK_LON_MIN || lon > UK_LON_MAX) {
    signals.push(sig('SUPPORTING_OUTSIDE_UK',
      `Coordinates ${lat.toFixed(3)},${lon.toFixed(3)} outside UK bounds`));
  }

  return signals;
}

function checkHighFreq(srcNodeId: string): Signal[] {
  const now = performance.now();
  let entry = nodeFreqTracker.get(trackerKey(srcNodeId));
  if (!entry) {
    evictOldest(nodeFreqTracker, SPAM_RUNTIME_TRACKER_MAX);
    entry = { times: [], namesSeen: new Map() };
    nodeFreqTracker.set(trackerKey(srcNodeId), entry);
  }
  entry.times.push(now);
  entry.times = entry.times.filter(t => now - t < HIGH_FREQ_WINDOW_MS);
  // The verdict only needs to know whether the latest 31 arrivals fit in a minute.
  if (entry.times.length > EXTREME_HIGH_FREQ_COUNT + 1) entry.times.shift();

  if (entry.times.length > EXTREME_HIGH_FREQ_COUNT) {
    return [sig('EXTREME_ADVERT_FLOOD',
      `More than ${EXTREME_HIGH_FREQ_COUNT} adverts in 60s from ${srcNodeId.slice(0, 12)}…`
    )];
  }
  return [];
}

function checkTimestamp(payloadTimestamp?: number): Signal[] {
  if (payloadTimestamp == null) return [];
  const serverNow = Math.floor(Date.now() / 1000);
  const drift = Math.abs(serverNow - payloadTimestamp);
  if (drift > TIMESTAMP_DRIFT_S) {
    return [sig('SUPPORTING_CLOCK_ANOMALY',
      `Payload timestamp ${payloadTimestamp} is ${Math.floor(drift / 3600)}h off from server time`
    )];
  }
  return [];
}

function checkNameRotation(srcNodeId: string, name: string): Signal[] {
  const now = performance.now();
  let entry = nodeFreqTracker.get(trackerKey(srcNodeId));
  if (!entry) {
    evictOldest(nodeFreqTracker, SPAM_RUNTIME_TRACKER_MAX);
    entry = { times: [], namesSeen: new Map() };
    nodeFreqTracker.set(trackerKey(srcNodeId), entry);
  }
  const nameKey = trackerKey(name);
  if (!entry.namesSeen.has(nameKey)) {
    evictOldest(entry.namesSeen, SPAM_NAMES_PER_NODE_MAX);
  }
  entry.namesSeen.set(nameKey, now);

  // Evict expired names
  for (const [n, ts] of entry.namesSeen) {
    if (now - ts > NAME_ROTATION_WINDOW_MS) entry.namesSeen.delete(n);
  }

  if (entry.namesSeen.size > NAME_ROTATION_THRESHOLD) {
    return [sig('PUBLIC_KEY_NAME_ROTATION',
      `Key ${srcNodeId.slice(0, 12)}… used ${entry.namesSeen.size} different names in 30 min`
    )];
  }
  return [];
}

function checkHopAnomaly(srcNodeId: string, hopCount?: number): Signal[] {
  if (hopCount == null) return [];
  const established = establishedNodes.get(srcNodeId);
  if (!established) return [];
  if (established.avgHops <= HOP_ANOMALY_ESTABLISHED && hopCount >= HOP_ANOMALY_THRESHOLD) {
    return [sig('HOP_ANOMALY',
      `Established node (avg hops ${established.avgHops.toFixed(1)}) suddenly at ${hopCount} hops`
    )];
  }
  return [];
}

async function checkSlowBurnClone(name: string, publicKey: string): Promise<Signal[]> {
  const stats = identityByName.get(name);
  const keyStats = stats?.keys.get(publicKey.toUpperCase());
  if (stats && keyStats && isEvidencedKey(keyStats) && stats.evidencedKeys >= 2 && stats.evidencedKeys < MULTI_KEY_CONFIRMED_THRESHOLD) {
    return [sig('SLOW_BURN_CLONE',
      `"${name}" has ${stats.evidencedKeys} evidenced public keys in the last ${IDENTITY_WINDOW_DAYS} days`
    )];
  }
  return [];
}

// ---------------------------------------------------------------------------
// Main evaluation function
// ---------------------------------------------------------------------------

export async function evaluateAdvert(advert: {
  name: string;
  publicKey: string;
  srcNodeId: string;
  lat?: number;
  lon?: number;
  hopCount?: number;
  payloadTimestamp?: number;
  network: string;
}): Promise<EvalResult> {
  const { name, publicKey, srcNodeId, lat, lon, hopCount, payloadTimestamp } = advert;
  const normalizedKey = publicKey.toUpperCase();

  if (!name || !isFullPublicKey(normalizedKey)) {
    return { signals: [], totalScore: 0, verdict: 'clean', canonicalKey: publicKey };
  }

  const nameStats = identityByName.get(name);
  const keyStats = nameStats?.keys.get(normalizedKey);
  const rotationStats = keyRotationByPublicKey.get(normalizedKey);
  const keyIsEvidenced = keyStats ? isEvidencedKey(keyStats) : false;

  const canonicalKey = nameStats?.canonicalKey ?? normalizedKey;
  const isKnown = knownNodeIds.has(srcNodeId);
  const signals: Signal[] = [];

  let identityConfirmed = false;
  let identityBurst = false;
  let keyRotationSevere = false;
  let extremeFlood = false;
  let largeLocationJump = false;

  if (nameStats && keyIsEvidenced && nameStats.evidencedKeys >= MULTI_KEY_CONFIRMED_THRESHOLD) {
    identityConfirmed = true;
    signals.push(sig('NAME_MULTI_KEY_CONFIRMED',
      `"${name}" has ${nameStats.evidencedKeys} evidenced public keys (${nameStats.keyCount} total keys, ${nameStats.totalAdverts} adverts) in the last ${IDENTITY_WINDOW_DAYS} days`));
  } else {
    signals.push(...await checkSlowBurnClone(name, normalizedKey));
  }

  if (nameStats && keyIsEvidenced && nameStats.maxEvidencedKeysPerHour >= MULTI_KEY_BURST_THRESHOLD) {
    identityBurst = true;
    signals.push(sig('NAME_MULTI_KEY_BURST',
      `"${name}" had ${nameStats.maxEvidencedKeysPerHour} evidenced keys in the same hour`));
  }

  if (
    rotationStats
    && rotationStats.nameCount >= SEVERE_KEY_ROTATION_NAMES
    && rotationStats.advertCount >= SEVERE_KEY_ROTATION_ADVERTS
  ) {
    keyRotationSevere = true;
    signals.push(sig('PUBLIC_KEY_NAME_ROTATION',
      `Key ${normalizedKey.slice(0, 12)}… used ${rotationStats.nameCount} names (${rotationStats.sampleNames.slice(0, 4).join(', ')})`));
  } else {
    signals.push(...checkNameRotation(normalizedKey, name));
  }

  const realtimeFloodSignals = isKnown ? [] : checkHighFreq(srcNodeId);
  if (
    realtimeFloodSignals.length > 0
    || (keyStats && keyStats.maxAdvertsPerMinute >= EXTREME_HIGH_FREQ_COUNT && keyStats.observerCount >= EVIDENCED_KEY_MIN_OBSERVERS)
  ) {
    extremeFlood = true;
    signals.push(...realtimeFloodSignals);
    if (keyStats && realtimeFloodSignals.length === 0) {
      signals.push(sig('EXTREME_ADVERT_FLOOD',
        `${keyStats.maxAdvertsPerMinute} adverts/minute for ${normalizedKey.slice(0, 12)}… across ${keyStats.observerCount} observers`));
    }
  }

  const locationSignals = checkLocationJump(srcNodeId, lat, lon);
  if (locationSignals.some((signal) => signal.name === 'LOCATION_JUMP_LARGE')) largeLocationJump = true;
  signals.push(...locationSignals);

  // These are supporting evidence only. They should never create a spam verdict by themselves.
  signals.push(...checkCoords(name, lat, lon));
  signals.push(...checkTimestamp(payloadTimestamp));
  if (!isKnown) signals.push(...checkHopAnomaly(srcNodeId, hopCount));

  const totalScore = signals.reduce((sum, s) => sum + s.score, 0);
  const supportingSignals = signals.filter((signal) =>
    signal.name === 'SUPPORTING_CLOCK_ANOMALY'
    || signal.name === 'SUPPORTING_OUTSIDE_UK'
    || signal.name === 'SUPPORTING_NULL_COORDS'
    || signal.name === 'LOCATION_JUMP_MEDIUM'
    || signal.name === 'LOCATION_JUMP_LARGE'
    || signal.name === 'HOP_ANOMALY'
  );

  let verdict: Verdict = 'clean';
  if (
    (identityBurst && ((nameStats?.evidencedKeys ?? 0) >= 6 || (nameStats?.strongKeys ?? 0) >= 2 || extremeFlood))
    || (identityConfirmed && (nameStats?.strongKeys ?? 0) >= 5 && (nameStats?.maxEvidencedKeysPerHour ?? 0) >= 3)
    || (keyRotationSevere && extremeFlood && totalScore >= BLOCK_THRESHOLD)
  ) {
    verdict = 'spam';
  } else if (
    identityConfirmed
    || identityBurst
    || keyRotationSevere
    || largeLocationJump
    || (extremeFlood && supportingSignals.length > 0 && totalScore >= FLAG_THRESHOLD)
  ) {
    verdict = 'suspect';
  }

  return { signals, totalScore, verdict, canonicalKey };
}
