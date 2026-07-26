import { pool, analyticsPool } from '../db/index.js';
import { UKMESH_NETWORKS } from '../networks.js';
import { normalizeMessage } from './normalize.js';
import type { Incident, IncidentStatus, MessageRecord, ObserverObservation, OriginEstimate } from './types.js';
import type { PublicIncident } from './sanitize.js';
import type { SpamMessageConfig } from './config.js';

// ---------------------------------------------------------------------------
// Persistence layer for message-spam detection.
//
// Reads decoded channel messages out of the existing `packets` hypertable and
// writes derived incidents into `spam_message_incidents` / `_members`.
// ---------------------------------------------------------------------------

const NETWORKS: readonly string[] = UKMESH_NETWORKS;

interface MsgRow {
  packet_hash: string;
  network: string;
  sender: string | null;
  message: string | null;
  channel_hash: string | null;
  summary: string | null;
  claimed_ts: string | null;
  observed_at: Date;
}

interface ObsRow {
  packet_hash: string;
  observer_id: string;
  lat: number;
  lon: number;
  min_hop: number | null;
  best_rssi: number | null;
  best_snr: number | null;
}

/** Extract a coarse channel label ("Public", "test", …) from the _summary prefix. */
function channelLabel(summary: string | null, channelHash: string | null): string {
  if (summary) {
    const m = summary.match(/^\[([^\]]+)\]/);
    if (m && m[1]) return m[1];
  }
  return channelHash ? `ch:${channelHash}` : 'unknown';
}

/**
 * Load decoded channel messages (one logical transmission per packet_hash)
 * within the analysis window, each with its geolocated observers.
 */
export type SpamAnalysisWindow = { start: Date; end: Date };

export async function countLogicalMessages(window: SpamAnalysisWindow): Promise<number> {
  const result = await analyticsPool.query<{ count: string }>(
    `SELECT COUNT(DISTINCT p.packet_hash)::text AS count
       FROM packets p
      WHERE p.packet_type = 5
        AND p.time >= $1
        AND p.time < $2
        AND p.network = ANY($3)
        AND p.payload ? 'decrypted'
        AND p.payload->'decrypted' ? 'message'`,
    [window.start, window.end, NETWORKS],
  );
  return Number(result.rows[0]?.count ?? 0);
}

export async function loadRecentMessages(
  cfg: SpamMessageConfig,
  fixedWindow?: SpamAnalysisWindow,
): Promise<MessageRecord[]> {
  const windowEnd = fixedWindow?.end ?? new Date();
  const windowStart = fixedWindow?.start
    ?? new Date(windowEnd.getTime() - cfg.analysisWindowHours * 60 * 60 * 1000);

  const msgRes = await analyticsPool.query<MsgRow>(
    `WITH logical_messages AS (
       SELECT DISTINCT ON (p.packet_hash)
          p.packet_hash,
          p.network,
          p.payload->'decrypted'->>'sender'    AS sender,
          p.payload->'decrypted'->>'message'   AS message,
          p.payload->>'channelHash'            AS channel_hash,
          p.payload->>'_summary'               AS summary,
          p.payload->'decrypted'->>'timestamp' AS claimed_ts,
          p.time                               AS observed_at
       FROM packets p
       WHERE p.packet_type = 5
         AND p.time >= $1
         AND p.time < $2
         AND p.network = ANY($3)
         AND p.payload ? 'decrypted'
         AND p.payload->'decrypted' ? 'message'
         AND COALESCE(p.payload->'decrypted'->>'sender', '') NOT LIKE '%🚫%'
         AND NOT EXISTS (
           SELECT 1 FROM nodes private_node
            WHERE private_node.name LIKE '%🚫%'
              AND private_node.node_id IN (p.rx_node_id, p.src_node_id)
         )
       ORDER BY p.packet_hash, p.time ASC
     ),
     signature_ranked AS (
       SELECT logical_messages.*,
              COUNT(*) OVER (
                PARTITION BY network,
                  MD5(LOWER(BTRIM(COALESCE(message, ''))))
              ) AS signature_count
         FROM logical_messages
     )
     SELECT packet_hash, network, sender, message, channel_hash, summary, claimed_ts, observed_at
       FROM signature_ranked
      ORDER BY (signature_count >= $4) DESC,
               signature_count DESC,
               observed_at DESC,
               packet_hash
      LIMIT $5`,
    [windowStart, windowEnd, NETWORKS, cfg.minTransmissions, cfg.maxCandidatePacketRows],
  );

  if (msgRes.rows.length === 0) return [];

  const selectedRows = msgRes.rows.slice(0, cfg.maxMessagesPerRun);
  const hashes = selectedRows.map((r) => r.packet_hash);
  const obsRes = await analyticsPool.query<ObsRow>(
    `WITH ranked_observers AS (
     SELECT p.packet_hash,
            p.rx_node_id        AS observer_id,
            n.lat, n.lon,
            MIN(p.hop_count)    AS min_hop,
            MAX(p.rssi)         AS best_rssi,
            MAX(p.snr)          AS best_snr
     FROM packets p
     JOIN nodes n ON n.node_id = p.rx_node_id
     WHERE p.packet_type = 5
       AND p.time >= $1
       AND p.time < $2
       AND p.network = ANY($3)
       AND p.rx_node_id IS NOT NULL
       AND n.lat IS NOT NULL AND n.lon IS NOT NULL
       AND (n.name IS NULL OR n.name NOT LIKE '%🚫%')
       AND NOT EXISTS (
         SELECT 1 FROM nodes private_node
          WHERE private_node.name LIKE '%🚫%'
            AND private_node.node_id IN (p.rx_node_id, p.src_node_id)
       )
       AND p.packet_hash = ANY($4)
     GROUP BY p.packet_hash, p.rx_node_id, n.lat, n.lon
     )
     SELECT packet_hash, observer_id, lat, lon, min_hop, best_rssi, best_snr
       FROM (
         SELECT ranked_observers.*,
                ROW_NUMBER() OVER (
                  PARTITION BY packet_hash
                  ORDER BY best_snr DESC NULLS LAST, observer_id
                ) AS observer_rank
           FROM ranked_observers
       ) bounded_observers
      WHERE observer_rank <= $5`,
    [windowStart, windowEnd, NETWORKS, hashes, cfg.maxObserversPerMessage],
  );

  const obsByHash = new Map<string, ObserverObservation[]>();
  for (const o of obsRes.rows) {
    const arr = obsByHash.get(o.packet_hash) ?? [];
    arr.push({
      observerId: o.observer_id,
      lat: Number(o.lat),
      lon: Number(o.lon),
      hopCount: o.min_hop == null ? null : Number(o.min_hop),
      rssi: o.best_rssi == null ? null : Number(o.best_rssi),
      snr: o.best_snr == null ? null : Number(o.best_snr),
    });
    obsByHash.set(o.packet_hash, arr);
  }

  const records: MessageRecord[] = [];
  for (const r of selectedRows) {
    const text = (r.message ?? '').slice(0, cfg.maxNormalizedChars);
    if (text.trim().length === 0) continue;
    const claimed = r.claimed_ts != null && /^\d+$/.test(r.claimed_ts) ? Number(r.claimed_ts) : undefined;
    records.push({
      id: r.packet_hash,
      network: r.network,
      sender: r.sender ?? 'unknown',
      text,
      norm: normalizeMessage(text),
      channelHash: r.channel_hash ?? '',
      channelLabel: channelLabel(r.summary, r.channel_hash),
      observedAt: r.observed_at.getTime(),
      claimedTs: claimed,
      observers: obsByHash.get(r.packet_hash) ?? [],
    });
  }
  return records;
}

export interface PersistResult {
  upserted: number;
  removed: number;
  active: number;
}

export interface PersistableIncident {
  incident: Incident;
  origin: OriginEstimate;
  status: IncidentStatus;
  publicJson: PublicIncident;
}

export async function withSpamAnalyzerLease<T>(task: () => Promise<T>): Promise<T | null> {
  const client = await pool.connect();
  const leaseKey = 'ukmesh-spam-message-analyzer-v2';
  try {
    const acquired = await client.query<{ acquired: boolean }>(
      'SELECT pg_try_advisory_lock(hashtext($1)) AS acquired',
      [leaseKey],
    );
    if (!acquired.rows[0]?.acquired) return null;
    try {
      return await task();
    } finally {
      await client.query('SELECT pg_advisory_unlock(hashtext($1))', [leaseKey]);
    }
  } finally {
    client.release();
  }
}

export async function expireSpamIncidentLifecycle(cfg: SpamMessageConfig): Promise<number> {
  const result = await pool.query(
    `UPDATE spam_message_incidents
        SET status = 'closed',
            public_json = jsonb_set(public_json, '{status}', '"closed"'::jsonb, true),
            updated_at = NOW()
      WHERE status = 'active'
        AND last_seen <= NOW() - ($1 * INTERVAL '1 millisecond')`,
    [cfg.ongoingWindowMs],
  );
  return result.rowCount ?? 0;
}

export function selectIncidentEvidenceMembers(
  members: MessageRecord[],
  limit: number,
  representativeText: string,
): MessageRecord[] {
  const boundedLimit = Math.max(1, Math.floor(limit));
  if (members.length <= boundedLimit) return [...members].sort((a, b) => a.observedAt - b.observedAt);
  const chronological = [...members].sort((a, b) => a.observedAt - b.observedAt);
  const selected = new Map<string, MessageRecord>();
  const add = (member: MessageRecord | undefined) => {
    if (member) selected.set(member.id, member);
  };
  add(chronological[0]);
  add(chronological.find((member) => member.text === representativeText));
  const newestBudget = Math.max(1, Math.floor(boundedLimit / 2));
  for (const member of chronological.slice(-newestBudget)) add(member);
  const remaining = boundedLimit - selected.size;
  if (remaining > 0) {
    const stride = Math.max(1, Math.floor(chronological.length / remaining));
    for (let index = 0; index < chronological.length && selected.size < boundedLimit; index += stride) {
      add(chronological[index]);
    }
  }
  for (let index = chronological.length - 1; index >= 0 && selected.size < boundedLimit; index -= 1) {
    add(chronological[index]);
  }
  return [...selected.values()].sort((a, b) => a.observedAt - b.observedAt);
}

/**
 * Replace the in-window incidents with a freshly computed set.
 * - Upserts each incident, preserving the earliest `first_seen` ever recorded.
 * - Deletes only in-window incidents that vanished this run (frozen history
 *   outside the window is left untouched).
 */
export async function persistIncidents(
  items: PersistableIncident[],
  cfg: SpamMessageConfig,
): Promise<PersistResult> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(`SET LOCAL statement_timeout = '${Math.trunc(cfg.dbStatementTimeoutMs)}ms'`);

    let active = 0;

    for (const { incident, origin, status, publicJson } of items) {
      if (status === 'active') active += 1;

      await client.query(
        `INSERT INTO spam_message_incidents
           (incident_key, network, status, first_seen, last_seen, message_count, observer_count,
            channels, username_variants, sender_names, representative_text, canonical_text,
            spam_marker, score, reasons, origin_lat, origin_lon, origin_radius_km, origin_region,
            origin_confidence, origin_level, public_json, updated_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,NOW())
         ON CONFLICT (incident_key) DO UPDATE SET
           status            = EXCLUDED.status,
           first_seen        = LEAST(spam_message_incidents.first_seen, EXCLUDED.first_seen),
           last_seen         = GREATEST(spam_message_incidents.last_seen, EXCLUDED.last_seen),
           message_count     = EXCLUDED.message_count,
           observer_count    = EXCLUDED.observer_count,
           channels          = EXCLUDED.channels,
           username_variants = EXCLUDED.username_variants,
           sender_names      = EXCLUDED.sender_names,
           representative_text = EXCLUDED.representative_text,
           canonical_text    = EXCLUDED.canonical_text,
           spam_marker       = EXCLUDED.spam_marker,
           score             = EXCLUDED.score,
           reasons           = EXCLUDED.reasons,
           origin_lat        = EXCLUDED.origin_lat,
           origin_lon        = EXCLUDED.origin_lon,
           origin_radius_km  = EXCLUDED.origin_radius_km,
           origin_region     = EXCLUDED.origin_region,
           origin_confidence = EXCLUDED.origin_confidence,
           origin_level      = EXCLUDED.origin_level,
           public_json       = EXCLUDED.public_json,
           updated_at        = NOW()`,
        [
          incident.key,
          incident.network,
          status,
          new Date(incident.firstSeen),
          new Date(incident.lastSeen),
          incident.messageCount,
          incident.observerCount,
          incident.channels,
          publicJson.usernameVariants,
          incident.senderNames,
          incident.representativeText,
          incident.canonicalText,
          incident.hasSpamMarker,
          incident.score,
          JSON.stringify(incident.reasons),
          origin.lat,
          origin.lon,
          origin.radiusKm,
          origin.region,
          origin.confidence,
          origin.level,
          JSON.stringify(publicJson),
        ],
      );

      // Replace members for this incident.
      await client.query('DELETE FROM spam_message_members WHERE incident_key = $1', [incident.key]);
      const evidenceMembers = selectIncidentEvidenceMembers(
        incident.members,
        cfg.maxEvidenceMembersPerIncident,
        incident.representativeText,
      );
      for (const m of evidenceMembers) {
        const obs = m.observers;
        const minHop = obs.reduce<number | null>((acc, o) => {
          if (o.hopCount == null) return acc;
          return acc == null ? o.hopCount : Math.min(acc, o.hopCount);
        }, null);
        const bestRssi = obs.reduce<number | null>((acc, o) => {
          if (o.rssi == null) return acc;
          return acc == null ? o.rssi : Math.max(acc, o.rssi);
        }, null);
        const bestSnr = obs.reduce<number | null>((acc, o) => {
          if (o.snr == null) return acc;
          return acc == null ? o.snr : Math.max(acc, o.snr);
        }, null);
        await client.query(
          `INSERT INTO spam_message_members
             (incident_key, packet_hash, network, observed_at, sender, channel_label, channel_hash,
              observer_count, min_hop_count, best_rssi, best_snr)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
           ON CONFLICT (incident_key, packet_hash) DO NOTHING`,
          [
            incident.key,
            m.id,
            m.network,
            new Date(m.observedAt),
            m.sender,
            m.channelLabel,
            m.channelHash,
            new Set(obs.map((o) => o.observerId)).size,
            minHop,
            bestRssi,
            bestSnr,
          ],
        );
      }
    }

    // A bounded pass is not absence evidence. Lifecycle expiry is independent,
    // so never delete a prior incident merely because this page did not see it.
    const removed = 0;

    // Re-evaluate active/closed for ALL incidents based on age (covers ones that
    // aged out of the window this run and should now flip to closed).
    await client.query(
      `UPDATE spam_message_incidents
         SET status = CASE WHEN last_seen > NOW() - ($1 * INTERVAL '1 millisecond')
                           THEN 'active' ELSE 'closed' END
       WHERE status = 'active'
         AND last_seen <= NOW() - ($1 * INTERVAL '1 millisecond')`,
      [cfg.ongoingWindowMs],
    );

    await client.query('COMMIT');
    return { upserted: items.length, removed, active };
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

// ---------------------------------------------------------------------------
// Public reads
// ---------------------------------------------------------------------------

export interface IncidentListOptions {
  status?: IncidentStatus;
  minConfidence?: number;
  limit?: number;
  offset?: number;
}

export async function getPublicIncidents(options: IncidentListOptions = {}): Promise<PublicIncident[]> {
  const params: unknown[] = [];
  const where: string[] = [];
  if (options.status) {
    params.push(options.status);
    where.push(`status = $${params.length}`);
  }
  if (options.minConfidence != null) {
    params.push(options.minConfidence);
    where.push(`score >= $${params.length}`);
  }
  const limit = Math.max(1, Math.min(200, Math.floor(options.limit ?? 100)));
  const offset = Math.max(0, Math.floor(options.offset ?? 0));
  params.push(limit, offset);
  const res = await pool.query<{ public_json: PublicIncident }>(
    `SELECT public_json
     FROM spam_message_incidents
     ${where.length ? `WHERE ${where.join(' AND ')}` : ''}
     ORDER BY (status = 'active') DESC, last_seen DESC
     LIMIT $${params.length - 1} OFFSET $${params.length}`,
    params,
  );
  return res.rows.map((r) => r.public_json);
}

export interface PublicStatus {
  ongoing: boolean;
  activeIncidents: number;
  totalIncidents: number;
  messagesLast24h: number;
  observersInvolved: number;
  lastIncidentAt: string | null;
  updatedAt: string;
}

export async function getPublicStatus(minConfidence = 0): Promise<PublicStatus> {
  const res = await pool.query<{
    active: string;
    total: string;
    last_incident: Date | null;
    msgs_24h: string;
    observers_24h: string;
  }>(
    `SELECT
        COUNT(*) FILTER (WHERE status = 'active')                         AS active,
        COUNT(*)                                                          AS total,
        MAX(last_seen)                                                    AS last_incident,
        COALESCE(SUM(message_count) FILTER (WHERE last_seen > NOW() - INTERVAL '24 hours'), 0)  AS msgs_24h,
        COALESCE(SUM(observer_count) FILTER (WHERE status = 'active'), 0) AS observers_24h
     FROM spam_message_incidents
     WHERE score >= $1`,
    [minConfidence],
  );
  const row = res.rows[0]!;
  const active = Number(row.active ?? 0);
  return {
    ongoing: active > 0,
    activeIncidents: active,
    totalIncidents: Number(row.total ?? 0),
    messagesLast24h: Number(row.msgs_24h ?? 0),
    observersInvolved: Number(row.observers_24h ?? 0),
    lastIncidentAt: row.last_incident ? row.last_incident.toISOString() : null,
    updatedAt: new Date().toISOString(),
  };
}

export interface PublicTimelineEntry {
  observedAt: string;
  channel: string;
  observerCount: number;
  minHopCount: number | null;
  bestRssi: number | null;
  bestSnr: number | null;
}

export async function getPublicIncident(
  key: string,
): Promise<(PublicIncident & { timeline: PublicTimelineEntry[] }) | null> {
  const res = await pool.query<{ public_json: PublicIncident }>(
    `SELECT public_json FROM spam_message_incidents WHERE incident_key = $1`,
    [key],
  );
  if (res.rows.length === 0) return null;
  const incident = res.rows[0]!.public_json;

  const members = await pool.query<{
    observed_at: Date;
    channel_label: string | null;
    observer_count: number;
    min_hop_count: number | null;
    best_rssi: number | null;
    best_snr: number | null;
  }>(
    `SELECT observed_at, channel_label, observer_count, min_hop_count, best_rssi, best_snr
     FROM spam_message_members
     WHERE incident_key = $1
     ORDER BY observed_at ASC
     LIMIT 500`,
    [key],
  );

  const timeline: PublicTimelineEntry[] = members.rows.map((m) => ({
    observedAt: m.observed_at.toISOString(),
    channel: m.channel_label ?? 'unknown',
    observerCount: Number(m.observer_count ?? 0),
    minHopCount: m.min_hop_count == null ? null : Number(m.min_hop_count),
    bestRssi: m.best_rssi == null ? null : Number(m.best_rssi),
    bestSnr: m.best_snr == null ? null : Number(m.best_snr),
  }));

  return { ...incident, timeline };
}
