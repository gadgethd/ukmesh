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
export async function loadRecentMessages(cfg: SpamMessageConfig): Promise<MessageRecord[]> {
  const hours = cfg.analysisWindowHours;

  const msgRes = await analyticsPool.query<MsgRow>(
    `SELECT DISTINCT ON (p.packet_hash)
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
       AND p.time > NOW() - ($1 * INTERVAL '1 hour')
       AND p.network = ANY($2)
       AND p.payload ? 'decrypted'
       AND p.payload->'decrypted' ? 'message'
     ORDER BY p.packet_hash, p.time ASC`,
    [hours, NETWORKS],
  );

  if (msgRes.rows.length === 0) return [];

  const hashes = msgRes.rows.map((r) => r.packet_hash);
  const obsRes = await analyticsPool.query<ObsRow>(
    `SELECT p.packet_hash,
            p.rx_node_id        AS observer_id,
            n.lat, n.lon,
            MIN(p.hop_count)    AS min_hop,
            MAX(p.rssi)         AS best_rssi,
            MAX(p.snr)          AS best_snr
     FROM packets p
     JOIN nodes n ON n.node_id = p.rx_node_id
     WHERE p.packet_type = 5
       AND p.time > NOW() - ($1 * INTERVAL '1 hour')
       AND p.network = ANY($2)
       AND p.rx_node_id IS NOT NULL
       AND n.lat IS NOT NULL AND n.lon IS NOT NULL
       AND p.packet_hash = ANY($3)
     GROUP BY p.packet_hash, p.rx_node_id, n.lat, n.lon`,
    [hours, NETWORKS, hashes],
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
  for (const r of msgRes.rows) {
    const text = r.message ?? '';
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

    const keptKeys: string[] = [];
    let active = 0;

    for (const { incident, origin, status, publicJson } of items) {
      keptKeys.push(incident.key);
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
      for (const m of incident.members) {
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

    // Remove in-window incidents that no longer cluster (stale), keep history.
    let removed = 0;
    if (keptKeys.length > 0) {
      const del = await client.query(
        `DELETE FROM spam_message_incidents
         WHERE last_seen > NOW() - ($1 * INTERVAL '1 hour')
           AND incident_key <> ALL($2)`,
        [cfg.analysisWindowHours, keptKeys],
      );
      removed = del.rowCount ?? 0;
    } else {
      const del = await client.query(
        `DELETE FROM spam_message_incidents
         WHERE last_seen > NOW() - ($1 * INTERVAL '1 hour')`,
        [cfg.analysisWindowHours],
      );
      removed = del.rowCount ?? 0;
    }

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
