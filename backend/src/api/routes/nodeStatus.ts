import { Router } from 'express';
import { query } from '../../db/index.js';
import { resolvePublicNetworkScope } from '../../http/requestScope.js';
import { normalizeObserverQuery } from '../utils/observer.js';
import { expandResolverScope } from '../../networks.js';
import {
  parseBoundedInteger,
  parseBoundedString,
} from '../utils/input.js';

const router = Router();

router.get('/node-status/latest', async (req, res) => {
  try {
    const network = resolvePublicNetworkScope(req.query['network'], req.headers);
    const networkValues = expandResolverScope(network);
    const observer = normalizeObserverQuery(req.query['observer']);

    const params: unknown[] = [networkValues];
    const identityConditions: string[] = [
      `(n.name IS NULL OR n.name NOT LIKE '%🚫%')`,
    ];
    if (observer) {
      params.push(observer);
      identityConditions.push(`n.node_id = meshcore_canonical_node_id($${params.length})`);
    }

    const identityWhereClause = `WHERE ${identityConditions.join(' AND ')}`;
    const result = await query<{
      time: string;
      node_id: string;
      network: string | null;
      battery_mv: number | null;
      uptime_secs: number | null;
      tx_air_secs: number | null;
      rx_air_secs: number | null;
      channel_utilization: number | null;
      air_util_tx: number | null;
      stats: Record<string, unknown> | null;
      name: string | null;
      iata: string | null;
      hardware_model: string | null;
      firmware_version: string | null;
    }>(
      `WITH identity_sources AS MATERIALIZED (
         SELECT n.node_id AS canonical_id,
                n.name,
                n.iata,
                n.hardware_model,
                n.firmware_version,
                source.source_node_id
         FROM node_identity_nodes n
         CROSS JOIN LATERAL unnest(n.identity_source_ids) AS source(source_node_id)
         ${identityWhereClause}
       ), latest AS (
         SELECT DISTINCT ON (s.canonical_id)
           nss.time::text,
           s.canonical_id AS node_id,
           nss.network,
           nss.battery_mv,
           nss.uptime_secs,
           nss.tx_air_secs,
           nss.rx_air_secs,
           nss.channel_utilization,
           nss.air_util_tx,
           nss.stats,
           s.name,
           s.iata,
           s.hardware_model,
           s.firmware_version
         FROM identity_sources s
         JOIN LATERAL (
           SELECT time, network, battery_mv, uptime_secs, tx_air_secs,
                  rx_air_secs, channel_utilization, air_util_tx, stats
           FROM node_status_samples
           WHERE node_id = s.source_node_id
             AND network = ANY($1::text[])
           ORDER BY time DESC
           LIMIT 1
         ) nss ON TRUE
         ORDER BY s.canonical_id, nss.time DESC
       )
       SELECT * FROM latest
       ORDER BY time DESC`,
      params,
    );

    res.json(result.rows);
  } catch (err) {
    console.error('[api] GET /node-status/latest', (err as Error).message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

router.get('/node-status/history', async (req, res) => {
  const requestedNodeId = parseBoundedString(req.query['nodeId'], {
    name: 'nodeId',
    maxLength: 64,
    pattern: /^[0-9a-fA-F]{64}$/,
  });
  const hours = parseBoundedInteger(req.query['hours'], {
    name: 'hours',
    defaultValue: 24,
    min: 1,
    max: 168,
  });
  try {
    const ESTIMATED_AIRTIME_SECONDS_PER_PUBLISH = 0.12;
    const network = resolvePublicNetworkScope(req.query['network'], req.headers);
    const networkValues = expandResolverScope(network);
    const observer = normalizeObserverQuery(req.query['observer']);
    let nodeId = requestedNodeId?.toLowerCase() ?? '';

    if (!nodeId) {
      const params: unknown[] = [];
      const conditions: string[] = [];
      params.push(networkValues);
      conditions.push(`nss.network = ANY($${params.length}::text[])`);
      if (observer) {
        params.push(observer);
        conditions.push(`nss.node_id = meshcore_canonical_node_id($${params.length})`);
      }
      const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
      const latestNode = await query<{ node_id: string }>(
        `SELECT nss.node_id
         FROM node_identity_status_samples nss
         JOIN node_identity_nodes n ON n.node_id = nss.node_id
         ${whereClause}
           AND (n.name IS NULL OR n.name NOT LIKE '%🚫%')
         ORDER BY nss.time DESC
         LIMIT 1`,
        params,
      );
      nodeId = latestNode.rows[0]?.node_id ?? '';
      if (!nodeId) {
        res.json({ nodeId: null, points: [] });
        return;
      }
    }

    const result = await query<{
      time: string;
      battery_mv: number | null;
      uptime_secs: number | null;
      channel_utilization: number | null;
      air_util_tx: number | null;
      heap_free: number | null;
      heap_min_free: number | null;
      uptime_ms: number | null;
      rx_publish_calls: number | null;
      tx_publish_calls: number | null;
      tx_queue_depth: number | null;
      tx_queue_depth_peak: number | null;
    }>(
      `SELECT
         time::text,
         battery_mv,
         uptime_secs,
         channel_utilization,
         air_util_tx,
         CASE
           WHEN jsonb_typeof(stats->'heap_free') = 'number' THEN (stats->>'heap_free')::double precision
           ELSE NULL
         END AS heap_free,
         CASE
           WHEN jsonb_typeof(stats->'heap_min_free') = 'number' THEN (stats->>'heap_min_free')::double precision
           ELSE NULL
         END AS heap_min_free,
         CASE
           WHEN jsonb_typeof(stats->'uptime_ms') = 'number' THEN (stats->>'uptime_ms')::double precision
           ELSE NULL
         END AS uptime_ms,
         CASE
           WHEN jsonb_typeof(stats->'rx_publish_calls') = 'number' THEN (stats->>'rx_publish_calls')::double precision
           ELSE NULL
         END AS rx_publish_calls,
         CASE
           WHEN jsonb_typeof(stats->'tx_publish_calls') = 'number' THEN (stats->>'tx_publish_calls')::double precision
           ELSE NULL
         END AS tx_publish_calls,
         CASE
           WHEN jsonb_typeof(stats->'tx_queue_depth') = 'number' THEN (stats->>'tx_queue_depth')::double precision
           ELSE NULL
         END AS tx_queue_depth,
         CASE
           WHEN jsonb_typeof(stats->'tx_queue_depth_peak') = 'number' THEN (stats->>'tx_queue_depth_peak')::double precision
           ELSE NULL
         END AS tx_queue_depth_peak
       FROM node_identity_nodes n
       CROSS JOIN LATERAL unnest(n.identity_source_ids) AS source(source_node_id)
       JOIN node_status_samples nss ON nss.node_id = source.source_node_id
       WHERE n.node_id = meshcore_canonical_node_id($1)
         AND nss.network = ANY($3::text[])
         AND (n.name IS NULL OR n.name NOT LIKE '%🚫%')
         AND nss.time > NOW() - ($2::text || ' hours')::interval
       ORDER BY time ASC`,
      [nodeId, String(hours), networkValues],
    );

    const points = result.rows.map((row, index) => {
      if (row.channel_utilization != null || row.air_util_tx != null) {
        return row;
      }

      const previous = index > 0 ? result.rows[index - 1] : null;
      const currentUptimeMs = row.uptime_ms;
      const previousUptimeMs = previous?.uptime_ms ?? null;
      const deltaUptimeSeconds =
        currentUptimeMs != null && previousUptimeMs != null && currentUptimeMs > previousUptimeMs
          ? (currentUptimeMs - previousUptimeMs) / 1000
          : null;

      if (!deltaUptimeSeconds || deltaUptimeSeconds <= 0) {
        return row;
      }

      const deltaRxCalls =
        row.rx_publish_calls != null && previous?.rx_publish_calls != null
          ? Math.max(0, row.rx_publish_calls - previous.rx_publish_calls)
          : 0;
      const deltaTxCalls =
        row.tx_publish_calls != null && previous?.tx_publish_calls != null
          ? Math.max(0, row.tx_publish_calls - previous.tx_publish_calls)
          : 0;

      const estimatedTxPct = Math.min(
        100,
        (deltaTxCalls * ESTIMATED_AIRTIME_SECONDS_PER_PUBLISH / deltaUptimeSeconds) * 100,
      );
      const estimatedTotalPct = Math.min(
        100,
        ((deltaTxCalls + deltaRxCalls) * ESTIMATED_AIRTIME_SECONDS_PER_PUBLISH / deltaUptimeSeconds) * 100,
      );

      return {
        ...row,
        channel_utilization: estimatedTotalPct,
        air_util_tx: estimatedTxPct,
      };
    });

    res.json({
      nodeId,
      points,
    });
  } catch (err) {
    console.error('[api] GET /node-status/history', (err as Error).message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

export default router;
