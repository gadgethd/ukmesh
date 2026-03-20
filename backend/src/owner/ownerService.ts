import type { QueryResultRow } from 'pg';

type QueryFn = <T extends QueryResultRow = QueryResultRow>(
  text: string,
  params?: unknown[],
) => Promise<{ rows: T[] }>;

type OwnerDashboard = {
  totals: {
    ownedNodes: number;
  };
} & Record<string, unknown>;

type OwnerSession = {
  nodeIds: string[];
  exp: number;
  mqttUsername?: string;
};

type OwnerLiveCacheEntry = {
  ts: number;
  data: unknown;
};

type OwnerServiceDeps = {
  ownerLiveCacheTtlMs: number;
  ownerLiveCache: Map<string, OwnerLiveCacheEntry>;
  verifyMqttCredentials: (mqttUsername: string, mqttPassword: string) => Promise<boolean>;
  resolveOwnerNodeIds: (mqttUsername: string) => Promise<string[]>;
  autoLinkOwnerNodeIds: (mqttUsername: string) => Promise<string[]>;
  buildOwnerDashboard: (nodeIds: string[]) => Promise<OwnerDashboard>;
  query: QueryFn;
};

export type OwnerService = ReturnType<typeof createOwnerService>;

export function createOwnerService(deps: OwnerServiceDeps) {
  const {
    ownerLiveCacheTtlMs,
    ownerLiveCache,
    verifyMqttCredentials,
    resolveOwnerNodeIds,
    autoLinkOwnerNodeIds,
    buildOwnerDashboard,
    query,
  } = deps;

  async function authenticateOwner(mqttUsername: string, mqttPassword: string): Promise<{ dashboard: OwnerDashboard; nodeIds: string[] }> {
    const authOk = await verifyMqttCredentials(mqttUsername, mqttPassword);
    if (!authOk) {
      throw new Error('INVALID_MQTT_CREDENTIALS');
    }

    let mappedNodeIds = await resolveOwnerNodeIds(mqttUsername);
    if (mappedNodeIds.length < 1) {
      mappedNodeIds = await autoLinkOwnerNodeIds(mqttUsername);
    }

    const dashboard = await buildOwnerDashboard(mappedNodeIds);
    if (dashboard.totals.ownedNodes < 1) {
      throw new Error('NO_ACTIVE_OWNER_NODE');
    }

    return { dashboard, nodeIds: mappedNodeIds };
  }

  async function getSessionDashboard(session: OwnerSession): Promise<{ dashboard: OwnerDashboard; nodeIds: string[] }> {
    const freshNodeIds = session.mqttUsername
      ? await resolveOwnerNodeIds(session.mqttUsername)
      : [];
    const nodeIds = freshNodeIds.length > 0 ? freshNodeIds : session.nodeIds;
    const dashboard = await buildOwnerDashboard(nodeIds);
    if (dashboard.totals.ownedNodes < 1) {
      throw new Error('NO_ACTIVE_OWNER_NODE');
    }
    return { dashboard, nodeIds };
  }

  async function getOwnerLiveData(ownedNodeIds: string[], requestedNodeId?: string): Promise<unknown> {
    if (ownedNodeIds.length < 1) {
      throw new Error('NO_OWNED_NODES');
    }

    const selectedNodeId = requestedNodeId
      ? ownedNodeIds.find((id) => id === requestedNodeId)
      : ownedNodeIds[0];
    if (!selectedNodeId) {
      throw new Error('NODE_NOT_OWNED');
    }

    const cacheEntry = ownerLiveCache.get(selectedNodeId);
    if (cacheEntry && Date.now() - cacheEntry.ts < ownerLiveCacheTtlMs) {
      return cacheEntry.data;
    }

    const [
      ownerNodeResult,
      incomingResult,
      packetResult,
      heardByResult,
      linkHealthResult,
      advertTrendResult,
      telemetryResult,
    ] = await Promise.all([
      query<{
        node_id: string;
        name: string | null;
        network: string;
        iata: string | null;
        advert_count: number | null;
        last_seen: string | null;
        lat: number | null;
        lon: number | null;
        role: number | null;
      }>(
        `SELECT node_id, name, network, iata, advert_count, last_seen, lat, lon, role
         FROM nodes
         WHERE node_id = $1
         LIMIT 1`,
        [selectedNodeId],
      ),
      query<{
        node_id: string;
        name: string | null;
        network: string | null;
        iata: string | null;
        lat: number | null;
        lon: number | null;
        packets_24h: number;
        last_seen: string | null;
      }>(
        `SELECT
           p.src_node_id AS node_id,
           n.name,
           n.network,
           n.iata,
           n.lat,
           n.lon,
           COUNT(*)::int AS packets_24h,
           MAX(p.time)::text AS last_seen
         FROM packets p
         LEFT JOIN nodes n ON n.node_id = p.src_node_id
         WHERE p.rx_node_id = $1
           AND p.hop_count = 0
           AND p.src_node_id IS NOT NULL
           AND p.src_node_id <> $1
           AND p.time > NOW() - INTERVAL '24 hours'
         GROUP BY p.src_node_id, n.name, n.network, n.iata, n.lat, n.lon
         ORDER BY packets_24h DESC
         LIMIT 250`,
        [selectedNodeId],
      ),
      query<{
        time: string;
        packet_type: number | null;
        route_type: number | null;
        hop_count: number | null;
        packet_hash: string | null;
        src_node_id: string | null;
        src_node_name: string | null;
        sender: string | null;
        body: string | null;
      }>(
        `WITH ranked AS (
           SELECT
             p.time,
             p.packet_type,
             p.route_type,
             p.hop_count,
             p.packet_hash,
             p.src_node_id,
             p.payload,
             ROW_NUMBER() OVER (
               PARTITION BY COALESCE(
                 p.packet_hash,
                 CONCAT_WS(':',
                   COALESCE(p.src_node_id, ''),
                   COALESCE(p.packet_type::text, ''),
                   COALESCE(p.route_type::text, ''),
                   COALESCE(p.hop_count::text, ''),
                   COALESCE(p.payload->'decrypted'->>'sender', ''),
                   COALESCE(p.payload->'decrypted'->>'text', ''),
                   DATE_TRUNC('second', p.time)::text
                 )
               )
               ORDER BY p.time DESC
             ) AS rn
           FROM packets p
           WHERE p.rx_node_id = $1
         )
         SELECT
           r.time::text AS time,
           r.packet_type,
           r.route_type,
           r.hop_count,
           r.packet_hash,
           r.src_node_id,
           src.name AS src_node_name,
           COALESCE(r.payload->'decrypted'->>'sender', src.name, r.src_node_id) AS sender,
           COALESCE(
             r.payload->'decrypted'->>'text',
             r.payload->'decrypted'->>'message',
             r.payload->'decrypted'->>'body',
             r.payload->'decoded'->>'text',
             r.payload->>'message'
           ) AS body
         FROM ranked r
         LEFT JOIN nodes src ON src.node_id = r.src_node_id
         WHERE r.rn = 1
         ORDER BY r.time DESC
         LIMIT 9`,
        [selectedNodeId],
      ),
      query<{
        node_id: string;
        name: string | null;
        network: string | null;
        iata: string | null;
        lat: number | null;
        lon: number | null;
        packets_24h: number;
        packets_7d: number;
        last_seen: string | null;
        best_hops: number | null;
      }>(
        `SELECT
           p.rx_node_id AS node_id,
           n.name,
           n.network,
           n.iata,
           n.lat,
           n.lon,
           COUNT(DISTINCT CASE WHEN p.time > NOW() - INTERVAL '24 hours' THEN p.packet_hash END)::int AS packets_24h,
           COUNT(DISTINCT p.packet_hash)::int AS packets_7d,
           MAX(p.time)::text AS last_seen,
           MIN(p.hop_count) AS best_hops
         FROM packets p
         LEFT JOIN nodes n ON n.node_id = p.rx_node_id
         WHERE p.src_node_id = $1
           AND p.rx_node_id IS NOT NULL
           AND p.rx_node_id <> $1
           AND p.time > NOW() - INTERVAL '7 days'
         GROUP BY p.rx_node_id, n.name, n.network, n.iata, n.lat, n.lon
         ORDER BY packets_24h DESC, packets_7d DESC, last_seen DESC
         LIMIT 20`,
        [selectedNodeId],
      ),
      query<{
        peer_node_id: string;
        peer_name: string | null;
        peer_network: string | null;
        owner_to_peer: number;
        peer_to_owner: number;
        observed_count: number;
        itm_path_loss_db: number | null;
        itm_viable: boolean | null;
        force_viable: boolean;
        last_observed: string | null;
      }>(
        `SELECT
           CASE WHEN nl.node_a_id = $1 THEN nl.node_b_id ELSE nl.node_a_id END AS peer_node_id,
           peer.name AS peer_name,
           peer.network AS peer_network,
           CASE WHEN nl.node_a_id = $1 THEN nl.count_a_to_b ELSE nl.count_b_to_a END AS owner_to_peer,
           CASE WHEN nl.node_a_id = $1 THEN nl.count_b_to_a ELSE nl.count_a_to_b END AS peer_to_owner,
           nl.observed_count,
           nl.itm_path_loss_db,
           nl.itm_viable,
           nl.force_viable,
           nl.last_observed::text AS last_observed
         FROM node_links nl
         JOIN nodes peer ON peer.node_id = CASE WHEN nl.node_a_id = $1 THEN nl.node_b_id ELSE nl.node_a_id END
         WHERE (nl.node_a_id = $1 OR nl.node_b_id = $1)
           AND (
             nl.force_viable = true
             OR nl.itm_viable = true
             OR (nl.itm_path_loss_db IS NOT NULL AND nl.itm_path_loss_db <= 137.5)
           )
         ORDER BY
           COALESCE(nl.itm_viable, false) DESC,
           nl.force_viable DESC,
           nl.observed_count DESC,
           nl.itm_path_loss_db ASC NULLS LAST
         LIMIT 12`,
        [selectedNodeId],
      ),
      query<{ bucket: string; adverts: number }>(
        `SELECT
           time_bucket('1 hour', time)::text AS bucket,
           COUNT(DISTINCT packet_hash)::int AS adverts
         FROM packets
         WHERE src_node_id = $1
           AND packet_type = 4
           AND time > NOW() - INTERVAL '24 hours'
         GROUP BY bucket
         ORDER BY bucket`,
        [selectedNodeId],
      ),
      query<{
        time: string;
        battery_mv: number | null;
        uptime_secs: string | null;
        tx_air_secs: string | null;
        rx_air_secs: string | null;
        channel_utilization: number | null;
        air_util_tx: number | null;
        uptime_ms: number | null;
        rx_publish_calls: number | null;
        tx_publish_calls: number | null;
      }>(
        `SELECT
           time::text AS time,
           battery_mv,
           uptime_secs::text AS uptime_secs,
           tx_air_secs::text AS tx_air_secs,
           rx_air_secs::text AS rx_air_secs,
           channel_utilization,
           air_util_tx,
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
           END AS tx_publish_calls
         FROM node_status_samples
         WHERE node_id = $1
           AND time > NOW() - INTERVAL '24 hours'
         ORDER BY time ASC`,
        [selectedNodeId],
      ),
    ]);

    const ownerNode = ownerNodeResult.rows[0];
    if (!ownerNode) {
      throw new Error('OWNER_NODE_NOT_FOUND');
    }

    const heardBy = heardByResult.rows.map((row) => ({
      ...row,
      packets_24h: Number(row.packets_24h ?? 0),
      packets_7d: Number(row.packets_7d ?? 0),
      best_hops: row.best_hops == null ? null : Number(row.best_hops),
      last_seen: row.last_seen ? new Date(row.last_seen).toISOString() : null,
    }));

    const linkHealth = linkHealthResult.rows.map((row) => ({
      ...row,
      owner_to_peer: Number(row.owner_to_peer ?? 0),
      peer_to_owner: Number(row.peer_to_owner ?? 0),
      observed_count: Number(row.observed_count ?? 0),
      itm_path_loss_db: row.itm_path_loss_db == null ? null : Number(row.itm_path_loss_db),
      itm_viable: row.itm_viable == null ? null : Boolean(row.itm_viable),
      force_viable: Boolean(row.force_viable),
      last_observed: row.last_observed ? new Date(row.last_observed).toISOString() : null,
    }));

    const advertTrend24h = (() => {
      const byHour = new Map<string, number>();
      for (const row of advertTrendResult.rows) {
        byHour.set(new Date(row.bucket).toISOString(), Number(row.adverts ?? 0));
      }
      const series: Array<{ bucket: string; adverts: number }> = [];
      const now = new Date();
      now.setUTCMinutes(0, 0, 0);
      for (let i = 23; i >= 0; i--) {
        const bucket = new Date(now.getTime() - i * 60 * 60 * 1000).toISOString();
        series.push({ bucket, adverts: byHour.get(bucket) ?? 0 });
      }
      return series;
    })();

    const telemetry24h = (() => {
      const ESTIMATED_AIRTIME_SECONDS_PER_PUBLISH = 0.12;
      const clamp = (value: number, min: number, max: number) => Math.min(max, Math.max(min, value));
      const bucketMs = 5 * 60 * 1000;
      type Point = {
        bucket: string;
        batteryPct: number | null;
        batteryMv: number | null;
        uptimeSecs: number | null;
        channelUtilPct: number | null;
        airUtilTxPct: number | null;
      };

      const samples = telemetryResult.rows.map((row) => ({
        timeMs: new Date(row.time).getTime(),
        batteryMv: row.battery_mv == null ? null : Number(row.battery_mv),
        uptimeSecs: row.uptime_secs == null ? null : Number(row.uptime_secs),
        uptimeMs: row.uptime_ms == null ? null : Number(row.uptime_ms),
        txAirSecs: row.tx_air_secs == null ? null : Number(row.tx_air_secs),
        rxAirSecs: row.rx_air_secs == null ? null : Number(row.rx_air_secs),
        channelUtilization: row.channel_utilization == null ? null : Number(row.channel_utilization),
        airUtilTx: row.air_util_tx == null ? null : Number(row.air_util_tx),
        rxPublishCalls: row.rx_publish_calls == null ? null : Number(row.rx_publish_calls),
        txPublishCalls: row.tx_publish_calls == null ? null : Number(row.tx_publish_calls),
      }));

      const bucketed = new Map<number, Point>();
      for (let i = 0; i < samples.length; i++) {
        const sample = samples[i]!;
        const prev = i > 0 ? samples[i - 1]! : null;
        const batteryPct = sample.batteryMv == null
          ? null
          : clamp(((sample.batteryMv - 3200) / 1000) * 100, 0, 100);

        let channelUtilPct = sample.channelUtilization;
        let airUtilTxPct = sample.airUtilTx;

        if ((channelUtilPct == null || airUtilTxPct == null) && prev) {
          const uptimeDelta = (sample.uptimeSecs ?? 0) - (prev.uptimeSecs ?? 0);
          const txDelta = (sample.txAirSecs ?? 0) - (prev.txAirSecs ?? 0);
          const rxDelta = (sample.rxAirSecs ?? 0) - (prev.rxAirSecs ?? 0);
          if (uptimeDelta > 0 && txDelta >= 0 && rxDelta >= 0) {
            if (airUtilTxPct == null) {
              airUtilTxPct = clamp((txDelta / uptimeDelta) * 100, 0, 100);
            }
            if (channelUtilPct == null) {
              channelUtilPct = clamp(((txDelta + rxDelta) / uptimeDelta) * 100, 0, 100);
            }
          }
        }

        if ((channelUtilPct == null || airUtilTxPct == null) && prev) {
          const currentUptimeMs = sample.uptimeMs;
          const previousUptimeMs = prev.uptimeMs;
          const deltaUptimeSeconds =
            currentUptimeMs != null && previousUptimeMs != null && currentUptimeMs > previousUptimeMs
              ? (currentUptimeMs - previousUptimeMs) / 1000
              : 0;
          const deltaRxCalls =
            sample.rxPublishCalls != null && prev.rxPublishCalls != null
              ? Math.max(0, sample.rxPublishCalls - prev.rxPublishCalls)
              : 0;
          const deltaTxCalls =
            sample.txPublishCalls != null && prev.txPublishCalls != null
              ? Math.max(0, sample.txPublishCalls - prev.txPublishCalls)
              : 0;

          if (deltaUptimeSeconds > 0) {
            if (airUtilTxPct == null) {
              airUtilTxPct = clamp(
                (deltaTxCalls * ESTIMATED_AIRTIME_SECONDS_PER_PUBLISH / deltaUptimeSeconds) * 100,
                0,
                100,
              );
            }
            if (channelUtilPct == null) {
              channelUtilPct = clamp(
                ((deltaTxCalls + deltaRxCalls) * ESTIMATED_AIRTIME_SECONDS_PER_PUBLISH / deltaUptimeSeconds) * 100,
                0,
                100,
              );
            }
          }
        }

        const bucketStartMs = Math.floor(sample.timeMs / bucketMs) * bucketMs;
        bucketed.set(bucketStartMs, {
          bucket: new Date(bucketStartMs).toISOString(),
          batteryPct: batteryPct == null ? null : Number(batteryPct.toFixed(1)),
          batteryMv: sample.batteryMv,
          uptimeSecs: sample.uptimeSecs == null ? null : Math.max(0, Math.round(sample.uptimeSecs)),
          channelUtilPct: channelUtilPct == null ? null : Number(channelUtilPct.toFixed(2)),
          airUtilTxPct: airUtilTxPct == null ? null : Number(airUtilTxPct.toFixed(2)),
        });
      }

      return Array.from(bucketed.values()).sort((a, b) => a.bucket.localeCompare(b.bucket));
    })();

    const alerts: Array<{ level: 'info' | 'warn' | 'error'; message: string }> = [];
    const ownerLastSeenMs = ownerNode.last_seen ? new Date(ownerNode.last_seen).getTime() : 0;
    const minsSinceSeen = ownerLastSeenMs ? Math.max(0, Math.round((Date.now() - ownerLastSeenMs) / 60000)) : null;
    const adverts24h = advertTrend24h.reduce((sum, point) => sum + point.adverts, 0);
    const viableLinks = linkHealth.filter((link) => link.itm_viable || link.force_viable);
    const roleLabel = ownerNode.role === 1 ? 'Companion' : ownerNode.role === 3 ? 'Room Server' : 'Repeater';
    if (minsSinceSeen == null) alerts.push({ level: 'error', message: `No last-seen timestamp is available for this ${roleLabel.toLowerCase()}.` });
    else if (minsSinceSeen >= 120) alerts.push({ level: 'error', message: `${roleLabel} has not been seen for ${minsSinceSeen} minutes.` });
    else if (minsSinceSeen >= 30) alerts.push({ level: 'warn', message: `${roleLabel} has been quiet for ${minsSinceSeen} minutes.` });
    else alerts.push({ level: 'info', message: `${roleLabel} is active and has checked in recently.` });

    if (adverts24h < 1) alerts.push({ level: 'warn', message: `No advert packets from this ${roleLabel.toLowerCase()} were recorded in the last 24 hours.` });
    if (heardBy.length < 1) alerts.push({ level: 'warn', message: `No other nodes have heard this ${roleLabel.toLowerCase()} in the last 7 days.` });
    if (viableLinks.length < 1) alerts.push({ level: 'warn', message: `No viable RF links are currently stored for this ${roleLabel.toLowerCase()}.` });

    const responseData = {
      nodeId: selectedNodeId,
      ownerNode: {
        ...ownerNode,
        advert_count: Number(ownerNode.advert_count ?? 0),
        last_seen: ownerNode.last_seen ? new Date(ownerNode.last_seen).toISOString() : null,
      },
      incomingPeers: incomingResult.rows.map((row) => ({
        ...row,
        packets_24h: Number(row.packets_24h ?? 0),
        last_seen: row.last_seen ? new Date(row.last_seen).toISOString() : null,
      })),
      heardBy,
      linkHealth,
      advertTrend24h,
      telemetry24h,
      alerts,
      recentPackets: packetResult.rows.map((row) => ({
        ...row,
        time: new Date(row.time).toISOString(),
      })),
    };
    ownerLiveCache.set(selectedNodeId, { ts: Date.now(), data: responseData });
    return responseData;
  }

  return {
    authenticateOwner,
    getSessionDashboard,
    getOwnerLiveData,
  };
}
