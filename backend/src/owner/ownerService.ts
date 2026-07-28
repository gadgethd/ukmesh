import type { OwnerRepository } from './ownerRepository.js';
import type { OwnerSession } from './ownerSession.js';

type OwnerDashboard = {
  totals: {
    ownedNodes: number;
  };
} & Record<string, unknown>;

type OwnerLiveCacheEntry = {
  ts: number;
  data: unknown;
};

type LastHopStrengthPoint = {
  bucket: string;
  lastHopNodeId: string | null;
  lastHopName: string;
  resolution: 'direct' | 'resolved' | 'inferred' | 'unresolved';
  avgSnr: number | null;
  avgRssi: number | null;
  sampleCount: number;
};

type OwnerLastHopResponse = {
  points: LastHopStrengthPoint[];
};

type OwnerLastHopCacheEntry = {
  ts: number;
  data: OwnerLastHopResponse;
  latestBucket: string | null;
};

type OwnerServiceDeps = {
  ownerLiveCacheTtlMs: number;
  ownerLiveCache: Map<string, OwnerLiveCacheEntry>;
  ownerDashboardCacheTtlMs: number;
  ownerLastHopCacheTtlMs: number;
  verifyMqttCredentials: (mqttUsername: string, mqttPassword: string) => Promise<boolean>;
  resolveOwnerNodeIds: (mqttUsername: string) => Promise<string[]>;
  autoLinkOwnerNodeIds: (mqttUsername: string) => Promise<string[]>;
  buildOwnerDashboard: (nodeIds: string[]) => Promise<OwnerDashboard>;
  repository: OwnerRepository;
  invalidateOwnerNodeIdCache: (mqttUsername: string) => void;
};

export function createOwnerService(deps: OwnerServiceDeps) {
  const {
    ownerLiveCacheTtlMs,
    ownerLiveCache,
    ownerDashboardCacheTtlMs,
    ownerLastHopCacheTtlMs,
    verifyMqttCredentials,
    resolveOwnerNodeIds,
    autoLinkOwnerNodeIds,
    buildOwnerDashboard,
    repository,
    invalidateOwnerNodeIdCache,
  } = deps;
  const ownerLastHopCache = new Map<string, OwnerLastHopCacheEntry>();
  const ownerDashboardCache = new Map<string, { ts: number; dashboard: OwnerDashboard; nodeIds: string[] }>();

  function dashboardCacheKey(mqttUsername: string): string {
    return `u:${mqttUsername.trim()}`;
  }

  // Best-effort background warm of the per-node live cache after login so the first
  // switch to each owned node is instant instead of running 9 cold queries. Sequential
  // to avoid a burst of DB load; failures are ignored (the endpoint retries on demand).
  const PREWARM_MAX_NODES = 6;
  async function prewarmOwnerLiveData(ownedNodeIds: string[]): Promise<void> {
    for (const nodeId of ownedNodeIds.slice(0, PREWARM_MAX_NODES)) {
      const cached = ownerLiveCache.get(nodeId);
      if (cached && Date.now() - cached.ts < ownerLiveCacheTtlMs) continue;
      try {
        await getOwnerLiveData(ownedNodeIds, nodeId);
      } catch {
        // ignore prewarm failures
      }
    }
  }

  function mapLastHopRows(rows: Array<{
    bucket: string;
    last_hop_node_id: string | null;
    last_hop_name: string;
    resolution: 'direct' | 'resolved' | 'inferred' | 'unresolved';
    avg_snr: number | null;
    avg_rssi: number | null;
    sample_count: number;
  }>): OwnerLastHopResponse {
    return {
      points: rows.map((row) => ({
        bucket: new Date(row.bucket).toISOString(),
        lastHopNodeId: row.last_hop_node_id,
        lastHopName: row.last_hop_name,
        resolution: row.resolution,
        avgSnr: row.avg_snr == null ? null : Number(row.avg_snr.toFixed(2)),
        avgRssi: row.avg_rssi == null ? null : Number(row.avg_rssi.toFixed(2)),
        sampleCount: Number(row.sample_count ?? 0),
      })),
    };
  }

  function trimLastHopPoints(points: LastHopStrengthPoint[]): LastHopStrengthPoint[] {
    const cutoff = Date.now() - (7 * 24 * 60 * 60 * 1000);
    return points
      .filter((point) => Date.parse(point.bucket) >= cutoff)
      .sort((a, b) => a.bucket.localeCompare(b.bucket));
  }

  function mergeLastHopPoints(existing: LastHopStrengthPoint[], recent: LastHopStrengthPoint[]): LastHopStrengthPoint[] {
    const merged = new Map<string, LastHopStrengthPoint>();
    for (const point of existing) {
      merged.set(`${point.bucket}|${point.lastHopNodeId ?? ''}|${point.lastHopName}|${point.resolution}`, point);
    }
    for (const point of recent) {
      merged.set(`${point.bucket}|${point.lastHopNodeId ?? ''}|${point.lastHopName}|${point.resolution}`, point);
    }
    return trimLastHopPoints(Array.from(merged.values()));
  }

  function latestBucketOf(points: LastHopStrengthPoint[]): string | null {
    return points.length > 0 ? points[points.length - 1]!.bucket : null;
  }

  async function buildOwnerLastHopResponse(nodeId: string, allOwnedNodeIds: string[], since?: string): Promise<OwnerLastHopResponse> {
    const lastHopStrengthResult = await repository.fetchLastHopStrength([nodeId], allOwnedNodeIds, since);
    return mapLastHopRows(lastHopStrengthResult.rows);
  }

  async function authenticateOwner(mqttUsername: string, mqttPassword: string): Promise<{ dashboard: OwnerDashboard; nodeIds: string[] }> {
    const authOk = await verifyMqttCredentials(mqttUsername, mqttPassword);
    if (!authOk) {
      throw new Error('INVALID_MQTT_CREDENTIALS');
    }

    const mappedNodeIds = await autoLinkOwnerNodeIds(mqttUsername);

    const dashboard = await buildOwnerDashboard(mappedNodeIds);
    if (dashboard.totals.ownedNodes < 1) {
      throw new Error('NO_ACTIVE_OWNER_NODE');
    }

    // Seed the session-dashboard cache so the first /owner/session poll (15s later)
    // is a hit, and warm the live cache so the first node switch is instant.
    ownerDashboardCache.set(dashboardCacheKey(mqttUsername), {
      ts: Date.now(),
      dashboard,
      nodeIds: mappedNodeIds,
    });
    void prewarmOwnerLiveData(mappedNodeIds);

    return { dashboard, nodeIds: mappedNodeIds };
  }

  async function getSessionDashboard(session: OwnerSession): Promise<{ dashboard: OwnerDashboard; nodeIds: string[] }> {
    const cacheKey = dashboardCacheKey(session.mqttUsername);
    const freshNodeIds = await resolveOwnerNodeIds(session.mqttUsername);
    if (freshNodeIds.length < 1) throw new Error('NO_ACTIVE_OWNER_NODE');
    const cached = ownerDashboardCache.get(cacheKey);
    if (cached
      && Date.now() - cached.ts < ownerDashboardCacheTtlMs
      && cached.nodeIds.length === freshNodeIds.length
      && freshNodeIds.every((nodeId) => cached.nodeIds.includes(nodeId))) {
      return { dashboard: cached.dashboard, nodeIds: cached.nodeIds };
    }

    const dashboard = await buildOwnerDashboard(freshNodeIds);
    if (dashboard.totals.ownedNodes < 1) {
      throw new Error('NO_ACTIVE_OWNER_NODE');
    }
    ownerDashboardCache.set(cacheKey, { ts: Date.now(), dashboard, nodeIds: freshNodeIds });
    return { dashboard, nodeIds: freshNodeIds };
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

    const {
      ownerNodeResult,
      incomingResult,
      packetResult,
      heardByResult,
      linkHealthResult,
      advertTrendResult,
      telemetryResult,
      packetsSentResult,
      packetsReceivedResult,
    } = await repository.fetchOwnerLiveData(selectedNodeId);

    const ownerNode = ownerNodeResult.rows[0];
    if (!ownerNode) {
      throw new Error('OWNER_NODE_NOT_FOUND');
    }

    // LoRa cannot reach beyond ~250 km direct — filter out senders with known coords that are further away.
    const MAX_DIRECT_SENDER_KM = 150;
    const filteredIncomingRows = (() => {
      const ownerLat = ownerNode.lat;
      const ownerLon = ownerNode.lon;
      if (ownerLat == null || ownerLon == null) return incomingResult.rows;
      return incomingResult.rows.filter((row) => {
        if (row.lat == null || row.lon == null) return true;
        const toRad = (d: number) => (d * Math.PI) / 180;
        const dLat = toRad(row.lat - ownerLat);
        const dLon = toRad(row.lon - ownerLon);
        const a =
          Math.sin(dLat / 2) ** 2 +
          Math.cos(toRad(ownerLat)) * Math.cos(toRad(row.lat)) * Math.sin(dLon / 2) ** 2;
        const distKm = 6371 * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return distKm <= MAX_DIRECT_SENDER_KM;
      });
    })();

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
    const latestTelemetry = telemetry24h[telemetry24h.length - 1];
    if (latestTelemetry?.batteryPct != null && latestTelemetry.batteryPct < 10) {
      alerts.push({ level: 'error', message: `Battery estimate is critically low at ${latestTelemetry.batteryPct.toFixed(0)}%.` });
    } else if (latestTelemetry?.batteryPct != null && latestTelemetry.batteryPct < 20) {
      alerts.push({ level: 'warn', message: `Battery estimate is low at ${latestTelemetry.batteryPct.toFixed(0)}%.` });
    }
    const staleViableLinks = viableLinks.filter((link) => (
      !link.last_observed || Date.now() - Date.parse(link.last_observed) > 7 * 24 * 60 * 60 * 1_000
    ));
    if (viableLinks.length > 0 && staleViableLinks.length === viableLinks.length) {
      alerts.push({ level: 'warn', message: 'All stored viable neighbours are based on evidence older than seven days.' });
    }
    const recentAdverts = advertTrend24h.slice(-6).reduce((sum, point) => sum + point.adverts, 0);
    const previousAdverts = advertTrend24h.slice(-12, -6).reduce((sum, point) => sum + point.adverts, 0);
    if (previousAdverts >= 3 && recentAdverts <= previousAdverts * 0.25) {
      alerts.push({ level: 'warn', message: `Advert activity dropped from ${previousAdverts} to ${recentAdverts} over the latest six-hour window.` });
    }

    const responseData = {
      nodeId: selectedNodeId,
      ownerNode: {
        ...ownerNode,
        advert_count: Number(ownerNode.advert_count ?? 0),
        last_seen: ownerNode.last_seen ? new Date(ownerNode.last_seen).toISOString() : null,
      },
      incomingPeers: filteredIncomingRows.map((row) => ({
        ...row,
        packets_24h: Number(row.packets_24h ?? 0),
        last_seen: row.last_seen ? new Date(row.last_seen).toISOString() : null,
      })),
      heardBy,
      linkHealth,
      advertTrend24h,
      telemetry24h,
      packetsSent24h: Number(packetsSentResult.rows[0]?.packets_24h ?? 0),
      packetsReceived24h: Number(packetsReceivedResult.rows[0]?.packets_24h ?? 0),
      alerts,
      recentPackets: packetResult.rows.map((row) => ({
        ...row,
        time: new Date(row.time).toISOString(),
      })),
    };
    ownerLiveCache.set(selectedNodeId, { ts: Date.now(), data: responseData });
    return responseData;
  }

  async function getOwnerLastHopStrength(ownedNodeIds: string[], requestedNodeId?: string): Promise<OwnerLastHopResponse> {
    if (ownedNodeIds.length < 1) {
      throw new Error('NO_OWNED_NODES');
    }

    const selectedNodeId = requestedNodeId
      ? ownedNodeIds.find((id) => id === requestedNodeId)
      : ownedNodeIds[0];
    if (!selectedNodeId) {
      throw new Error('NODE_NOT_OWNED');
    }

    const cacheKey = selectedNodeId;
    const cacheEntry = ownerLastHopCache.get(cacheKey);
    if (cacheEntry && Date.now() - cacheEntry.ts < ownerLastHopCacheTtlMs) {
      return cacheEntry.data;
    }

    let responseData: OwnerLastHopResponse;
    if (!cacheEntry || !cacheEntry.latestBucket) {
      responseData = await buildOwnerLastHopResponse(selectedNodeId, ownedNodeIds);
    } else {
      const recent = await buildOwnerLastHopResponse(selectedNodeId, ownedNodeIds, cacheEntry.latestBucket);
      responseData = {
        points: mergeLastHopPoints(cacheEntry.data.points, recent.points),
      };
    }

    ownerLastHopCache.set(cacheKey, {
      ts: Date.now(),
      data: responseData,
      latestBucket: latestBucketOf(responseData.points),
    });
    return responseData;
  }

  function clearOwnerSession(mqttUsername: string): void {
    const cacheKey = dashboardCacheKey(mqttUsername);
    const nodeIds = ownerDashboardCache.get(cacheKey)?.nodeIds ?? [];
    ownerDashboardCache.delete(cacheKey);
    for (const nodeId of nodeIds) {
      ownerLiveCache.delete(nodeId);
      ownerLastHopCache.delete(nodeId);
    }
    invalidateOwnerNodeIdCache(mqttUsername);
  }

  return {
    authenticateOwner,
    getSessionDashboard,
    getOwnerLiveData,
    getOwnerLastHopStrength,
    clearOwnerSession,
  };
}
