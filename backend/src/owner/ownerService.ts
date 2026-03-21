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

type OwnerServiceDeps = {
  ownerLiveCacheTtlMs: number;
  ownerLiveCache: Map<string, OwnerLiveCacheEntry>;
  ownerLastHopCacheTtlMs: number;
  verifyMqttCredentials: (mqttUsername: string, mqttPassword: string) => Promise<boolean>;
  resolveOwnerNodeIds: (mqttUsername: string) => Promise<string[]>;
  autoLinkOwnerNodeIds: (mqttUsername: string) => Promise<string[]>;
  listMappedOwnerNodeIds: () => Promise<string[]>;
  buildOwnerDashboard: (nodeIds: string[]) => Promise<OwnerDashboard>;
  repository: OwnerRepository;
};

export type OwnerService = ReturnType<typeof createOwnerService>;

export function createOwnerService(deps: OwnerServiceDeps) {
  const {
    ownerLiveCacheTtlMs,
    ownerLiveCache,
    ownerLastHopCacheTtlMs,
    verifyMqttCredentials,
    resolveOwnerNodeIds,
    autoLinkOwnerNodeIds,
    listMappedOwnerNodeIds,
    buildOwnerDashboard,
    repository,
  } = deps;
  const ownerLastHopCache = new Map<string, OwnerLiveCacheEntry>();
  let ownerLastHopRefreshInFlight = false;

  async function buildOwnerLastHopResponse(nodeId: string): Promise<{ points: LastHopStrengthPoint[] }> {
    const lastHopStrengthResult = await repository.fetchLastHopStrength([nodeId]);
    return {
      points: lastHopStrengthResult.rows.map((row) => ({
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

  async function refreshOwnerLastHopCacheForNode(nodeId: string): Promise<void> {
    const responseData = await buildOwnerLastHopResponse(nodeId);
    ownerLastHopCache.set(nodeId, { ts: Date.now(), data: responseData });
  }

  async function refreshAllOwnerLastHopCaches(tag: 'initial' | 'scheduled'): Promise<void> {
    if (ownerLastHopRefreshInFlight) {
      console.warn(`[owner-last-hop] ${tag} refresh skipped; previous refresh still running`);
      return;
    }
    ownerLastHopRefreshInFlight = true;
    try {
      const nodeIds = Array.from(new Set((await listMappedOwnerNodeIds()).map((nodeId) => nodeId.trim().toUpperCase()).filter(Boolean)));
      for (const nodeId of nodeIds) {
        try {
          await refreshOwnerLastHopCacheForNode(nodeId);
        } catch (err) {
          console.error(`[owner-last-hop] failed to refresh ${nodeId}:`, (err as Error).message);
        }
      }
      console.log(`[owner-last-hop] ${tag} refresh complete for ${nodeIds.length} node(s)`);
    } catch (err) {
      console.error(`[owner-last-hop] ${tag} refresh failed`, (err as Error).message);
    } finally {
      ownerLastHopRefreshInFlight = false;
    }
  }

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

    const {
      ownerNodeResult,
      incomingResult,
      packetResult,
      heardByResult,
      linkHealthResult,
      advertTrendResult,
      telemetryResult,
    } = await repository.fetchOwnerLiveData(selectedNodeId);

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

  async function getOwnerLastHopStrength(ownedNodeIds: string[], requestedNodeId?: string): Promise<{ points: LastHopStrengthPoint[] }> {
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
      return cacheEntry.data as { points: LastHopStrengthPoint[] };
    }

    const responseData = await buildOwnerLastHopResponse(selectedNodeId);
    ownerLastHopCache.set(cacheKey, { ts: Date.now(), data: responseData });
    return responseData;
  }

  void refreshAllOwnerLastHopCaches('initial');
  const ownerLastHopRefreshTimer = setInterval(() => {
    void refreshAllOwnerLastHopCaches('scheduled');
  }, ownerLastHopCacheTtlMs);
  ownerLastHopRefreshTimer.unref?.();

  return {
    authenticateOwner,
    getSessionDashboard,
    getOwnerLiveData,
    getOwnerLastHopStrength,
  };
}
