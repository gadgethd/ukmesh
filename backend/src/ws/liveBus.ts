import { Redis } from 'ioredis';
import { getNodePrivacyRows } from '../repositories/nodeReads.js';
import { getRedisPublisherOptions, getRedisUrl } from '../platform/config/redis.js';
import { RedisPublicationGate, RedisPublicationUnavailableError } from '../queue/publicationGate.js';
import { publicLiveOutcomesTotal } from '../metrics/websocket.js';
import type { LivePacket, WSMessage } from '../types/index.js';
import { isPathObservation, type PathObservation } from '../path-beta/observation.js';
import { PublicWsPrivacyIndex } from './privacy.js';

const LIVE_CHANNEL = 'meshcore:live';
const PATH_CHANNEL = 'meshcore:path-observation';
const PATH_CACHE_RESET = 'reset';

/** The broker-facing process can publish with no HTTP listener or subscriber running. */
export function createLivePublisher() {
  const redis = new Redis(getRedisUrl(), getRedisPublisherOptions());
  redis.on('error', (error) => console.error('[redis/live-publisher]', error.message));
  const gate = new RedisPublicationGate();
  const privacy = new PublicWsPrivacyIndex();
  let pathGap = true;
  let refreshing: Promise<void> | undefined;
  function refresh() {
    if (refreshing) return;
    refreshing = Promise.all([getNodePrivacyRows('ukmesh'), getNodePrivacyRows('test')])
      .then(([production, test]) => { privacy.replace([...production, ...test]); })
      .catch((error: unknown) => console.error('[redis/live-publisher] privacy refresh failed:', (error as Error).message))
      .finally(() => { refreshing = undefined; });
  }
  refresh();
  const timer = setInterval(refresh, 5 * 60_000).unref();

  function publish(channel: string, data: string) {
    void gate.run(redis, Buffer.byteLength(data), () => redis.publish(channel, data)).catch((error: unknown) => {
      if (channel === PATH_CHANNEL) pathGap = true;
      // PostgreSQL supplies reconnect snapshots. Do not queue live events across a privacy-generation change.
      publicLiveOutcomesTotal.inc({ outcome: error instanceof RedisPublicationUnavailableError
        ? `redis_${error.reason}` : 'redis_publish_failed' });
    });
  }
  function resetPathCache() {
    pathGap = false;
    publish(PATH_CHANNEL, PATH_CACHE_RESET);
  }
  // An ingest restart can interrupt the gap between a database commit and its notification.
  redis.on('ready', resetPathCache);

  return {
    packet(packet: LivePacket) {
      if (!privacy.packetHasPrivateParticipant(packet)) {
        publish(LIVE_CHANNEL, JSON.stringify({ type: 'packet', data: packet, ts: Date.now() } satisfies WSMessage));
      }
    },
    nodeSeen(nodeId: string, meta?: { network?: string; observerId?: string }) {
      if (!privacy.isReady || privacy.hasNode(nodeId)) return;
      const ts = Date.now();
      publish(LIVE_CHANNEL, JSON.stringify({ type: 'node_update', data: {
        nodeId: nodeId.toLowerCase(), network: meta?.network, observerId: meta?.observerId?.toLowerCase(), ts,
      }, ts } satisfies WSMessage));
    },
    nodeUpsert(node: Record<string, unknown>) {
      if (!privacy.filterMessage({ type: 'node_upsert', data: node, ts: Date.now() } as WSMessage)) return;
      const data = { ...node };
      for (const key of ['node_id', 'observer_id', 'public_key']) {
        const value = data[key];
        if (typeof value === 'string') data[key] = value.toLowerCase();
      }
      publish(LIVE_CHANNEL, JSON.stringify({ type: 'node_upsert', data, ts: Date.now() }));
    },
    pathObservation(hint: PathObservation) {
      if (pathGap && redis.status === 'ready') resetPathCache();
      publish(PATH_CHANNEL, JSON.stringify(hint));
    },
    async close() {
      clearInterval(timer);
      redis.disconnect();
      await refreshing;
    },
  };
}

export function subscribeLive(options: {
  onLive?: (data: string) => void;
  onPathObservation?: (data: PathObservation) => void;
  onPathGap?: () => void;
}) {
  const redis = new Redis(getRedisUrl(), { ...getRedisPublisherOptions(), autoResubscribe: false });
  const channels = [options.onLive ? LIVE_CHANNEL : null, options.onPathObservation ? PATH_CHANNEL : null]
    .filter((channel): channel is string => channel !== null);
  redis.on('error', (error) => console.error('[redis/live-subscriber]', error.message));
  let closed = false;
  redis.on('close', () => { if (!closed) options.onPathGap?.(); });
  redis.on('ready', () => {
    // A channel-specific ACL denial must not prevent an authorized subscription.
    for (const channel of channels) {
      void redis.subscribe(channel).then(() => {
        if (!closed && channel === PATH_CHANNEL) options.onPathGap?.();
      }).catch((error: unknown) => console.error('[redis/live-subscriber] subscribe failed:', (error as Error).message));
    }
  });
  redis.on('message', (channel, message) => {
    if (channel === LIVE_CHANNEL) { options.onLive?.(message); return; }
    if (channel !== PATH_CHANNEL || message.length > 512) return;
    if (message === PATH_CACHE_RESET) { options.onPathGap?.(); return; }
    let data: unknown;
    try { data = JSON.parse(message); } catch { return; }
    if (isPathObservation(data)) options.onPathObservation?.(data);
  });
  return () => {
    closed = true;
    redis.disconnect();
  };
}
