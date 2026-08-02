import { Redis } from 'ioredis';
import { getRedisConnectionOptions, getRedisUrl } from '../platform/config/redis.js';
import {
  admitLinkV3Job,
  linkObservationIdentity,
  linkPhysicalIdentity,
  type LinkQueueAdmission,
} from './linkQueueV3.js';
const LINK_JOB_QUEUE = 'meshcore:link_jobs';
const LINK_QUEUE_V3_PRODUCER_ENABLED = (process.env['LINK_QUEUE_V3_PRODUCER_ENABLED'] ?? '1') === '1';

let pub: Redis | null = null;
let admissionClosed = false;

function getPublisher(): Redis {
  if (admissionClosed) throw new Error('QUEUE_ADMISSION_CLOSED');
  if (pub) return pub;

  pub = new Redis(getRedisUrl(), getRedisConnectionOptions());
  pub.on('error', (e: Error) => console.error('[redis/queue-pub] error', e.message));
  return pub;
}

export function stopQueueAdmission(): void {
  admissionClosed = true;
}

export async function closeQueuePublisher(): Promise<void> {
  admissionClosed = true;
  if (!pub) return;
  await pub.quit();
  pub = null;
}

/** Push a link observation job for a received packet with relay path data. */
export function queueLinkJob(
  packetHash: string,
  rxNodeId: string,
  srcNodeId: string | undefined,
  pathHashes: string[],
  hopCount: number | undefined,
  pathHashSizeBytes: number | undefined,
  generation?: string,
): Promise<LinkQueueAdmission | null> {
  if (admissionClosed) return Promise.resolve({ status: 'worker_unavailable', jobId: null });
  if (!pathHashes.length || (pathHashSizeBytes ?? 1) <= 1) return Promise.resolve(null);
  const publisher = getPublisher();
  if (!LINK_QUEUE_V3_PRODUCER_ENABLED) {
    return publisher.lpush(LINK_JOB_QUEUE, JSON.stringify({
      type: 'observe',
      packet_hash: packetHash,
      rx_node_id: rxNodeId,
      src_node_id: srcNodeId,
      path_hashes: pathHashes,
      hop_count: hopCount,
      path_hash_size_bytes: pathHashSizeBytes,
    })).then(() => ({ status: 'accepted', jobId: `legacy:${packetHash}:${rxNodeId}` }));
  }
  const identity = linkObservationIdentity(packetHash, rxNodeId, generation);
  const logicalIdentity = linkObservationIdentity(packetHash, rxNodeId);
  return admitLinkV3Job(publisher, {
    version: 3,
    type: 'observe',
    job_id: identity.jobId,
    dedupe_key: identity.dedupeKey,
    logical_job_id: logicalIdentity.jobId,
    packet_hash: packetHash,
    rx_node_id: rxNodeId,
    src_node_id: srcNodeId,
    path_hashes: pathHashes,
    hop_count: hopCount,
    path_hash_size_bytes: pathHashSizeBytes,
    ...(generation ? { generation } : {}),
  });
}

/** Push a physical pair evaluation job for two positioned repeater nodes. */
export function queuePhysicalLinkJob(
  nodeAId: string,
  nodeBId: string,
  generation?: string,
): Promise<LinkQueueAdmission | null> {
  if (admissionClosed) return Promise.resolve({ status: 'worker_unavailable', jobId: null });
  if (!nodeAId || !nodeBId || nodeAId === nodeBId) return Promise.resolve(null);
  const publisher = getPublisher();
  const identity = linkPhysicalIdentity(nodeAId, nodeBId, generation);
  if (!LINK_QUEUE_V3_PRODUCER_ENABLED) {
    return publisher.lpush(LINK_JOB_QUEUE, JSON.stringify({
      type: 'physical_pair',
      node_a_id: identity.nodeAId,
      node_b_id: identity.nodeBId,
    })).then(() => ({ status: 'accepted', jobId: `legacy:${identity.nodeAId}:${identity.nodeBId}` }));
  }
  return admitLinkV3Job(publisher, {
    version: 3,
    type: 'physical_pair',
    job_id: identity.jobId,
    dedupe_key: identity.dedupeKey,
    node_a_id: identity.nodeAId,
    node_b_id: identity.nodeBId,
    ...(generation ? { generation } : {}),
  });
}
