import { Redis } from 'ioredis';

const VIEWSHED_JOB_QUEUE = 'meshcore:viewshed_jobs';
const VIEWSHED_PENDING_SET = 'meshcore:viewshed_pending';
const LINK_JOB_QUEUE = 'meshcore:link_jobs';

let pub: Redis | null = null;

function getPublisher(): Redis {
  if (pub) return pub;

  const redisUrl = process.env['REDIS_URL'] ?? 'redis://redis:6379';
  pub = new Redis(redisUrl);
  pub.on('error', (e: Error) => console.error('[redis/queue-pub] error', e.message));
  return pub;
}

export async function closeQueuePublisher(): Promise<void> {
  if (!pub) return;
  await pub.quit();
  pub = null;
}

/** Push a viewshed calculation job for a node with a known position. */
export function queueViewshedJob(nodeId: string, lat: number, lon: number): void {
  const publisher = getPublisher();
  const job = JSON.stringify({ node_id: nodeId, lat, lon });
  void publisher
    .sadd(VIEWSHED_PENDING_SET, nodeId)
    .then((added) => {
      if (added === 1) {
        return publisher.lpush(VIEWSHED_JOB_QUEUE, job);
      }
      return 0;
    })
    .catch((e: Error) => console.error('[redis/queue-pub] viewshed enqueue error', e.message));
}

/** Push a link observation job for a received packet with relay path data. */
export function queueLinkJob(
  rxNodeId: string,
  srcNodeId: string | undefined,
  pathHashes: string[],
  hopCount: number | undefined,
): void {
  if (!pathHashes.length) return;
  void getPublisher().lpush(LINK_JOB_QUEUE, JSON.stringify({
    rx_node_id: rxNodeId,
    src_node_id: srcNodeId,
    path_hashes: pathHashes,
    hop_count: hopCount,
  }));
}
