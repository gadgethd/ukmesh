import type { getNodes } from '../db/index.js';
import { isPrivateNode } from '../api/utils/privateNode.js';

/** Load the shared map-visible set plus public endpoints referenced by the
 * actual, bounded initial packet/message payload. Viable link endpoints are
 * deliberately excluded: a link is not evidence of present activity, and
 * including them re-adds stale markers the map is meant to hide. */
export async function loadInitialMapNodes(
  lookup: typeof getNodes,
  network: string | undefined,
  observer: string | undefined,
  packets: readonly { rx_node_id?: unknown; src_node_id?: unknown }[],
  region?: string,
  signal?: AbortSignal,
) {
  const referencedNodeIds = new Set<string>();
  const remember = (id: unknown) => {
    if (typeof id === 'string' && id.trim()) referencedNodeIds.add(id.trim().toUpperCase());
  };
  for (const packet of packets) {
    remember(packet.src_node_id);
    remember(packet.rx_node_id);
  }
  const nodes = await lookup(network, observer, 'slim', signal, region, {
    mapVisible: true,
    referencedNodeIds: [...referencedNodeIds],
  });
  return nodes.filter((node) => !isPrivateNode(node.name));
}
