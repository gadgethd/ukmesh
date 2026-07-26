import type { LivePacket, WSMessage } from '../types/index.js';
import { isPrivateNode } from '../api/utils/privateNode.js';

export class PublicWsPrivacyIndex {
  private readonly nodeIds = new Set<string>();
  private readonly prefixes = new Set<string>();
  private ready = false;

  get isReady(): boolean {
    return this.ready;
  }

  replace(nodes: Array<{ node_id?: unknown; name?: unknown }>): void {
    this.nodeIds.clear();
    this.prefixes.clear();
    for (const node of nodes) {
      if (isPrivateNode(typeof node.name === 'string' ? node.name : null)) {
        this.remember(String(node.node_id ?? ''));
      }
    }
    this.ready = true;
  }

  remember(nodeId: string): void {
    const normalized = nodeId.trim().toLowerCase();
    if (!/^[0-9a-f]{64}$/.test(normalized)) return;
    this.nodeIds.add(normalized);
    for (const length of [2, 4, 6]) this.prefixes.add(normalized.slice(0, length));
  }

  hasNode(nodeId: unknown): boolean {
    return this.nodeIds.has(String(nodeId ?? '').toLowerCase());
  }

  packetHasPrivateParticipant(packet: Partial<LivePacket>): boolean {
    if (!this.ready) return true;
    if (packet.packetType === 4) {
      const payload = packet.payload;
      const appData = payload?.['appData'];
      const name = (
        appData && typeof appData === 'object'
          ? (appData as Record<string, unknown>)['name']
          : payload?.['name']
      );
      if (isPrivateNode(typeof name === 'string' ? name : null)) {
        // Advert payload privacy is authoritative for this packet. Remember a
        // valid identity for later events, but suppress even malformed adverts
        // so the first opt-out packet cannot race the batched node upsert.
        this.remember(String(packet.srcNodeId ?? ''));
        return true;
      }
    }
    if (this.hasNode(packet.rxNodeId) || this.hasNode(packet.srcNodeId)) return true;
    if (packet.path == null) return false;
    if (!Array.isArray(packet.path)) return true;
    const size = Number(packet.pathHashSizeBytes ?? 0);
    if (packet.path.length > 0 && (!Number.isInteger(size) || size < 1 || size > 3)) return true;
    if (size < 1 || size > 3) return false;
    return packet.path.some((hash) => (
      typeof hash !== 'string'
      || hash.length !== size * 2
      || !/^[0-9a-f]+$/i.test(hash)
      || this.prefixes.has(hash.toLowerCase())
    ));
  }

  filterMessage(message: WSMessage): WSMessage | null {
    if (message.type === 'packet') {
      return this.packetHasPrivateParticipant(message.data as Partial<LivePacket>) ? null : message;
    }
    if (message.type === 'node_upsert') {
      const node = message.data as Record<string, unknown>;
      const nodeId = String(node['node_id'] ?? '');
      if (isPrivateNode(typeof node['name'] === 'string' ? node['name'] : null)) {
        this.remember(nodeId);
        return null;
      }
      return this.hasNode(nodeId) ? null : message;
    }
    if (message.type === 'node_update') {
      return this.hasNode((message.data as { nodeId?: string }).nodeId) ? null : message;
    }
    if (message.type === 'coverage_update') {
      return this.hasNode((message.data as { node_id?: string }).node_id) ? null : message;
    }
    if (message.type === 'link_update') {
      const data = message.data as { node_a_id?: string; node_b_id?: string };
      return this.hasNode(data.node_a_id) || this.hasNode(data.node_b_id) ? null : message;
    }
    return message;
  }
}
