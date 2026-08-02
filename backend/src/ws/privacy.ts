import type { LivePacket, WSMessage } from '../types/index.js';
import { isPrivateNode } from '../api/utils/privateNode.js';

export class PublicWsPrivacyIndex {
  private readonly nodeIds = new Set<string>();
  private ready = false;

  get isReady(): boolean {
    return this.ready;
  }

  replace(nodes: Array<{ node_id?: unknown; name?: unknown }>): void {
    this.nodeIds.clear();
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
    // MQTT ingest and persisted packet rows share the same materialized
    // visibility decision. Missing state is rejected rather than re-running
    // path-prefix privacy logic on every WebSocket subscriber.
    return packet.visibilityOk !== true;
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
    if (message.type === 'link_update') {
      const data = message.data as { node_a_id?: string; node_b_id?: string };
      return this.hasNode(data.node_a_id) || this.hasNode(data.node_b_id) ? null : message;
    }
    return message;
  }
}
