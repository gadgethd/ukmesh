import type { LivePacket, WSMessage } from '../types/index.js';
import { isPrivateNode } from '../api/utils/privateNode.js';

export class PublicWsPrivacyIndex {
  private readonly nodeIds = new Set<string>();
  private ready = false;
  private revision = 0;

  constructor(private readonly onChange: () => void = () => {}) {}

  get isReady(): boolean {
    return this.ready;
  }

  get currentRevision(): number {
    return this.revision;
  }

  replace(nodes: Array<{ node_id?: unknown; name?: unknown }>): boolean {
    const next = new Set<string>();
    for (const node of nodes) {
      if (isPrivateNode(typeof node.name === 'string' ? node.name : null)) {
        const normalized = String(node.node_id ?? '').trim().toLowerCase();
        if (/^[0-9a-f]{64}$/.test(normalized)) next.add(normalized);
      }
    }
    const changed = !this.ready
      || next.size !== this.nodeIds.size
      || [...next].some((nodeId) => !this.nodeIds.has(nodeId));
    if (changed) {
      this.nodeIds.clear();
      for (const nodeId of next) this.nodeIds.add(nodeId);
    }
    this.ready = true;
    if (changed) {
      this.revision += 1;
      this.onChange();
    }
    return changed;
  }

  remember(nodeId: string): boolean {
    const normalized = nodeId.trim().toLowerCase();
    if (!/^[0-9a-f]{64}$/.test(normalized) || this.nodeIds.has(normalized)) return false;
    this.nodeIds.add(normalized);
    this.revision += 1;
    this.onChange();
    return true;
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
