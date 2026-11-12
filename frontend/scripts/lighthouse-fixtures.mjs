import { chartStats } from '../test/e2e/fixtures/chartStats.mjs';

// CI exercises populated views using invented data. The owned sample-stack
// benchmark separately measures real origin work, transfer and provider behavior.
const at = '2026-11-12T00:00:00.000Z';
const members = Array.from({ length: 24 }, (_, i) => ({
  nodeId: `${'A'.repeat(62)}${i.toString(16).padStart(2, '0')}`,
  name: `Invented relay ${i + 1}`, role: 2, lat: 52 + i * 0.065,
  lon: -3 + (i % 5) * 0.2, packets: 100 + i, isBridge: false,
}));
const messages = Array.from({ length: 50 }, (_, i) => ({
  time: new Date(Date.parse(at) - i * 1000).toISOString(), packet_hash: `CI${i.toString(16).padStart(14, '0')}`,
  packet_type: 5, rx_node_id: members[0].nodeId, src_node_id: members[1].nodeId,
  summary: `[Public] Invented relay: Synthetic CI message ${i + 1}`,
  payload: { decrypted: { sender: 'Invented relay', message: `Synthetic CI message ${i + 1}` } },
  hop_count: 1, path_hashes: ['AA'], path_hash_size_bytes: 1, network: 'ukmesh',
}));
export const basemapStyle = { version: 8, glyphs: 'https://tiles.openfreemap.org/fonts/{fontstack}/{range}.pbf',
  sources: { openmaptiles: { type: 'vector', tiles: ['https://tiles.openfreemap.org/ci/{z}/{x}/{y}.pbf'] } },
  layers: [{ id: 'background', type: 'background', paint: { 'background-color': '#080d14' } }] };

export function publicFixture(path, query = new URLSearchParams()) {
  const recent = () => messages.map((row, index) => ({ ...row, time: new Date(Date.now() - index * 1000).toISOString() }));
  switch (path) {
    case '/api/runtime-config': return { version: 1, packetArcs: false, heatmap: false, regionScoping: true,
      privacyGeneration: 1, refreshAfterSeconds: 30 };
    case '/api/owner/session': return { ok: false };
    case '/api/spam/messages/status': return { ongoing: false, activeIncidents: 0, totalIncidents: 0,
      messagesLast24h: 0, observersInvolved: 0, lastIncidentAt: null, updatedAt: at };
    case '/api/stats/charts': return chartStats;
    case '/api/stats/observers/live': return { generatedAt: at, windowMinutes: 15, regions: [] };
    case '/api/stats': return { totalNodes: 24, packetsDay: 120, longestHop: 4, internationalNodes: 0 };
    case '/api/regions/summary': return { generatedAt: at, windowDays: 92, regions: [{ name: 'synthetic-region',
      packetsTotal: 1000, memberCount: 24, repeaterMemberCount: 24, confidence: 'confirmed', firstSeen: at,
      lastSeen: at, members }] };
    case '/api/regions/repeaters': return { generatedAt: at, windowDays: 92, repeaters: members.map(member => ({
      ...member, status: 'green', evidence: 'scoped', scopedPackets: 100, unscopedPackets7d: 10,
      regions: { 'synthetic-region': 100 } })) };
    case '/api/regions/unattributed': return { generatedAt: at, windowDays: 92, clusters: [], totals: { clusters: 0, packets: 0, nodes: 0 } };
    case '/api/nodes/map': return { nodes: members.map(member => ({ node_id: member.nodeId, name: member.name,
      lat: member.lat, lon: member.lon, role: 2, is_online: true, last_seen: at })),
      page: { complete: true, nextCursor: null, snapshot: at } };
    case '/api/packets/recent': return recent().slice(0, Number(query.get('limit')) || 50);
    case '/api/messages/recent': return recent();
    case '/api/tags/recent': return [];
    case '/api/observer-activity': return [];
    case '/api/companion-activity': return [];
    default: return undefined;
  }
}
