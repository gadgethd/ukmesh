import { test } from 'node:test';
import assert from 'node:assert/strict';
import { resolveSpamOrigin } from './spamResolver.js';
import { DEFAULT_SPAM_MESSAGE_CONFIG as CFG } from './config.js';

// Fake DB: the resolver issues three queries — receptions (path + observer
// hop/location), located nodes, and node_links adjacency. We answer all three
// from in-memory fixtures.
type Row = Record<string, unknown>;
type Reception = { rx_node_id: string; hop_count: number; path_hashes: string[]; olat: number; olon: number };
type NodeLoc = { node_id: string; name: string | null; lat: number; lon: number };
type Link = { node_a_id: string; node_b_id: string };

function fakeQuery(receptions: Reception[], nodes: NodeLoc[], links: Link[] = []) {
  return async <T extends Row = Row>(text: string): Promise<{ rows: T[] }> => {
    if (/FROM packets/.test(text)) return { rows: receptions as unknown as T[] };
    if (/FROM node_links/.test(text)) return { rows: links as unknown as T[] };
    if (/FROM nodes/.test(text)) return { rows: nodes as unknown as T[] };
    return { rows: [] };
  };
}

const SOLENT = { lat: 50.757, lon: -1.549 };
function rx(n: number, path: string[], hop = 2): Reception {
  return { rx_node_id: 'OBS1', hop_count: hop, path_hashes: path, olat: SOLENT.lat, olon: SOLENT.lon };
}

test('chain-walk anchors the first repeater near the closest observer', async () => {
  // Closest observer (2 hops, Solent) heard "5F -> A1 -> observer". Walking back:
  // A1 resolves near the observer, then 5F resolves to the IW node next to it —
  // not the northern 5F node.
  const receptions = Array.from({ length: 6 }, () => rx(0, ['5F', 'A1']));
  const nodes: NodeLoc[] = [
    { node_id: '5FAA11', name: 'IW Repeater', lat: 50.596, lon: -1.203 },
    { node_id: '5FBB22', name: 'Northern Repeater', lat: 53.16, lon: -1.66 },
    { node_id: 'A1CC33', name: 'Second hop', lat: 50.8, lon: -1.3 },
  ];
  const origin = await resolveSpamOrigin(['p1', 'p2', 'p3', 'p4', 'p5', 'p6'], 'ukmesh', fakeQuery(receptions, nodes), CFG);
  assert.ok(origin, 'should resolve a path origin');
  assert.ok(Math.abs(origin!.lat! - 50.596) < 0.1, `anchored on the IW repeater, got ${origin!.lat}`);
  assert.equal(origin!.region, 'Hampshire & Solent');
  assert.equal(origin!.level, 'high');
  assert.ok(origin!.reasons.some((r) => /adjacency|closest observer/.test(r)));
});

test('confirmed RF-link adjacency overrides pure proximity', async () => {
  // Two candidates for "5F": one is GEOGRAPHICALLY closer to the second hop but
  // has no link; the other is further but is a confirmed node_links neighbour of
  // the second hop. The adjacency one must win (a real hop beats mere nearness).
  const receptions = Array.from({ length: 4 }, () => rx(0, ['5F', 'A1']));
  const nodes: NodeLoc[] = [
    { node_id: '5FNEAR', name: 'near but unlinked', lat: 50.85, lon: -1.35 }, // ~12km from A1 node
    { node_id: '5FLINK', name: 'linked', lat: 50.55, lon: -1.05 }, // ~40km from A1 node
    { node_id: 'A1HOP0', name: 'second hop', lat: 50.8, lon: -1.3 },
  ];
  const links: Link[] = [{ node_a_id: 'A1HOP0', node_b_id: '5FLINK' }];
  const origin = await resolveSpamOrigin(['p1', 'p2', 'p3', 'p4'], 'ukmesh', fakeQuery(receptions, nodes, links), CFG);
  assert.ok(origin);
  assert.ok(Math.abs(origin!.lat! - 50.55) < 0.1, `should follow the confirmed link, got ${origin!.lat}`);
});

test('ignores distant high-hop receptions, keeping only the closest cohort', async () => {
  const receptions: Reception[] = [
    ...Array.from({ length: 4 }, () => rx(0, ['5F', 'A1'], 2)),
    ...Array.from({ length: 8 }, () => ({ rx_node_id: 'OBSN', hop_count: 12, path_hashes: ['F0', 'B2'], olat: 54.9, olon: -1.6 })),
  ];
  const nodes: NodeLoc[] = [
    { node_id: '5FAA11', name: 'IW Repeater', lat: 50.596, lon: -1.203 },
    { node_id: 'A1CC33', name: 'second hop', lat: 50.8, lon: -1.3 },
    { node_id: 'F0DD44', name: 'Tyne Repeater', lat: 54.95, lon: -1.64 },
    { node_id: 'B2EE55', name: 'Tyne hop', lat: 54.9, lon: -1.62 },
  ];
  const origin = await resolveSpamOrigin(['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l'], 'ukmesh', fakeQuery(receptions, nodes), CFG);
  assert.ok(origin);
  assert.ok(origin!.lat! < 51.5, `should stay south near the close observer, got ${origin!.lat}`);
});

test('returns null when too few close receptions resolve', async () => {
  const receptions = [rx(0, ['5F'])];
  const nodes: NodeLoc[] = [{ node_id: '5FAA11', name: 'IW', lat: 50.596, lon: -1.203 }];
  const origin = await resolveSpamOrigin(['p1'], 'ukmesh', fakeQuery(receptions, nodes), CFG);
  assert.equal(origin, null);
});
