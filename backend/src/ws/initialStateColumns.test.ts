import assert from 'node:assert/strict';
import test from 'node:test';
import {
  encodeInitialStateColumns, decodeInitialStateColumns, decodeInitialStateMessage,
  type InitialState,
} from '../contracts/initialStateColumns.generated.js';

const snapshot = (nodes: Record<string, unknown>[] = []): InitialState => ({
  type: 'initial_state', data: { nodes, packets: [], messages: [], viable_links: [] }, ts: 123,
});
const wire = (value: unknown) => JSON.parse(JSON.stringify(value));
const roundtrip = (value: InitialState) => decodeInitialStateColumns(wire(encodeInitialStateColumns(value)));

test('all collections, row order and JSON value semantics match ordinary legacy wire bytes', () => {
  const value = snapshot([
    { node_id: 'one', absent: undefined, empty: null, time: new Date('2026-11-16T01:02:03.456Z'),
      text: 'café 🚲\n\\"', negative: -0, infinity: Infinity, nan: NaN,
      nested: { omitted: undefined, values: [undefined, NaN, null, true, { text: 'nested' }] } },
    { different: false, node_id: 'two', invalidTime: new Date(NaN) },
    Object.fromEntries([['__proto__', { safe: true }], ['constructor', 'ordinary field']]),
    {},
  ]);
  value.data.packets = [{ hash: 'first', payload: { array: [1, null, 'x'] } }, { hash: 'second' }];
  value.data.messages = [{ channel: 'public', text: 'received unchanged' }];
  value.data.viable_links = [{ a: 'one', b: 'two', loss: 123.456789, viable: true }];
  const restored = roundtrip(value);
  assert.deepEqual(restored, wire(value));
  assert.equal(JSON.stringify(restored), JSON.stringify(value));
  assert.equal(Object.getPrototypeOf(restored.data.nodes[2]), Object.prototype);
  assert.equal(({} as Record<string, unknown>)['safe'], undefined);
  assert.equal(value.data.nodes[0]!['absent'], undefined);
});

test('empty collections and changing row shapes preserve omission separately from null', () => {
  assert.deepEqual(roundtrip(snapshot()), snapshot());
  const value = snapshot([{ id: 1, a: undefined }, { id: 2, a: null }, { a: 3, id: 4 }, { id: 5 }]);
  assert.equal(JSON.stringify(roundtrip(value)), JSON.stringify(value));
});

test('shape reuse retains every row and field across a complete graph', () => {
  const value = snapshot(Array.from({ length: 500 }, (_, n) => ({
    node_id: String(n), lat: 50 + n / 1000, lon: -1, public_key: `key-${n}`,
    hardware_model: n % 2 ? null : undefined, last_seen: new Date(1_700_000_000_000 + n),
  })));
  value.data.viable_links = Array.from({ length: 500 }, (_, n) => ({
    node_a_id: String(n), node_b_id: String((n + 1) % 500), itm_path_loss_db: n / 7,
  }));
  const encoded = wire(encodeInitialStateColumns(value));
  assert.equal(encoded.data.nodes.shapes.length, 2);
  assert.equal(JSON.stringify(roundtrip(value)), JSON.stringify(value));
  assert.ok(Buffer.byteLength(JSON.stringify(encoded)) < Buffer.byteLength(JSON.stringify(value)));
});

test('decoder rejects malformed versions, shapes, indices and envelopes', () => {
  const good = wire(encodeInitialStateColumns(snapshot([{ id: 'one' }])));
  const changes: Array<(v: typeof good) => void> = [
    v => { v.format = 'future'; }, v => { v.ts = -1; }, v => { v.extra = true; },
    v => { delete v.data.messages; }, v => { v.data.nodes.shapes = [['id', 'id']]; },
    v => { v.data.nodes.shapes.push(['unused']); }, v => { v.data.nodes.shapes.push(['id']); },
    v => { v.data.nodes.rows[0][0] = 2; }, v => { v.data.nodes.rows[0][0] = 0.5; },
    v => { v.data.nodes.rows[0].push('extra'); }, v => { delete v.data.nodes.rows[0][1]; },
  ];
  for (const change of changes) {
    const invalid = structuredClone(good); change(invalid);
    assert.throws(() => decodeInitialStateColumns(invalid), /Invalid initial-state/);
  }
});

test('only advertised compact initial messages decode; legacy and live messages retain identity', () => {
  const legacy = snapshot([{ id: 'one' }]);
  const live = { type: 'packet', data: { packetHash: 'one' }, ts: 10 };
  assert.equal(decodeInitialStateMessage(legacy), legacy);
  assert.equal(decodeInitialStateMessage(live), live);
  assert.deepEqual(decodeInitialStateMessage(wire(encodeInitialStateColumns(legacy))), legacy);
  assert.throws(() => decodeInitialStateMessage({ ...legacy, format: 'unknown' }), /Invalid initial-state/);
});
