import assert from 'node:assert/strict';
import test from 'node:test';
import { measureSnapshotWeight } from './cacheWeight.js';

const signal = () => new AbortController().signal;
const reference = (value: unknown) => Buffer.byteLength(JSON.stringify(value));

test('complete initial and viable-link cache records retain exact legacy JSON weights', async () => {
  const nodes = [{ node_id: 'one', name: 'café 🚲\n\\"', omitted: undefined, nullable: null,
    last_seen: new Date('2026-11-16T01:02:03.456Z'), values: [undefined, NaN, Infinity, -0],
    nested: { text: 'unchanged', missing: undefined } }, { node_id: 'two', last_seen: new Date(NaN) }];
  const links = [{ node_a_id: 'one', node_b_id: 'two', itm_path_loss_db: 123.456789, itm_viable: true }];
  for (const value of [
    { ts: 123, generation: 18, nodes, packets: [], messages: [{ summary: 'received unchanged' }], viableLinks: links },
    { ts: 123, data: links },
    { ts: 123, data: [] },
  ]) assert.equal(await measureSnapshotWeight(value, signal()), reference(value));
});

test('own keys, unknown future row arrays, empty rows and array null semantics retain weight', async () => {
  const value = Object.fromEntries([
    ['__proto__', { value: 'ordinary metadata' }], ['generation', 1],
    ['futureRows', [{ extra: true }, {}, null, undefined, NaN]], ['empty', []], ['omitted', undefined],
  ]);
  assert.equal(await measureSnapshotWeight(value, signal()), reference(value));
  assert.equal(({} as Record<string, unknown>)['value'], undefined);
});

test('the complete weight remains exact above a cache admission bound', async () => {
  const value = { ts: 1, data: Array.from({ length: 1000 }, (_, index) => ({ index, text: 'x'.repeat(256) })) };
  const actual = await measureSnapshotWeight(value, signal());
  assert.ok(actual > 1024);
  assert.equal(actual, reference(value));
});

test('serialization failures retain Infinity cache rejection without swallowing cancellation', async () => {
  const cycle: Record<string, unknown> = {}; cycle['self'] = cycle;
  for (const value of [{ data: [cycle] }, { data: [{ value: 1n }] }]) {
    assert.throws(() => reference(value));
    assert.equal(await measureSnapshotWeight(value, signal()), Infinity);
  }
  const controller = new AbortController(), reason = new Error('owned fetch canceled');
  controller.abort(reason);
  await assert.rejects(measureSnapshotWeight({ data: [cycle] }, controller.signal), error => error === reason);
});

test('large snapshots allow other event-loop work and keep their arrays and values unchanged', async () => {
  const row = { id: 'one', at: new Date('2026-11-16T01:02:03.456Z'), text: 'x'.repeat(128) };
  const value = { ts: 1, data: Array.from({ length: 50_000 }, () => row) };
  let turnObserved = false;
  const timer = setTimeout(() => { turnObserved = true; }, 0);
  try {
    assert.equal(await measureSnapshotWeight(value, signal()), reference(value));
    assert.equal(turnObserved, true);
    assert.equal(value.data.length, 50_000); assert.equal(value.data[0], row);
    assert.equal(row.at.toISOString(), '2026-11-16T01:02:03.456Z');
  } finally { clearTimeout(timer); }
});

test('cancellation between slices reaches the owner before a weight can be published', async () => {
  const value = { data: Array.from({ length: 50_000 }, (_, index) => ({ index, text: 'x'.repeat(128) })) };
  const controller = new AbortController(), reason = new Error('owned fetch deadline');
  const timer = setTimeout(() => controller.abort(reason), 0);
  let completed = false;
  try {
    await assert.rejects(measureSnapshotWeight(value, controller.signal).then(() => { completed = true; }), error => error === reason);
    assert.equal(completed, false);
  } finally { clearTimeout(timer); }
});
