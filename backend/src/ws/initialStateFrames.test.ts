import assert from 'node:assert/strict';
import test from 'node:test';
import { decodeInitialStateMessage, type InitialState } from '../contracts/initialStateColumns.generated.js';
import { prepareInitialStateFrame, renderInitialStateFrame } from './initialStateFrames.js';

test('reused snapshot frames preserve each timestamp and apply the exact UTF-8 cap', () => {
  const data: InitialState['data'] = {
    nodes: [{ node_id: 'AA', name: '日本 🚲', last_seen: new Date(0), online: undefined }],
    packets: [{ packet_hash: 'ab', path: ['ab', null] }],
    messages: [{ summary: 'dated \\"ts\\": 0' }],
    viable_links: [{ node_a_id: 'AA', node_b_id: 'BB' }],
  };
  const decodedData = JSON.parse(JSON.stringify(data)) as InitialState['data'];
  for (const columns of [false, true]) {
    const frame = prepareInitialStateFrame(data, columns);
    for (const ts of [123, 1_700_000_000_001]) {
      const totalBytes = frame.bytes + String(ts).length;
      const rendered = renderInitialStateFrame(frame, ts, totalBytes);
      assert.ok(rendered);
      assert.equal(Buffer.byteLength(rendered), totalBytes);
      assert.equal(renderInitialStateFrame(frame, ts, totalBytes - 1), undefined);
      assert.deepEqual(decodeInitialStateMessage(JSON.parse(rendered)), { type: 'initial_state', data: decodedData, ts });
    }
  }
});
