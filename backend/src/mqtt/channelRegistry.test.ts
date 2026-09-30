import assert from 'node:assert/strict';
import test from 'node:test';
import { buildChannelEntries, PUBLIC_CHANNELS, VALIDATED_CHANNELS } from './channelRegistry.js';

test('committed channel material is explicitly classified as public', () => {
  assert.strictEqual(VALIDATED_CHANNELS, PUBLIC_CHANNELS);
  assert.ok(PUBLIC_CHANNELS.length > 0);
  assert.ok(buildChannelEntries().every((entry) => entry.classification === 'public'));
});

test('environment-supplied channel keys are classified as confidential', () => {
  const entry = buildChannelEntries(`private:${'a'.repeat(32)}`)
    .find((channel) => channel.name === 'private');

  assert.equal(entry?.classification, 'confidential');
});
