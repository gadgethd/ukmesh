import assert from 'node:assert/strict';
import test from 'node:test';
import { buildChannelEntries, channelHashesForName, isPublicChannelName, PUBLIC_CHANNELS, sanitizePublicPacketPayload, VALIDATED_CHANNELS } from './channelRegistry.js';

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

test('confidential channels are not public unless explicitly allowlisted', () => {
  assert.equal(isPublicChannelName('private', ''), false);
  assert.equal(isPublicChannelName('private', 'private'), true);
  assert.equal(isPublicChannelName('Public', ''), true);
});

test('public packet payloads keep plaintext only for public channels', () => {
  const secrets = `private:${'a'.repeat(32)}`;
  const channelHash = channelHashesForName('private', secrets)[0]!;
  const payload = {
    other: 1,
    _summary: '[private] hello',
    decrypted: { channelHash, message: 'hello' },
  };

  const stripped = sanitizePublicPacketPayload(payload, secrets, '');
  assert.equal(stripped?.['decrypted'], undefined);
  assert.equal(stripped?.['_summary'], undefined);
  assert.equal(stripped?.['other'], 1);

  const kept = sanitizePublicPacketPayload(payload, secrets, 'private');
  assert.deepEqual(kept?.['decrypted'], { channelHash, message: 'hello' });
});
