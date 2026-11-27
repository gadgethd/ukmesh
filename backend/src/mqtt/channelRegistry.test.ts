import assert from 'node:assert/strict';
import test from 'node:test';
import { beaconPublicationRegistry, buildChannelEntries, channelHashesForName, isPublicChannelName, PUBLIC_CHANNELS, sanitizePublicPacketPayload } from './channelRegistry.js';

test('committed channel material is explicitly classified as public', () => {
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

test('Beacon publication binds approved material to a full fingerprint and stable channel ID', () => {
  const original = beaconPublicationRegistry();
  const key = '42'.repeat(16);
  const secrets = ` Public :${'19'.repeat(16)}, opted-in :${key},confidential:${'ad'.repeat(16)}`;
  assert.deepEqual(beaconPublicationRegistry(secrets), original);
  const allowed = beaconPublicationRegistry(secrets, ' OPTED-IN ');
  const approved = allowed.channels.find(({ name }) => name === 'opted-in');
  assert.equal(allowed.channels.length, original.channels.length + 1);
  assert.match(approved?.keyFingerprint ?? '', /^[a-f0-9]{64}$/);
  assert.equal(approved?.secretHex, key);
  const rotated = beaconPublicationRegistry(`opted-in:${'13'.repeat(16)}`, 'opted-in');
  assert.equal(rotated.channels.find(({ name }) => name === 'opted-in')?.id, approved?.id);
  assert.notEqual(rotated.registryVersion, allowed.registryVersion);
  assert.throws(() => beaconPublicationRegistry(secrets, 'public'), /CHANNEL_NAME_CONFLICT/);
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
