import assert from 'node:assert/strict';
import { Buffer } from 'node:buffer';
import { createCipheriv, createHash } from 'node:crypto';
import test from 'node:test';
import { MeshCoreDecoder } from '@michaelhart/meshcore-decoder';
import {
  buildChannelEntries,
  buildSummary,
  decodeMeshosSignedGroupText,
  MESHOS_SIGNED_MARKER,
} from './channelRegistry.js';
import { decodePacketCompat } from './decodePacket.js';

const PUBLIC2_SECRET = '8b4b705b080c0d943b1c80f6b3ef6b6d';

/** Build a flood group-text packet carrying the supplied 2-byte tag and optional binary tail. */
function buildGroupTextPacket(
  secretHex: string,
  text: string,
  macHex: string,
  options: { timestamp?: number; tailHex?: string } = {},
): string {
  const timestamp = options.timestamp ?? Math.floor(Date.now() / 1000) - 60;
  const body = Buffer.from(text, 'utf8');
  const tail = Buffer.from(options.tailHex ?? '', 'hex');
  const plaintext = Buffer.alloc(5 + body.length + tail.length);
  plaintext.writeUInt32LE(timestamp, 0);
  plaintext[4] = 0x00; // plain text, first attempt
  body.copy(plaintext, 5);
  tail.copy(plaintext, 5 + body.length);

  const padded = Buffer.alloc(Math.ceil(plaintext.length / 16) * 16);
  plaintext.copy(padded);

  const cipher = createCipheriv('aes-128-ecb', Buffer.from(secretHex, 'hex'), null);
  cipher.setAutoPadding(false);
  const ciphertext = Buffer.concat([cipher.update(padded), cipher.final()]);

  const channelHash = createHash('sha256').update(Buffer.from(secretHex, 'hex')).digest().subarray(0, 1);
  const payload = Buffer.concat([channelHash, Buffer.from(macHex, 'hex'), ciphertext]);
  // version 0, payload type 5 (group text), route flood; zero hops
  return Buffer.concat([Buffer.from([0x15, 0x00]), payload]).toString('hex').toUpperCase();
}

test('recovers MeshOS-signed group text when the standard MAC does not verify', () => {
  const entries = buildChannelEntries('');
  const rawHex = buildGroupTextPacket(PUBLIC2_SECRET, 'Pink Rabbit: hello mesh', '0000');

  const standard = decodePacketCompat(rawHex, MeshCoreDecoder.createKeyStore({ channelSecrets: [PUBLIC2_SECRET] }));
  const decoded = standard.decoded?.payload?.decoded as Parameters<typeof decodeMeshosSignedGroupText>[0];
  assert.equal(
    (decoded as { decrypted?: unknown }).decrypted,
    undefined,
    'standard decoder must reject the non-standard tag',
  );

  const recovered = decodeMeshosSignedGroupText(decoded, entries);
  assert.equal(recovered?.channel, 'public2');
  assert.equal(recovered?.text, 'Pink Rabbit: hello mesh');

  const summary = buildSummary(5, decoded, rawHex, entries);
  assert.equal(summary, `[public2] Pink Rabbit: hello mesh · ${MESHOS_SIGNED_MARKER}`);
});

test('truncates the binary signature tail instead of leaking control bytes', () => {
  const entries = buildChannelEntries('');
  const rawHex = buildGroupTextPacket(PUBLIC2_SECRET, 'Lee G3LJL: testing', '0000', { tailHex: 'F0FF01'.repeat(6) });
  const decoded = decodePacketCompat(rawHex, MeshCoreDecoder.createKeyStore({ channelSecrets: [PUBLIC2_SECRET] }))
    .decoded?.payload?.decoded;

  const recovered = decodeMeshosSignedGroupText(decoded as Parameters<typeof decodeMeshosSignedGroupText>[0], entries);
  assert.equal(recovered?.text, 'Lee G3LJL: testing…');
});

test('leaves genuinely foreign ciphertext encrypted', () => {
  const entries = buildChannelEntries('');
  // 32 bytes of non-decrypting ciphertext under the public2 hash byte.
  const rawHex = `1500660000${'AB'.repeat(32)}`.toUpperCase();
  const decoded = decodePacketCompat(rawHex, MeshCoreDecoder.createKeyStore({ channelSecrets: [PUBLIC2_SECRET] }))
    .decoded?.payload?.decoded;

  assert.equal(decodeMeshosSignedGroupText(decoded as Parameters<typeof decodeMeshosSignedGroupText>[0], entries), undefined);
  assert.equal(buildSummary(5, decoded, rawHex, entries), '[encrypted]');
});
