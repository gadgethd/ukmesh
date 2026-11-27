import assert from 'node:assert/strict';
import { Buffer } from 'node:buffer';
import { createCipheriv, createHash, createHmac } from 'node:crypto';
import test from 'node:test';
import { MeshCoreDecoder } from '@michaelhart/meshcore-decoder';
import {
  buildChannelEntries,
  buildSummary,
  hasMeshosSignature,
  isMeshosSignedPlaintext,
  MESHOS_SIGNED_MARKER,
} from './channelRegistry.js';
import { decodePacketCompat } from './decodePacket.js';

// Public 2 secret used by the repo's VALIDATED_CHANNELS.
const PUBLIC2_SECRET = '8b4b705b080c0d943b1c80f6b3ef6b6d';

/**
 * Build a MAC-valid flood group-text packet.
 * MeshOS signed ending (mirrors SignatureFreeZone.h): text + 0x00 + 'S' 0x01 +
 * signer key id (8B) + Ed25519 signature (64B) + zero pad.
 */
function buildGroupTextPacket(
  secretHex: string,
  text: string,
  options: { timestamp?: number; signed?: boolean } = {},
): string {
  const timestamp = options.timestamp ?? Math.floor(Date.now() / 1000) - 60;
  const body = Buffer.from(text, 'utf8');
  const tail = options.signed
    ? Buffer.concat([Buffer.from([0x00, 0x53, 0x01]), Buffer.alloc(8, 0xab), Buffer.alloc(64, 0x11)])
    : Buffer.alloc(0);
  const plaintext = Buffer.alloc(5 + body.length + tail.length);
  plaintext.writeUInt32LE(timestamp, 0);
  plaintext[4] = 0x00; // plain text
  body.copy(plaintext, 5);
  tail.copy(plaintext, 5 + body.length);

  const padded = Buffer.alloc(Math.ceil(plaintext.length / 16) * 16);
  plaintext.copy(padded);

  const cipher = createCipheriv('aes-128-ecb', Buffer.from(secretHex, 'hex'), null);
  cipher.setAutoPadding(false);
  const ciphertext = Buffer.concat([cipher.update(padded), cipher.final()]);

  const macKey = Buffer.concat([Buffer.from(secretHex, 'hex'), Buffer.alloc(16)]);
  const mac = createHmac('sha256', macKey).update(ciphertext).digest().subarray(0, 2);
  const channelHash = createHash('sha256').update(Buffer.from(secretHex, 'hex')).digest().subarray(0, 1);

  const payload = Buffer.concat([channelHash, mac, ciphertext]);
  // version 0, payload type 5 (group text), route flood; zero hops
  return Buffer.concat([Buffer.from([0x15, 0x00]), payload]).toString('hex').toUpperCase();
}

function decodedPayload(rawHex: string) {
  return decodePacketCompat(rawHex, MeshCoreDecoder.createKeyStore({ channelSecrets: [PUBLIC2_SECRET] }))
    .decoded?.payload?.decoded;
}

test('detects the MeshOS signed ending on authenticated group text', () => {
  const rawHex = buildGroupTextPacket(PUBLIC2_SECRET, 'Pink Rabbit: hello mesh', { signed: true });
  const decoded = decodedPayload(rawHex);
  assert.ok(decoded?.decrypted, 'signed packets still authenticate as standard MeshCore');

  const entries = buildChannelEntries('');
  assert.equal(hasMeshosSignature(decoded as never, entries), true);
  assert.equal(buildSummary(5, decoded, rawHex, entries), `[public2] Pink Rabbit: hello mesh · ${MESHOS_SIGNED_MARKER}`);
});

test('plain group text gets no signature marker', () => {
  const rawHex = buildGroupTextPacket(PUBLIC2_SECRET, 'M0CNQ: morning all');
  const decoded = decodedPayload(rawHex);
  const entries = buildChannelEntries('');

  assert.equal(hasMeshosSignature(decoded as never, entries), false);
  assert.equal(buildSummary(5, decoded, rawHex, entries), '[public2] M0CNQ: morning all');
});

test('a MAC-failing packet stays encrypted and is never labelled MeshOS', () => {
  const rawHex = buildGroupTextPacket(PUBLIC2_SECRET, 'Lee G3LJL: hello', { signed: true });
  const tampered = `${rawHex.slice(0, -4)}0000`;
  const decoded = decodedPayload(tampered);

  assert.equal(decoded?.decrypted, undefined);
  assert.equal(buildSummary(5, decoded, tampered, buildChannelEntries('')), '[encrypted]');
});

test('detector requires the exact tail shape', () => {
  const base = Buffer.alloc(96);
  base.writeUInt32LE(Math.floor(Date.now() / 1000), 0);
  base[4] = 0;
  Buffer.from('sender: hi').copy(base, 5);
  base[15] = 0x00;
  base[16] = 0x53;
  base[17] = 0x01;
  assert.equal(isMeshosSignedPlaintext(base), true);

  const wrongVersion = Buffer.from(base);
  wrongVersion[17] = 0x02;
  assert.equal(isMeshosSignedPlaintext(wrongVersion), false);

  const noTerminator = Buffer.from(base);
  noTerminator[15] = 0x41;
  assert.equal(isMeshosSignedPlaintext(noTerminator), false);
});
