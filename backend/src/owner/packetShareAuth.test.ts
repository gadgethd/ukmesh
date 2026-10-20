import assert from 'node:assert/strict';
import test from 'node:test';
import { verifyAuthToken } from '@michaelhart/meshcore-decoder';
import { generatePacketShareIdentity, packetShareCredentials } from './packetShareAuth.js';
import { decryptPacketShareSecret, encryptPacketShareSecret } from './packetShareCrypto.js';
import { destinationConfigured, type PacketShareDestinationRow } from './packetShareConfig.js';

const encryptionKey = 'test-packet-share-encryption-key';

test('generated MeshCore relay keys sign verifiable, renewable ScotMesh JWTs', async () => {
  const identity = await generatePacketShareIdentity(encryptionKey);
  assert.match(identity.username, /^v1_[0-9A-F]{64}$/);
  assert.match(decryptPacketShareSecret(identity.privateKeyCiphertext, encryptionKey), /^[0-9a-f]{128}$/);
  const destination: PacketShareDestinationRow = {
    destination_id: 'scotmesh', display_name: 'ScotMesh',
    broker_url: 'wss://scotmesh-mqtt.mm7roq.compute.oarc.uk:443/',
    topic_template: 'meshcore/MME/{observerId}/packets',
    broker_username: identity.username, broker_password_ciphertext: identity.privateKeyCiphertext,
    auth_mode: 'meshcore_jwt', auth_audience: 'scotmesh-mqtt.mm7roq.compute.oarc.uk', enabled: true,
  };
  assert.equal(destinationConfigured(destination), true);
  const now = Math.floor(Date.now() / 1_000);
  const credentials = await packetShareCredentials(destination, now, encryptionKey);
  const claims = await verifyAuthToken(credentials.password!, identity.username.slice(3));
  assert.equal(claims?.aud, destination.auth_audience);
  assert.equal(claims?.publicKey, identity.username.slice(3));
  assert.equal(claims?.iat, now);
  assert.equal(claims?.exp, now + 3_600);
  const renewed = await packetShareCredentials(destination, now + 60, encryptionKey);
  assert.notEqual(renewed.password, credentials.password);
  assert.equal(renewed.expiresAt, now + 3_660);
  const otherIdentity = await generatePacketShareIdentity(encryptionKey);
  await assert.rejects(packetShareCredentials({ ...destination, broker_username: otherIdentity.username }, now, encryptionKey), /does not match/);
  assert.equal(destinationConfigured({ ...destination, auth_audience: 'another-broker.example' }), false);
  assert.equal(destinationConfigured({ ...destination, broker_url: 'ws://scotmesh-mqtt.mm7roq.compute.oarc.uk:443/' }), false);
  assert.equal(destinationConfigured(destination, false), false);
});

test('existing password and anonymous destinations keep their authentication', async () => {
  const destination: PacketShareDestinationRow = {
    destination_id: 'remote', display_name: 'Remote', broker_url: 'mqtts://example.test:8883',
    topic_template: 'packets', broker_username: 'owner',
    broker_password_ciphertext: encryptPacketShareSecret(' existing-password ', encryptionKey), enabled: true,
  };
  const credentials = await packetShareCredentials(destination, undefined, encryptionKey);
  assert.equal(credentials.username, 'owner');
  assert.equal(credentials.password, ' existing-password ');
  assert.equal(credentials.expiresAt, Infinity);
  const anonymous = await packetShareCredentials({ ...destination, broker_username: null, broker_password_ciphertext: null });
  assert.equal(anonymous.username, undefined);
  assert.equal(anonymous.password, undefined);
});
