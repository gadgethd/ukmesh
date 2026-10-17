import { createHash, randomBytes } from 'node:crypto';
import { createAuthToken, Utils } from '@michaelhart/meshcore-decoder';
import type { PacketShareDestinationRow } from './packetShareConfig.js';
import { decryptPacketShareSecret, encryptPacketShareSecret } from './packetShareCrypto.js';

const TOKEN_TTL_SECONDS = 3_600;

/** orlp/ed25519 uses a clamped SHA-512 expansion of a random 32-byte seed. */
export async function generatePacketShareIdentity(encryptionKey?: string): Promise<{
  username: string;
  privateKeyCiphertext: string;
}> {
  const privateKey = createHash('sha512').update(randomBytes(32)).digest();
  privateKey[0] = privateKey[0]! & 248;
  privateKey[31] = (privateKey[31]! & 63) | 64;
  try {
    const privateKeyHex = privateKey.toString('hex');
    const publicKey = (await Utils.derivePublicKey(privateKeyHex)).toUpperCase();
    return {
      username: `v1_${publicKey}`,
      privateKeyCiphertext: encryptPacketShareSecret(privateKeyHex, encryptionKey),
    };
  } finally {
    privateKey.fill(0);
  }
}

export async function packetShareCredentials(
  destination: PacketShareDestinationRow,
  nowSeconds = Math.floor(Date.now() / 1_000),
  encryptionKey?: string,
): Promise<{ username?: string; password?: string; expiresAt: number }> {
  const username = destination.broker_username?.trim() || undefined;
  const secret = destination.broker_password_ciphertext
    ? decryptPacketShareSecret(destination.broker_password_ciphertext, encryptionKey)
    : undefined;
  if (destination.auth_mode !== 'meshcore_jwt') {
    return { username, password: secret, expiresAt: Infinity };
  }
  const privateKey = secret?.trim();
  if (!username || !/^v1_[0-9A-F]{64}$/.test(username)
    || !privateKey || !/^[0-9a-fA-F]{128}$/.test(privateKey)
    || !destination.auth_audience) throw new Error('MeshCore signing credentials are incomplete');
  const publicKey = username.slice(3);
  if ((await Utils.derivePublicKey(privateKey)).toUpperCase() !== publicKey) {
    throw new Error('MeshCore signing key does not match the public key');
  }
  const expiresAt = nowSeconds + TOKEN_TTL_SECONDS;
  const password = await createAuthToken({
    publicKey,
    aud: destination.auth_audience,
    iat: nowSeconds,
    exp: expiresAt,
  }, privateKey, publicKey);
  return { username, password, expiresAt };
}
