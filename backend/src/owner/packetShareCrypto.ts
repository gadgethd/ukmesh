import { createCipheriv, createDecipheriv, createHash, randomBytes } from 'node:crypto';

const CIPHERTEXT_VERSION = 'v1';

function keyFromSecret(secret: string): Buffer {
  const normalized = secret.trim();
  if (normalized.length < 16) throw new Error('OWNER_PACKET_SHARE_ENCRYPTION_KEY is too short');
  return createHash('sha256').update(normalized).digest();
}

function configuredKey(): Buffer {
  const secret = String(process.env['OWNER_PACKET_SHARE_ENCRYPTION_KEY'] ?? '');
  if (!secret) throw new Error('OWNER_PACKET_SHARE_ENCRYPTION_KEY is not set');
  return keyFromSecret(secret);
}

/** Encrypt a destination broker password for storage in the primary database. */
export function encryptPacketShareSecret(secret: string, keySecret?: string): string {
  if (!secret) throw new Error('packet share secret is empty');
  const iv = randomBytes(12);
  const cipher = createCipheriv('aes-256-gcm', keySecret ? keyFromSecret(keySecret) : configuredKey(), iv);
  const ciphertext = Buffer.concat([cipher.update(secret, 'utf8'), cipher.final()]);
  const tag = cipher.getAuthTag();
  return [
    CIPHERTEXT_VERSION,
    iv.toString('base64url'),
    tag.toString('base64url'),
    ciphertext.toString('base64url'),
  ].join('.');
}

/** Decrypt a destination broker password; malformed or unauthenticated data fails closed. */
export function decryptPacketShareSecret(ciphertext: string, keySecret?: string): string {
  const [version, ivB64, tagB64, bodyB64] = ciphertext.split('.');
  if (version !== CIPHERTEXT_VERSION || !ivB64 || !tagB64 || !bodyB64) {
    throw new Error('invalid packet share secret format');
  }
  const iv = Buffer.from(ivB64, 'base64url');
  const tag = Buffer.from(tagB64, 'base64url');
  const body = Buffer.from(bodyB64, 'base64url');
  if (iv.length !== 12 || tag.length !== 16 || body.length < 1) {
    throw new Error('invalid packet share secret encoding');
  }
  const decipher = createDecipheriv('aes-256-gcm', keySecret ? keyFromSecret(keySecret) : configuredKey(), iv);
  decipher.setAuthTag(tag);
  return Buffer.concat([decipher.update(body), decipher.final()]).toString('utf8');
}

export function isPacketShareSecretCiphertext(ciphertext: string): boolean {
  const [version, ivB64, tagB64, bodyB64] = ciphertext.split('.');
  if (version !== CIPHERTEXT_VERSION || !ivB64 || !tagB64 || !bodyB64) return false;
  if (![ivB64, tagB64, bodyB64].every((value) => /^[A-Za-z0-9_-]+$/.test(value))) return false;
  return Buffer.from(ivB64, 'base64url').length === 12
    && Buffer.from(tagB64, 'base64url').length === 16
    && Buffer.from(bodyB64, 'base64url').length > 0;
}

export function hasPacketShareEncryptionKey(): boolean {
  return String(process.env['OWNER_PACKET_SHARE_ENCRYPTION_KEY'] ?? '').trim().length >= 16;
}
