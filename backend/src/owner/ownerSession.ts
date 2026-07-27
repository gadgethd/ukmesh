import type { Request } from 'express';
import { createCipheriv, createDecipheriv, createHash, randomBytes } from 'node:crypto';

export type OwnerSession = {
  v: 2;
  mqttUsername: string;
  exp: number;
  legacy?: boolean;
};

function getOwnerCookieKey(): Buffer {
  const secret = process.env['OWNER_COOKIE_SECRET'];
  if (!secret) throw new Error('OWNER_COOKIE_SECRET environment variable is not set');
  return createHash('sha256').update(secret).digest();
}

export function encryptOwnerSession(payload: OwnerSession): string {
  const iv = randomBytes(12);
  const key = getOwnerCookieKey();
  const cipher = createCipheriv('aes-256-gcm', key, iv);
  const plaintext = Buffer.from(JSON.stringify({
    v: 2,
    mqttUsername: payload.mqttUsername.trim(),
    exp: payload.exp,
  }), 'utf8');
  const encrypted = Buffer.concat([cipher.update(plaintext), cipher.final()]);
  const tag = cipher.getAuthTag();
  return `${iv.toString('base64url')}.${tag.toString('base64url')}.${encrypted.toString('base64url')}`;
}

export function decryptOwnerSession(token: string): OwnerSession | null {
  try {
    const [ivB64, tagB64, ciphertextB64] = token.split('.');
    if (!ivB64 || !tagB64 || !ciphertextB64) return null;
    const iv = Buffer.from(ivB64, 'base64url');
    const tag = Buffer.from(tagB64, 'base64url');
    const ciphertext = Buffer.from(ciphertextB64, 'base64url');
    const key = getOwnerCookieKey();
    const decipher = createDecipheriv('aes-256-gcm', key, iv);
    decipher.setAuthTag(tag);
    const decoded = Buffer.concat([decipher.update(ciphertext), decipher.final()]).toString('utf8');
    const parsed = JSON.parse(decoded) as Record<string, unknown>;
    const mqttUsername = typeof parsed['mqttUsername'] === 'string'
      ? parsed['mqttUsername'].trim()
      : '';
    const exp = parsed['exp'];
    if (!mqttUsername || typeof exp !== 'number' || !Number.isFinite(exp)) return null;
    if (parsed['v'] === 2) return { v: 2, mqttUsername, exp };

    // A username-bearing v1 cookie is an identity hint only. Embedded node IDs
    // are ignored and current server-side authorization is re-read.
    if (Array.isArray(parsed['nodeIds'])) {
      return { v: 2, mqttUsername, exp, legacy: true };
    }
    return null;
  } catch {
    return null;
  }
}

function readCookieValue(cookieHeader: string | undefined, key: string): string | null {
  if (!cookieHeader) return null;
  const parts = cookieHeader.split(';');
  for (const part of parts) {
    const trimmed = part.trim();
    if (!trimmed.startsWith(`${key}=`)) continue;
    return decodeURIComponent(trimmed.slice(key.length + 1));
  }
  return null;
}

export function getOwnerSession(req: Request, ownerCookieName: string): OwnerSession | null {
  const token = readCookieValue(req.headers.cookie, ownerCookieName);
  if (!token) return null;
  const session = decryptOwnerSession(token);
  if (!session || session.exp <= Date.now()) return null;
  return session;
}

export function isSecureRequest(req: { secure: boolean; headers: Record<string, string | string[] | undefined> }): boolean {
  if (req.secure) return true;
  const proto = String(req.headers['x-forwarded-proto'] ?? '').toLowerCase();
  return proto === 'https';
}
