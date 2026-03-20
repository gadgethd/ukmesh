import type { Request } from 'express';
import { createCipheriv, createDecipheriv, createHash, randomBytes } from 'node:crypto';

export type OwnerSession = {
  nodeIds: string[];
  exp: number;
  mqttUsername?: string;
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
  const plaintext = Buffer.from(JSON.stringify(payload), 'utf8');
  const encrypted = Buffer.concat([cipher.update(plaintext), cipher.final()]);
  const tag = cipher.getAuthTag();
  return `${iv.toString('base64url')}.${tag.toString('base64url')}.${encrypted.toString('base64url')}`;
}

function decryptOwnerSession(token: string): OwnerSession | null {
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
    const parsed = JSON.parse(decoded) as Partial<OwnerSession>;
    if (!Array.isArray(parsed.nodeIds) || typeof parsed.exp !== 'number') return null;
    const nodeIds = parsed.nodeIds
      .map((value) => String(value).trim().toUpperCase())
      .filter((value) => /^[0-9A-F]{64}$/.test(value));
    if (nodeIds.length < 1) return null;
    const mqttUsername = typeof parsed.mqttUsername === 'string' ? parsed.mqttUsername.trim() : undefined;
    return { nodeIds, exp: parsed.exp, mqttUsername: mqttUsername || undefined };
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
