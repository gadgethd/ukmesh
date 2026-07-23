import type { Request, Response } from 'express';
import { isIP } from 'node:net';

function normalizeIp(value: string | undefined): string {
  const raw = String(value ?? '').trim();
  if (!raw) return '';
  const first = raw.split(',')[0]?.trim() ?? '';
  if (first.startsWith('::ffff:')) return first.slice(7);
  return first;
}

function isPrivateClientIp(ip: string): boolean {
  const normalized = normalizeIp(ip);
  if (!normalized) return false;
  if (normalized === '::1' || normalized === '127.0.0.1') return true;
  if (normalized.startsWith('10.')) return true;
  if (normalized.startsWith('192.168.')) return true;
  if (/^172\.(1[6-9]|2\d|3[0-1])\./.test(normalized)) return true;
  if (/^(fc|fd)/i.test(normalized)) return true;
  if (/^fe80:/i.test(normalized)) return true;
  return false;
}

export function requireLocalOnly(req: Request, res: Response): boolean {
  // Forwarded headers are set by the proxy chain for public traffic. A genuine
  // local request has none of these, or only carries private hop addresses.
  // If ANY forwarded address is a public IP, this is proxied public traffic —
  // reject it before falling through to the candidate check. Without this guard
  // an attacker could spoof X-Forwarded-For: 192.168.x.x to pass the check, and
  // req.socket.remoteAddress is always the private proxy container anyway.
  const forwarded = [
    normalizeIp(String(req.headers['cf-connecting-ip'] ?? '')),
    normalizeIp(String(req.headers['x-forwarded-for'] ?? '')),
    normalizeIp(String(req.headers['x-real-ip'] ?? '')),
  ].filter(Boolean);

  if (forwarded.some((ip) => !isPrivateClientIp(ip))) {
    res.status(403).json({ error: 'Local access only' });
    return false;
  }

  const candidates = [
    normalizeIp(req.ip ?? ''),
    normalizeIp(req.socket.remoteAddress ?? ''),
    ...forwarded,
  ].filter(Boolean) as string[];

  if (candidates.some((ip) => isPrivateClientIp(ip) || (isIP(ip) === 0 && ip === 'localhost'))) {
    return true;
  }

  res.status(403).json({ error: 'Local access only' });
  return false;
}
