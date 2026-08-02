import type { Request, Response } from 'express';
import { isIP } from 'node:net';
import { isTrustedProxyPeer } from '../../http/trustedProxy.js';
import {
  operatorTokenIsConfigured,
  verifyOperatorToken,
} from '../../security/operatorAuth.js';

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
  const peer = normalizeIp(req.socket.remoteAddress ?? '');

  if (
    (forwarded.length > 0 && !isTrustedProxyPeer(peer))
    || forwarded.some((ip) => !isPrivateClientIp(ip))
  ) {
    res.status(403).json({ error: 'Local access only' });
    return false;
  }

  const candidates = [
    peer,
    ...forwarded,
  ].filter(Boolean) as string[];

  if (!candidates.some((ip) => isPrivateClientIp(ip) || (isIP(ip) === 0 && ip === 'localhost'))) {
    res.status(403).json({ error: 'Local access only' });
    return false;
  }

  const expected = process.env['OPERATOR_SITE_TOKEN'];
  const authorization = String(req.headers.authorization ?? '');
  const bearer = authorization.startsWith('Bearer ') ? authorization.slice(7).trim() : '';
  const provided = String(req.headers['x-operator-token'] ?? bearer);
  if (!operatorTokenIsConfigured(expected) || !verifyOperatorToken(expected, provided)) {
    res.status(401).json({ error: 'Operator authentication required' });
    return false;
  }
  return true;
}
