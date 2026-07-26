import type { IncomingMessage } from 'node:http';
import { isIP } from 'node:net';

function normalizeIp(value: string | undefined): string {
  const raw = String(value ?? '').trim();
  if (raw.startsWith('::ffff:')) return raw.slice(7);
  return raw;
}

function configuredPeers(): Set<string> {
  return new Set(
    String(process.env['TRUSTED_PROXY_PEERS'] ?? '')
      .split(',')
      .map((value) => normalizeIp(value))
      .filter(Boolean),
  );
}

const trustedPeers = configuredPeers();

export function isTrustedProxyPeer(ip: string): boolean {
  return trustedPeers.has(normalizeIp(ip));
}

export function trustedClientIp(req: IncomingMessage): string {
  const peer = normalizeIp(req.socket.remoteAddress);
  if (!isTrustedProxyPeer(peer)) return peer;

  // Trusted Nginx overwrites X-Forwarded-For with one validated client value.
  // Do not walk or append a caller-controlled forwarding chain here.
  const raw = Array.isArray(req.headers['x-forwarded-for'])
    ? req.headers['x-forwarded-for'][0]
    : req.headers['x-forwarded-for'];
  const forwarded = normalizeIp(String(raw ?? '').split(',')[0]);
  return isIP(forwarded) ? forwarded : peer;
}

export function reloadTrustedProxyPeersForTest(values: string[]): void {
  trustedPeers.clear();
  for (const value of values) {
    const normalized = normalizeIp(value);
    if (normalized) trustedPeers.add(normalized);
  }
}
