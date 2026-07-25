import { lookup } from 'node:dns/promises';
import { isIP } from 'node:net';
import type { IncomingMessage } from 'node:http';

const staticProxyPeers = String(process.env['TRUSTED_PROXY_IPS'] ?? '127.0.0.1,::1')
  .split(',')
  .map((value) => normalizeIp(value))
  .filter(Boolean);
const trustedProxyHosts = String(
  process.env['TRUSTED_PROXY_HOSTS'] ?? 'app-ukmesh,website-ukmesh',
)
  .split(',')
  .map((value) => value.trim())
  .filter(Boolean);
const trustedProxyPeers = new Set(staticProxyPeers);

export function normalizeIp(value: string): string {
  const trimmed = value.trim();
  const unwrapped = trimmed.startsWith('[') && trimmed.endsWith(']')
    ? trimmed.slice(1, -1)
    : trimmed;
  return unwrapped.startsWith('::ffff:') ? unwrapped.slice(7) : unwrapped;
}

export function isTrustedProxyPeer(ip: string): boolean {
  return trustedProxyPeers.has(normalizeIp(ip));
}

export async function refreshTrustedProxyPeers(): Promise<void> {
  const resolved = new Set(staticProxyPeers);
  await Promise.all(trustedProxyHosts.map(async (host) => {
    try {
      const addresses = await lookup(host, { all: true, verbatim: true });
      for (const address of addresses) resolved.add(normalizeIp(address.address));
    } catch {
      // A proxy container may not exist in development. Static peers remain.
    }
  }));
  trustedProxyPeers.clear();
  for (const peer of resolved) trustedProxyPeers.add(peer);
}

export function trustedClientIp(req: IncomingMessage): string {
  const socketPeer = normalizeIp(req.socket.remoteAddress ?? 'unknown');
  if (!isTrustedProxyPeer(socketPeer)) return socketPeer;
  const forwarded = req.headers['x-forwarded-for'];
  const first = Array.isArray(forwarded) ? forwarded[0] : forwarded?.split(',')[0];
  const candidate = normalizeIp(String(first ?? ''));
  return isIP(candidate) ? candidate : socketPeer;
}
