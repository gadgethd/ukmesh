import { promises as dns } from 'node:dns';
import https from 'node:https';
import { BlockList, isIP } from 'node:net';
import { webhookDeliveriesTotal } from '../metrics.js';

const MAX_URL_LENGTH = 2_048;
const MAX_RESPONSE_BYTES = 64 * 1_024;
const DELIVERY_TIMEOUT_MS = 8_000;
const MAX_CONCURRENCY = Math.max(
  1,
  Math.min(16, Number(process.env['OWNER_WEBHOOK_MAX_CONCURRENCY'] ?? 4) || 4),
);

const blocked = new BlockList();
for (const [network, prefix] of [
  ['0.0.0.0', 8], ['10.0.0.0', 8], ['100.64.0.0', 10], ['127.0.0.0', 8],
  ['169.254.0.0', 16], ['172.16.0.0', 12], ['192.0.0.0', 24],
  ['192.0.2.0', 24], ['192.168.0.0', 16], ['198.18.0.0', 15],
  ['198.51.100.0', 24], ['203.0.113.0', 24], ['224.0.0.0', 4],
  ['240.0.0.0', 4],
] as const) {
  blocked.addSubnet(network, prefix, 'ipv4');
}
for (const [network, prefix] of [
  ['::', 128], ['::1', 128], ['fc00::', 7], ['fe80::', 10],
  ['ff00::', 8], ['2001:db8::', 32], ['2001:10::', 28],
] as const) {
  blocked.addSubnet(network, prefix, 'ipv6');
}

export interface WebhookDnsResolver {
  resolve4(hostname: string): Promise<string[]>;
  resolve6(hostname: string): Promise<string[]>;
}

export interface ResolvedWebhookTarget {
  url: URL;
  addresses: Array<{ address: string; family: 4 | 6 }>;
}

function normalizeHostname(hostname: string): string {
  return hostname.startsWith('[') && hostname.endsWith(']')
    ? hostname.slice(1, -1)
    : hostname;
}

export function isPublicWebhookAddress(address: string): boolean {
  const normalized = normalizeHostname(address).toLowerCase();
  const family = isIP(normalized);
  if (family === 0) return false;
  // IPv4-mapped IPv6 forms are intentionally rejected instead of normalized;
  // this removes ambiguity between URL, DNS, and socket parsers.
  if (family === 6 && normalized.startsWith('::ffff:')) return false;
  return !blocked.check(normalized, family === 4 ? 'ipv4' : 'ipv6');
}

async function resolveFamily(
  resolver: WebhookDnsResolver,
  family: 4 | 6,
  hostname: string,
): Promise<Array<{ address: string; family: 4 | 6 }>> {
  try {
    const answers = family === 4
      ? await resolver.resolve4(hostname)
      : await resolver.resolve6(hostname);
    return answers.map((address) => ({ address, family }));
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    if (['ENODATA', 'ENOTFOUND', 'EAI_AGAIN'].includes(String(code))) return [];
    throw error;
  }
}

export async function resolveWebhookTarget(
  rawUrl: string,
  resolver: WebhookDnsResolver = dns,
): Promise<ResolvedWebhookTarget> {
  const input = rawUrl.trim();
  if (!input || input.length > MAX_URL_LENGTH) throw new Error('WEBHOOK_URL_INVALID_LENGTH');

  let url: URL;
  try {
    url = new URL(input);
  } catch {
    throw new Error('WEBHOOK_URL_INVALID');
  }
  if (url.protocol !== 'https:') throw new Error('WEBHOOK_HTTPS_REQUIRED');
  if (url.username || url.password) throw new Error('WEBHOOK_CREDENTIALS_FORBIDDEN');
  if (url.hash) throw new Error('WEBHOOK_FRAGMENT_FORBIDDEN');
  if (url.port && url.port !== '443') throw new Error('WEBHOOK_PORT_FORBIDDEN');

  const hostname = normalizeHostname(url.hostname);
  const literalFamily = isIP(hostname);
  const addresses = literalFamily
    ? [{ address: hostname, family: literalFamily as 4 | 6 }]
    : (await Promise.all([
      resolveFamily(resolver, 4, hostname),
      resolveFamily(resolver, 6, hostname),
    ])).flat();
  if (addresses.length < 1) throw new Error('WEBHOOK_DNS_EMPTY');
  if (addresses.some(({ address }) => !isPublicWebhookAddress(address))) {
    throw new Error('WEBHOOK_ADDRESS_FORBIDDEN');
  }
  return { url, addresses };
}

let activeDeliveries = 0;
const deliveryWaiters: Array<() => void> = [];

async function acquireDeliverySlot(): Promise<() => void> {
  if (activeDeliveries >= MAX_CONCURRENCY) {
    await new Promise<void>((resolve) => deliveryWaiters.push(resolve));
  }
  activeDeliveries += 1;
  return () => {
    activeDeliveries -= 1;
    deliveryWaiters.shift()?.();
  };
}

export async function deliverWebhook(
  rawUrl: string,
  payload: unknown,
): Promise<{ status: number; destinationHost: string }> {
  const release = await acquireDeliverySlot();
  try {
    // Resolve immediately before every attempt. The socket lookup is then
    // pinned to that validated answer while TLS still verifies the URL host.
    const target = await resolveWebhookTarget(rawUrl);
    const selected = target.addresses[0]!;
    const body = Buffer.from(JSON.stringify(payload), 'utf8');
    const status = await new Promise<number>((resolve, reject) => {
      const request = https.request(target.url, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'content-length': String(body.byteLength),
        },
        servername: isIP(normalizeHostname(target.url.hostname))
          ? undefined
          : normalizeHostname(target.url.hostname),
        lookup: (_hostname, _options, callback) => {
          callback(null, selected.address, selected.family);
        },
        timeout: DELIVERY_TIMEOUT_MS,
      }, (response) => {
        let received = 0;
        response.on('data', (chunk: Buffer) => {
          received += chunk.byteLength;
          if (received > MAX_RESPONSE_BYTES) {
            response.destroy(new Error('WEBHOOK_RESPONSE_TOO_LARGE'));
          }
        });
        response.on('error', reject);
        response.on('end', () => {
          const code = response.statusCode ?? 0;
          if (code < 200 || code >= 300) {
            reject(new Error(`WEBHOOK_HTTP_${code}`));
            return;
          }
          resolve(code);
        });
      });
      request.on('timeout', () => request.destroy(new Error('WEBHOOK_TIMEOUT')));
      request.on('error', reject);
      request.end(body);
    });
    webhookDeliveriesTotal.inc({ outcome: 'success' });
    return { status, destinationHost: target.url.hostname.toLowerCase() };
  } catch (error) {
    webhookDeliveriesTotal.inc({ outcome: 'failure' });
    throw error;
  } finally {
    release();
  }
}
