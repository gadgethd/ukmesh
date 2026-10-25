import type { LivePacket } from '../types/index.js';

const PUBLIC_FEED_PROTOCOLS = new Set(['mqtt:', 'mqtts:', 'ws:', 'wss:']);
const MAX_RAW_HEX_LENGTH = 128 * 1024;
const MAX_PAYLOAD_BYTES = 64 * 1024;
const MAX_ORIGIN_LENGTH = 256;

/** Curated namespace exposed to authenticated public-feed subscribers. */
export const PUBLIC_FEED_TOPIC_FILTER = 'public/+/+/packets';
const PUBLIC_FEED_TOPIC_RE = /^public\/[A-Z0-9]{2,8}\/[0-9A-F]{64}\/packets$/;

export type PublicFeedConfig = {
  operatorEnabled: boolean;
  configured: boolean;
  brokerUrl: string | null;
  /**
   * Kept for backwards-compatible config parsing. The public feed is served
   * by the UKmesh broker itself, so it never uses a separate publisher
   * credential.
   */
  username: string | null;
  password: string | null;
};

export type PublicFeedPacketPayload = {
  raw: string;
  origin?: string;
  timestamp?: string;
  SNR?: number;
  RSSI?: number;
  score?: number;
  direction?: 'rx' | 'tx';
};

export type PublicFeedPacket = {
  topic: string;
  payload: PublicFeedPacketPayload;
};

function envBoolean(value: string | undefined, fallback: boolean): boolean {
  if (value == null || value.trim() === '') return fallback;
  return ['1', 'true', 'yes', 'on'].includes(value.trim().toLowerCase());
}

function validBrokerUrl(rawUrl: string): boolean {
  try {
    const url = new URL(rawUrl);
    return PUBLIC_FEED_PROTOCOLS.has(url.protocol)
      && Boolean(url.hostname)
      && !url.username
      && !url.password;
  } catch {
    return false;
  }
}

export function getPublicFeedConfig(env: NodeJS.ProcessEnv = process.env): PublicFeedConfig {
  const operatorEnabled = envBoolean(env['PUBLIC_FEED_PUBLISH_ENABLED'], false);
  const brokerUrl = String(env['PUBLIC_FEED_BROKER_URL'] ?? '').trim() || null;
  const username = String(env['PUBLIC_FEED_BROKER_USERNAME'] ?? '').trim() || null;
  const password = env['PUBLIC_FEED_BROKER_PASSWORD'] ?? null;
  const configured = operatorEnabled
    && brokerUrl !== null
    && validBrokerUrl(brokerUrl);
  return {
    operatorEnabled,
    configured,
    brokerUrl,
    username,
    password,
  };
}

function finiteNumber(value: unknown): number | undefined {
  if (typeof value === 'number') return Number.isFinite(value) ? value : undefined;
  if (typeof value !== 'string' || value.trim() === '') return undefined;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function boundedNumber(value: unknown, minimum: number, maximum: number): number | undefined {
  const parsed = finiteNumber(value);
  return parsed !== undefined && parsed >= minimum && parsed <= maximum ? parsed : undefined;
}

function timestampValue(value: unknown, fallbackMs: number): string {
  const candidate = typeof value === 'string' || typeof value === 'number'
    ? new Date(value)
    : new Date(fallbackMs);
  const date = Number.isFinite(candidate.getTime()) ? candidate : new Date(fallbackMs);
  if (!Number.isFinite(date.getTime())) throw new Error('public feed packet timestamp is invalid');
  return date.toISOString();
}

function packetRegion(packet: LivePacket): string {
  const region = String(packet.iata ?? '').trim().toUpperCase();
  if (!/^[A-Z0-9]{2,8}$/.test(region)) {
    throw new Error('public feed packet region is invalid');
  }
  return region;
}

function packetObserverId(packet: LivePacket): string {
  const observerId = String(packet.rxNodeId ?? '').trim().toUpperCase();
  if (!/^[0-9A-F]{64}$/.test(observerId)) {
    throw new Error('public feed packet observer ID is invalid');
  }
  return observerId;
}

function packetRaw(upstreamEnvelope: Record<string, unknown>): string {
  const raw = typeof upstreamEnvelope['raw'] === 'string'
    ? upstreamEnvelope['raw'].trim().toUpperCase()
    : '';
  if (
    raw.length < 2
    || raw.length > MAX_RAW_HEX_LENGTH
    || raw.length % 2 !== 0
    || !/^[0-9A-F]+$/.test(raw)
  ) {
    throw new Error('public feed packet raw bytes are missing or invalid');
  }
  return raw;
}

function copyOrigin(payload: PublicFeedPacketPayload, value: unknown): void {
  if (typeof value !== 'string') return;
  const origin = value.trim().slice(0, MAX_ORIGIN_LENGTH);
  if (origin) payload.origin = origin;
}

function copyRadioMetric(
  payload: PublicFeedPacketPayload,
  key: 'SNR' | 'RSSI' | 'score',
  value: unknown,
  minimum: number,
  maximum: number,
): void {
  const parsed = boundedNumber(value, minimum, maximum);
  if (parsed !== undefined) payload[key] = parsed;
}

/**
 * Produce the canonical CoreScope-compatible public packet envelope. Only the
 * original raw packet and bounded radio metadata are copied; decoded content
 * and the private relay wrapper never cross this boundary.
 */
export function buildPublicFeedPacket(
  packet: LivePacket,
  upstreamEnvelope: Record<string, unknown>,
): PublicFeedPacket {
  const payload: PublicFeedPacketPayload = {
    raw: packetRaw(upstreamEnvelope),
    timestamp: timestampValue(upstreamEnvelope['timestamp'], packet.ts),
  };
  copyOrigin(payload, upstreamEnvelope['origin']);
  copyRadioMetric(payload, 'SNR', upstreamEnvelope['SNR'], -200, 200);
  copyRadioMetric(payload, 'RSSI', upstreamEnvelope['RSSI'], -200, 100);
  copyRadioMetric(payload, 'score', upstreamEnvelope['score'], -1_000_000, 1_000_000);

  const direction = typeof upstreamEnvelope['direction'] === 'string'
    ? upstreamEnvelope['direction'].trim().toLowerCase()
    : typeof packet.direction === 'string'
      ? packet.direction.trim().toLowerCase()
      : 'rx';
  payload.direction = direction === 'tx' ? 'tx' : 'rx';

  if (Buffer.byteLength(JSON.stringify(payload), 'utf8') > MAX_PAYLOAD_BYTES) {
    throw new Error('public feed packet payload exceeds configured limit');
  }

  return {
    topic: `public/${packetRegion(packet)}/${packetObserverId(packet)}/packets`,
    payload,
  };
}

export function isPublicFeedPacketTopic(topic: string): boolean {
  return PUBLIC_FEED_TOPIC_RE.test(topic.trim());
}

export function publicFeedEventKey(packet: LivePacket): string {
  const observerId = packetObserverId(packet);
  return `${observerId}:${packet.packetHash}`;
}

export function publicFeedPacketIsEligible(packet: LivePacket): boolean {
  return packet.visibilityOk === true
    && packet.network !== 'test'
    && typeof packet.rxNodeId === 'string'
    && /^[0-9A-F]{64}$/.test(packet.rxNodeId)
    && /^[0-9A-F]{16,128}$/.test(packet.packetHash)
    && /^[A-Z0-9]{2,8}$/.test(String(packet.iata ?? '').trim().toUpperCase());
}

export function isValidPublicFeedBrokerUrl(rawUrl: string | null | undefined): boolean {
  return typeof rawUrl === 'string'
    && rawUrl.trim().length > 0
    && validBrokerUrl(rawUrl.trim());
}
