import type { LivePacket } from '../types/index.js';
import { isPacketShareSecretCiphertext } from './packetShareCrypto.js';

const MAX_TOPIC_LENGTH = 512;
const MAX_PAYLOAD_BYTES = 64 * 1024;
const PACKET_SHARE_PROTOCOLS = new Set(['mqtt:', 'mqtts:', 'ws:', 'wss:']);
const PACKET_SHARE_TOPIC_PLACEHOLDERS = /\{(network|iata|observerId|packetHash)\}/g;

export type PacketShareDestinationRow = {
  destination_id: string;
  display_name: string;
  website_url?: string | null;
  description?: string | null;
  broker_url: string | null;
  topic_template: string | null;
  broker_username: string | null;
  broker_password_ciphertext: string | null;
  enabled: boolean;
};

export type PacketShareDestinationView = {
  id: string;
  name: string;
  websiteUrl: string | null;
  description: string | null;
  configured: boolean;
  selected: boolean;
  lastForwardedAt: string | null;
};

export type PacketShareTarget = PacketShareDestinationRow & {
  owner_username: string;
  node_id: string;
};

export type PacketShareForwardPayload = Record<string, string>;

function packetTopicValue(packet: LivePacket): Record<string, string> {
  return {
    network: String(packet.network ?? 'ukmesh').trim().toLowerCase() || 'ukmesh',
    iata: String(packet.iata ?? 'UNK').trim().toUpperCase() || 'UNK',
    observerId: String(packet.rxNodeId ?? '').trim().toUpperCase(),
    packetHash: packet.packetHash,
  };
}

export function isValidPacketShareBrokerUrl(rawUrl: string | null): boolean {
  if (!rawUrl?.trim()) return false;
  try {
    const url = new URL(rawUrl.trim());
    return PACKET_SHARE_PROTOCOLS.has(url.protocol)
      && Boolean(url.hostname)
      && !url.username
      && !url.password;
  } catch {
    return false;
  }
}

export function isValidPacketShareWebsiteUrl(rawUrl: string | null | undefined): boolean {
  if (!rawUrl?.trim()) return false;
  try {
    const url = new URL(rawUrl.trim());
    return url.protocol === 'https:'
      && Boolean(url.hostname)
      && !url.username
      && !url.password;
  } catch {
    return false;
  }
}

export function isValidPacketShareTopicTemplate(template: string | null): boolean {
  if (!template || !template.trim() || template.length > MAX_TOPIC_LENGTH) return false;
  if (/[\u0000-\u001f\u007f]/.test(template) || template.includes('#') || template.includes('+')) return false;
  const withPlaceholdersReplaced = template.replace(PACKET_SHARE_TOPIC_PLACEHOLDERS, 'value');
  if (withPlaceholdersReplaced.includes('{') || withPlaceholdersReplaced.includes('}')) return false;
  return withPlaceholdersReplaced.split('/').every((part) => part.length > 0);
}

export function destinationConfigured(
  destination: Pick<PacketShareDestinationRow, 'enabled' | 'broker_url' | 'topic_template' | 'broker_username' | 'broker_password_ciphertext'>,
  encryptionKeyAvailable = true,
): boolean {
  if (!destination.enabled
    || !isValidPacketShareBrokerUrl(destination.broker_url)
    || !isValidPacketShareTopicTemplate(destination.topic_template)) return false;
  const username = destination.broker_username?.trim() || '';
  const password = destination.broker_password_ciphertext?.trim() || '';
  if (Boolean(username) !== Boolean(password)) return false;
  if (password && (!encryptionKeyAvailable || !isPacketShareSecretCiphertext(password))) return false;
  return true;
}

export function renderPacketShareTopic(template: string, packet: LivePacket): string {
  if (!isValidPacketShareTopicTemplate(template)) {
    throw new Error('invalid packet share destination topic');
  }
  const values = packetTopicValue(packet);
  const rendered = template.replace(/\{(network|iata|observerId|packetHash)\}/g, (_, key: keyof typeof values) => values[key]);
  if (
    rendered.length < 1
    || rendered.length > MAX_TOPIC_LENGTH
    || rendered.includes('#')
    || rendered.includes('+')
    || /[\u0000-\u001f\u007f]/.test(rendered)
    || rendered.split('/').some((part) => part.length < 1)
  ) {
    throw new Error('invalid packet share destination topic');
  }
  return rendered;
}

function copyEnvelopeValue(target: PacketShareForwardPayload, source: Record<string, unknown>, key: string): void {
  const value = source[key];
  if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
    target[key] = String(value);
  }
}

function padDatePart(value: number): string {
  return String(value).padStart(2, '0');
}

function packetDateParts(packet: LivePacket, timestamp: unknown): { time: string; date: string } {
  const candidate = typeof timestamp === 'string' || typeof timestamp === 'number'
    ? new Date(timestamp)
    : new Date(packet.ts);
  const date = Number.isFinite(candidate.getTime()) ? candidate : new Date(packet.ts);
  if (!Number.isFinite(date.getTime())) return { time: '', date: '' };
  return {
    time: `${padDatePart(date.getUTCHours())}:${padDatePart(date.getUTCMinutes())}:${padDatePart(date.getUTCSeconds())}`,
    date: `${padDatePart(date.getUTCDate())}/${padDatePart(date.getUTCMonth() + 1)}/${date.getUTCFullYear()}`,
  };
}

/** Build a bounded flat mctomqtt-compatible packet without forwarding decoded content. */
export function buildPacketSharePayload(
  packet: LivePacket,
  upstreamEnvelope: Record<string, unknown>,
): PacketShareForwardPayload {
  const envelope: PacketShareForwardPayload = {};
  for (const key of [
    'raw', 'hash', 'packet_type', 'SNR', 'RSSI', 'score', 'route', 'len',
    'payload_len', 'direction', 'origin', 'origin_id', 'timestamp', 'type', 'time', 'date',
  ]) {
    copyEnvelopeValue(envelope, upstreamEnvelope, key);
  }

  if (!('timestamp' in envelope)) envelope.timestamp = new Date(packet.ts).toISOString();
  if (!('hash' in envelope)) envelope.hash = packet.packetHash;
  if (!('type' in envelope)) envelope.type = 'PACKET';
  if (!('direction' in envelope)) envelope.direction = 'rx';
  if (!('origin_id' in envelope) && packet.rxNodeId) envelope.origin_id = packet.rxNodeId;
  if (!('packet_type' in envelope) && packet.packetType != null) envelope.packet_type = String(packet.packetType);
  if (!('len' in envelope) && typeof envelope.raw === 'string') envelope.len = String(Math.floor(envelope.raw.length / 2));

  const dateParts = packetDateParts(packet, envelope.timestamp);
  if (dateParts.time) envelope.time = dateParts.time;
  if (dateParts.date) envelope.date = dateParts.date;

  if (Buffer.byteLength(JSON.stringify(envelope), 'utf8') > MAX_PAYLOAD_BYTES) {
    throw new Error('packet share payload exceeds configured limit');
  }
  return envelope;
}

export function isForwardablePacket(packet: LivePacket): boolean {
  return packet.visibilityOk === true
    && packet.network !== 'test'
    && typeof packet.rxNodeId === 'string'
    && /^[0-9A-F]{64}$/.test(packet.rxNodeId)
    && /^[0-9A-F]{16,128}$/.test(packet.packetHash);
}

export function packetShareEventKey(ownerUsername: string, destinationId: string, packet: LivePacket): string {
  return `${ownerUsername}:${destinationId}:${packet.rxNodeId}:${packet.packetHash}`;
}

export function publicDestinationView(
  destination: PacketShareDestinationRow,
  selected: boolean,
  encryptionKeyAvailable: boolean,
  lastForwardedAt: string | null = null,
): PacketShareDestinationView {
  return {
    id: destination.destination_id,
    name: destination.display_name,
    websiteUrl: isValidPacketShareWebsiteUrl(destination.website_url)
      ? destination.website_url!.trim()
      : null,
    description: destination.description?.trim() || null,
    configured: destinationConfigured(destination, encryptionKeyAvailable),
    selected,
    lastForwardedAt,
  };
}
