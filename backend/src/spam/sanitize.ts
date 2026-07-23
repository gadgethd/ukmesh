import type { Incident, IncidentStatus, OriginEstimate } from './types.js';
import type { SpamMessageConfig } from './config.js';
import { SPAM_MARKER } from './normalize.js';

// ---------------------------------------------------------------------------
// Public sanitization
//
// The detection layer works with raw senders, exact coordinates and observer
// ids. None of that may reach a public response. This module is the single
// gate that turns an internal Incident into a publishable, privacy-safe shape.
// ---------------------------------------------------------------------------

const SAMPLE_MAX_LEN = 160;

/**
 * Redact a username to a short hint that still conveys *similarity* between
 * variants (so "John" / "John2" / "John_UK" read as related) without
 * publishing the full handle. e.g. "John_UK" -> "Jo…".
 */
export function sanitizeUsername(name: string): string {
  const trimmed = (name ?? '').trim();
  if (trimmed.length === 0) return 'unknown';
  // Keep a short, non-identifying prefix only.
  const visible = trimmed.slice(0, Math.min(2, trimmed.length));
  return `${visible}…`;
}

/** De-duplicate sanitized username hints, preserving order. */
export function sanitizeUsernames(names: string[]): string[] {
  const out: string[] = [];
  const seen = new Set<string>();
  for (const n of names) {
    const s = sanitizeUsername(n);
    if (!seen.has(s)) {
      seen.add(s);
      out.push(s);
    }
  }
  return out.slice(0, 8);
}

/**
 * Produce a safe, representative sample of the spam text: strip URLs (the
 * marker becomes a neutral note), drop @mentions and long hex/number runs that
 * could identify a node, and truncate.
 */
export function sanitizeSample(text: string, hasSpamMarker: boolean): string {
  let s = (text ?? '').normalize('NFKC');
  // Replace URLs (incl. the marker) with neutral placeholders.
  s = s.replace(/\b(?:https?:\/\/)?(?:www\.)?[a-z0-9-]+(?:\.[a-z0-9-]+)+(?:\/[^\s]*)?/gi, (m) =>
    m.toLowerCase().includes(SPAM_MARKER) ? '[spam-link]' : '[link]',
  );
  // Strip @mentions of handles/node names.
  s = s.replace(/@\[[^\]]*\]/g, '[mention]').replace(/@\S+/g, '[mention]');
  // Strip long hex/number runs (possible ids / keys).
  s = s.replace(/\b[0-9a-f]{8,}\b/gi, '[id]');
  s = s.replace(/\s+/g, ' ').trim();
  if (s.length > SAMPLE_MAX_LEN) s = `${s.slice(0, SAMPLE_MAX_LEN - 1).trimEnd()}…`;
  if (s.length === 0) return hasSpamMarker ? '[spam-link]' : '[message]';
  return s;
}

/** Coarsen a coordinate to a privacy-safe grid (default ~0.1° ≈ 11 km). */
export function coarsenCoord(value: number, stepDeg: number): number {
  return Math.round(value / stepDeg) * stepDeg;
}

export interface PublicOrigin {
  region: string;
  confidence: number;
  level: OriginEstimate['level'];
  /** Coarse heat zone for the map: coarsened centre + radius. null if insufficient. */
  zone: { lat: number; lon: number; radiusKm: number } | null;
  observerCount: number;
  reasons: string[];
}

export function sanitizeOrigin(origin: OriginEstimate, cfg: SpamMessageConfig): PublicOrigin {
  const zone =
    origin.lat != null && origin.lon != null && origin.radiusKm != null && origin.level !== 'insufficient'
      ? {
          lat: Number(coarsenCoord(origin.lat, cfg.coarsenStepDeg).toFixed(3)),
          lon: Number(coarsenCoord(origin.lon, cfg.coarsenStepDeg).toFixed(3)),
          radiusKm: origin.radiusKm,
        }
      : null;
  return {
    region: origin.region,
    confidence: Number(origin.confidence.toFixed(2)),
    level: origin.level,
    zone,
    observerCount: origin.observerCount,
    reasons: origin.reasons,
  };
}

export interface PublicIncident {
  id: string;
  status: IncidentStatus;
  network: string;
  firstSeen: string;
  lastSeen: string;
  messageCount: number;
  observerCount: number;
  channels: string[];
  similarUsernames: string[];
  usernameVariants: number;
  sampleMessage: string;
  spamMarker: boolean;
  confidence: number;
  reasons: string[];
  origin: PublicOrigin;
}

/** Turn an internal incident + origin estimate into a fully public-safe object. */
export function sanitizeIncident(
  incident: Incident,
  origin: OriginEstimate,
  status: IncidentStatus,
  cfg: SpamMessageConfig,
): PublicIncident {
  return {
    id: incident.key,
    status,
    network: incident.network,
    firstSeen: new Date(incident.firstSeen).toISOString(),
    lastSeen: new Date(incident.lastSeen).toISOString(),
    messageCount: incident.messageCount,
    observerCount: incident.observerCount,
    channels: incident.channels,
    similarUsernames: sanitizeUsernames(incident.senderNames),
    usernameVariants: new Set(incident.senderNames.map((n) => n.trim().toLowerCase())).size,
    sampleMessage: sanitizeSample(incident.representativeText, incident.hasSpamMarker),
    spamMarker: incident.hasSpamMarker,
    confidence: Number(incident.score.toFixed(2)),
    reasons: incident.reasons,
    origin: sanitizeOrigin(origin, cfg),
  };
}
