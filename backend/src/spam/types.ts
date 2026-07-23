// ---------------------------------------------------------------------------
// Shared types for message-spam detection
// ---------------------------------------------------------------------------

/** Result of normalizing a single message body for comparison. */
export interface NormalizedMessage {
  /** The original, untouched message text. */
  original: string;
  /** Lowercased / de-punctuated / de-duplicated text used for fuzzy matching. */
  normalized: string;
  /** Whitespace-separated tokens of the normalized text. */
  tokens: string[];
  /** Canonicalized URLs found in the message. */
  urls: string[];
  /** True if the message references the ukmesh.com/spam marker. */
  hasSpamMarker: boolean;
}

/** One observer's reception of a message (used for origin estimation). */
export interface ObserverObservation {
  observerId: string;
  lat: number;
  lon: number;
  hopCount?: number | null;
  rssi?: number | null;
  snr?: number | null;
}

/** One logical message transmission (one packet_hash) with all its observers. */
export interface MessageRecord {
  /** packet_hash — unique per transmission. */
  id: string;
  network: string;
  /** Raw sender display name (kept local; never exposed publicly). */
  sender: string;
  /** Raw message text (kept local; never exposed publicly). */
  text: string;
  norm: NormalizedMessage;
  channelHash: string;
  channelLabel: string;
  /** Earliest observed time (ms epoch). */
  observedAt: number;
  /** Sender-claimed timestamp (seconds epoch) if present. */
  claimedTs?: number;
  observers: ObserverObservation[];
}

/** A detected spam incident (cluster of similar messages in a time window). */
export interface Incident {
  /** Deterministic key derived from network + canonical text (stable across runs). */
  key: string;
  network: string;
  members: MessageRecord[];
  firstSeen: number;
  lastSeen: number;
  /** Distinct transmissions (== members.length). */
  messageCount: number;
  /** Distinct geolocated observers across all members. */
  observerCount: number;
  /** Channel labels involved (e.g. "Public", "test"). */
  channels: string[];
  /** Distinct raw sender names (kept local). */
  senderNames: string[];
  /** Modal normalized text — the canonical signature of the cluster. */
  canonicalText: string;
  /** Representative raw message (kept local; sanitized before publishing). */
  representativeText: string;
  hasSpamMarker: boolean;
  /** Whether any member carried a URL. */
  hasUrl: boolean;
  /** 0..1 detection/severity confidence for "this is spam". */
  score: number;
  /** Human-readable factors behind the detection score. */
  reasons: string[];
}

export type ConfidenceLevel = 'high' | 'medium' | 'low' | 'insufficient';

/** Coarse origin estimate for an incident. */
export interface OriginEstimate {
  /** Estimated centroid (precise; coarsened before publishing). */
  lat: number | null;
  lon: number | null;
  /** Coarse uncertainty radius (km). */
  radiusKm: number | null;
  /** Nearest named coarse region, or a generic fallback. */
  region: string;
  /** 0..1 confidence in the estimate. */
  confidence: number;
  level: ConfidenceLevel;
  /** Distinct geolocated observers used. */
  observerCount: number;
  /** Explanation of the factors behind the estimate. */
  reasons: string[];
}

export type IncidentStatus = 'active' | 'closed';
