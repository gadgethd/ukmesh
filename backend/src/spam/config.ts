// ---------------------------------------------------------------------------
// Message-spam detection configuration
//
// Every threshold / window is tunable here (and most via environment variables)
// so operators can adjust sensitivity without touching detection code.
// ---------------------------------------------------------------------------

function envNum(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw == null || raw.trim() === '') return fallback;
  const parsed = Number(raw);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function envBool(name: string, fallback: boolean): boolean {
  const raw = process.env[name];
  if (raw == null || raw.trim() === '') return fallback;
  return !['0', 'false', 'no', 'off'].includes(raw.trim().toLowerCase());
}

export interface SpamMessageConfig {
  /** How far back the periodic analyzer looks for messages to (re)cluster. */
  analysisWindowHours: number;
  /** Interval between periodic analyzer runs (ms). */
  analyzerIntervalMs: number;
  /** Whether the in-process periodic analyzer runs at all. */
  analyzerEnabled: boolean;

  // --- clustering ---
  /** Min normalized-text similarity (0..1) for two messages to join a cluster. */
  textSimThreshold: number;
  /** Lower text threshold accepted when usernames are also similar. */
  textSimThresholdWithName: number;
  /** Min username similarity (0..1) treated as supporting evidence. */
  usernameSimThreshold: number;
  /** Two messages can only join the same incident if seen within this gap (ms). */
  joinWindowMs: number;
  /** A cluster must contain at least this many distinct transmissions to be an incident. */
  minTransmissions: number;
  /** ...and at least this many must fall inside one burst window. */
  burstWindowMs: number;
  minBurst: number;

  // --- qualification (separating abuse from normal chatter) ---
  /** Canonical text must be at least this long (unless it carries a URL/marker). */
  minContentChars: number;
  /** Above this many *dissimilar* senders the cluster is treated as broad
   *  legitimate chatter (e.g. lots of people sending "test"), not spam. */
  maxIndependentSenders: number;
  /** A single sender accounting for at least this share of messages qualifies. */
  dominantSenderShare: number;
  /** At/above this message count a qualified cluster is treated as a sustained
   *  flood and scored up — an automated, high-volume abuse signal on its own. */
  floodMinMessages: number;
  /** Channel labels excluded from spam detection (the test channel by design
   *  carries repeated test traffic that is not abuse). */
  excludeChannels: string[];
  /** Public lists / "ongoing" status only count incidents at/above this score. */
  publicMinScore: number;

  // --- lifecycle ---
  /** Incident is "active" (ongoing) while its last message is within this window. */
  ongoingWindowMs: number;

  // --- origin estimation ---
  /** Resolve relay paths to anchor the origin on the near-source repeater
   *  (much tighter than observer locations) when paths are available. */
  originUsePaths: boolean;
  /** Max distinct transmissions sampled for path resolution per incident. */
  originPathMaxPackets: number;
  /** Min resolved near-source repeaters before a path anchor is trusted. */
  originPathMinVotes: number;
  /** Repeaters within this distance of the top near-source repeater form the
   *  consensus cluster; resolutions further out are treated as outliers. */
  originPathClusterKm: number;
  /** Weight of an ambiguously-resolved repeater vote relative to a certain one. */
  originPathAmbiguousWeight: number;
  /** Min distinct geolocated observers before any origin estimate is attempted. */
  originMinObservers: number;
  /** Observers that heard the message within this many hops are treated as
   *  "near the source" and anchor the estimate (distant relays only broaden it). */
  originNearHopMax: number;
  /** Width (in hops) of the closest-receiver cohort: observers within
   *  min-hop + this slack anchor the estimate even when nobody heard it within
   *  `originNearHopMax` hops (sparse-coverage areas). */
  originNearHopSlack: number;
  /** Rough LoRa link range (km) per hop, used as the origin radius allowance:
   *  even a near receiver only pins the source to within ~hop × this. */
  originPerHopKm: number;
  /** Floor for the reported coarse radius (km) — never claim more precision. */
  originMinRadiusKm: number;
  /** Public coordinate coarsening step in degrees (~0.1deg ≈ 11 km). */
  coarsenStepDeg: number;
  /** Max distance (km) to snap an estimate to a named region centroid. */
  regionSnapKm: number;
}

export function loadSpamMessageConfig(): SpamMessageConfig {
  return {
    analysisWindowHours: envNum('SPAM_MESSAGE_WINDOW_HOURS', 24),
    analyzerIntervalMs: envNum('SPAM_MESSAGE_INTERVAL_MS', 5 * 60 * 1000),
    analyzerEnabled: envBool('SPAM_MESSAGE_ANALYZER_ENABLED', true),

    textSimThreshold: envNum('SPAM_MESSAGE_TEXT_SIM', 0.82),
    textSimThresholdWithName: envNum('SPAM_MESSAGE_TEXT_SIM_NAME', 0.6),
    usernameSimThreshold: envNum('SPAM_MESSAGE_NAME_SIM', 0.7),
    joinWindowMs: envNum('SPAM_MESSAGE_JOIN_WINDOW_MIN', 30) * 60 * 1000,
    minTransmissions: envNum('SPAM_MESSAGE_MIN_TRANSMISSIONS', 8),
    burstWindowMs: envNum('SPAM_MESSAGE_BURST_WINDOW_MIN', 10) * 60 * 1000,
    minBurst: envNum('SPAM_MESSAGE_MIN_BURST', 3),

    minContentChars: envNum('SPAM_MESSAGE_MIN_CONTENT_CHARS', 12),
    maxIndependentSenders: envNum('SPAM_MESSAGE_MAX_INDEPENDENT_SENDERS', 4),
    dominantSenderShare: envNum('SPAM_MESSAGE_DOMINANT_SENDER_SHARE', 0.6),
    floodMinMessages: envNum('SPAM_MESSAGE_FLOOD_MIN_MESSAGES', 25),
    excludeChannels: (process.env['SPAM_MESSAGE_EXCLUDE_CHANNELS'] ?? 'test')
      .split(',')
      .map((s) => s.trim().toLowerCase())
      .filter(Boolean),
    publicMinScore: envNum('SPAM_MESSAGE_PUBLIC_MIN_SCORE', 0.5),

    ongoingWindowMs: envNum('SPAM_MESSAGE_ONGOING_WINDOW_MIN', 30) * 60 * 1000,

    originUsePaths: envBool('SPAM_MESSAGE_ORIGIN_USE_PATHS', true),
    originPathMaxPackets: envNum('SPAM_MESSAGE_ORIGIN_PATH_MAX_PACKETS', 40),
    originPathMinVotes: envNum('SPAM_MESSAGE_ORIGIN_PATH_MIN_VOTES', 3),
    originPathClusterKm: envNum('SPAM_MESSAGE_ORIGIN_PATH_CLUSTER_KM', 45),
    originPathAmbiguousWeight: envNum('SPAM_MESSAGE_ORIGIN_PATH_AMBIGUOUS_WEIGHT', 0.4),
    originMinObservers: envNum('SPAM_MESSAGE_ORIGIN_MIN_OBSERVERS', 2),
    originNearHopMax: envNum('SPAM_MESSAGE_ORIGIN_NEAR_HOP_MAX', 2),
    originNearHopSlack: envNum('SPAM_MESSAGE_ORIGIN_NEAR_HOP_SLACK', 1),
    originPerHopKm: envNum('SPAM_MESSAGE_ORIGIN_PER_HOP_KM', 12),
    originMinRadiusKm: envNum('SPAM_MESSAGE_ORIGIN_MIN_RADIUS_KM', 8),
    coarsenStepDeg: envNum('SPAM_MESSAGE_COARSEN_STEP_DEG', 0.1),
    regionSnapKm: envNum('SPAM_MESSAGE_REGION_SNAP_KM', 70),
  };
}

/** Default config snapshot, used by tests and as a base for overrides. */
export const DEFAULT_SPAM_MESSAGE_CONFIG: SpamMessageConfig = loadSpamMessageConfig();
