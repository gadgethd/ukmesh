import { createHash } from 'node:crypto';
import type { Incident, IncidentStatus, MessageRecord } from './types.js';
import type { SpamMessageConfig } from './config.js';
import { messageSimilarity, usernameSimilarity } from './similarity.js';
import { normalizeUsername } from './normalize.js';

// Trivial / connectivity tokens that are normal mesh chatter, never spam on
// their own. A cluster made up solely of these (and short) is not an incident.
const BENIGN_TOKENS = new Set([
  'test', 'testing', 'tested', 'ack', 'nack', 'rx', 'tx', 'ping', 'pong',
  'hello', 'hi', 'hey', 'hiya', 'gm', 'gn', 'morning', 'evening', 'afternoon',
  'cq', 'hfcond', 'qsl', '73', 'ok', 'yes', 'no', 'thanks', 'ty', 'lol',
]);

// ---------------------------------------------------------------------------
// Incident clustering
//
// Groups near-duplicate messages that arrive close together in time into
// "suspected spam clusters". Username similarity is supporting evidence only:
// it can lower the text-similarity bar but never creates a cluster on its own.
// ---------------------------------------------------------------------------

interface OpenCluster {
  members: MessageRecord[];
  lastSeen: number;
  /** Cached representative member used for fast comparison. */
  representative: MessageRecord;
}

function combinedJoinScore(
  candidate: MessageRecord,
  cluster: OpenCluster,
  cfg: SpamMessageConfig,
): number {
  const rep = cluster.representative;
  const textSim = messageSimilarity(candidate.norm, rep.norm);
  const nameSim = usernameSimilarity(candidate.sender, rep.sender);

  // Two messages both carrying the spam-page marker are very likely related.
  const bothMarker = candidate.norm.hasSpamMarker && rep.norm.hasSpamMarker;

  if (textSim >= cfg.textSimThreshold) return textSim;
  if (bothMarker && textSim >= cfg.textSimThresholdWithName * 0.75) return Math.max(textSim, 0.8);
  if (textSim >= cfg.textSimThresholdWithName && nameSim >= cfg.usernameSimThreshold) {
    return (textSim + nameSim) / 2;
  }
  return 0;
}

/** Most frequent normalized text in a set of members (the cluster signature). */
function modalNormalized(members: MessageRecord[]): MessageRecord {
  const counts = new Map<string, number>();
  for (const m of members) {
    counts.set(m.norm.normalized, (counts.get(m.norm.normalized) ?? 0) + 1);
  }
  let best = members[0]!;
  let bestCount = -1;
  for (const m of members) {
    const c = counts.get(m.norm.normalized)!;
    // Tie-break on earliest, for stable keys as the cluster grows.
    if (c > bestCount || (c === bestCount && m.observedAt < best.observedAt)) {
      best = m;
      bestCount = c;
    }
  }
  return best;
}

function incidentKey(network: string, canonicalText: string): string {
  const h = createHash('sha1').update(`${network}|${canonicalText}`).digest('hex');
  return h.slice(0, 16);
}

/** Max number of members falling inside any single sliding burst window. */
function maxBurstDensity(sorted: MessageRecord[], windowMs: number): number {
  let best = 0;
  let start = 0;
  for (let end = 0; end < sorted.length; end++) {
    while (sorted[end]!.observedAt - sorted[start]!.observedAt > windowMs) start++;
    best = Math.max(best, end - start + 1);
  }
  return best;
}

/** Summary of who is sending the messages in a cluster. */
interface SenderProfile {
  distinctSenders: number;
  /** Largest share of messages from a single (normalized) sender. */
  dominantShare: number;
  /** >=2 distinct senders that are fuzzy-similar variants of each other. */
  rotatingSimilarNames: boolean;
}

function profileSenders(members: MessageRecord[], cfg: SpamMessageConfig): SenderProfile {
  const counts = new Map<string, number>();
  for (const m of members) {
    const key = normalizeUsername(m.sender) || '∅';
    counts.set(key, (counts.get(key) ?? 0) + 1);
  }
  const distinct = [...counts.keys()].filter((k) => k !== '∅');
  const distinctSenders = distinct.length || (counts.has('∅') ? 1 : 0);
  let maxCount = 0;
  for (const c of counts.values()) maxCount = Math.max(maxCount, c);
  const dominantShare = members.length > 0 ? maxCount / members.length : 0;

  let rotatingSimilarNames = false;
  for (let i = 0; i < distinct.length && !rotatingSimilarNames; i++) {
    for (let j = i + 1; j < distinct.length; j++) {
      if (usernameSimilarity(distinct[i]!, distinct[j]!) >= cfg.usernameSimThreshold) {
        rotatingSimilarNames = true;
        break;
      }
    }
  }
  return { distinctSenders, dominantShare, rotatingSimilarNames };
}

/** Whether the canonical text is only trivial/connectivity tokens. */
function isBenignChatter(canonical: string): boolean {
  const tokens = canonical.split(' ').filter((t) => t && !t.startsWith('url:'));
  if (tokens.length === 0) return true;
  return tokens.every((t) => BENIGN_TOKENS.has(t));
}

/**
 * Whether the canonical text is *dominated* by trivial/connectivity tokens —
 * i.e. once benign words, bare numbers and 1–2 char fragments are removed,
 * essentially nothing substantive remains ("test", "rx ack 73", "test 123").
 *
 * A long templated message that merely *contains* "test" (e.g. an automated
 * "ANDY KIRBY MESHCORE(TM) 00:16:4: !test" flood) keeps real words like
 * "andy"/"kirby"/"meshcore" and is therefore NOT benign-dominated — so it is
 * not mistaken for ordinary connectivity testing.
 */
function isBenignDominated(canonical: string): boolean {
  const tokens = canonical.split(' ').filter((t) => t && !t.startsWith('url:'));
  if (tokens.length === 0) return true;
  const substantive = tokens.filter(
    (t) => t.length >= 3 && !/^\d+$/.test(t) && !BENIGN_TOKENS.has(t),
  );
  return substantive.length === 0;
}

/** Does some canonical URL repeat across two or more transmissions? */
function repeatedUrl(members: MessageRecord[]): boolean {
  const counts = new Map<string, number>();
  for (const m of members) {
    for (const u of new Set(m.norm.urls)) counts.set(u, (counts.get(u) ?? 0) + 1);
  }
  for (const c of counts.values()) if (c >= 2) return true;
  return false;
}

/**
 * Decide whether a cluster is a genuine suspected-spam incident rather than
 * normal chatter. This is the gate that protects ordinary users: broad
 * participation, trivial content, or test traffic never qualifies.
 */
function qualifies(
  canonical: string,
  hasMarker: boolean,
  hasUrl: boolean,
  sender: SenderProfile,
  cfg: SpamMessageConfig,
): boolean {
  const substantive = canonical.length >= cfg.minContentChars || hasUrl || hasMarker;
  if (!substantive) return false;
  if (isBenignChatter(canonical) && !hasUrl && !hasMarker) return false;

  // Many *dissimilar* people sending the same thing = legitimate (a meme, a
  // greeting, a coordinated test) — not one actor abusing the network.
  const broadChatter =
    sender.distinctSenders > cfg.maxIndependentSenders &&
    !sender.rotatingSimilarNames &&
    sender.dominantShare < cfg.dominantSenderShare;
  if (broadChatter) return false;

  const senderSuspicious =
    sender.distinctSenders <= 1 ||
    sender.dominantShare >= cfg.dominantSenderShare ||
    sender.rotatingSimilarNames;

  // A few dissimilar senders, no link, no marker -> not enough to call spam.
  if (!senderSuspicious && !hasUrl && !hasMarker) return false;

  return true;
}

function scoreIncident(
  members: MessageRecord[],
  hasMarker: boolean,
  hasUrl: boolean,
  sender: SenderProfile,
  cfg: SpamMessageConfig,
): { score: number; reasons: string[] } {
  const reasons: string[] = [];
  const sorted = [...members].sort((a, b) => a.observedAt - b.observedAt);
  const count = members.length;
  const burst = maxBurstDensity(sorted, cfg.burstWindowMs);
  const urlRepeats = repeatedUrl(members);
  const canonicalText = modalNormalized(sorted).norm.normalized;

  // Volume + burst density form the baseline.
  const volume = Math.min(1, (count - cfg.minTransmissions + 1) / 8);
  const density = Math.min(1, burst / Math.max(cfg.minBurst * 2, 4));
  let score = 0.3 * volume + 0.2 * density;

  if (hasMarker) score += 0.25;
  if (urlRepeats) score += 0.2;
  else if (hasUrl) score += 0.1;
  if (sender.rotatingSimilarNames) score += 0.25;
  else if (sender.distinctSenders <= 1 && sender.dominantShare >= 0.8) score += 0.1;

  // Connectivity/test floods with no link, marker or name-rotation are noise,
  // not promotional/abusive spam — dampen so they fall below the public floor.
  // Only content that is *dominated* by benign tokens counts (so a templated
  // flood that merely contains the word "test" is not let off the hook).
  const looksLikeTesting =
    isBenignDominated(canonicalText) && !hasUrl && !hasMarker && !sender.rotatingSimilarNames;

  // A sustained, high-volume repeat of the same templated message is itself a
  // strong automated-abuse signal, regardless of what it says — but we never
  // reward what looks like ordinary connectivity testing, so a benign flood
  // stays suppressed no matter how large it gets.
  const isFlood = count >= cfg.floodMinMessages;
  if (isFlood && !looksLikeTesting) score += 0.2;

  if (looksLikeTesting) score *= 0.6;

  score = Math.max(0, Math.min(1, score));

  reasons.push(`${count} near-duplicate messages`);
  reasons.push(`up to ${burst} within ${Math.round(cfg.burstWindowMs / 60000)} min`);
  if (isFlood) reasons.push('sustained high-volume flood');
  if (sender.rotatingSimilarNames) reasons.push(`${sender.distinctSenders} similar (rotating) sender names`);
  else if (sender.distinctSenders <= 1) reasons.push('single repeating sender');
  if (urlRepeats) reasons.push('same link repeated across messages');
  else if (hasUrl) reasons.push('contains a link');
  if (hasMarker) reasons.push('references ukmesh.com/spam marker');
  if (looksLikeTesting) reasons.push('resembles connectivity/test traffic (low weight)');

  return { score, reasons };
}

function finalizeIncident(members: MessageRecord[], cfg: SpamMessageConfig): Incident | null {
  const sorted = [...members].sort((a, b) => a.observedAt - b.observedAt);
  const rep = modalNormalized(sorted);
  const network = sorted[0]!.network;

  const observerIds = new Set<string>();
  const channels = new Set<string>();
  const senderNames = new Set<string>();
  let hasMarker = false;
  let hasUrl = false;
  for (const m of sorted) {
    for (const o of m.observers) observerIds.add(o.observerId);
    if (m.channelLabel) channels.add(m.channelLabel);
    if (m.sender) senderNames.add(m.sender);
    if (m.norm.hasSpamMarker) hasMarker = true;
    if (m.norm.urls.length > 0) hasUrl = true;
  }

  const canonical = rep.norm.normalized;
  const sender = profileSenders(sorted, cfg);
  if (!qualifies(canonical, hasMarker, hasUrl, sender, cfg)) return null;

  const { score, reasons } = scoreIncident(sorted, hasMarker, hasUrl, sender, cfg);

  return {
    key: incidentKey(network, canonical),
    network,
    members: sorted,
    firstSeen: sorted[0]!.observedAt,
    lastSeen: sorted[sorted.length - 1]!.observedAt,
    messageCount: sorted.length,
    observerCount: observerIds.size,
    channels: [...channels].sort(),
    senderNames: [...senderNames],
    canonicalText: canonical,
    representativeText: rep.text,
    hasSpamMarker: hasMarker,
    hasUrl,
    score,
    reasons,
  };
}

/**
 * Cluster a batch of message records into spam incidents.
 *
 * Online greedy clustering over time-sorted records: each message joins the
 * best still-open cluster (one active within `joinWindowMs`) above threshold,
 * or starts a new one. Clusters that never reach the burst minimum are dropped
 * — ordinary repeated chatter ("test", "hello") is not an incident unless it
 * forms a genuine burst.
 */
export function clusterMessages(records: MessageRecord[], cfg: SpamMessageConfig): Incident[] {
  const excluded = new Set(cfg.excludeChannels.map((c) => c.toLowerCase()));
  const eligible =
    excluded.size > 0
      ? records.filter((r) => !excluded.has((r.channelLabel ?? '').toLowerCase()))
      : records;
  const sorted = [...eligible].sort((a, b) => a.observedAt - b.observedAt);
  const open: OpenCluster[] = [];
  const closed: OpenCluster[] = [];

  for (const rec of sorted) {
    // Retire clusters whose last activity is older than the join window.
    for (let i = open.length - 1; i >= 0; i--) {
      if (rec.observedAt - open[i]!.lastSeen > cfg.joinWindowMs) {
        closed.push(open[i]!);
        open.splice(i, 1);
      }
    }

    let bestIdx = -1;
    let bestScore = 0;
    for (let i = 0; i < open.length; i++) {
      const cl = open[i]!;
      if (cl.representative.network !== rec.network) continue;
      const s = combinedJoinScore(rec, cl, cfg);
      if (s > bestScore) {
        bestScore = s;
        bestIdx = i;
      }
    }

    if (bestIdx >= 0) {
      const cl = open[bestIdx]!;
      cl.members.push(rec);
      cl.lastSeen = rec.observedAt;
      // Keep representative as the modal message so the key stays stable.
      cl.representative = modalNormalized(cl.members);
    } else {
      open.push({ members: [rec], lastSeen: rec.observedAt, representative: rec });
    }
  }

  const all = [...closed, ...open];
  const incidents: Incident[] = [];
  for (const cl of all) {
    if (cl.members.length < cfg.minTransmissions) continue;
    const sortedMembers = [...cl.members].sort((a, b) => a.observedAt - b.observedAt);
    if (maxBurstDensity(sortedMembers, cfg.burstWindowMs) < cfg.minBurst) continue;
    const incident = finalizeIncident(cl.members, cfg);
    if (incident) incidents.push(incident);
  }

  incidents.sort((a, b) => b.lastSeen - a.lastSeen);
  return incidents;
}

/** Whether an incident is still ongoing given the current time. */
export function incidentStatus(lastSeen: number, now: number, cfg: SpamMessageConfig): IncidentStatus {
  return now - lastSeen <= cfg.ongoingWindowMs ? 'active' : 'closed';
}

export { incidentKey };
