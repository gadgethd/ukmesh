import { test } from 'node:test';
import assert from 'node:assert/strict';
import { clusterMessages, incidentStatus } from './cluster.js';
import { messageSimilarity } from './similarity.js';
import { normalizeMessage } from './normalize.js';
import { DEFAULT_SPAM_MESSAGE_CONFIG, type SpamMessageConfig } from './config.js';
import type { MessageRecord } from './types.js';

const MIN = 60 * 1000;

// Clustering-mechanics tests use a low transmission floor so fixtures stay
// small; the production threshold is policy and is asserted separately below.
function cfg(overrides: Partial<SpamMessageConfig> = {}): SpamMessageConfig {
  return { ...DEFAULT_SPAM_MESSAGE_CONFIG, minTransmissions: 3, minBurst: 3, ...overrides };
}

let seq = 0;
function rec(text: string, sender: string, tMinutes: number, channelLabel = 'Public'): MessageRecord {
  return {
    id: `hash-${seq++}`,
    network: 'ukmesh',
    sender,
    text,
    norm: normalizeMessage(text),
    channelHash: '80',
    channelLabel,
    observedAt: tMinutes * MIN,
    observers: [],
  };
}

test('a burst of near-duplicate messages forms one incident', () => {
  const records = [
    rec('FREE crypto airdrop, click here now!!!', 'Promo', 0),
    rec('free crypto airdrop click here now', 'Promo', 2),
    rec('Free crypto airdrop — click here now.', 'Promo', 4),
    rec('free crypto airdrop, click here now', 'Promo', 6),
  ];
  const incidents = clusterMessages(records, cfg());
  assert.equal(incidents.length, 1);
  assert.equal(incidents[0]!.messageCount, 4);
  assert.equal(incidents[0]!.network, 'ukmesh');
});

test('production threshold: a small handful of repeats is not spam', () => {
  // Real legit chatter tops out at a few near-identical repeats; the default
  // threshold must not flag a 4-message cluster.
  assert.ok(DEFAULT_SPAM_MESSAGE_CONFIG.minTransmissions >= 5, 'default floor should exclude tiny clusters');
  const text = 'cheap followers and likes dm me for the best rates today';
  const records = Array.from({ length: 4 }, (_, i) => rec(text, 'GrowthBot', i * 2));
  assert.equal(clusterMessages(records, DEFAULT_SPAM_MESSAGE_CONFIG).length, 0);
  // A genuine sustained flood still qualifies.
  const flood = Array.from({ length: 12 }, (_, i) => rec(text, 'GrowthBot', i * 2));
  assert.equal(clusterMessages(flood, DEFAULT_SPAM_MESSAGE_CONFIG).length, 1);
});

test('two near-duplicates do not reach the minimum-transmission bar', () => {
  const records = [
    rec('join my channel for free stuff', 'X', 0),
    rec('join my channel for free stuff!', 'X', 1),
  ];
  assert.equal(clusterMessages(records, cfg({ minTransmissions: 3 })).length, 0);
});

test('ordinary unrelated chatter does not form an incident', () => {
  const records = [
    rec('anyone around in leeds tonight', 'A', 0),
    rec('repeater back online after the storm', 'B', 3),
    rec('whats everyones antenna setup', 'C', 6),
  ];
  assert.equal(clusterMessages(records, cfg()).length, 0);
});

test('spam interleaved with normal chatter is still isolated', () => {
  const records = [
    rec('buy followers cheap visit shop.com/x', 'Bot', 0),
    rec('hi all good morning', 'Neighbour', 1),
    rec('buy followers cheap visit shop.com/x', 'Bot', 2),
    rec('hows the weather up north', 'Friend', 3),
    rec('buy followers cheap visit shop.com/x', 'Bot', 4),
  ];
  const incidents = clusterMessages(records, cfg());
  assert.equal(incidents.length, 1);
  assert.equal(incidents[0]!.messageCount, 3);
  assert.equal(incidents[0]!.hasSpamMarker, false);
});

test('messages spread beyond the join window do not merge', () => {
  const records = [
    rec('same spam text here today', 'S', 0),
    rec('same spam text here today', 'S', 45),
    rec('same spam text here today', 'S', 90),
  ];
  // 45-minute gaps exceed the 30-minute join window -> three lone clusters.
  assert.equal(clusterMessages(records, cfg({ joinWindowMs: 30 * MIN })).length, 0);
});

test('similar usernames are supporting evidence that lowers the text bar', () => {
  const t1 = 'buy cheap meds online fast delivery uk';
  const t2 = 'buy cheap meds online quick shipping uk';
  const sim = messageSimilarity(normalizeMessage(t1), normalizeMessage(t2));
  // Precondition: these sit in the mid band (related, not near-identical).
  assert.ok(sim > 0.5 && sim < 0.9, `expected mid-band similarity, got ${sim}`);

  const conf = cfg({ textSimThreshold: 0.9, textSimThresholdWithName: 0.5, usernameSimThreshold: 0.7 });

  const dissimilarNames = [
    rec(t1, 'Alice', 0),
    rec(t2, 'Bob', 2),
    rec(t1, 'Carol', 4),
  ];
  assert.equal(clusterMessages(dissimilarNames, conf).length, 0, 'dissimilar names should not cluster mid-band text');

  const similarNames = [
    rec(t1, 'PharmaDeal', 0),
    rec(t2, 'PharmaDeal2', 2),
    rec(t1, 'PharmaDeal_UK', 4),
  ];
  const incidents = clusterMessages(similarNames, conf);
  assert.equal(incidents.length, 1, 'similar names should pull mid-band text into one cluster');
  assert.equal(incidents[0]!.messageCount, 3);
});

test('trivial connectivity messages never form an incident', () => {
  const records = [
    rec('test', 'Alice', 0),
    rec('Test!', 'Alice', 1),
    rec('test', 'Alice', 2),
    rec('TEST', 'Alice', 3),
  ];
  // Same sender flooding "test", but content is trivial -> not spam.
  assert.equal(clusterMessages(records, cfg()).length, 0);
});

test('the same substantive text from many unrelated senders is treated as chatter', () => {
  const text = 'good morning everyone hope you all have a great friday';
  const records = [
    rec(text, 'Alice', 0),
    rec(text, 'Bob', 1),
    rec(text, 'Carol', 2),
    rec(text, 'Dave', 3),
    rec(text, 'Erin', 4),
    rec(text, 'Frank', 5),
  ];
  // 6 dissimilar senders, no link/marker -> broad chatter, not one abuser.
  assert.equal(clusterMessages(records, cfg()).length, 0);
});

test('a single sender flooding substantive text is an incident', () => {
  const text = 'cheap followers and likes dm me for the best rates today';
  const records = [rec(text, 'GrowthBot', 0), rec(text, 'GrowthBot', 2), rec(text, 'GrowthBot', 4)];
  const incidents = clusterMessages(records, cfg());
  assert.equal(incidents.length, 1);
  assert.equal(incidents[0]!.messageCount, 3);
});

test('a templated "!test" flood (benign word, substantive body) is caught as high-confidence spam', () => {
  // Real-world pattern: an automated flood of "ANDY KIRBY MESHCORE(TM) 00:16:4: !test"
  // from a blank sender. It contains the benign word "test" but is dominated by
  // real words + an incrementing counter, so it must NOT be dampened as testing.
  const records: MessageRecord[] = [];
  for (let i = 0; i < 30; i++) {
    const counter = `00:${10 + Math.floor(i / 6)}:${i % 6}`;
    records.push(rec(`ANDY KIRBY MESHCORE(TM) ${counter}: !test`, '', i * 2));
  }
  const incidents = clusterMessages(records, cfg());
  assert.equal(incidents.length, 1, 'the flood should form a single incident');
  assert.equal(incidents[0]!.messageCount, 30);
  assert.ok(
    incidents[0]!.score >= DEFAULT_SPAM_MESSAGE_CONFIG.publicMinScore,
    `expected score above the public floor, got ${incidents[0]!.score}`,
  );
  assert.ok(incidents[0]!.score >= 0.66, `expected high confidence, got ${incidents[0]!.score}`);
  assert.ok(incidents[0]!.reasons.some((r) => /flood/i.test(r)));
});

test('a benign-dominated flood (just test/connectivity tokens) stays below the public floor', () => {
  // Qualifies past the trivial gate (the "1234" token means it is not *all* benign)
  // but is still dominated by connectivity tokens, so the dampener keeps it private.
  const records: MessageRecord[] = [];
  for (let i = 0; i < 30; i++) records.push(rec('test rx ack 1234 test ok', 'Tester', i * 2));
  const incidents = clusterMessages(records, cfg());
  if (incidents.length > 0) {
    assert.ok(
      incidents[0]!.score < DEFAULT_SPAM_MESSAGE_CONFIG.publicMinScore,
      `benign-dominated flood should stay below the public floor, got ${incidents[0]!.score}`,
    );
  }
});

test('messages on excluded channels (the test channel) are ignored', () => {
  const text = 'cheap followers and likes dm me for the best rates today';
  const records = [
    rec(text, 'GrowthBot', 0, 'test'),
    rec(text, 'GrowthBot', 2, 'test'),
    rec(text, 'GrowthBot', 4, 'test'),
  ];
  assert.equal(clusterMessages(records, cfg({ excludeChannels: ['test'] })).length, 0);
});

test('a repeated link raises the confidence score', () => {
  const withLink = clusterMessages(
    [
      rec('great deal here shop.example.com/x', 'PromoA', 0),
      rec('great deal here shop.example.com/x', 'PromoA', 2),
      rec('great deal here shop.example.com/x', 'PromoA', 4),
    ],
    cfg(),
  );
  assert.equal(withLink.length, 1);
  assert.ok(withLink[0]!.hasUrl);
  assert.ok(withLink[0]!.reasons.some((r) => /link/i.test(r)));
});

test('incidentStatus reflects ongoing vs cooled-down incidents', () => {
  const now = 100 * MIN;
  const conf = cfg({ ongoingWindowMs: 30 * MIN });
  assert.equal(incidentStatus(now - 5 * MIN, now, conf), 'active');
  assert.equal(incidentStatus(now - 60 * MIN, now, conf), 'closed');
});

test('exact repeated spam cannot be evicted by newer unrelated candidate clusters', () => {
  const target = 'persistent promotional flood visit target.example.com now';
  const records: MessageRecord[] = [rec(target, 'TargetBot', 0)];
  for (let minute = 1; minute <= 20; minute += 1) {
    records.push(rec(`unrelated decoy message number ${minute}`, `Decoy${minute}`, minute));
  }
  records.push(rec(target, 'TargetBot', 21), rec(target, 'TargetBot', 22));
  const incidents = clusterMessages(records, cfg({
    maxCandidateClusters: 1,
    joinWindowMs: 60 * MIN,
    burstWindowMs: 60 * MIN,
  }));
  const targetIncident = incidents.find((incident) =>
    incident.members.some((member) => member.text === target));
  assert.ok(targetIncident);
  assert.equal(targetIncident.messageCount, 3);
});

test('canonical URL index prevents interleaved decoys evicting variant spam', () => {
  const records: MessageRecord[] = [];
  let minute = 0;
  for (let targetIndex = 0; targetIndex < 8; targetIndex += 1) {
    records.push(rec(
      `limited offer code ${targetIndex} visit https://spam.example/deal`,
      'TargetBot',
      minute++,
    ));
    if (targetIndex === 7) continue;
    for (let decoyIndex = 0; decoyIndex < 65; decoyIndex += 1) {
      records.push(rec(
        `unrelated digest ${targetIndex}-${decoyIndex} weather radio update`,
        `Decoy${targetIndex}-${decoyIndex}`,
        minute++,
      ));
    }
  }

  const incidents = clusterMessages(records, cfg({
    minTransmissions: 8,
    minBurst: 8,
    maxCandidateClusters: 64,
    joinWindowMs: 1_000 * MIN,
    burstWindowMs: 1_000 * MIN,
  }));
  const target = incidents.find((incident) => incident.members.some((member) =>
    member.norm.urls.includes('spam.example/deal')));
  assert.ok(target);
  assert.equal(target.messageCount, 8);
});
