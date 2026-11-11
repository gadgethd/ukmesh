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

test('a rotating-ID botnet flood (many single-shot machine-named senders) is caught', () => {
  // Real-world pattern (Oct 2026): the anti-Kirby spammer rotated through
  // fresh keygen-style node identities, each posting the same slogan once.
  // These are not "many independent people": every identity appears exactly
  // once in the window and the names are machine-generated — so the cluster
  // must qualify as an incident, not be dismissed as broad chatter.
  const text = 'still clinging to the trademarks like a desperate leech';
  const ids = [
    '0CYT64KV', '1KG3RWU7', '16V0OP2W', '2H6WCDJW', '5ZA1J31M',
    '6KE2E6P6', '7F32S5OK', 'C23R8J39', 'DP8INYG2', 'G6B8QHA3',
  ];
  const records = ids.map((id, i) => rec(text, id, i));
  const incidents = clusterMessages(records, cfg());
  assert.equal(incidents.length, 1, 'the rotating-ID flood should form one incident');
  assert.equal(incidents[0]!.messageCount, ids.length);
  assert.ok(
    incidents[0]!.score >= DEFAULT_SPAM_MESSAGE_CONFIG.publicMinScore,
    `expected score above the public floor, got ${incidents[0]!.score}`,
  );
  assert.ok(incidents[0]!.reasons.some((r) => /single-shot/i.test(r)));
});

test('single-shot human chatter is still treated as broad chatter', () => {
  // Guard for the rotating-identity exemption: many one-shot senders with
  // human names (a meme / coordinated greeting) must stay chatter.
  const text = 'happy birthday to the repeater that never sleeps';
  const names = ['Alice', 'Bob', 'Carol', 'Dave', 'Erin', 'Frank', 'Grace', 'Heidi'];
  const records = names.map((n, i) => rec(text, n, i));
  assert.equal(clusterMessages(records, cfg()).length, 0);
});

// Invented names/text with the reported burst shapes; no production records.
const machineIds = ['R7K2X9PM', '5BQ8T3NW', 'Z6D4V1CK', '8JF2G9SL', 'N1X8R4BT', '6WK3P9DZ', 'T9L5B2CQ'];
const promoText = 'Try our new radio community at https://mesh.example.org/join today';

test('production policy catches a seven-message rotating-ID promo in two minutes', () => {
  const records = machineIds.map((id, i) => rec(promoText, id, i / 3));
  const incidents = clusterMessages(records, DEFAULT_SPAM_MESSAGE_CONFIG);
  assert.equal(incidents.length, 1);
  assert.equal(incidents[0]!.messageCount, 7);
  assert.ok(incidents[0]!.score >= DEFAULT_SPAM_MESSAGE_CONFIG.publicMinScore);
  assert.ok(incidents[0]!.reasons.some((reason) => reason.includes('single-shot')));
});

test('only the full rotating-ID profile gets the lower six-transmission floor', () => {
  const rotating = machineIds.slice(0, 6).map((id, i) => rec(promoText, id, i / 3));
  assert.equal(clusterMessages(rotating, DEFAULT_SPAM_MESSAGE_CONFIG).length, 1);
  assert.equal(clusterMessages(rotating.slice(0, 5), DEFAULT_SPAM_MESSAGE_CONFIG).length, 0);
  assert.equal(clusterMessages(
    rotating.map((record) => ({ ...record, sender: 'PromoRelay' })), DEFAULT_SPAM_MESSAGE_CONFIG,
  ).length, 0, 'a repeating sender/link alone must still need eight transmissions');
  assert.equal(clusterMessages(
    rotating.map((record, i) => ({ ...record, sender: `RadioFriend${i}` })), DEFAULT_SPAM_MESSAGE_CONFIG,
  ).length, 0, 'similar human names do not get the rotating-ID exception');
  assert.equal(clusterMessages(
    rotating.map((record, i) => ({ ...record, observedAt: i * 11 * MIN })), DEFAULT_SPAM_MESSAGE_CONFIG,
  ).length, 0, 'one-shot IDs without a burst are insufficient');
});

test('seven automatic SNR acknowledgements still fail the ordinary eight-message floor', () => {
  const records = Array.from({ length: 7 }, (_, i) => rec('Automatic SNR acknowledgement received at relay', 'AckRelay', i / 3));
  assert.equal(DEFAULT_SPAM_MESSAGE_CONFIG.minTransmissions, 8);
  assert.equal(clusterMessages(records, DEFAULT_SPAM_MESSAGE_CONFIG).length, 0);
});

test('rotating-ID exception preserves benign, excluded-channel and established-sender guards', () => {
  assert.equal(clusterMessages(machineIds.map((id, i) => rec('test rx ack 73', id, i / 3)), DEFAULT_SPAM_MESSAGE_CONFIG).length, 0);
  assert.equal(clusterMessages(machineIds.map((id, i) => rec(promoText, id, i / 3, 'test')), DEFAULT_SPAM_MESSAGE_CONFIG).length, 0);
  const records = machineIds.map((id, i) => rec(promoText, id, i / 3));
  records.push(...machineIds.map((id, i) => rec(`Independent conversation ${i}`, id, 20 + i)));
  assert.equal(clusterMessages(records, DEFAULT_SPAM_MESSAGE_CONFIG).length, 0, 'reused IDs are not single-shot');
});

test('eleven single-shot IDs with four machine-style names clear the production share gate', () => {
  // Seven vowel-rich IDs deliberately fail the existing machine-name heuristic.
  const ids = [...machineIds.slice(0, 4), 'A9UQO2EN', 'E4BIA8UT', 'Y7OKA3IR', 'O5TUE6AW', 'I2VAE9OX', 'U8AFO1EL', 'Q3EUI7AB'];
  const records = ids.map((id, i) => rec('The fictional trademark campaign keeps repeating this canned slogan', id, i / 6));
  assert.equal(clusterMessages(records, { ...DEFAULT_SPAM_MESSAGE_CONFIG, rotatingMachineNameShare: 0.5 }).length, 0,
    'the fixture must reproduce the old share-gate rejection');
  const incidents = clusterMessages(records, DEFAULT_SPAM_MESSAGE_CONFIG);
  assert.equal(incidents.length, 1);
  assert.ok(incidents[0]!.score >= DEFAULT_SPAM_MESSAGE_CONFIG.publicMinScore);
  assert.ok(incidents[0]!.reasons.some((reason) => reason.includes('36% machine-style')));
  assert.equal(clusterMessages(records.slice(1), DEFAULT_SPAM_MESSAGE_CONFIG).length, 1, '3/10 is the inclusive boundary');
  assert.equal(clusterMessages(records.slice(2), DEFAULT_SPAM_MESSAGE_CONFIG).length, 0, '2/9 stays below the share gate');
});

test('same-signature bursts form one campaign before the incident cap, preserving strongest evidence', () => {
  const text = 'Repeated campaign promotion at https://campaign.example.org/join';
  const older = Array.from({ length: 17 }, (_, i) => rec(text, 'CampaignRelay', i / 3));
  const newer = Array.from({ length: 8 }, (_, i) => rec(text, 'CampaignRelay', 120 + i / 3));
  const other = Array.from({ length: 8 }, (_, i) => rec('Separate offer at https://other.example.net', 'OtherRelay', 60 + i / 3));
  older[0]!.observers = [{ observerId: 'west', lat: 53, lon: -2 }];
  newer[0]!.observers = [{ observerId: 'east', lat: 53, lon: -1 }];
  const config = { ...DEFAULT_SPAM_MESSAGE_CONFIG, maxIncidentsPerRun: 2 };
  const campaign = clusterMessages([...older, ...newer], config);
  assert.equal(campaign.length, 1, 'same incident key must be written only once');
  assert.equal(campaign[0]!.messageCount, 25, 'count the union of distinct transmissions');
  assert.equal(campaign[0]!.members.length, 25);
  assert.equal(campaign[0]!.observerCount, 2);
  assert.equal(campaign[0]!.firstSeen, older[0]!.observedAt);
  assert.equal(campaign[0]!.lastSeen, newer.at(-1)!.observedAt);
  assert.equal(campaign[0]!.score, clusterMessages(older, config)[0]!.score, 'a weaker new burst cannot dilute strong evidence');
  assert.match(campaign[0]!.reasons.join(' '), /2 bursts/);
  const records = [...older, ...other, ...newer];
  const incidents = clusterMessages(records, config);
  assert.equal(new Set(incidents.map((incident) => incident.key)).size, 2, 'duplicate keys cannot crowd another campaign out of the cap');
  assert.deepEqual(clusterMessages([...records].reverse(), config), incidents, 'input order cannot choose the winning burst');
});
