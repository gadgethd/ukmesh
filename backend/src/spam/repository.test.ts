import assert from 'node:assert/strict';
import test from 'node:test';
import pg from 'pg';
import { normalizeMessage } from './normalize.js';
import { loadRecentMessages, persistIncidents, selectIncidentEvidenceMembers } from './repository.js';
import { buildIncidents } from './analyzer.js';
import { DEFAULT_SPAM_MESSAGE_CONFIG as CFG } from './config.js';
import { spamAnalysisTruncationsTotal } from '../metrics.js';
import type { MessageRecord } from './types.js';

function member(index: number, text = `message-${index}`): MessageRecord {
  return {
    id: `hash-${index}`,
    network: 'ukmesh',
    sender: 'sender',
    text,
    norm: normalizeMessage(text),
    channelHash: '80',
    channelLabel: 'Public',
    observedAt: index,
    observers: [],
  };
}

test('bounded incident evidence retains first, representative, and newest observations', () => {
  const members = Array.from({ length: 20 }, (_, index) =>
    member(index, index === 7 ? 'representative' : `message-${index}`));
  const selected = selectIncidentEvidenceMembers(members, 8, 'representative');
  assert.equal(selected.length, 8);
  assert.ok(selected.some((item) => item.id === 'hash-0'));
  assert.ok(selected.some((item) => item.id === 'hash-7'));
  assert.ok(selected.some((item) => item.id === 'hash-19'));
  assert.deepEqual(selected.map((item) => item.observedAt), [...selected].map((item) => item.observedAt).sort((a, b) => a - b));
});

function row(index: number, summary = '[Public] synthetic message') {
  return {
    packet_hash: `packet-${index}`,
    network: 'ukmesh',
    sender: 'FixtureRelay',
    message: 'Synthetic repeated promotional message at https://radio.example.org',
    channel_hash: '80',
    summary,
    claimed_ts: null,
    observed_at: new Date(1_000_000 + index * 1_000),
  };
}

test('a busy 2,907-message day retains the seven-ID burst beyond the former 2,000 cap', async (t) => {
  const rows = Array.from({ length: 2_900 }, (_, index) => row(index, '[test] synthetic connectivity traffic'));
  const ids = ['R7K2X9PM', '5BQ8T3NW', 'Z6D4V1CK', '8JF2G9SL', 'N1X8R4BT', '6WK3P9DZ', 'T9L5B2CQ'];
  rows.push(...ids.map((sender, index) => ({ ...row(2_900 + index), sender })));
  t.mock.method(pg.Pool.prototype, 'query', async (sql: string, params: unknown[]) => {
    if (sql.includes('WITH logical_messages')) {
      assert.equal(params.at(-1), CFG.maxCandidatePacketRows + 1, 'candidate query needs a truncation lookahead');
      return { rows };
    }
    assert.match(sql, /ranked_observers/);
    assert.equal((params[3] as string[]).length, rows.length);
    return { rows: [] };
  });
  const loaded = await loadRecentMessages(CFG);
  assert.equal(loaded.length, 2_907);
  const items = buildIncidents(loaded, rows.at(-1)!.observed_at.getTime(), CFG);
  assert.equal(items.length, 1);
  assert.equal(items[0]!.publicJson.messageCount, 7);
  assert.equal(items[0]!.publicJson.status, 'active');
});

test('candidate lookahead and message caps report actual truncation without leaking evidence', async (t) => {
  const warnings: string[] = [];
  t.mock.method(console, 'warn', (message: string) => warnings.push(message));
  spamAnalysisTruncationsTotal.reset();
  t.mock.method(pg.Pool.prototype, 'query', async (sql: string, params: unknown[]) => {
    if (sql.includes('WITH logical_messages')) {
      assert.equal(params.at(-1), 4);
      return { rows: Array.from({ length: 4 }, (_, index) => row(index)) };
    }
    assert.equal((params[3] as string[]).length, 2, 'discard the lookahead before loading observers');
    return { rows: [] };
  });
  const loaded = await loadRecentMessages({ ...CFG, maxCandidatePacketRows: 3, maxMessagesPerRun: 2 });
  assert.equal(loaded.length, 2);
  assert.deepEqual(warnings, [
    '[spam-msg] truncated phase=candidates limit=3 observed_at_least=4 omitted_at_least=1',
    '[spam-msg] truncated phase=messages limit=2 observed_at_least=3 omitted_at_least=1',
  ]);
  const metric = await spamAnalysisTruncationsTotal.get();
  assert.deepEqual(metric.values.map((value) => [value.labels.phase, value.value]), [['candidates', 1], ['messages', 1]]);
});

test('a full candidate page without the lookahead row is not reported as truncated', async (t) => {
  const warnings: string[] = [];
  t.mock.method(console, 'warn', (message: string) => warnings.push(message));
  t.mock.method(pg.Pool.prototype, 'query', async (sql: string) => ({
    rows: sql.includes('WITH logical_messages') ? [row(0), row(1), row(2)] : [],
  }));
  assert.equal((await loadRecentMessages({ ...CFG, maxCandidatePacketRows: 3, maxMessagesPerRun: 3 })).length, 3);
  assert.deepEqual(warnings, []);
});

test('publication rejects duplicate incident keys before any database mutation', async (t) => {
  t.mock.method(pg.Pool.prototype, 'connect', () => { throw new Error('database must not be reached'); });
  const records = Array.from({ length: 8 }, (_, index) => member(index, 'Synthetic promotion at https://radio.example.org'));
  const items = buildIncidents(records, 10, CFG);
  assert.equal(items.length, 1);
  await assert.rejects(persistIncidents([...items, ...items], CFG, {
    runId: 'synthetic-run', workload: 'spam-analysis', scope: 'public', leaseToken: 'fixture',
  }), /SPAM_DUPLICATE_INCIDENT_KEYS/);
});
