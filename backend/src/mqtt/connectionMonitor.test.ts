import assert from 'node:assert/strict';
import { appendFileSync, mkdirSync, mkdtempSync, rmSync, statSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import test, { type TestContext } from 'node:test';
import { fileURLToPath } from 'node:url';
import { metricsRegistry, mqttDeniedPublishesTotal } from '../metrics.js';

// Importing the audit repository creates a lazy pool. All writes below use the
// injected recorder, and even an accidental connection cannot reach a real DB.
process.env['OWNER_DATABASE_URL'] = 'postgresql://unused:unused@127.0.0.1:1/unused';
process.env['MQTT_CONN_MONITOR_CLIENT_TTL_MS'] = '900000';
const { observeBrokerLogLine, scanBrokerLogRange } = await import('./connectionMonitor.js');

const KEY = '67969D8FF8A2DE6FFF873638CCAB3F70E38111BE3BD599F75E9D21D907B217B5';
const RAW = `meshcore/LBA/${'A1'.repeat(32)}/raw`;
const PACKETS = `meshcore/EXT/${KEY}/packets`;
const connection = (client: string) => `1792760000: New client connected from 192.0.2.1:1 as ${client} (p4, c1, k60, u'nick').`;
const denial = (client: string, topic: string) => `1792760001: Denied PUBLISH from ${client} (d0, q0, r0, m0, '${topic}', ... (42 bytes))`;

function fixture(t: TestContext, lines: string[]): string {
  const root = fileURLToPath(new URL('../../../.ukmesh-tools/tmp/', import.meta.url));
  mkdirSync(root, { recursive: true });
  const dir = mkdtempSync(path.join(root, 'broker-denial-'));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  const log = path.join(dir, 'mosquitto.log');
  writeFileSync(log, `${lines.join('\n')}\n`);
  return log;
}

function captureLogs(t: TestContext): unknown[][] {
  const warnings: unknown[][] = [];
  t.mock.method(console, 'warn', (...args: unknown[]) => warnings.push(args));
  t.mock.method(console, 'log', () => undefined);
  return warnings;
}

function detailRows(warnings: unknown[][]): Record<string, unknown>[] {
  return warnings.filter(([message]) => message === '[conn-monitor] denied publish')
    .map(([, json]) => JSON.parse(String(json)) as Record<string, unknown>);
}

test('all denied topics are observable without a login; expired correlations cannot imply ownership', () => {
  assert.deepEqual(observeBrokerLogLine(denial('unseen-raw', RAW), 1), {
    kind: 'denied-publish', mqttUsername: null, clientId: 'unseen-raw', topic: RAW, claimedNodeId: null,
  });
  observeBrokerLogLine(connection('expires'), 1);
  assert.equal(observeBrokerLogLine(denial('expires', PACKETS), 900002)?.mqttUsername, null);
  assert.equal(observeBrokerLogLine('not a broker observation'), null);
});

test('file scans count every denial, aggregate client/topic details, and keep unsupported topics out of owner audit', async (t) => {
  mqttDeniedPublishesTotal.reset();
  const warnings = captureLogs(t);
  const log = fixture(t, [
    connection('scan-client'),
    denial('scan-client', RAW), denial('scan-client', RAW),
    denial('scan-client', PACKETS), denial('scan-client', PACKETS),
    denial('unknown-client', 'meshcore'),
    'Denied PUBLISH from incomplete',
  ]);
  const recorded: string[][] = [];
  await scanBrokerLogRange(log, 0, statSync(log).size - 1, 'live', async (...args) => { recorded.push(args); });
  assert.deepEqual(recorded, [['nick', KEY]]);
  assert.deepEqual(detailRows(warnings), [
    { phase: 'live', clientId: 'scan-client', topic: RAW, mqttUsername: 'nick', count: 2, truncated: false },
    { phase: 'live', clientId: 'scan-client', topic: PACKETS, mqttUsername: 'nick', count: 2, truncated: false },
    { phase: 'live', clientId: 'unknown-client', topic: 'meshcore', mqttUsername: null, count: 1, truncated: false },
  ]);
  const scrape = await metricsRegistry.metrics();
  assert.match(scrape, /meshcore_mqtt_denied_publishes_total\{kind="raw",phase="live"\} 2/);
  assert.match(scrape, /meshcore_mqtt_denied_publishes_total\{kind="packets",phase="live"\} 2/);
  assert.match(scrape, /meshcore_mqtt_denied_publishes_total\{kind="other",phase="live"\} 1/);
  for (const value of ['scan-client', KEY, RAW, 'nick']) assert.equal(scrape.includes(value), false);
});

test('startup history and appended live ranges remain separate; raw-only clients never invoke the DB audit', async (t) => {
  mqttDeniedPublishesTotal.reset();
  const warnings = captureLogs(t);
  const log = fixture(t, [connection('history-client'), denial('history-client', RAW)]);
  const position = statSync(log).size;
  const noAudit = async () => assert.fail('unsupported topics must not enter owner audit');
  await scanBrokerLogRange(log, 0, position - 1, 'history', noAudit);
  appendFileSync(log, `${denial('history-client', RAW)}\n`);
  await scanBrokerLogRange(log, position, statSync(log).size - 1, 'live', noAudit);
  const scrape = await metricsRegistry.metrics();
  assert.match(scrape, /kind="raw",phase="history"\} 1/);
  assert.match(scrape, /kind="raw",phase="live"\} 1/);
  assert.deepEqual(detailRows(warnings).map((row) => row.phase), ['history', 'live']);
});

test('detail overflow is explicit and never drops counts or creates unbounded metric labels', async (t) => {
  mqttDeniedPublishesTotal.reset();
  const warnings = captureLogs(t);
  const log = fixture(t, [
    ...Array.from({ length: 70 }, (_, i) => denial(`overflow-${i}`, `unknown/${i}`)),
    denial('overflow-0', 'unknown/0'),
  ]);
  await scanBrokerLogRange(log, 0, statSync(log).size - 1, 'live', async () => assert.fail('no owner audit'));
  const rows = detailRows(warnings);
  assert.equal(rows.length, 64);
  assert.equal(rows[0]?.count, 2);
  assert.deepEqual(warnings.at(-1), ['[conn-monitor] denied publish details omitted', { phase: 'live', count: 6 }]);
  const metric = await mqttDeniedPublishesTotal.get();
  assert.deepEqual(metric.values.map(({ labels, value }) => ({ labels, value })), [
    { labels: { kind: 'other', phase: 'live' }, value: 71 },
  ]);
});

test('a denial split across polling ranges is counted once after its newline arrives', async (t) => {
  mqttDeniedPublishesTotal.reset();
  const warnings = captureLogs(t);
  const log = fixture(t, [connection('split-\u00e9')]);
  const start = statSync(log).size;
  const bytes = Buffer.from(`${denial('split-\u00e9', RAW)}\r\n`);
  // Split inside a UTF-8 client character to exercise byte offsets as well as
  // incomplete records. Nothing should be counted on the first read.
  const split = bytes.indexOf(Buffer.from('\u00e9')) + 1;
  appendFileSync(log, bytes.subarray(0, split));
  const noAudit = async () => assert.fail('no owner audit');
  const firstPosition = await scanBrokerLogRange(log, 0, statSync(log).size - 1, 'history', noAudit);
  assert.equal(firstPosition, start);
  assert.equal((await mqttDeniedPublishesTotal.get()).values.length, 0);
  assert.equal(detailRows(warnings).length, 0);
  appendFileSync(log, bytes.subarray(split));
  const end = statSync(log).size;
  assert.equal(await scanBrokerLogRange(log, firstPosition, end - 1, 'live', noAudit), end);
  assert.deepEqual(detailRows(warnings), [
    { phase: 'live', clientId: 'split-\u00e9', topic: RAW, mqttUsername: 'nick', count: 1, truncated: false },
  ]);
  assert.deepEqual((await mqttDeniedPublishesTotal.get()).values.map(({ value }) => value), [1]);
});

test('log details escape control text and bound long fields', async (t) => {
  const warnings = captureLogs(t);
  const clientId = `client\t${'x'.repeat(600)}`;
  const topic = `unknown/\t${'y'.repeat(600)}`;
  const log = fixture(t, [denial(clientId, topic)]);
  await scanBrokerLogRange(log, 0, statSync(log).size - 1, 'live', async () => assert.fail('no owner audit'));
  const row = detailRows(warnings)[0]!;
  assert.equal(row.clientId, clientId.slice(0, 512));
  assert.equal(row.topic, topic.slice(0, 512));
  assert.equal(row.truncated, true);
  assert.equal(String(warnings[0]?.[1]).includes('\t'), false);
});

test('audit failures cannot erase denial telemetry or trigger a log-range replay', async (t) => {
  mqttDeniedPublishesTotal.reset();
  const warnings = captureLogs(t);
  const errors = t.mock.method(console, 'error', () => undefined);
  const log = fixture(t, [connection('failing-audit'), denial('failing-audit', PACKETS)]);
  await scanBrokerLogRange(log, 0, statSync(log).size - 1, 'live', async () => { throw new Error('audit unavailable'); });
  assert.equal(detailRows(warnings)[0]?.count, 1);
  assert.equal(errors.mock.callCount(), 1);
  assert.equal((await mqttDeniedPublishesTotal.get()).values[0]?.value, 1);
});
