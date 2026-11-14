import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { promisify } from 'node:util';
import { LOOKUP_TIMELINE_MAX_EVENTS, LookupTimeline } from './lookupTimeline.js';

test('actual nested boundaries retain UTC, monotonic duration and failed partial work', async () => {
  let offset = 100, wall = '2026-11-14T10:00:00.000Z';
  const timeline = new LookupTimeline({ utc: () => wall, monotonic: () => offset });
  await assert.rejects(timeline.span({ phase: 'trial', trial: 1 }, async () => {
    offset += 10;
    await timeline.span({ phase: 'case', trial: 1, case: 'map-page' }, async () => {
      offset += 5; wall = '2026-11-14T09:59:59.000Z';
      throw new Error('observed HTTP failure');
    });
  }), /observed HTTP failure/);
  const evidence = timeline.finish(false);
  assert.equal(evidence.passed, false);
  assert.equal(evidence.durationMs, 15);
  assert.deepEqual(evidence.events.map(row => [row.phase, row.edge, row.parent, row.offsetMs, row.outcome]), [
    ['trial', 'start', null, 0, undefined], ['case', 'start', 0, 10, undefined],
    ['case', 'finish', 0, 15, 'failed'], ['trial', 'finish', null, 15, 'failed'],
  ]);
  assert.equal(evidence.events[2]!.at, wall, 'retain an actual wall-clock correction; never invent UTC from offsets');
  assert.deepEqual(evidence.events.map(row => row.sequence), [0, 1, 2, 3]);
  assert.throws(() => timeline.finish(false), /already finished/);
});

test('bounded markers refuse excess work and incomplete or invalid clocks', async () => {
  const timeline = new LookupTimeline(); let calls = 0;
  for (let index = 0; index < LOOKUP_TIMELINE_MAX_EVENTS / 2; index++) {
    await timeline.span({ phase: 'case', case: 'bounded' }, async () => { calls++; });
  }
  await assert.rejects(timeline.span({ phase: 'overflow' }, async () => { calls++; }), /budget/);
  assert.equal(calls, LOOKUP_TIMELINE_MAX_EVENTS / 2);
  assert.equal(timeline.finish(false).events.length, LOOKUP_TIMELINE_MAX_EVENTS);
  let release!: () => void;
  const open = new LookupTimeline();
  const work = open.span({ phase: 'trial' }, () => new Promise<void>(resolve => { release = resolve; }));
  assert.throws(() => open.finish(true), /incomplete/);
  release(); await work; assert.equal(open.finish(true).events.length, 2);
  assert.throws(() => new LookupTimeline({ utc: () => 'invalid', monotonic: () => 0 }), /clock/);
  let monotonic = 100;
  assert.throws(() => new LookupTimeline({ utc: () => new Date().toISOString(), monotonic: () => monotonic-- }), /clock/);
});

test('native setup failure flushes real partial evidence and refuses to overwrite an older timeline', async t => {
  const root = await mkdtemp(path.join(os.tmpdir(), 'lookup-timeline-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  await writeFile(path.join(root, 'owned.ts'), 'export const fixture = true;\n');
  const output = path.join(root, 'output');
  const run = () => promisify(execFile)(process.execPath, ['--max-old-space-size=128', '--import',
    path.resolve('node_modules/tsx/dist/loader.mjs'), path.resolve('src/tools/benchmarkLookups.ts'),
    '--source-root', root, '--output', output], {
    env: { ...process.env, DATABASE_URL: 'postgresql://unused@not-local.invalid/fixture' },
    timeout: 15000,
  });
  await assert.rejects(run(), /local|localhost|loopback/);
  const before = await readFile(path.join(output, 'timeline.json'), 'utf8');
  const evidence = JSON.parse(before);
  assert.equal(evidence.passed, false);
  assert.deepEqual(evidence.events.map((row: { phase: string; edge: string }) => [row.phase, row.edge]),
    [['benchmark', 'start'], ['benchmark', 'finish']]);
  assert.equal(evidence.events[1].outcome, 'failed');
  assert.ok(evidence.durationMs >= evidence.events[1].offsetMs);
  await assert.rejects(run(), /EEXIST/);
  assert.equal(await readFile(path.join(output, 'timeline.json'), 'utf8'), before);
});


test('origin and event preemption remain explicit monotonic brackets around actual UTC reads', async () => {
  let mono = 100, captures = 0;
  const epoch = Date.parse('2026-11-14T10:00:00.000Z');
  const timeline = new LookupTimeline({ monotonic: () => mono, utc: () => {
    const at = new Date(epoch + Math.floor(mono - 100)).toISOString();
    if (captures === 0) mono += 2.4;
    if (captures === 2) mono += 8;
    captures++; return at;
  } });
  await timeline.span({ phase: 'case', trial: 1, case: 'map-page' }, async () => { mono += 10; });
  const evidence = timeline.finish(true);
  assert.equal(evidence.version, 2);
  assert.equal(evidence.originClock.monotonicBeforeMs, 100);
  assert.ok(Math.abs(evidence.originClock.monotonicAfterMs - 102.4) < 1e-9);
  assert.equal(evidence.originClock.at, '2026-11-14T10:00:00.000Z');
  assert.equal(evidence.events[1]!.clock.at, '2026-11-14T10:00:00.012Z');
  assert.equal(evidence.events[1]!.clock.monotonicAfterMs - evidence.events[1]!.clock.monotonicBeforeMs, 8);
  assert.equal(evidence.finishedClock.at, '2026-11-14T10:00:00.020Z');
  assert.equal(evidence.events[1]!.offsetMs, evidence.events[1]!.clock.monotonicAfterMs - evidence.originClock.monotonicAfterMs);
});

test('scheduled work receives the exact existing start bracket without extra clock reads', async () => {
  let reads=0,mono=100;
  const timeline=new LookupTimeline({monotonic:()=>{reads++;return mono;},utc:()=>new Date(0).toISOString()});
  let start;
  await timeline.span({phase:'demand'},async boundary=>{start=structuredClone(boundary);mono+=120000;});
  const value=timeline.finish(true);
  assert.deepEqual(start,value.events[0]);assert.equal(timeline.originMonotonicMs,value.originMonotonicMs);
  assert.equal(reads,8);assert.equal(value.events[1]!.offsetMs,120000);
});
