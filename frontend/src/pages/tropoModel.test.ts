import assert from 'node:assert/strict';
import test from 'node:test';
import {
  bestStep,
  formatValid,
  initialStepIndex,
  isQuiet,
  tierFor,
  type TropoIndex,
  type TropoStep,
} from './tropoModel.js';

function step(index: number, maxIndex: number, valid = '2026-11-27T12:00:00Z'): TropoStep {
  return {
    index,
    forecast_hour: index * 3,
    valid,
    file: `step-${String(index).padStart(3, '0')}.png`,
    max_index: maxIndex,
    peak_duct: 0,
    pct_strong: 0,
    pct_ducting: 0,
    peak: { lat: 54, lon: -2 },
  };
}

function payload(steps: TropoStep[]): TropoIndex {
  return {
    generated_at: '2026-11-27T12:05:00Z',
    run: '2026-11-27T06:00:00Z',
    cycle: '06Z',
    model: 'NOAA GFS 0.25°',
    bounds: { north: 62, south: 49, west: -12, east: 5 },
    width: 552,
    height: 424,
    steps,
    source: 'NOAA/NCEP GFS',
    attribution: 'test',
  };
}

test('tierFor respects the ducting threshold', () => {
  assert.equal(tierFor(0).label, 'Normal');
  assert.equal(tierFor(39.9).label, 'Normal');
  assert.equal(tierFor(40).shortLabel, 'Slight');
  assert.equal(tierFor(69).shortLabel, 'Fair');
  assert.equal(tierFor(100).shortLabel, 'Ducting');
  assert.equal(tierFor(150).shortLabel, 'Intense');
});

test('formatValid renders a UTC forecast label', () => {
  const label = formatValid('2026-11-27T15:00:00Z');
  assert.match(label, /Fri/);
  assert.match(label, /15:00/);
  assert.match(label, /UTC/);
});

test('bestStep picks the highest index and isQuiet needs every step quiet', () => {
  const data = payload([step(0, 32), step(1, 88), step(2, 61)]);
  assert.equal(bestStep(data)?.index, 1);
  assert.equal(isQuiet(data), false);
  assert.equal(isQuiet(payload([step(0, 12), step(1, 39)])), true);
});

test('initialStepIndex picks the first step at or after now', () => {
  const future = new Date(Date.now() + 24 * 3_600_000).toISOString();
  const data = payload([step(0, 10, '2020-01-01T00:00:00Z'), step(1, 10, future)]);
  assert.equal(initialStepIndex(data), 1);
});
