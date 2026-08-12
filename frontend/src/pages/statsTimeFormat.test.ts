import assert from 'node:assert/strict';
import test from 'node:test';
import { fmtAxisDay, fmtAxisTime, fmtPeakHour } from './statsTimeFormat.js';

test('formats ISO timestamps as local time labels', () => {
  const iso = new Date('2026-08-07T09:05:00.000Z').toISOString();
  // Must equal the locale's own local-time rendering of the same instant
  // (24h vs 12h AM/PM depends on the runtime locale — never assert a shape).
  assert.equal(
    fmtAxisTime(iso),
    new Date(iso).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
  );
});

test('day labels carry weekday, month and day-of-month in local time', () => {
  const iso = new Date('2026-08-07T23:30:00.000Z').toISOString();
  // The day must be the viewer-local calendar day of the instant.
  assert.equal(
    fmtAxisDay(iso),
    new Date(iso).toLocaleDateString('en-GB', { weekday: 'short', month: 'short', day: 'numeric' }),
  );
});

test('peak-hour labels are compact local date+time', () => {
  const iso = new Date('2026-08-12T07:00:00.000Z').toISOString();
  assert.equal(
    fmtPeakHour(iso),
    new Date(iso).toLocaleString('en-GB', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' }),
  );
});

test('peak-hour falls back to the raw value when unparseable', () => {
  assert.equal(fmtPeakHour('11:00'), '11:00');
  assert.equal(fmtPeakHour(''), '');
  assert.equal(fmtPeakHour(null), 'null');
});

test('falls back to the raw value when the input is not parseable', () => {
  // Legacy cached chart snapshots may still hold "HH:MM" server-formatted strings.
  assert.equal(fmtAxisTime('11:00'), '11:00');
  assert.equal(fmtAxisDay('11:00'), '11:00');
  assert.equal(fmtAxisTime(''), '');
  assert.equal(fmtAxisTime(null), 'null');
});
