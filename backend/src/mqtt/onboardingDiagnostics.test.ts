import assert from 'node:assert/strict';
import test from 'node:test';
import { createMqttOnboardingDiagnostics, MQTT_ONBOARDING_DIAGNOSTIC_TTL_MS as TTL } from './onboardingDiagnostics.js';

test('default-root denials produce an account-scoped repair hint without claiming a node', () => {
  const now = Date.parse('2026-10-23T12:00:00Z');
  const diagnostics = createMqttOnboardingDiagnostics(() => now);
  try {
    for (const topic of ['meshcore', 'meshcore/status', 'meshcore/packets', 'meshcore/neighbors', 'meshcore/neighbours']) {
      diagnostics.record('alice', topic, now);
      const hint = diagnostics.get('alice');
      assert.equal(hint?.code, 'MQTT_DEFAULT_TOPIC_ROOT');
      assert.match(hint!.message, /meshcore\/\{IATA\}\/\{PUBLIC_KEY\}\/packets/);
      assert.equal(hint!.observedAt, '2026-10-23T12:00:00.000Z');
      assert.equal(diagnostics.get('bob'), null);
    }
    for (const topic of [`meshcore/NCL/${'A'.repeat(64)}/packets`, 'meshcore/other', 'meshcoreevil', 'ukmesh', 'public/x', 'meshcore/']) {
      diagnostics.record('bob', topic, now);
      assert.equal(diagnostics.get('bob'), null, topic);
    }
  } finally { diagnostics.close(); }
});

test('diagnostics expire from event time, ignore history and future timestamps, and keep newest evidence', () => {
  let now = Date.parse('2026-10-23T12:00:00Z');
  const diagnostics = createMqttOnboardingDiagnostics(() => now);
  try {
    for (const observedAt of [now - TTL, now + 1, Number.NaN]) {
      diagnostics.record('alice', 'meshcore', observedAt);
      assert.equal(diagnostics.get('alice'), null);
    }
    diagnostics.record('alice', 'meshcore', now - TTL + 1);
    now += 1;
    assert.equal(diagnostics.get('alice'), null);
    diagnostics.record('alice', 'meshcore', now);
    diagnostics.record('alice', 'meshcore/status', now - 1);
    assert.match(diagnostics.get('alice')!.message, /to "meshcore"/);
    now += TTL;
    assert.equal(diagnostics.get('alice'), null);
  } finally { diagnostics.close(); }
});

test('diagnostic storage evicts cold accounts at its entry cap', () => {
  const now = Date.now();
  const diagnostics = createMqttOnboardingDiagnostics(() => now, 2);
  try {
    for (const username of ['alice', 'bob', 'charlie']) diagnostics.record(username, 'meshcore', now);
    assert.equal(diagnostics.get('alice'), null);
    assert.ok(diagnostics.get('bob'));
    assert.ok(diagnostics.get('charlie'));
  } finally { diagnostics.close(); }
});
