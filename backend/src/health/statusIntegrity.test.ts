import assert from 'node:assert/strict';
import test from 'node:test';
import {
  databaseVacuumBacklogProblem,
  privacyRematerializationBacklogProblem,
  privacyGenerationProblem,
  summarizeAuthoritativeHealth,
  toPublicHealthOverview,
} from './status.js';
import { evaluatePrivacyGenerations } from '../privacy/generationHealth.js';

test('anonymous frontend diagnostics cannot forge health severity', () => {
  assert.deepEqual(summarizeAuthoritativeHealth([], 1_000_000), {
    status: 'healthy',
    frontendErrors: 1_000_000,
  });
  assert.deepEqual(summarizeAuthoritativeHealth([
    { code: 'queue', severity: 'warning', message: 'bounded fixture' },
  ], 0), {
    status: 'degraded',
    frontendErrors: 0,
  });
  assert.deepEqual(summarizeAuthoritativeHealth([
    { code: 'dependency', severity: 'critical', message: 'bounded fixture' },
  ], Number.NaN), {
    status: 'critical',
    frontendErrors: 0,
  });
});

test('privacy rematerialization backlog ignores active work and classifies stale or failed work', () => {
  const state = {
    pending: 0, processing: 1, failed: 0, failedRetryable: 0,
    failedExhausted: 0, oldestPendingAgeSeconds: null,
  };
  assert.equal(privacyRematerializationBacklogProblem(state), null);
  assert.equal(privacyRematerializationBacklogProblem({ ...state, pending: 1, oldestPendingAgeSeconds: 1_801 })?.severity, 'warning');
  assert.equal(privacyRematerializationBacklogProblem({ ...state, failed: 1, failedRetryable: 1 })?.severity, 'warning');
  const exhausted = privacyRematerializationBacklogProblem({ ...state, failed: 1, failedExhausted: 1 });
  assert.equal(exhausted?.severity, 'critical');
  assert.match(exhausted?.message ?? '', /pending=0 processing=1 failed=1 oldest_pending_age_minutes=0/);
  assert.equal(privacyRematerializationBacklogProblem({ ...state, pending: 1, oldestPendingAgeSeconds: 21_601 })?.severity, 'critical');
});

test('vacuum backlog counts preserve warning and critical severity thresholds', () => {
  assert.equal(databaseVacuumBacklogProblem(0), null);
  assert.equal(databaseVacuumBacklogProblem(1)?.severity, 'warning');
  assert.equal(databaseVacuumBacklogProblem(2)?.severity, 'warning');
  assert.equal(databaseVacuumBacklogProblem(3)?.severity, 'critical');
  assert.equal(databaseVacuumBacklogProblem(Number.NaN), null);
  assert.equal(databaseVacuumBacklogProblem(-1), null);
  assert.equal(databaseVacuumBacklogProblem(Number.POSITIVE_INFINITY), null);
});

test('privacy generation drift with an empty queue is an authoritative, publicly redacted incident', () => {
  const healthy = evaluatePrivacyGenerations({
    desiredGeneration: '42', materializedGeneration: '42', appliedGeneration: '42', queueActive: false,
  });
  assert.equal(privacyGenerationProblem(healthy), null);
  const drift = evaluatePrivacyGenerations({ ...healthy, desiredGeneration: '43', appliedGeneration: null });
  const problem = privacyGenerationProblem(drift)!;
  assert.equal(summarizeAuthoritativeHealth([problem], 0).status, 'critical');
  const publicHealth = toPublicHealthOverview({
    status: 'critical', problems: [problem], privacy_visibility: drift,
    system: { disk: { used_pct: 0 }, generated_at: 'fixture' },
    maintenance: { active: false, message: null }, ingest: { packet_age_minutes: 0 }, workers: [],
  } as never);
  assert.deepEqual(publicHealth.incidents, [{ code: 'privacy_generation_unhealthy', severity: 'critical' }]);
  assert.doesNotMatch(JSON.stringify(publicHealth), /desiredGeneration|materializedGeneration|appliedGeneration|desired_ahead/);
});
