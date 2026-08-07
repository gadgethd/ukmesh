import assert from 'node:assert/strict';
import test from 'node:test';
import { generateKeyPairSync, sign } from 'node:crypto';
import { mkdtempSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {
  assertDataLifecycleGate,
  configuredLifecycleTargets,
  lifecyclePolicy,
} from './dataLifecycle.js';

function receiptEnvironment(restoreVerifiedAt = '2026-07-28T12:00:00.000Z') {
  const dir = mkdtempSync(path.join(os.tmpdir(), 'meshcore-lifecycle-receipt-'));
  const pair = generateKeyPairSync('rsa', { modulusLength: 2048 });
  const receiptPath = path.join(dir, 'receipt.json');
  const verifyKeyPath = path.join(dir, 'verify.pem');
  const payload = {
    format: 'meshcore-restore-receipt-v1',
    receipt_id: 'restore-20260728T120000Z',
    backup_id: 'backup-20260728T100000Z',
    backup_completed_at: '2026-07-28T10:30:00.000Z',
    restore_started_at: '2026-07-28T11:00:00.000Z',
    restore_verified_at: restoreVerifiedAt,
    restore_duration_seconds: 3600,
    archive_sha256: 'a'.repeat(64),
    source_revision: 'b'.repeat(40),
    schema_version: 29,
    datasets: ['analytics', 'owner_auth', 'mosquitto', 'redis', 'configuration'],
    checks: {
      migrations: 'passed',
      integrity: 'passed',
      owner_lookup: 'passed',
      readiness: 'passed',
    },
    status: 'verified',
  };
  const bytes = Buffer.from(`${JSON.stringify(payload)}\n`);
  writeFileSync(receiptPath, bytes);
  writeFileSync(`${receiptPath}.sig`, sign('sha256', bytes, pair.privateKey));
  writeFileSync(verifyKeyPath, pair.publicKey.export({ type: 'spki', format: 'pem' }));
  return {
    DATA_LIFECYCLE_RESTORE_RECEIPT_PATH: receiptPath,
    DATA_LIFECYCLE_RECEIPT_VERIFY_KEY_PATH: verifyKeyPath,
  };
}

test('data lifecycle targets are closed and destructive gates fail closed', () => {
  assert.equal(lifecyclePolicy('packets').retention, '180 days');
  assert.equal(lifecyclePolicy('node_neighbor_samples').retention, '7 days');
  assert.throws(() => lifecyclePolicy('made_up'), /unsupported/);
  assert.throws(() => configuredLifecycleTargets('packets,made_up'), /unsupported/);

  const now = new Date('2026-07-29T12:00:00.000Z');
  const validEnv = {
    DATA_LIFECYCLE_RETENTION_ENABLED: 'true',
    DATA_LIFECYCLE_COMPRESSION_ENABLED: 'true',
    DATA_LIFECYCLE_RETENTION_TARGETS: 'packets',
    ...receiptEnvironment(),
  };
  assert.equal(assertDataLifecycleGate({
    action: 'retention',
    target: 'packets',
    approval: 'apply-data-lifecycle-retention-packets',
    env: validEnv,
    now,
  }).table, 'packets');
  assert.throws(() => assertDataLifecycleGate({
    action: 'retention',
    target: 'packets',
    approval: 'wrong',
    env: validEnv,
    now,
  }), /--approve/);
  assert.throws(() => assertDataLifecycleGate({
    action: 'retention',
    target: 'packets',
    approval: 'apply-data-lifecycle-retention-packets',
    env: validEnv,
    now: new Date('2026-08-10T12:00:00.000Z'),
  }), /last 7 days/);
  assert.throws(() => assertDataLifecycleGate({
    action: 'compression',
    target: 'frontend_error_events',
    approval: 'apply-data-lifecycle-compression-frontend_error_events',
    env: {
      ...validEnv,
      DATA_LIFECYCLE_RETENTION_TARGETS: 'frontend_error_events',
    },
    now,
  }), /not a compression target/);
});
