import assert from 'node:assert/strict';
import test from 'node:test';
import { generateKeyPairSync, sign } from 'node:crypto';
import { mkdtempSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {
  assertFreshRestoreReceipt,
  loadVerifiedRestoreReceipt,
  parseRestoreReceipt,
} from './receipt.js';

function receipt() {
  return {
    format: 'meshcore-restore-receipt-v1',
    receipt_id: 'restore-20260729T120000Z',
    backup_id: 'backup-20260729T110000Z',
    backup_completed_at: '2026-07-29T11:30:00.000Z',
    restore_started_at: '2026-07-29T12:00:00.000Z',
    restore_verified_at: '2026-07-29T12:45:00.000Z',
    restore_duration_seconds: 2700,
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
}

test('loads only signed, complete restore receipts', () => {
  const dir = mkdtempSync(path.join(os.tmpdir(), 'meshcore-receipt-'));
  const pair = generateKeyPairSync('rsa', { modulusLength: 2048 });
  const receiptPath = path.join(dir, 'receipt.json');
  const signaturePath = `${receiptPath}.sig`;
  const verifyKeyPath = path.join(dir, 'verify.pem');
  const bytes = Buffer.from(`${JSON.stringify(receipt())}\n`);
  writeFileSync(receiptPath, bytes);
  writeFileSync(signaturePath, sign('sha256', bytes, pair.privateKey));
  writeFileSync(verifyKeyPath, pair.publicKey.export({ type: 'spki', format: 'pem' }));

  const loaded = loadVerifiedRestoreReceipt({ receiptPath, verifyKeyPath });
  assert.equal(loaded.backup_id, 'backup-20260729T110000Z');
  assertFreshRestoreReceipt(loaded, {
    now: new Date('2026-07-30T12:00:00.000Z'),
    maximumAgeDays: 7,
  });

  writeFileSync(receiptPath, Buffer.from(`${JSON.stringify({ ...receipt(), status: 'failed' })}\n`));
  assert.throws(
    () => loadVerifiedRestoreReceipt({ receiptPath, verifyKeyPath }),
    /signature verification failed/,
  );
});

test('rejects incomplete or stale restore evidence', () => {
  const missingRedis = receipt();
  missingRedis.datasets = missingRedis.datasets.filter((dataset) => dataset !== 'redis');
  assert.throws(() => parseRestoreReceipt(missingRedis), /missing redis/);
  const parsed = parseRestoreReceipt(receipt());
  assert.throws(() => assertFreshRestoreReceipt(parsed, {
    now: new Date('2026-08-20T12:00:00.000Z'),
  }), /last 7 days/);
});
