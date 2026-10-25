import assert from 'node:assert/strict';
import test from 'node:test';
import { generateKeyPairSync, sign } from 'node:crypto';
import { mkdirSync, mkdtempSync, readFileSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {
  RESTORE_RECEIPT_VERIFY_KEY_SHA256_ENV,
  assertFreshRestoreReceipt,
  loadVerifiedRestoreReceipt,
  parseRestoreReceipt,
  restoreReceiptKeyFingerprint,
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

/** The receipt and its verify key live in separate, sibling directories: the
 * key is provisioned as its own secret and never published with the receipt. */
function signedReceiptFixture() {
  const root = mkdtempSync(path.join(os.tmpdir(), 'meshcore-receipt-'));
  const receiptDirectory = path.join(root, 'receipts');
  const keyDirectory = path.join(root, 'trusted-keys');
  mkdirSync(receiptDirectory);
  mkdirSync(keyDirectory);
  const pair = generateKeyPairSync('rsa', { modulusLength: 2048 });
  const receiptPath = path.join(receiptDirectory, 'receipt.json');
  const signaturePath = `${receiptPath}.sig`;
  const verifyKeyPath = path.join(keyDirectory, 'verify.pem');
  const publicKey = pair.publicKey.export({ type: 'spki', format: 'pem' });
  const bytes = Buffer.from(`${JSON.stringify(receipt())}\n`);
  writeFileSync(receiptPath, bytes);
  writeFileSync(signaturePath, sign('sha256', bytes, pair.privateKey));
  writeFileSync(verifyKeyPath, publicKey);
  return {
    receiptPath,
    signaturePath,
    verifyKeyPath,
    fingerprint: restoreReceiptKeyFingerprint(publicKey),
  };
}

test('loads only signed, complete restore receipts', () => {
  const fixture = signedReceiptFixture();
  const { receiptPath, verifyKeyPath, fingerprint } = fixture;

  const loaded = loadVerifiedRestoreReceipt({ receiptPath, verifyKeyPath, verifyKeySha256: fingerprint });
  assert.equal(loaded.backup_id, 'backup-20260729T110000Z');
  assertFreshRestoreReceipt(loaded, {
    now: new Date('2026-07-30T12:00:00.000Z'),
    maximumAgeDays: 7,
  });

  writeFileSync(receiptPath, Buffer.from(`${JSON.stringify({ ...receipt(), status: 'failed' })}\n`));
  assert.throws(
    () => loadVerifiedRestoreReceipt({ receiptPath, verifyKeyPath, verifyKeySha256: fingerprint }),
    /signature verification failed/,
  );
});

test('restore receipt verification needs a separately provisioned, pinned key', () => {
  const fixture = signedReceiptFixture();
  const base = { receiptPath: fixture.receiptPath, verifyKeyPath: fixture.verifyKeyPath };
  const previousPin = process.env[RESTORE_RECEIPT_VERIFY_KEY_SHA256_ENV];
  delete process.env[RESTORE_RECEIPT_VERIFY_KEY_SHA256_ENV];
  try {
    assert.throws(() => loadVerifiedRestoreReceipt(base), /VERIFY_KEY_SHA256 is required/);
  } finally {
    if (previousPin !== undefined) process.env[RESTORE_RECEIPT_VERIFY_KEY_SHA256_ENV] = previousPin;
  }

  const other = generateKeyPairSync('rsa', { modulusLength: 2048 });
  const otherKeyPath = path.join(path.dirname(fixture.verifyKeyPath), 'other.pem');
  writeFileSync(otherKeyPath, other.publicKey.export({ type: 'spki', format: 'pem' }));
  assert.throws(() => loadVerifiedRestoreReceipt({
    ...base,
    verifyKeyPath: otherKeyPath,
    verifyKeySha256: fixture.fingerprint,
  }), /does not match the pinned fingerprint/);

  assert.throws(() => loadVerifiedRestoreReceipt({
    ...base,
    verifyKeyPath: path.join(path.dirname(fixture.verifyKeyPath), 'missing.pem'),
    verifyKeySha256: fixture.fingerprint,
  }), /not readable/);

  // The key must never travel with the artifact it verifies, even if pinned.
  const coLocatedKey = path.join(path.dirname(fixture.receiptPath), 'verify.pem');
  writeFileSync(coLocatedKey, readFileSync(fixture.verifyKeyPath));
  assert.throws(() => loadVerifiedRestoreReceipt({
    ...base,
    verifyKeyPath: coLocatedKey,
    verifyKeySha256: fixture.fingerprint,
  }), /outside the receipt directory/);

  // A receipt that names a different signing key is refused outright.
  const foreignClaim = Buffer.from(`${JSON.stringify({
    ...receipt(),
    verify_key_sha256: `sha256:${'0'.repeat(64)}`,
  })}\n`);
  writeFileSync(fixture.receiptPath, foreignClaim);
  writeFileSync(fixture.signaturePath, sign('sha256', foreignClaim, generateKeyPairSync('rsa', {
    modulusLength: 2048,
  }).privateKey));
  assert.throws(() => loadVerifiedRestoreReceipt({
    ...base,
    verifyKeySha256: fixture.fingerprint,
  }), /signature verification failed/);

  // A correctly provisioned key and pin still verify, including via the env pin.
  const restored = signedReceiptFixture();
  process.env[RESTORE_RECEIPT_VERIFY_KEY_SHA256_ENV] = restored.fingerprint;
  try {
    assert.equal(
      loadVerifiedRestoreReceipt({
        receiptPath: restored.receiptPath,
        verifyKeyPath: restored.verifyKeyPath,
      }).backup_id,
      'backup-20260729T110000Z',
    );
  } finally {
    if (previousPin === undefined) delete process.env[RESTORE_RECEIPT_VERIFY_KEY_SHA256_ENV];
    else process.env[RESTORE_RECEIPT_VERIFY_KEY_SHA256_ENV] = previousPin;
  }
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
