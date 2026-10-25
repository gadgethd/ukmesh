import { createHash, createPublicKey, verify } from 'node:crypto';
import { readFileSync, realpathSync } from 'node:fs';
import path from 'node:path';

/** Out-of-band pin for the restore receipt verify key. The key must be
 * provisioned as its own secret and never travel with the receipt it verifies. */
export const RESTORE_RECEIPT_VERIFY_KEY_SHA256_ENV = 'DATA_LIFECYCLE_RECEIPT_VERIFY_KEY_SHA256';

export const REQUIRED_BACKUP_DATASETS = Object.freeze([
  'analytics',
  'owner_auth',
  'mosquitto',
  'redis',
  'configuration',
] as const);

export type VerifiedRestoreReceipt = Readonly<{
  format: 'meshcore-restore-receipt-v1';
  receipt_id: string;
  backup_id: string;
  backup_completed_at: string;
  restore_started_at: string;
  restore_verified_at: string;
  restore_duration_seconds: number;
  archive_sha256: string;
  source_revision: string;
  schema_version: number;
  datasets: readonly string[];
  checks: Readonly<{
    migrations: 'passed';
    integrity: 'passed';
    owner_lookup: 'passed';
    readiness: 'passed';
  }>;
  status: 'verified';
}>;

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function requiredString(
  object: Record<string, unknown>,
  key: string,
  pattern?: RegExp,
): string {
  const value = object[key];
  if (typeof value !== 'string' || !value || (pattern && !pattern.test(value))) {
    throw new Error(`restore receipt has invalid ${key}`);
  }
  return value;
}

function parseTimestamp(
  object: Record<string, unknown>,
  key: string,
): { text: string; milliseconds: number } {
  const text = requiredString(object, key);
  const milliseconds = Date.parse(text);
  if (!Number.isFinite(milliseconds)) throw new Error(`restore receipt has invalid ${key}`);
  return { text, milliseconds };
}

export function parseRestoreReceipt(value: unknown): VerifiedRestoreReceipt {
  if (!isRecord(value)) throw new Error('restore receipt must be an object');
  if (value['format'] !== 'meshcore-restore-receipt-v1') {
    throw new Error('unsupported restore receipt format');
  }
  if (value['status'] !== 'verified') throw new Error('restore receipt is not verified');
  const backupCompleted = parseTimestamp(value, 'backup_completed_at');
  const restoreStarted = parseTimestamp(value, 'restore_started_at');
  const restoreVerified = parseTimestamp(value, 'restore_verified_at');
  if (restoreStarted.milliseconds < backupCompleted.milliseconds) {
    throw new Error('restore receipt predates its backup');
  }
  if (restoreVerified.milliseconds < restoreStarted.milliseconds) {
    throw new Error('restore verification predates restore start');
  }
  const datasets = value['datasets'];
  if (!Array.isArray(datasets) || datasets.some((dataset) => typeof dataset !== 'string')) {
    throw new Error('restore receipt has invalid datasets');
  }
  for (const required of REQUIRED_BACKUP_DATASETS) {
    if (!datasets.includes(required)) throw new Error(`restore receipt is missing ${required}`);
  }
  const checks = value['checks'];
  if (
    !isRecord(checks)
    || checks['migrations'] !== 'passed'
    || checks['integrity'] !== 'passed'
    || checks['owner_lookup'] !== 'passed'
    || checks['readiness'] !== 'passed'
  ) {
    throw new Error('restore receipt checks are incomplete');
  }
  const duration = value['restore_duration_seconds'];
  const schemaVersion = value['schema_version'];
  if (typeof duration !== 'number' || !Number.isFinite(duration) || duration < 0) {
    throw new Error('restore receipt has invalid restore_duration_seconds');
  }
  if (!Number.isSafeInteger(schemaVersion) || Number(schemaVersion) < 0) {
    throw new Error('restore receipt has invalid schema_version');
  }
  return Object.freeze({
    format: 'meshcore-restore-receipt-v1',
    receipt_id: requiredString(value, 'receipt_id', /^[a-zA-Z0-9_.:-]{8,160}$/),
    backup_id: requiredString(value, 'backup_id', /^[a-zA-Z0-9_.:-]{8,160}$/),
    backup_completed_at: backupCompleted.text,
    restore_started_at: restoreStarted.text,
    restore_verified_at: restoreVerified.text,
    restore_duration_seconds: duration,
    archive_sha256: requiredString(value, 'archive_sha256', /^[a-f0-9]{64}$/),
    source_revision: requiredString(value, 'source_revision', /^[a-f0-9]{40}$/),
    schema_version: Number(schemaVersion),
    datasets: Object.freeze([...datasets]),
    checks: Object.freeze({
      migrations: 'passed',
      integrity: 'passed',
      owner_lookup: 'passed',
      readiness: 'passed',
    }),
    status: 'verified',
  });
}

/** Fingerprint over the DER SubjectPublicKeyInfo, so a PEM re-wrap cannot
 * change it and `openssl pkey -pubin -outform DER | sha256sum` agrees. */
export function restoreReceiptKeyFingerprint(publicKey: Buffer | string): string {
  const der = createPublicKey(publicKey).export({ type: 'spki', format: 'der' });
  return `sha256:${createHash('sha256').update(der).digest('hex')}`;
}

function normalizeFingerprint(value: string, source: string): string {
  const trimmed = value.trim().toLowerCase();
  if (!trimmed) {
    throw new Error(`${source} is required: pin the restore receipt verify key fingerprint outside the receipt directory`);
  }
  const bare = trimmed.startsWith('sha256:') ? trimmed.slice('sha256:'.length) : trimmed;
  if (!/^[a-f0-9]{64}$/.test(bare)) {
    throw new Error(`${source} must be a sha256 fingerprint of 64 hex characters`);
  }
  return `sha256:${bare}`;
}

function resolveForComparison(target: string): string {
  try {
    return realpathSync(target);
  } catch {
    return path.resolve(target);
  }
}

/** A receipt, its signature, and its verify key must never be published
 * together: whoever can replace the receipt could otherwise supply the key
 * that blesses it. The key is a separately provisioned secret. */
function assertVerifyKeyIsSeparatelyProvisioned(receiptPath: string, verifyKeyPath: string): void {
  const receiptDirectory = path.dirname(resolveForComparison(receiptPath));
  const keyDirectory = path.dirname(resolveForComparison(verifyKeyPath));
  const relative = path.relative(receiptDirectory, keyDirectory);
  if (relative === '' || (!relative.startsWith('..') && !path.isAbsolute(relative))) {
    throw new Error(
      'restore receipt verify key must be provisioned outside the receipt directory',
    );
  }
}

export function loadVerifiedRestoreReceipt(options: {
  receiptPath: string | undefined;
  signaturePath?: string | undefined;
  verifyKeyPath: string | undefined;
  verifyKeySha256?: string | undefined;
}): VerifiedRestoreReceipt {
  const receiptPath = String(options.receiptPath ?? '').trim();
  const verifyKeyPath = String(options.verifyKeyPath ?? '').trim();
  if (!receiptPath) throw new Error('DATA_LIFECYCLE_RESTORE_RECEIPT_PATH is required');
  if (!verifyKeyPath) throw new Error('DATA_LIFECYCLE_RECEIPT_VERIFY_KEY_PATH is required');
  const pinned = normalizeFingerprint(
    String(options.verifyKeySha256 ?? process.env[RESTORE_RECEIPT_VERIFY_KEY_SHA256_ENV] ?? ''),
    RESTORE_RECEIPT_VERIFY_KEY_SHA256_ENV,
  );
  assertVerifyKeyIsSeparatelyProvisioned(receiptPath, verifyKeyPath);
  const signaturePath = String(options.signaturePath ?? `${receiptPath}.sig`).trim();
  const receiptBytes = readFileSync(receiptPath);
  const signature = readFileSync(signaturePath);
  let verifyKey: Buffer;
  try {
    verifyKey = readFileSync(verifyKeyPath);
  } catch {
    throw new Error(`restore receipt verify key is not readable at ${verifyKeyPath}`);
  }
  if (restoreReceiptKeyFingerprint(verifyKey) !== pinned) {
    throw new Error('restore receipt verify key does not match the pinned fingerprint');
  }
  if (!verify('sha256', receiptBytes, verifyKey, signature)) {
    throw new Error('restore receipt signature verification failed');
  }
  const parsed = JSON.parse(receiptBytes.toString('utf8')) as unknown;
  const claimed = isRecord(parsed) && typeof parsed['verify_key_sha256'] === 'string'
    ? normalizeFingerprint(parsed['verify_key_sha256'], 'verify_key_sha256')
    : undefined;
  if (claimed !== undefined && claimed !== pinned) {
    throw new Error('restore receipt was signed by a key that does not match the pinned fingerprint');
  }
  return parseRestoreReceipt(parsed);
}

export function assertFreshRestoreReceipt(
  receipt: VerifiedRestoreReceipt,
  options: { now?: Date; maximumAgeDays?: number } = {},
): void {
  const now = options.now ?? new Date();
  const maximumAgeDays = Math.min(30, Math.max(1, options.maximumAgeDays ?? 7));
  const restoredAt = Date.parse(receipt.restore_verified_at);
  const backupAt = Date.parse(receipt.backup_completed_at);
  const maximumAgeMs = maximumAgeDays * 24 * 60 * 60_000;
  for (const [name, timestamp] of [['restore verification', restoredAt], ['backup', backupAt]] as const) {
    const ageMs = now.getTime() - timestamp;
    if (ageMs < -5 * 60_000 || ageMs > maximumAgeMs) {
      throw new Error(`a signed ${name} from the last ${maximumAgeDays} days is required`);
    }
  }
}
