import { readFileSync } from 'node:fs';
import { verify } from 'node:crypto';

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

export function loadVerifiedRestoreReceipt(options: {
  receiptPath: string | undefined;
  signaturePath?: string | undefined;
  verifyKeyPath: string | undefined;
}): VerifiedRestoreReceipt {
  const receiptPath = String(options.receiptPath ?? '').trim();
  const verifyKeyPath = String(options.verifyKeyPath ?? '').trim();
  if (!receiptPath) throw new Error('DATA_LIFECYCLE_RESTORE_RECEIPT_PATH is required');
  if (!verifyKeyPath) throw new Error('DATA_LIFECYCLE_RECEIPT_VERIFY_KEY_PATH is required');
  const signaturePath = String(options.signaturePath ?? `${receiptPath}.sig`).trim();
  const receiptBytes = readFileSync(receiptPath);
  const signature = readFileSync(signaturePath);
  const verifyKey = readFileSync(verifyKeyPath);
  if (!verify('sha256', receiptBytes, verifyKey, signature)) {
    throw new Error('restore receipt signature verification failed');
  }
  return parseRestoreReceipt(JSON.parse(receiptBytes.toString('utf8')) as unknown);
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
