import {
  assertFreshRestoreReceipt,
  loadVerifiedRestoreReceipt,
} from '../backup/receipt.js';

export type DataLifecyclePolicy = Readonly<{
  table: string;
  timestampColumn: string;
  retention: string;
  kind: 'hypertable' | 'row-table';
  compressAfter?: string;
  compressionSegmentBy?: string;
  featureImpact: readonly string[];
}>;

export const DATA_LIFECYCLE_POLICIES: readonly DataLifecyclePolicy[] = Object.freeze([
  {
    table: 'packets',
    timestampColumn: 'time',
    retention: '30 days',
    kind: 'hypertable',
    compressAfter: '7 days',
    compressionSegmentBy: 'network',
    featureImpact: [
      'message content and raw packet data older than 30 days are removed',
      'raw pathing data is preserved content-stripped in packet_paths (no retention)',
      'public and owner packet/path views are limited to 30 days or less',
    ],
  },
  {
    table: 'node_status_samples',
    timestampColumn: 'time',
    retention: '180 days',
    kind: 'hypertable',
    compressAfter: '7 days',
    compressionSegmentBy: 'network',
    featureImpact: [
      'owner status telemetry older than 180 days becomes restore-only',
    ],
  },
  {
    table: 'node_neighbor_samples',
    timestampColumn: 'time',
    retention: '7 days',
    kind: 'hypertable',
    compressAfter: '1 day',
    compressionSegmentBy: 'network',
    featureImpact: [
      'owner heard-neighbor history older than seven days becomes restore-only',
    ],
  },
  {
    table: 'frontend_error_events',
    timestampColumn: 'time',
    retention: '30 days',
    kind: 'row-table',
    featureImpact: ['anonymous browser diagnostic trends older than 30 days are removed'],
  },
  {
    table: 'packet_decryptions',
    timestampColumn: 'created_at',
    retention: '30 days',
    kind: 'row-table',
    featureImpact: ['decrypted packet content is diagnostics-only and removed after 30 days'],
  },
  {
    table: 'worker_health_snapshots',
    timestampColumn: 'ts',
    retention: '7 days',
    kind: 'row-table',
    featureImpact: ['operator worker-health history older than seven days is removed'],
  },
  {
    table: 'operational_check_results',
    timestampColumn: 'ts',
    retention: '14 days',
    kind: 'row-table',
    featureImpact: ['synthetic journey history older than 14 days is removed'],
  },
  {
    table: 'observer_region_packet_sightings',
    timestampColumn: 'last_seen',
    retention: '8 days',
    kind: 'row-table',
    featureImpact: ['observer-region packet rollups retain the advertised seven-day window'],
  },
  {
    table: 'observer_region_observer_sightings',
    timestampColumn: 'last_seen',
    retention: '31 days',
    kind: 'row-table',
    featureImpact: ['stale observer-region evidence older than the 30-day cleanup window is removed'],
  },
  {
    table: 'owner_alert_deliveries',
    timestampColumn: 'created_at',
    retention: '90 days',
    kind: 'row-table',
    featureImpact: ['delivered owner-alert audit rows older than 90 days are removed'],
  },
  {
    table: 'observer_registration_requests',
    timestampColumn: 'updated_at',
    retention: '365 days',
    kind: 'row-table',
    featureImpact: [
      'terminal observer registration PII older than one year is removed; pending requests expire after 90 days',
    ],
  },
  {
    table: 'operator_audit_events',
    timestampColumn: 'created_at',
    retention: '730 days',
    kind: 'row-table',
    featureImpact: ['operator action evidence older than two years becomes restore-only'],
  },
  {
    table: 'link_job_commits',
    timestampColumn: 'completed_at',
    retention: '180 days',
    kind: 'row-table',
    featureImpact: ['link job idempotency evidence older than 180 days is removed'],
  },
  {
    table: 'ml_model_variant_packet_results',
    timestampColumn: 'created_at',
    retention: '180 days',
    kind: 'row-table',
    featureImpact: ['per-packet model evaluation evidence older than 180 days is removed'],
  },
]);

export function lifecyclePolicy(table: string): DataLifecyclePolicy {
  const policy = DATA_LIFECYCLE_POLICIES.find((candidate) => candidate.table === table);
  if (!policy) throw new Error(`unsupported data lifecycle target: ${table}`);
  return policy;
}

export function configuredLifecycleTargets(raw: string | undefined): Set<string> {
  const targets = new Set(
    String(raw ?? '').split(',').map((value) => value.trim()).filter(Boolean),
  );
  for (const target of targets) lifecyclePolicy(target);
  return targets;
}

export function assertDataLifecycleGate(options: {
  action: 'compression' | 'retention';
  target: string;
  approval: string | undefined;
  env?: NodeJS.ProcessEnv;
  now?: Date;
}): DataLifecyclePolicy {
  const env = options.env ?? process.env;
  const now = options.now ?? new Date();
  const policy = lifecyclePolicy(options.target);
  const flag = options.action === 'compression'
    ? 'DATA_LIFECYCLE_COMPRESSION_ENABLED'
    : 'DATA_LIFECYCLE_RETENTION_ENABLED';
  if (env[flag] !== 'true') throw new Error(`${flag}=true is required`);
  if (options.action === 'compression' && policy.kind !== 'hypertable') {
    throw new Error(`${policy.table} is not a compression target`);
  }
  const targets = configuredLifecycleTargets(env['DATA_LIFECYCLE_RETENTION_TARGETS']);
  if (!targets.has(policy.table)) {
    throw new Error(`DATA_LIFECYCLE_RETENTION_TARGETS must include ${policy.table}`);
  }
  const expectedApproval = `apply-data-lifecycle-${options.action}-${policy.table}`;
  if (options.approval !== expectedApproval) {
    throw new Error(`--approve=${expectedApproval} is required`);
  }
  const receipt = loadVerifiedRestoreReceipt({
    receiptPath: env['DATA_LIFECYCLE_RESTORE_RECEIPT_PATH'],
    signaturePath: env['DATA_LIFECYCLE_RESTORE_RECEIPT_SIGNATURE_PATH'],
    verifyKeyPath: env['DATA_LIFECYCLE_RECEIPT_VERIFY_KEY_PATH'],
  });
  const maximumAgeDays = Number(env['DATA_LIFECYCLE_RECEIPT_MAX_AGE_DAYS'] ?? 7);
  assertFreshRestoreReceipt(receipt, {
    now,
    maximumAgeDays: Number.isFinite(maximumAgeDays) ? maximumAgeDays : 7,
  });
  return policy;
}
