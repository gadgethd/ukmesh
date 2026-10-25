/**
 * Ingest envelope safety: the two ways an upstream MQTT envelope could reach
 * storage without the ingest path actually vouching for it.
 *
 * 1. Integer domain (#11). Envelope numbers are only checked for JS finiteness
 *    by `toNum`, but several of them land in PostgreSQL INTEGER/BIGINT columns.
 *    `pg` sends a JS number as its decimal text, so `4.5` fails the integer
 *    cast (SQLSTATE 22P02) and `3e9` fails the range check (22003). Both are
 *    deterministic for that message: the durable handler withholds the PUBACK,
 *    the broker redelivers, and the same insert fails forever. Rejecting the
 *    value here — and classifying the same SQLSTATE classes as permanent if one
 *    still reaches the database — turns a livelock into one counted drop.
 *
 * 2. Unverified envelope content (#12). When no verified decode produced a
 *    payload, the stored payload is the publisher-controlled envelope. Its
 *    `_summary` and `decrypted` keys are indistinguishable downstream from
 *    values our decoder produced (and `payload.decrypted.sender` is promoted to
 *    `packets.companion_sender`), so they are stripped before persistence.
 *
 * Pure module: no imports, no I/O, so the policy is testable without a database.
 */

/** PostgreSQL int4 (`INTEGER`) domain. Values outside it fail with 22003. */
const INT4 = { min: -2_147_483_648, max: 2_147_483_647 } as const;
/** PostgreSQL int8 (`BIGINT`) bounds representable exactly by a JS number. */
const INT8_SAFE = { min: Number.MIN_SAFE_INTEGER, max: Number.MAX_SAFE_INTEGER } as const;

/**
 * Integer columns ingest writes from an upstream envelope, with the domain
 * that column accepts. Every entry is an actual INTEGER/BIGINT column in
 * `schema/base.sql`; floats (`rssi`, `snr`) are handled separately.
 */
export const ENVELOPE_INTEGER_COLUMNS = {
  packet_type: INT4, // packets.packet_type INTEGER
  hop_count: INT4, // packets.hop_count / packet_paths.hop_count INTEGER
  path_hash_size_bytes: INT4, // packets.path_hash_size_bytes INTEGER
  advert_count: INT4, // packets.advert_count INTEGER
  battery_mv: INT4, // node_status_samples.battery_mv INTEGER
  uptime_secs: INT8_SAFE, // node_status_samples.uptime_secs BIGINT
  tx_air_secs: INT8_SAFE, // node_status_samples.tx_air_secs BIGINT
  rx_air_secs: INT8_SAFE, // node_status_samples.rx_air_secs BIGINT
} as const;

export type EnvelopeIntegerField = keyof typeof ENVELOPE_INTEGER_COLUMNS;

export type IntegerViolationReason = 'not_integer' | 'out_of_range' | 'not_finite';

export type IntegerDomainViolation = {
  /** Column or field name, e.g. `packet_type` or `snr`. */
  field: string;
  value: unknown;
  reason: IntegerViolationReason;
};

/** Check one value against its column domain. `undefined`/non-numeric is absent, not invalid. */
export function checkEnvelopeInteger(
  field: EnvelopeIntegerField,
  value: number | undefined,
): IntegerDomainViolation | null {
  if (value == null) return null;
  const { min, max } = ENVELOPE_INTEGER_COLUMNS[field];
  if (!Number.isFinite(value)) return { field, value, reason: 'not_finite' };
  if (!Number.isInteger(value)) return { field, value, reason: 'not_integer' };
  if (value < min || value > max) return { field, value, reason: 'out_of_range' };
  return null;
}

/** Values of the `packets` insert that came from the wire/envelope. */
export type PacketEnvelopeIntegers = {
  packetType?: number;
  hopCount?: number;
  pathHashSizeBytes?: number;
  advertCount?: number;
  snr?: number;
  rssi?: number;
};

/**
 * Integer-column violations for a packet observation. `rssi`/`snr` are
 * DOUBLE PRECISION, so finiteness is their whole domain — they are included so
 * a non-finite value can never be serialised into a float column even if a
 * future decoder or coerter lets one through.
 */
export function packetEnvelopeViolations(input: PacketEnvelopeIntegers): IntegerDomainViolation[] {
  const violations: IntegerDomainViolation[] = [];
  const add = (v: IntegerDomainViolation | null): void => {
    if (v) violations.push(v);
  };
  add(checkEnvelopeInteger('packet_type', input.packetType));
  add(checkEnvelopeInteger('hop_count', input.hopCount));
  add(checkEnvelopeInteger('path_hash_size_bytes', input.pathHashSizeBytes));
  add(checkEnvelopeInteger('advert_count', input.advertCount));
  for (const field of ['snr', 'rssi'] as const) {
    const value = input[field];
    if (value != null && !Number.isFinite(value)) {
      violations.push({ field, value, reason: 'not_finite' });
    }
  }
  return violations;
}

function toRecord(value: unknown): Record<string, unknown> | undefined {
  return value && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown>
    : undefined;
}

function toNum(value: unknown): number | undefined {
  if (typeof value === 'number') return Number.isFinite(value) ? value : undefined;
  if (typeof value === 'string' && value.trim() !== '') {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : undefined;
  }
  return undefined;
}

/**
 * Stats keys the status envelope feeds into integer columns. Aliases mirror
 * `readNum` in statusTelemetry.ts: the first key present with a numeric value
 * wins, a present-but-unparseable key falls through to the next alias.
 */
const STATUS_INTEGER_ALIASES: ReadonlyArray<readonly [EnvelopeIntegerField, readonly string[]]> = [
  ['battery_mv', ['battery_mv', 'batteryMv']],
  ['uptime_secs', ['uptime_secs', 'uptimeSecs']],
  ['tx_air_secs', ['tx_air_secs', 'txAirSecs']],
  ['rx_air_secs', ['rx_air_secs', 'rxAirSecs']],
];

/**
 * Integer-column violations in a status envelope's `stats`. `uptime_ms` is not
 * checked: statusTelemetry derives seconds from it with `Math.floor`, which is
 * always an integer by construction.
 */
export function statusEnvelopeViolations(json: Record<string, unknown>): IntegerDomainViolation[] {
  const stats = toRecord(json['stats']);
  if (!stats) return [];
  const violations: IntegerDomainViolation[] = [];
  for (const [field, keys] of STATUS_INTEGER_ALIASES) {
    for (const key of keys) {
      if (!(key in stats)) continue;
      const value = toNum(stats[key]);
      if (value == null) continue;
      const violation = checkEnvelopeInteger(field, value);
      if (violation) violations.push(violation);
      break;
    }
  }
  return violations;
}

/**
 * SQLSTATE classes that are deterministic for a given row: class 22
 * (data_exception — invalid text representation, numeric out of range,
 * datetime overflow) and class 23 (integrity_constraint_violation). Retrying
 * the identical insert cannot succeed, so the message is quarantined instead.
 * Class 08 (connection) and class 40 (serialization/deadlock) stay retryable.
 */
const PERMANENT_SQLSTATE_PREFIXES = ['22', '23'] as const;

const PERMANENT_ERROR_MESSAGE =
  /invalid input syntax for type (?:smallint|integer|bigint|numeric)|out of range for type (?:smallint|integer|bigint)|violates (?:check|not-null|foreign key|unique|exclusion) constraint/i;

/** True when re-running the same insert cannot succeed (a data/constraint error). */
export function isPermanentPersistenceError(error: unknown): boolean {
  if (typeof error === 'object' && error !== null && 'code' in error) {
    const code = (error as { code?: unknown }).code;
    if (typeof code === 'string' && PERMANENT_SQLSTATE_PREFIXES.some((prefix) => code.startsWith(prefix))) {
      return true;
    }
  }
  const message = error instanceof Error
    ? error.message
    : (typeof error === 'string' ? error : '');
  return message !== '' && PERMANENT_ERROR_MESSAGE.test(message);
}

/**
 * What the durable ingest path should do with a persistence failure:
 * acknowledge-and-quarantine a deterministic data error, or withhold the ack
 * and let the broker redeliver a transient one.
 */
export type IngestFailureAction = 'ack_quarantine' | 'retry';

export function ingestFailureAction(error: unknown): IngestFailureAction {
  return isPermanentPersistenceError(error) ? 'ack_quarantine' : 'retry';
}

/** Envelope keys that only a verified decode is allowed to populate. */
export const DERIVED_PAYLOAD_FIELDS = ['_summary', 'decrypted'] as const;

/**
 * Copy of an upstream envelope with publisher-supplied derived fields removed.
 * Used both for the persisted payload and for the envelope handed to live
 * consumers, so a forged `_summary`/`decrypted` is never presented as decoded.
 */
export function stripDerivedPacketFields(
  envelope: Record<string, unknown>,
): Record<string, unknown> {
  const sanitized: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(envelope)) {
    if ((DERIVED_PAYLOAD_FIELDS as readonly string[]).includes(key)) continue;
    sanitized[key] = value;
  }
  return sanitized;
}

/**
 * Payload to persist. A verified decode's output is used as-is; otherwise the
 * raw envelope is stored with all derived fields removed, leaving `summary`
 * (and `companion_sender`, derived from `decrypted`) null.
 */
export function buildPersistedPacketPayload(
  verifiedPayload: Record<string, unknown> | undefined,
  envelope: Record<string, unknown>,
  direction?: 'rx' | 'tx',
): Record<string, unknown> {
  if (verifiedPayload) {
    return direction ? { ...verifiedPayload, direction } : verifiedPayload;
  }
  const sanitized = stripDerivedPacketFields(envelope);
  if (direction) sanitized['direction'] = direction;
  return sanitized;
}
