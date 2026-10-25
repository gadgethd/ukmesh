export type StatusTelemetrySample = {
  status?: string;
  batteryMv?: number;
  uptimeSecs?: number;
  txAirSecs?: number;
  rxAirSecs?: number;
  channelUtilization?: number;
  airUtilTx?: number;
  stats?: Record<string, unknown>;
};

export type NodeStatusSampleInput = StatusTelemetrySample & {
  nodeId: string;
  network: string;
};

type StatusSampleOptions = {
  nodeId: string;
  network: string;
  allowRawStatsOnly?: boolean;
};

type StatusSampleWriter = (sample: NodeStatusSampleInput) => Promise<void>;

const OWNER_STATUS_STATS_KEYS = new Set([
  'batteryMv', 'uptime_secs', 'uptimeSecs', 'uptime_ms', 'uptimeMs',
  'tx_air_secs', 'txAirSecs', 'rx_air_secs', 'rxAirSecs',
  'battery_mv', 'solar_mv', 'board_temp_c', 'wifi_rssi', 'wifi_ssid', 'wifi_uptime_ms',
  'ntp_synced', 'ntp_sync_age_ms', 'boot_count', 'reset_reason', 'max_loop_ms',
  'max_loop_at_ms', 'nodes_heard_24h', 'channel_utilization', 'air_util_tx', 'air_util_rx',
  'last_rx_rssi', 'last_rx_snr', 'tx_power_dbm', 'config_version', 'config_crc32',
  'fs_free_bytes', 'fs_total_bytes', 'nvs_free_entries', 'channel_id', 'git_commit', 'boot_epoch',
]);

function toRecord(value: unknown): Record<string, unknown> | undefined {
  return value && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown>
    : undefined;
}

function toNum(value: unknown): number | undefined {
  if (typeof value === 'number') return Number.isFinite(value) ? value : undefined;
  if (typeof value === 'string' && value.trim() !== '') {
    const number = Number(value);
    return Number.isFinite(number) ? number : undefined;
  }
  return undefined;
}

function readNum(obj: Record<string, unknown> | undefined, ...keys: string[]): number | undefined {
  if (!obj) return undefined;
  for (const key of keys) {
    if (!(key in obj)) continue;
    const number = toNum(obj[key]);
    if (number != null) return number;
  }
  return undefined;
}

function wholeCounter(value: number | undefined, max = Number.MAX_SAFE_INTEGER): number | undefined {
  if (value == null || value < 0 || value > max) return undefined;
  const whole = Math.trunc(value);
  return Number.isSafeInteger(whole) ? whole : undefined;
}

/**
 * Only a deterministic PostgreSQL data exception may be acknowledged without
 * storing a status write. Connection failures, timeouts and schema errors must
 * still reach the durable handler so it can retry without losing observations.
 * Wait for every write before returning, including when one of them fails.
 */
export async function persistStatusWrites(
  writes: Promise<unknown>[],
  onRejected: (code: string) => void,
): Promise<void> {
  const results = await Promise.allSettled(writes);
  const failures: unknown[] = [];
  for (const result of results) {
    if (result.status !== 'rejected') continue;
    const error: unknown = result.reason;
    const code = error && typeof error === 'object' && 'code' in error ? error.code : undefined;
    if (typeof code === 'string' && /^22[A-Z0-9]{3}$/.test(code)) {
      onRejected(code);
    } else {
      failures.push(error);
    }
  }
  if (failures.length > 0) throw failures[0];
}

function envelopeStatus(json: Record<string, unknown>): string | undefined {
  if (typeof json['status'] !== 'string') return undefined;
  const status = json['status'].trim();
  return status || undefined;
}

export function extractStatusTelemetry(
  json: Record<string, unknown>,
  options?: { allowRawStatsOnly?: boolean },
): StatusTelemetrySample | null {
  const status = envelopeStatus(json);
  // The LWT carries boot-fresh statistics. Ignore it on the same path whether
  // it arrives live after a session drop or as a retained replay on subscribe.
  if (status?.toLowerCase() === 'offline') return null;

  const stats = toRecord(json['stats']);
  const hasRawStats = Boolean(stats && Object.keys(stats).length > 0);
  const hasOwnerStats = Boolean(
    stats && (
      Object.keys(stats).some((key) => OWNER_STATUS_STATS_KEYS.has(key))
      || toRecord(stats['mqtt'])
    ),
  );
  // These columns are INTEGER/BIGINT. Some firmware reports fractional
  // airtime seconds: keep whole seconds here and preserve full precision in
  // stats. Unrepresentable counters become NULL rather than poisoning MQTT.
  const batteryMv = wholeCounter(readNum(stats, 'battery_mv', 'batteryMv'), 2_147_483_647);
  const uptimeSecs = (() => {
    const direct = wholeCounter(readNum(stats, 'uptime_secs', 'uptimeSecs'));
    if (direct != null) return direct;
    const uptimeMs = readNum(stats, 'uptime_ms', 'uptimeMs');
    if (uptimeMs == null) return undefined;
    return wholeCounter(uptimeMs / 1000);
  })();
  const txAirSecs = wholeCounter(readNum(stats, 'tx_air_secs', 'txAirSecs'));
  const rxAirSecs = wholeCounter(readNum(stats, 'rx_air_secs', 'rxAirSecs'));
  const channelUtilization = readNum(
    stats,
    'channel_utilization',
    'channel_utilization_pct',
    'channel_util',
    'channelUtil',
    'channelUtilization',
  );
  const airUtilTx = readNum(
    stats,
    'air_util_tx',
    'air_util_tx_pct',
    'tx_air_util',
    'tx_air_utilization',
    'airUtilTx',
  );

  if (
    batteryMv == null
    && uptimeSecs == null
    && txAirSecs == null
    && rxAirSecs == null
    && channelUtilization == null
    && airUtilTx == null
  ) {
    if ((options?.allowRawStatsOnly || hasOwnerStats) && hasRawStats) {
      return {
        status,
        batteryMv,
        uptimeSecs,
        txAirSecs,
        rxAirSecs,
        channelUtilization,
        airUtilTx,
        stats,
      };
    }
    return null;
  }

  return {
    status,
    batteryMv,
    uptimeSecs,
    txAirSecs,
    rxAirSecs,
    channelUtilization,
    airUtilTx,
    stats,
  };
}

export async function persistStatusTelemetrySample(
  json: Record<string, unknown>,
  options: StatusSampleOptions,
  writeSample: StatusSampleWriter,
): Promise<boolean> {
  const telemetry = extractStatusTelemetry(json, {
    allowRawStatsOnly: options.allowRawStatsOnly,
  });
  if (!telemetry) return false;

  await writeSample({
    nodeId: options.nodeId,
    network: options.network,
    ...telemetry,
  });
  return true;
}
