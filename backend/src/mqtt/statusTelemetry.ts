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
  const batteryMv = readNum(stats, 'battery_mv', 'batteryMv');
  const uptimeSecs = (() => {
    const direct = readNum(stats, 'uptime_secs', 'uptimeSecs');
    if (direct != null) return direct;
    const uptimeMs = readNum(stats, 'uptime_ms', 'uptimeMs');
    if (uptimeMs == null) return undefined;
    return Math.floor(uptimeMs / 1000);
  })();
  const txAirSecs = readNum(stats, 'tx_air_secs', 'txAirSecs');
  const rxAirSecs = readNum(stats, 'rx_air_secs', 'rxAirSecs');
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
