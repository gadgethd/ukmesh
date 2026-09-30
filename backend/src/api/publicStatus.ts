/** Approved fields for the unauthenticated node status response contracts. */
export type PublicStatusCounters = {
  battery_mv: number | null;
  uptime_secs: number | null;
  tx_air_secs: number | null;
  rx_air_secs: number | null;
  channel_utilization: number | null;
  air_util_tx: number | null;
};

export type PublicLatestNodeStatusDTO = PublicStatusCounters & {
  time: string;
  node_id: string;
  network: string | null;
  name: string | null;
  iata: string | null;
};

export type PublicMqttNodeStatusDTO = PublicStatusCounters & {
  node_id: string;
  name: string | null;
  last_seen: string;
  packets_24h: string;
};

type PublicStatusCounterSource = Partial<PublicStatusCounters>;

function publicStatusCounters(row: PublicStatusCounterSource): PublicStatusCounters {
  return {
    battery_mv: row.battery_mv ?? null,
    uptime_secs: row.uptime_secs ?? null,
    tx_air_secs: row.tx_air_secs ?? null,
    rx_air_secs: row.rx_air_secs ?? null,
    channel_utilization: row.channel_utilization ?? null,
    air_util_tx: row.air_util_tx ?? null,
  };
}

export function toPublicLatestNodeStatusDTO(
  row: PublicLatestNodeStatusDTO,
): PublicLatestNodeStatusDTO {
  return {
    time: row.time,
    node_id: row.node_id,
    network: row.network,
    name: row.name,
    iata: row.iata,
    ...publicStatusCounters(row),
  };
}

export function toPublicMqttNodeStatusDTO(
  row: PublicMqttNodeStatusDTO,
): PublicMqttNodeStatusDTO {
  return {
    node_id: row.node_id,
    name: row.name,
    last_seen: row.last_seen,
    ...publicStatusCounters(row),
    packets_24h: row.packets_24h,
  };
}
