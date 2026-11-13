import React from 'react';
import {
  formatDurationMs,
  formatEpochSeconds,
  type OwnerStatus,
} from '../../features/owner/model.js';

function numberValue(value: number | null, suffix = ''): string {
  if (value == null || !Number.isFinite(value)) return '—';
  return `${value.toLocaleString(undefined, { maximumFractionDigits: 2 })}${suffix}`;
}

function textValue(value: string | null): string {
  return value?.trim() ? value : '—';
}

const StatusGroup: React.FC<{ title: string; children: React.ReactNode }> = ({ title, children }) => (
  <section className="owner-status-group">
    <h3>{title}</h3>
    <dl>{children}</dl>
  </section>
);

export const OwnerStatusFields: React.FC<{ status: OwnerStatus | null }> = ({ status }) => {
  if (!status) {
    return <p className="prose-note owner-status-empty">No status telemetry has been received for this node yet.</p>;
  }

  const ntpState = status.ntp_synced == null ? '—' : status.ntp_synced ? 'Synced' : 'Not synced';
  const ntpAge = status.ntp_synced === false ? 'Not synced' : formatDurationMs(status.ntp_sync_age_ms);
  const fsFree = numberValue(status.fs_free_bytes, ' B');
  const fsTotal = numberValue(status.fs_total_bytes, ' B');

  const groups: { title: string; fields: [string, string][] }[] = [
    { title: 'System', fields: [
      ['Board temperature', numberValue(status.board_temp_c, ' °C')],
      ['Wi-Fi SSID', textValue(status.wifi_ssid)],
      ['Wi-Fi uptime', formatDurationMs(status.wifi_uptime_ms)],
      ['NTP', ntpState],
      ['NTP sync age', ntpAge],
      ['Boot count', numberValue(status.boot_count)],
      ['Reset reason', textValue(status.reset_reason)],
      ['Boot epoch', formatEpochSeconds(status.boot_epoch)],
      ['Git commit', textValue(status.git_commit)],
    ] },
    { title: 'Power', fields: [
      ['Battery', numberValue(status.battery_mv, ' mV')],
      ['Solar', numberValue(status.solar_mv, ' mV')],
    ] },
    { title: 'RF', fields: [
      ['Wi-Fi RSSI', numberValue(status.wifi_rssi, ' dBm')],
      ['Channel ID', numberValue(status.channel_id)],
      ['Channel utilization', numberValue(status.channel_utilization, ' %')],
      ['Air utilization TX', numberValue(status.air_util_tx, ' %')],
      ['Air utilization RX', numberValue(status.air_util_rx, ' %')],
      ['Last RX RSSI', numberValue(status.last_rx_rssi, ' dBm')],
      ['Last RX SNR', numberValue(status.last_rx_snr, ' dB')],
      ['TX power', numberValue(status.tx_power_dbm, ' dBm')],
      ['Nodes heard (24h)', numberValue(status.nodes_heard_24h)],
    ] },
    { title: 'Config', fields: [
      ['Max loop', formatDurationMs(status.max_loop_ms)],
      ['Max loop at', formatDurationMs(status.max_loop_at_ms)],
      ['Config version', textValue(status.config_version)],
      ['Config CRC32', textValue(status.config_crc32)],
      ['Filesystem free / total', `${fsFree} / ${fsTotal}`],
      ['NVS free entries', numberValue(status.nvs_free_entries)],
    ] },
    { title: 'MQTT', fields: [
      ['Broker URI', textValue(status.mqtt.broker_uri)],
      ['Broker username', textValue(status.mqtt.broker_username)],
      ['Uptime', formatDurationMs(status.mqtt.uptime_ms)],
      ['Reconnects (1h)', numberValue(status.mqtt.reconnect_attempts_1h)],
      ['Status publishes', numberValue(status.mqtt.session_status_publishes)],
      ['Packet publishes', numberValue(status.mqtt.session_packet_publishes)],
      ['Last offline epoch', formatEpochSeconds(status.mqtt.last_offline_epoch)],
    ] },
  ];
  return <div className="owner-status-groups">
    {groups.map(group => <StatusGroup key={group.title} title={group.title}>
      {group.fields.map(([label, value]) => <div className="owner-status-field" key={label}><dt>{label}</dt><dd>{value}</dd></div>)}
    </StatusGroup>)}
  </div>;
};
