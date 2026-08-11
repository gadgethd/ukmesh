import React from 'react';
import {
  formatDurationMs,
  formatEpochSeconds,
  type OwnerStatus,
} from './ownerPortalModel.js';

function numberValue(value: number | null, suffix = ''): string {
  if (value == null || !Number.isFinite(value)) return '—';
  return `${value.toLocaleString(undefined, { maximumFractionDigits: 2 })}${suffix}`;
}

function textValue(value: string | null): string {
  return value?.trim() ? value : '—';
}

const StatusField: React.FC<{ label: string; value: string }> = ({ label, value }) => (
  <div className="owner-status-field">
    <dt>{label}</dt>
    <dd>{value}</dd>
  </div>
);

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

  return (
    <div className="owner-status-groups">
      <StatusGroup title="System">
        <StatusField label="Board temperature" value={numberValue(status.board_temp_c, ' °C')} />
        <StatusField label="Wi-Fi SSID" value={textValue(status.wifi_ssid)} />
        <StatusField label="Wi-Fi uptime" value={formatDurationMs(status.wifi_uptime_ms)} />
        <StatusField label="NTP" value={ntpState} />
        <StatusField label="NTP sync age" value={ntpAge} />
        <StatusField label="Boot count" value={numberValue(status.boot_count)} />
        <StatusField label="Reset reason" value={textValue(status.reset_reason)} />
        <StatusField label="Boot epoch" value={formatEpochSeconds(status.boot_epoch)} />
        <StatusField label="Git commit" value={textValue(status.git_commit)} />
      </StatusGroup>

      <StatusGroup title="Power">
        <StatusField label="Battery" value={numberValue(status.battery_mv, ' mV')} />
        <StatusField label="Solar" value={numberValue(status.solar_mv, ' mV')} />
      </StatusGroup>

      <StatusGroup title="RF">
        <StatusField label="Wi-Fi RSSI" value={numberValue(status.wifi_rssi, ' dBm')} />
        <StatusField label="Channel ID" value={numberValue(status.channel_id)} />
        <StatusField label="Channel utilization" value={numberValue(status.channel_utilization, ' %')} />
        <StatusField label="Air utilization TX" value={numberValue(status.air_util_tx, ' %')} />
        <StatusField label="Air utilization RX" value={numberValue(status.air_util_rx, ' %')} />
        <StatusField label="Last RX RSSI" value={numberValue(status.last_rx_rssi, ' dBm')} />
        <StatusField label="Last RX SNR" value={numberValue(status.last_rx_snr, ' dB')} />
        <StatusField label="TX power" value={numberValue(status.tx_power_dbm, ' dBm')} />
        <StatusField label="Nodes heard (24h)" value={numberValue(status.nodes_heard_24h)} />
      </StatusGroup>

      <StatusGroup title="Config">
        <StatusField label="Max loop" value={formatDurationMs(status.max_loop_ms)} />
        <StatusField label="Max loop at" value={formatDurationMs(status.max_loop_at_ms)} />
        <StatusField label="Config version" value={textValue(status.config_version)} />
        <StatusField label="Config CRC32" value={textValue(status.config_crc32)} />
        <StatusField label="Filesystem free / total" value={`${fsFree} / ${fsTotal}`} />
        <StatusField label="NVS free entries" value={numberValue(status.nvs_free_entries)} />
      </StatusGroup>

      <StatusGroup title="MQTT">
        <StatusField label="Broker URI" value={textValue(status.mqtt.broker_uri)} />
        <StatusField label="Broker username" value={textValue(status.mqtt.broker_username)} />
        <StatusField label="Uptime" value={formatDurationMs(status.mqtt.uptime_ms)} />
        <StatusField label="Reconnects (1h)" value={numberValue(status.mqtt.reconnect_attempts_1h)} />
        <StatusField label="Status publishes" value={numberValue(status.mqtt.session_status_publishes)} />
        <StatusField label="Packet publishes" value={numberValue(status.mqtt.session_packet_publishes)} />
        <StatusField label="Last offline epoch" value={formatEpochSeconds(status.mqtt.last_offline_epoch)} />
      </StatusGroup>
    </div>
  );
};
