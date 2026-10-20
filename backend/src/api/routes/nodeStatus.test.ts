import assert from 'node:assert/strict';
import test from 'node:test';
import { toPublicLatestNodeStatusDTO } from '../publicStatus.js';

test('latest node status DTO omits owner-only and unknown telemetry fields', () => {
  const storedRow = {
    time: '2026-09-23T12:00:00.000Z',
    node_id: 'a'.repeat(64),
    network: 'ukmesh',
    name: 'Public node',
    iata: 'NCL',
    battery_mv: 3910,
    uptime_secs: 7200,
    tx_air_secs: 14,
    rx_air_secs: 28,
    channel_utilization: 7,
    air_util_tx: 3,
    hardware_model: 'private model',
    firmware_version: 'private firmware',
    stats: {
      wifi_ssid: 'private wifi',
      reset_reason: 'private reset detail',
      fs_free_bytes: 1234,
      mqtt: { broker_uri: 'mqtts://private', broker_username: 'private-user' },
      future_owner_field: 'private future value',
    },
    future_status_field: 'private future value',
  };

  assert.deepEqual(toPublicLatestNodeStatusDTO(storedRow), {
    time: storedRow.time,
    node_id: storedRow.node_id,
    network: storedRow.network,
    name: storedRow.name,
    iata: storedRow.iata,
    battery_mv: storedRow.battery_mv,
    uptime_secs: storedRow.uptime_secs,
    tx_air_secs: storedRow.tx_air_secs,
    rx_air_secs: storedRow.rx_air_secs,
    channel_utilization: storedRow.channel_utilization,
    air_util_tx: storedRow.air_util_tx,
  });
});
