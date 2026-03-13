import React, { useEffect, useMemo, useState } from 'react';
import { Area, AreaChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import { getCurrentSite } from '../../config/site.js';
import { uncachedEndpoint, withScopeParams } from '../../utils/api.js';

type DevNode = {
  node_id: string;
  name?: string;
  iata?: string;
  last_seen: string;
  is_online: boolean;
  hardware_model?: string;
  public_key?: string;
};

type DevStatusSample = {
  time: string;
  node_id: string;
  network?: string | null;
  battery_mv?: number | null;
  uptime_secs?: number | null;
  tx_air_secs?: number | null;
  rx_air_secs?: number | null;
  channel_utilization?: number | null;
  air_util_tx?: number | null;
  stats?: Record<string, unknown> | null;
  name?: string | null;
  iata?: string | null;
  hardware_model?: string | null;
  firmware_version?: string | null;
};

type DevStatusHistoryPoint = {
  time: string;
  battery_mv?: number | null;
  uptime_secs?: number | null;
  channel_utilization?: number | null;
  air_util_tx?: number | null;
  heap_free?: number | null;
  heap_min_free?: number | null;
  tx_queue_depth?: number | null;
  tx_queue_depth_peak?: number | null;
};

type DevStatusHistoryResponse = {
  nodeId: string | null;
  points: DevStatusHistoryPoint[];
};

type DevPacket = {
  time: string;
  packet_hash: string;
  rx_node_id?: string;
  src_node_id?: string;
  packet_type?: number;
  hop_count?: number;
  rssi?: number;
  snr?: number;
  payload?: Record<string, unknown>;
};

const TYPE_LABELS: Record<number, string> = {
  0: 'REQ',
  1: 'RSP',
  2: 'DM',
  3: 'ACK',
  4: 'ADV',
  5: 'GRP',
  6: 'DAT',
  7: 'ANON',
  8: 'PATH',
  9: 'TRC',
  11: 'CTL',
};

function timeAgo(ts?: string | null): string {
  if (!ts) return 'never';
  const ageMs = Math.max(0, Date.now() - Date.parse(ts));
  const sec = Math.floor(ageMs / 1000);
  if (sec < 60) return `${sec}s ago`;
  if (sec < 3600) return `${Math.floor(sec / 60)}m ago`;
  if (sec < 86400) return `${Math.floor(sec / 3600)}h ago`;
  return `${Math.floor(sec / 86400)}d ago`;
}

function shortNode(id?: string): string {
  if (!id) return 'unknown';
  return `${id.slice(0, 8)}…${id.slice(-6)}`;
}

function packetSummary(packet: DevPacket): string {
  const payload = packet.payload ?? {};
  const appData = payload['appData'] as Record<string, unknown> | undefined;
  const candidate = [
    typeof appData?.['name'] === 'string' ? appData['name'] : undefined,
    typeof payload['origin'] === 'string' ? payload['origin'] : undefined,
    typeof appData?.['text'] === 'string' ? appData['text'] : undefined,
    typeof payload['summary'] === 'string' ? payload['summary'] : undefined,
  ].find(Boolean);
  return String(candidate ?? 'No decoded summary');
}

function labelize(key: string): string {
  return key
    .replace(/_/g, ' ')
    .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
    .replace(/^./, (c) => c.toUpperCase());
}

function formatTelemetryValue(value: unknown): string {
  if (typeof value === 'boolean') return value ? 'true' : 'false';
  if (typeof value === 'number') return Number.isFinite(value) ? value.toLocaleString() : String(value);
  if (typeof value === 'string') return value;
  if (value === null || value === undefined) return '—';
  return JSON.stringify(value);
}

function formatUptime(value: unknown, fallbackSeconds?: number | null): string {
  let totalSeconds: number | null = null;
  if (typeof value === 'number' && Number.isFinite(value)) {
    totalSeconds = value > 86_400 ? Math.floor(value / 1000) : Math.floor(value);
  } else if (fallbackSeconds != null && Number.isFinite(fallbackSeconds)) {
    totalSeconds = Math.floor(fallbackSeconds);
  }
  if (totalSeconds == null || totalSeconds < 0) return '—';
  const days = Math.floor(totalSeconds / 86400);
  const hours = Math.floor((totalSeconds % 86400) / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  if (days > 0) return `${days}d ${hours}h`;
  if (hours > 0) return `${hours}h ${minutes}m`;
  return `${Math.max(0, minutes)}m`;
}

function formatKilobytes(value: unknown): string {
  if (typeof value !== 'number' || !Number.isFinite(value)) return '—';
  const kb = value / 1024;
  return `${kb >= 100 ? kb.toFixed(0) : kb.toFixed(1)} KB`;
}

function formatCompactNumber(value: unknown): string {
  if (typeof value !== 'number' || !Number.isFinite(value)) return '—';
  return value.toLocaleString();
}

const AXIS_COLOR = '#3a5070';
const LABEL_COLOR = '#6b8aaa';

type TelemetryChartMetric = {
  key: keyof DevStatusHistoryPoint;
  title: string;
  color: string;
  formatValue: (value: number | null | undefined) => string;
  yDomain?: [number, number | 'auto'];
};

const TELEMETRY_CHARTS: TelemetryChartMetric[] = [
  {
    key: 'channel_utilization',
    title: 'Channel Utilization',
    color: '#00c4ff',
    formatValue: (value) => value == null ? '—' : `${value.toFixed(1)}%`,
    yDomain: [0, 'auto'],
  },
  {
    key: 'air_util_tx',
    title: 'Air Util TX',
    color: '#ff9f43',
    formatValue: (value) => value == null ? '—' : `${value.toFixed(1)}%`,
    yDomain: [0, 'auto'],
  },
  {
    key: 'heap_free',
    title: 'Heap Free',
    color: '#ce93d8',
    formatValue: (value) => formatKilobytes(value),
  },
  {
    key: 'heap_min_free',
    title: 'Heap Min Free',
    color: '#8bc34a',
    formatValue: (value) => formatKilobytes(value),
  },
  {
    key: 'tx_queue_depth',
    title: 'TX Queue Depth',
    color: '#ff6b6b',
    formatValue: (value) => value == null ? '—' : formatCompactNumber(value),
    yDomain: [0, 'auto'],
  },
  {
    key: 'tx_queue_depth_peak',
    title: 'TX Queue Peak',
    color: '#ffd166',
    formatValue: (value) => value == null ? '—' : formatCompactNumber(value),
    yDomain: [0, 'auto'],
  },
];

const GraphTooltip: React.FC<{
  active?: boolean;
  payload?: Array<{ value: number }>;
  label?: string;
  formatter: (value: number | null | undefined) => string;
}> = ({ active, payload, label, formatter }) => {
  if (!active || !payload?.length) return null;
  return (
    <div className="dev-telemetry-chart__tooltip">
      {label ? <p>{new Date(label).toLocaleString()}</p> : null}
      <strong>{formatter(payload[0]?.value)}</strong>
    </div>
  );
};

const TelemetryChartCard: React.FC<{
  title: string;
  color: string;
  points: DevStatusHistoryPoint[];
  metric: keyof DevStatusHistoryPoint;
  formatter: (value: number | null | undefined) => string;
  yDomain?: [number, number | 'auto'];
}> = ({ title, color, points, metric, formatter, yDomain }) => {
  const chartData = points
    .map((point) => ({ time: point.time, value: point[metric] }))
    .filter((entry): entry is { time: string; value: number } => typeof entry.value === 'number' && Number.isFinite(entry.value));
  const latestValue = chartData.length > 0 ? chartData[chartData.length - 1]?.value : null;
  return (
    <article className="dev-telemetry-chart">
      <div className="dev-telemetry-chart__head">
        <div>
          <h3>{title}</h3>
          <p>Last 24h</p>
        </div>
        <strong>{formatter(latestValue)}</strong>
      </div>
      <div className="dev-telemetry-chart__body">
        {chartData.length > 0 ? (
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={chartData} margin={{ top: 6, right: 6, left: 0, bottom: 0 }}>
              <XAxis dataKey="time" hide />
              <YAxis hide domain={yDomain ?? ['auto', 'auto']} axisLine={{ stroke: AXIS_COLOR }} tickLine={false} tick={{ fill: LABEL_COLOR, fontSize: 11 }} />
              <Tooltip content={<GraphTooltip formatter={formatter} />} />
              <Area
                type="monotone"
                dataKey="value"
                stroke={color}
                fill={color}
                fillOpacity={0.18}
                strokeWidth={2}
                dot={false}
                isAnimationActive={false}
              />
            </AreaChart>
          </ResponsiveContainer>
        ) : (
          <div className="dev-telemetry-chart__empty">No samples yet</div>
        )}
      </div>
      <div className="dev-telemetry-chart__foot">
        <span>{chartData.length} samples</span>
      </div>
    </article>
  );
};

export const DevHomePage: React.FC = () => {
  const site = getCurrentSite();
  const scope = useMemo(() => ({ network: site.networkFilter, observer: site.observerId }), [site.networkFilter, site.observerId]);
  const [nodes, setNodes] = useState<DevNode[]>([]);
  const [packets, setPackets] = useState<DevPacket[]>([]);
  const [statusSamples, setStatusSamples] = useState<DevStatusSample[]>([]);
  const [statusHistory, setStatusHistory] = useState<DevStatusHistoryPoint[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;

    const load = async () => {
      try {
        const [nodesRes, packetsRes, statusRes] = await Promise.all([
          fetch(uncachedEndpoint(withScopeParams('/api/nodes', scope)), { cache: 'no-store' }),
          fetch(uncachedEndpoint(withScopeParams('/api/packets/recent?limit=15&raw=1', scope)), { cache: 'no-store' }),
          fetch(uncachedEndpoint(withScopeParams('/api/node-status/latest', scope)), { cache: 'no-store' }),
        ]);

        if (!nodesRes.ok || !packetsRes.ok || !statusRes.ok) {
          throw new Error(`HTTP ${nodesRes.status}/${packetsRes.status}/${statusRes.status}`);
        }

        const [nodesJson, packetsJson, statusJson] = await Promise.all([
          nodesRes.json() as Promise<DevNode[]>,
          packetsRes.json() as Promise<DevPacket[]>,
          statusRes.json() as Promise<DevStatusSample[]>,
        ]);

        if (cancelled) return;
        setNodes(nodesJson);
        setPackets(packetsJson);
        setStatusSamples(statusJson);
        const latestNodeId = statusJson[0]?.node_id;
        if (latestNodeId) {
          const historyRes = await fetch(uncachedEndpoint(withScopeParams(`/api/node-status/history?nodeId=${encodeURIComponent(latestNodeId)}&hours=24`, scope)), { cache: 'no-store' });
          if (!historyRes.ok) {
            throw new Error(`HTTP ${historyRes.status}`);
          }
          const historyJson = await historyRes.json() as DevStatusHistoryResponse;
          if (!cancelled) {
            setStatusHistory(historyJson.points ?? []);
          }
        } else {
          setStatusHistory([]);
        }
        setError(null);
      } catch (err) {
        if (cancelled) return;
        setError((err as Error).message);
      }
    };

    void load();
    const timer = setInterval(() => void load(), 3000);
    return () => {
      cancelled = true;
      clearInterval(timer);
    };
  }, [scope]);

  const nodeMap = useMemo(() => new Map(nodes.map((node) => [node.node_id, node])), [nodes]);
  const latestPacket = packets[0];
  const latestStatus = statusSamples[0];
  const latestObserver = latestPacket?.rx_node_id ? nodeMap.get(latestPacket.rx_node_id) : undefined;
  const recentPackets = useMemo(() => packets.slice(0, 15), [packets]);
  const latestStats = useMemo(
    () => (latestStatus?.stats && typeof latestStatus.stats === 'object' ? latestStatus.stats : null),
    [latestStatus],
  );
  const mqttTelemetryEntries = useMemo(() => {
    const mqttStats = latestStats
      ? (latestStats['mqtt'] as Record<string, unknown> | undefined)
      : undefined;
    if (!mqttStats || typeof mqttStats !== 'object') return [] as Array<[string, unknown]>;
    return Object.entries(mqttStats);
  }, [latestStats]);
  const mqttServerCount = 3;
  const derivedTelemetryBoxes = useMemo(() => {
    const stats = latestStats ?? {};
    return [
      ['Uptime', formatUptime(stats['uptime_ms'], latestStatus?.uptime_secs ?? null)],
      ['Loop Iterations', formatCompactNumber(stats['loop_iterations'])],
      ['WiFi Reconnect Attempts', formatCompactNumber(stats['wifi_reconnect_attempts'])],
      ['RX Publish Calls', formatCompactNumber(stats['rx_publish_calls'])],
      ['TX Publish Calls', formatCompactNumber(stats['tx_publish_calls'])],
      ['Publish Skipped (No Connection)', formatCompactNumber(stats['publish_skipped_no_connection'])],
      ['Forward Successes', formatCompactNumber(stats['forward_successes'])],
      ['Forward Successes (Flood)', formatCompactNumber(stats['forward_successes_flood'])],
      ['Forward Successes (Direct)', formatCompactNumber(stats['forward_successes_direct'])],
      ['Forward Failures', formatCompactNumber(stats['forward_failures'])],
      ['WiFi Connected', formatTelemetryValue(stats['wifi_connected'])],
      ['Heap Min Seen Since Boot', formatKilobytes(stats['heap_min_seen_since_boot'])],
    ] as Array<[string, string]>;
  }, [latestStats, latestStatus?.uptime_secs]);

  return (
    <section className="dev-status-page">
      <div className="dev-status-page__header">
        <h1>{site.displayName}</h1>
        <p>Isolated MQTT test feed status.</p>
        <span className="dev-status-page__last-seen">
          Last seen: {latestPacket ? timeAgo(latestPacket.time) : 'never'}
        </span>
      </div>

      {error && <p className="prose-note">Diagnostics fetch error: {error}</p>}

      <div className="dev-status-grid">
        <section className="dev-status-card">
          <h2>Status</h2>
          <div className="dev-status-list">
            <div><span>Feed</span><strong>{latestPacket ? 'Receiving packets' : 'Waiting for packets'}</strong></div>
            <div><span>MQTT servers connected</span><strong>{mqttServerCount}</strong></div>
            <div><span>Last packet</span><strong>{latestPacket ? timeAgo(latestPacket.time) : 'never'}</strong></div>
            <div><span>Isolation</span><strong>network=test</strong></div>
          </div>
        </section>

        <section className="dev-status-card">
          <h2>Latest packet</h2>
          <div className="dev-status-list">
            <div><span>Observer</span><strong>{latestObserver?.name ?? 'Unknown observer'}</strong></div>
            <div><span>Type</span><strong>{latestPacket?.packet_type !== undefined ? (TYPE_LABELS[latestPacket.packet_type] ?? `T${latestPacket.packet_type}`) : '—'}</strong></div>
            <div><span>Signal</span><strong>{latestPacket?.rssi !== undefined || latestPacket?.snr !== undefined ? `${latestPacket.rssi ?? '—'} / ${latestPacket.snr ?? '—'}` : '—'}</strong></div>
            <div><span>Hash</span><strong className="dev-status-mono">{latestPacket?.packet_hash ?? '—'}</strong></div>
            <div className="dev-status-list__summary"><span>Decoded summary</span><strong>{latestPacket ? packetSummary(latestPacket) : 'No test packets have arrived yet.'}</strong></div>
          </div>
        </section>
      </div>

      <section className="dev-status-card dev-status-card--wide">
        <h2>Latest telemetry</h2>
        {latestStatus ? (
          <div className="dev-telemetry">
            <div className="dev-status-list dev-status-list--compact">
              <div><span>Node</span><strong>{latestStatus.name ?? shortNode(latestStatus.node_id)}</strong></div>
              <div><span>Sample</span><strong>{timeAgo(latestStatus.time)}</strong></div>
              <div><span>Firmware</span><strong>{latestStatus.firmware_version ?? '—'}</strong></div>
              <div><span>Model</span><strong>{latestStatus.hardware_model ?? '—'}</strong></div>
            </div>
            <div className="dev-telemetry__section">
              <h3>Graphs</h3>
              <div className="dev-telemetry__charts">
                {TELEMETRY_CHARTS.map((chart) => (
                  <TelemetryChartCard
                    key={chart.key}
                    title={chart.title}
                    color={chart.color}
                    points={statusHistory}
                    metric={chart.key}
                    formatter={chart.formatValue}
                    yDomain={chart.yDomain}
                  />
                ))}
              </div>
            </div>
            <div className="dev-telemetry__section">
              <h3>Stats</h3>
              <div className="dev-telemetry__grid">
                {derivedTelemetryBoxes.map(([key, value]) => (
                  <div key={key} className="dev-telemetry__item">
                    <span>{key}</span>
                    <strong>{value}</strong>
                  </div>
                ))}
              </div>
            </div>
            <div className="dev-telemetry__section">
              <h3>MQTT</h3>
              <div className="dev-telemetry__grid">
                {mqttTelemetryEntries.length > 0 ? mqttTelemetryEntries.map(([key, value]) => (
                  <div key={key} className="dev-telemetry__item">
                    <span>{labelize(key)}</span>
                    <strong>{formatTelemetryValue(value)}</strong>
                  </div>
                )) : (
                  <div className="dev-status-empty">No nested MQTT stats in the latest status payload.</div>
                )}
              </div>
            </div>
          </div>
        ) : (
          <p className="dev-status-note">No status telemetry has arrived yet.</p>
        )}
      </section>

      <section className="dev-status-card dev-status-card--wide dev-status-card--fixed">
        <h2>Live packets</h2>
        <div className="dev-status-table-wrap">
          <table className="dev-status-table">
            <thead>
              <tr>
                <th>Time</th>
                <th>Type</th>
                <th>Observer</th>
                <th>Signal</th>
                <th>Hash</th>
                <th>Summary</th>
              </tr>
            </thead>
            <tbody>
              {recentPackets.length > 0 ? recentPackets.map((packet) => (
                <tr key={`${packet.packet_hash}-${packet.time}`}>
                  <td>{new Date(packet.time).toLocaleTimeString()}</td>
                  <td>{packet.packet_type !== undefined ? (TYPE_LABELS[packet.packet_type] ?? `T${packet.packet_type}`) : '—'}</td>
                  <td>{packet.rx_node_id ? (nodeMap.get(packet.rx_node_id)?.name ?? shortNode(packet.rx_node_id)) : 'unknown'}</td>
                  <td>{packet.rssi !== undefined || packet.snr !== undefined ? `${packet.rssi ?? '—'} / ${packet.snr ?? '—'}` : '—'}</td>
                  <td className="dev-status-mono">{packet.packet_hash}</td>
                  <td>{packetSummary(packet)}</td>
                </tr>
              )) : (
                <tr>
                  <td colSpan={6} className="dev-status-empty">No test packets have arrived yet.</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </section>
    </section>
  );
};
