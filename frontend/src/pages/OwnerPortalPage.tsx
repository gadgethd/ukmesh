import React, { FormEvent, useEffect, useMemo, useState } from 'react';
import maplibregl from 'maplibre-gl';
import 'maplibre-gl/dist/maplibre-gl.css';
import { Area, AreaChart, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import { DEFAULT_CENTER, MAP_STYLE } from '../components/Map/mapConfig';

type OwnerNode = {
  node_id: string;
  name: string | null;
  network: string;
  last_seen: string | null;
  advert_count: number;
  lat: number | null;
  lon: number | null;
  iata: string | null;
  role: number | null;
};

function nodeRoleLabel(role: number | null): string {
  if (role === 1) return 'Companion';
  if (role === 3) return 'Room Server';
  return 'Repeater';
}

type OwnerDashboard = {
  nodes: OwnerNode[];
  totals: {
    ownedNodes: number;
    packets24h: number;
    packets7d: number;
    packetsReceived24h: number;
  };
  roadmap: string[];
};

type OwnerSessionResponse = {
  ok: boolean;
  dashboard: OwnerDashboard;
  mqttUsername?: string | null;
};

const OWNER_SESSION_EVENT = 'meshcore-owner-session';

function publishOwnerSession(mqttUsername: string | null) {
  window.dispatchEvent(new CustomEvent(OWNER_SESSION_EVENT, { detail: { mqttUsername } }));
}

type LivePeer = {
  node_id: string;
  name: string | null;
  network: string | null;
  iata: string | null;
  lat: number | null;
  lon: number | null;
  packets_24h: number;
  last_seen: string | null;
};

type LivePacket = {
  time: string;
  packet_type: number | null;
  route_type: number | null;
  hop_count: number | null;
  packet_hash: string | null;
  src_node_id: string | null;
  src_node_name: string | null;
  sender: string | null;
  body: string | null;
};

type OwnerLiveResponse = {
  nodeId: string;
  ownerNode: OwnerNode;
  incomingPeers: LivePeer[];
  heardBy: Array<LivePeer & { packets_7d: number; best_hops: number | null }>;
  linkHealth: Array<{
    peer_node_id: string;
    peer_name: string | null;
    peer_network: string | null;
    owner_to_peer: number;
    peer_to_owner: number;
    observed_count: number;
    itm_path_loss_db: number | null;
    itm_viable: boolean | null;
    force_viable: boolean;
    last_observed: string | null;
  }>;
  advertTrend24h: Array<{ bucket: string; adverts: number }>;
  telemetry24h: Array<{
    bucket: string;
    batteryPct: number | null;
    batteryMv: number | null;
    uptimeSecs: number | null;
    channelUtilPct: number | null;
    airUtilTxPct: number | null;
  }>;
  alerts: Array<{ level: 'info' | 'warn' | 'error'; message: string }>;
  recentPackets: LivePacket[];
};

type LastHopStrengthPoint = {
  bucket: string;
  lastHopNodeId: string | null;
  lastHopName: string;
  resolution: 'direct' | 'resolved' | 'inferred' | 'unresolved';
  avgSnr: number | null;
  avgRssi: number | null;
  sampleCount: number;
};

type OwnerLastHopStrengthResponse = {
  points: LastHopStrengthPoint[];
};

type MappedPeer = LivePeer & { lat: number; lon: number };

function fmtTs(ts: string | null): string {
  if (!ts) return 'No recent activity';
  return new Date(ts).toLocaleString();
}

function isValidMapCoord(lat: number | null, lon: number | null): boolean {
  if (lat == null || lon == null) return false;
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return false;
  if (Math.abs(lat) < 5 && Math.abs(lon) < 5) return false;
  if (lat < -90 || lat > 90 || lon < -180 || lon > 180) return false;
  return true;
}

const PACKET_LABELS: Record<number, string> = {
  0: 'Request',
  1: 'Response',
  2: 'DM',
  3: 'Ack',
  4: 'Advert',
  5: 'GroupText',
  6: 'GroupData',
  7: 'AnonReq',
  8: 'Path',
  9: 'Trace',
};

const ROUTE_LABELS: Record<number, string> = {
  0: 'Flood',
  1: 'Direct',
  2: 'Guided',
  3: 'Opportunistic',
};

const AXIS_COLOR = '#3a5070';
const LABEL_COLOR = '#6b8aaa';
const TIP_BG = '#0d1520';
const TIP_BORDER = 'rgba(0,196,255,0.25)';

function cleanPacketBody(packet: LivePacket): string | null {
  const body = packet.body?.trim();
  if (!body) return null;
  if (/^\d+$/.test(body) && body === String(packet.packet_type ?? '')) return null;
  return body;
}

function formatCompactTs(ts: string | null): string {
  if (!ts) return '-';
  return new Date(ts).toLocaleString([], {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  });
}

function formatPathLoss(value: number | null): string {
  if (value == null || !Number.isFinite(value)) return '-';
  return `${value.toFixed(1)} dB`;
}

function linkBadge(link: OwnerLiveResponse['linkHealth'][number]): string {
  if (link.force_viable) return 'Forced';
  if (link.itm_viable) return 'Viable';
  if (link.itm_path_loss_db != null && link.itm_path_loss_db <= 137.5) return 'Weak';
  return 'Unproven';
}

const TrendBars: React.FC<{ points: Array<{ bucket: string; adverts: number }> }> = ({ points }) => {
  const max = Math.max(1, ...points.map((point) => point.adverts));
  return (
    <div className="owner-trend">
      <div className="owner-trend__bars" aria-label="Advert trend for the last 24 hours">
        {points.map((point) => {
          const height = Math.max(10, Math.round((point.adverts / max) * 100));
          return (
            <div
              key={point.bucket}
              className="owner-trend__bar"
              title={`${formatCompactTs(point.bucket)} · ${point.adverts} advert${point.adverts === 1 ? '' : 's'}`}
              style={{ height: `${height}%` }}
            />
          );
        })}
      </div>
      <div className="owner-trend__meta">
        <span>24h advert trend</span>
        <strong>{points.reduce((sum, point) => sum + point.adverts, 0)}</strong>
      </div>
    </div>
  );
};

type TelemetryPoint = OwnerLiveResponse['telemetry24h'][number];

const TELEMETRY_SERIES = [
  {
    key: 'batteryPct' as const,
    title: 'Battery',
    suffix: '%',
    stroke: '#6ddc7a',
    meta: (point: TelemetryPoint | null) => point?.batteryMv == null ? 'No data' : `${point.batteryMv} mV`,
  },
  {
    key: 'channelUtilPct' as const,
    title: 'Channel Utilization',
    suffix: '%',
    stroke: '#00c4ff',
    meta: () => 'Rolling from status samples',
  },
  {
    key: 'airUtilTxPct' as const,
    title: 'Air Util TX',
    suffix: '%',
    stroke: '#ff9f43',
    meta: () => 'Rolling TX air time',
  },
] as const;

function formatUptime(seconds: number | null): string {
  if (seconds == null || !Number.isFinite(seconds) || seconds < 0) return '—';
  const total = Math.floor(seconds);
  const days = Math.floor(total / 86400);
  const hours = Math.floor((total % 86400) / 3600);
  const minutes = Math.floor((total % 3600) / 60);
  if (days > 0) return `${days}d ${hours}h`;
  if (hours > 0) return `${hours}h ${minutes}m`;
  return `${minutes}m`;
}

const OwnerTelemetryTooltip: React.FC<{
  active?: boolean;
  payload?: Array<{ value: number }>;
  label?: string;
  suffix: string;
}> = ({ active, payload, label, suffix }) => {
  if (!active || !payload?.length) return null;
  return (
    <div style={{ background: TIP_BG, border: `1px solid ${TIP_BORDER}`, borderRadius: 6, padding: '8px 12px', fontSize: 12 }}>
      {label ? <p style={{ color: LABEL_COLOR, margin: '0 0 4px' }}>{formatCompactTs(label)}</p> : null}
      <p style={{ color: '#e8f0fb', margin: 0 }}>
        <strong>{payload[0]?.value?.toFixed(1)}{suffix}</strong>
      </p>
    </div>
  );
};

const LAST_HOP_COLORS = ['#00c4ff', '#6ddc7a', '#ff9f43', '#f472b6', '#a78bfa', '#facc15'];
const MIN_LAST_HOP_SAMPLES = 10;

const LastHopStrengthTooltip: React.FC<{
  active?: boolean;
  payload?: Array<{ dataKey: string; value: number; color: string; payload: Record<string, unknown> }>;
  label?: string;
  seriesByKey: Map<string, { label: string; totalSamples: number; bucketCount: number }>;
}> = ({ active, payload, label, seriesByKey }) => {
  if (!active || !payload?.length) return null;
  return (
    <div style={{ background: TIP_BG, border: `1px solid ${TIP_BORDER}`, borderRadius: 6, padding: '8px 12px', fontSize: 12 }}>
      {label ? <p style={{ color: LABEL_COLOR, margin: '0 0 6px' }}>{formatCompactTs(label)}</p> : null}
      {payload.map((entry) => {
        const series = seriesByKey.get(entry.dataKey);
        const sampleCount = Number((entry.payload?.[`${entry.dataKey}__samples`] as number | undefined) ?? 0);
        return (
          <p key={entry.dataKey} style={{ color: '#e8f0fb', margin: '2px 0' }}>
            <strong style={{ color: entry.color }}>{series?.label ?? entry.dataKey}</strong>
            {`: ${Number(entry.value).toFixed(1)} dB`}
            {sampleCount > 0 ? ` (${sampleCount} sample${sampleCount === 1 ? '' : 's'})` : ''}
          </p>
        );
      })}
    </div>
  );
};

const LastHopStrengthChart: React.FC<{ points: LastHopStrengthPoint[] }> = ({ points }) => {
  const [selectedSeriesKey, setSelectedSeriesKey] = useState<string | null>(null);
  const visiblePoints = useMemo(
    () => points.filter((point) => point.resolution !== 'unresolved'),
    [points],
  );

  const chartState = useMemo(() => {
    const totals = new Map<string, {
      key: string;
      label: string;
      totalSamples: number;
      bucketCount: number;
      buckets: Set<string>;
      resolution: LastHopStrengthPoint['resolution'];
    }>();
    for (const point of visiblePoints) {
      const key = point.lastHopNodeId ?? `unresolved:${point.lastHopName}`;
      const existing = totals.get(key);
      if (existing) {
        existing.totalSamples += point.sampleCount;
        existing.buckets.add(point.bucket);
        existing.bucketCount = existing.buckets.size;
      } else {
        totals.set(key, {
          key,
          label: point.lastHopName,
          totalSamples: point.sampleCount,
          bucketCount: 1,
          buckets: new Set([point.bucket]),
          resolution: point.resolution,
        });
      }
    }

    const allSeries = Array.from(totals.values())
      .filter((series) => series.totalSamples >= MIN_LAST_HOP_SAMPLES)
      .sort((a, b) => b.totalSamples - a.totalSamples || a.label.localeCompare(b.label))
    const selected = selectedSeriesKey
      ? allSeries.filter((series) => series.key === selectedSeriesKey)
      : allSeries.slice(0, 6);
    const selectedKeys = new Set(selected.map((series) => series.key));
    const seriesByKey = new Map(allSeries.map((series) => [series.key, series] as const));

    const bucketed = new Map<string, Record<string, string | number | null>>();
    for (const point of visiblePoints) {
      const key = point.lastHopNodeId ?? `unresolved:${point.lastHopName}`;
      if (!selectedKeys.has(key)) continue;
      const entry = bucketed.get(point.bucket) ?? { bucket: point.bucket };
      entry[key] = point.avgSnr;
      entry[`${key}__samples`] = point.sampleCount;
      bucketed.set(point.bucket, entry);
    }

    return {
      allSeries,
      series: selected,
      seriesByKey,
      data: Array.from(bucketed.values()).sort((a, b) => String(a.bucket).localeCompare(String(b.bucket))),
    };
  }, [selectedSeriesKey, visiblePoints]);

  useEffect(() => {
    if (selectedSeriesKey && !chartState.allSeries.some((series) => series.key === selectedSeriesKey)) {
      setSelectedSeriesKey(null);
    }
  }, [chartState.allSeries, selectedSeriesKey]);

  if (chartState.series.length < 1 || chartState.data.length < 1) {
    return (
      <article className="owner-telemetry-metric">
        <div className="owner-panel__head owner-panel__head--compact">
          <div>
            <h3>RX Strength by Last Hop</h3>
            <p>Average SNR over the last 7 days</p>
          </div>
        </div>
        <div className="owner-telemetry-metric__chart">
          <div className="owner-telemetry-metric__empty">No last-hop repeaters with 10+ samples yet</div>
        </div>
        <div className="owner-telemetry-metric__footer">
          <span>Last 7d</span>
          <span>Minimum 10 samples</span>
        </div>
      </article>
    );
  }

  const latestBucket = chartState.data[chartState.data.length - 1];
  const latestActive = chartState.series
    .map((series) => {
      const value = latestBucket?.[series.key];
      return typeof value === 'number'
        ? `${series.label}: ${value.toFixed(1)} dB`
        : null;
    })
    .filter(Boolean)
    .slice(0, 2)
    .join(' · ');

  return (
      <article className="owner-telemetry-metric owner-telemetry-metric--wide owner-telemetry-metric--tall">
        <div className="owner-panel__head owner-panel__head--compact">
          <div>
            <h3>RX Strength by Last Hop</h3>
            <p>{latestActive || 'Average SNR over the last 7 days'}</p>
          </div>
          <strong className="owner-telemetry-metric__value">{chartState.series.length}</strong>
        </div>
        <div className="owner-telemetry-metric__chart">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={chartState.data} margin={{ top: 6, right: 10, left: 0, bottom: 0 }}>
              <XAxis dataKey="bucket" hide />
            <YAxis
              width={28}
              axisLine={{ stroke: AXIS_COLOR }}
              tickLine={false}
              tick={{ fill: LABEL_COLOR, fontSize: 11 }}
              domain={['auto', 'auto']}
              />
              <Tooltip content={<LastHopStrengthTooltip seriesByKey={chartState.seriesByKey} />} />
              {chartState.series.map((series, index) => (
                <Line
                  key={series.key}
                type="monotone"
                dataKey={series.key}
                stroke={LAST_HOP_COLORS[index % LAST_HOP_COLORS.length]}
                strokeWidth={2}
                dot={false}
                connectNulls
                isAnimationActive={false}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
        </div>
        <div className="owner-telemetry-metric__footer">
          <span>Last 7d</span>
          <span>{selectedSeriesKey ? 'Filtered to one repeater' : `${chartState.allSeries.length} repeaters with 10+ samples`}</span>
        </div>
        <div className="owner-telemetry-metric__series-list">
          <button
            type="button"
            className={`owner-telemetry-metric__series-btn${selectedSeriesKey == null ? ' owner-telemetry-metric__series-btn--active' : ''}`}
            onClick={() => setSelectedSeriesKey(null)}
          >
            All
          </button>
          {chartState.allSeries.map((series) => (
            <button
              key={series.key}
              type="button"
              className={`owner-telemetry-metric__series-btn${selectedSeriesKey === series.key ? ' owner-telemetry-metric__series-btn--active' : ''}`}
              onClick={() => setSelectedSeriesKey(series.key)}
              title={`${series.label} · ${series.bucketCount} hourly point${series.bucketCount === 1 ? '' : 's'} · ${series.totalSamples} packets in last 7d`}
            >
              {series.label} ({series.bucketCount}h)
            </button>
          ))}
        </div>
      </article>
  );
};

const TelemetryMiniChart: React.FC<{
  title: string;
  stroke: string;
  suffix: string;
  points: TelemetryPoint[];
  metric: keyof Pick<TelemetryPoint, 'batteryPct' | 'channelUtilPct' | 'airUtilTxPct'>;
  meta: (point: TelemetryPoint | null) => string;
}> = ({ title, stroke, suffix, points, metric, meta }) => {
  const chartData = points
    .map((point) => ({ bucket: point.bucket, value: point[metric] }))
    .filter((entry): entry is { bucket: string; value: number } => entry.value != null);
  const latest = points.length > 0 ? points[points.length - 1]! : null;
  const latestValue = latest?.[metric] ?? null;

  return (
    <article className="owner-telemetry-metric">
      <div className="owner-panel__head owner-panel__head--compact">
        <div>
          <h3>{title}</h3>
          <p>{meta(latest)}</p>
        </div>
        <strong className="owner-telemetry-metric__value">
          {latestValue == null ? '—' : `${latestValue.toFixed(1)}${suffix}`}
        </strong>
      </div>
      <div className="owner-telemetry-metric__chart">
        {chartData.length > 0 ? (
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={chartData} margin={{ top: 6, right: 6, left: 0, bottom: 0 }}>
              <XAxis dataKey="bucket" hide />
              <YAxis
                hide
                domain={[0, 100]}
                axisLine={{ stroke: AXIS_COLOR }}
                tickLine={false}
                tick={{ fill: LABEL_COLOR, fontSize: 11 }}
              />
              <Tooltip content={<OwnerTelemetryTooltip suffix={suffix} />} />
              <Area
                type="monotone"
                dataKey="value"
                stroke={stroke}
                fill={stroke}
                fillOpacity={0.18}
                strokeWidth={2}
                dot={false}
                isAnimationActive={false}
              />
            </AreaChart>
          </ResponsiveContainer>
        ) : (
          <div className="owner-telemetry-metric__empty">No telemetry yet</div>
        )}
      </div>
      <div className="owner-telemetry-metric__footer">
        <span>Last 24h</span>
        <span>{chartData.length} samples</span>
      </div>
    </article>
  );
};

const TelemetryStatCard: React.FC<{
  title: string;
  value: string;
  meta: string;
}> = ({ title, value, meta }) => (
  <article className="owner-telemetry-metric owner-telemetry-metric--stat">
    <div className="owner-panel__head owner-panel__head--compact">
      <div>
        <h3>{title}</h3>
        <p>{meta}</p>
      </div>
    </div>
    <div className="owner-telemetry-metric__stat">
      <strong>{value}</strong>
    </div>
    <div className="owner-telemetry-metric__footer">
      <span>Last 24h</span>
      <span>Latest sample</span>
    </div>
  </article>
);

const OwnerMapView: React.FC<{
  ownerCoord: { lat: number; lon: number } | null;
  peers: MappedPeer[];
  allPoints: Array<{ lat: number; lon: number }>;
}> = ({ ownerCoord, peers, allPoints }) => {
  const containerRef = React.useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!containerRef.current) return;

    const map = new maplibregl.Map({
      container: containerRef.current,
      style: MAP_STYLE,
      center: [DEFAULT_CENTER[1], DEFAULT_CENTER[0]],
      zoom: 7,
      attributionControl: false,
    });

    // Keep the map as a static regional backdrop while preserving marker clicks.
    map.scrollZoom.disable();
    map.boxZoom.disable();
    map.dragPan.disable();
    map.dragRotate.disable();
    map.doubleClickZoom.disable();
    map.keyboard.disable();
    map.touchZoomRotate.disable();

    const EMPTY: GeoJSON.FeatureCollection = { type: 'FeatureCollection', features: [] };
    const popup = new maplibregl.Popup({
      closeButton: true,
      closeOnClick: true,
      offset: 14,
      maxWidth: '260px',
    });

    const applyViewport = () => {
      map.resize();
      if (allPoints.length === 1) {
        map.setCenter([allPoints[0].lon, allPoints[0].lat]);
        map.setZoom(8);
        return;
      }
      if (allPoints.length > 1) {
        const centerPoints = peers.length > 0 ? peers : allPoints;
        const centerLons = centerPoints.map((pt) => pt.lon);
        const centerLats = centerPoints.map((pt) => pt.lat);
        const centerLon = (Math.min(...centerLons) + Math.max(...centerLons)) / 2;
        const centerLat = (Math.min(...centerLats) + Math.max(...centerLats)) / 2;
        const maxLonDelta = Math.max(...allPoints.map((pt) => Math.abs(pt.lon - centerLon)));
        const maxLatDelta = Math.max(...allPoints.map((pt) => Math.abs(pt.lat - centerLat)));
        const lonExtent = Math.max(0.02, maxLonDelta * 2.25);
        const latExtent = Math.max(0.02, maxLatDelta * 2.25);
        const paddedBounds = new maplibregl.LngLatBounds(
          [centerLon - lonExtent, centerLat - latExtent],
          [centerLon + lonExtent, centerLat + latExtent],
        );
        const padding = { top: 24, right: 24, bottom: 24, left: 24 };
        const camera = map.cameraForBounds(paddedBounds, { padding });
        map.jumpTo({
          center: [centerLon, centerLat],
          zoom: camera?.zoom ?? map.getZoom(),
          bearing: 0,
          pitch: 0,
        });
      }
    };

    const buildNodeFC = (): GeoJSON.FeatureCollection => ({
      type: 'FeatureCollection',
      features: [
        ...(ownerCoord ? [{
          type: 'Feature' as const,
          geometry: { type: 'Point' as const, coordinates: [ownerCoord.lon, ownerCoord.lat] },
          properties: {
            kind: 'owner',
            name: 'Selected owner node',
            details: ownerCoord ? `${ownerCoord.lat.toFixed(4)}, ${ownerCoord.lon.toFixed(4)}` : '',
          },
        }] : []),
        ...peers.map((p) => ({
          type: 'Feature' as const,
          geometry: { type: 'Point' as const, coordinates: [p.lon, p.lat] },
          properties: {
            kind: 'peer',
            name: p.name ?? p.node_id,
            details: `${p.packets_24h} packets / 24h${p.iata ? ` · ${p.iata}` : ''}${p.network ? ` · ${p.network}` : ''}`,
          },
        })),
      ],
    });

    const buildLineFC = (): GeoJSON.FeatureCollection => ({
      type: 'FeatureCollection',
      features: ownerCoord ? peers.map((p) => ({
        type: 'Feature' as const,
        geometry: { type: 'LineString' as const, coordinates: [[ownerCoord.lon, ownerCoord.lat], [p.lon, p.lat]] },
        properties: {},
      })) : [],
    });

    map.on('load', () => {
      map.addSource('owner-lines', { type: 'geojson', data: buildLineFC() });
      map.addSource('owner-nodes', { type: 'geojson', data: buildNodeFC() });

      map.addLayer({ id: 'owner-lines-layer', type: 'line', source: 'owner-lines',
        paint: { 'line-color': '#00c4ff', 'line-width': 1.5, 'line-opacity': 0.6 } });
      map.addLayer({ id: 'owner-nodes-layer', type: 'circle', source: 'owner-nodes',
        paint: {
          'circle-radius': ['case', ['==', ['get', 'kind'], 'owner'], 8, 6.5],
          'circle-color': ['case', ['==', ['get', 'kind'], 'owner'], 'rgba(0,196,255,0.18)', 'rgba(255,179,0,0.18)'],
          'circle-stroke-width': 2,
          'circle-stroke-color': ['case', ['==', ['get', 'kind'], 'owner'], '#00c4ff', '#ffb300'],
        } });

      map.on('mouseenter', 'owner-nodes-layer', () => {
        map.getCanvas().style.cursor = 'pointer';
      });
      map.on('mouseleave', 'owner-nodes-layer', () => {
        map.getCanvas().style.cursor = '';
      });
      map.on('click', 'owner-nodes-layer', (event) => {
        const feature = event.features?.[0];
        if (!feature || feature.geometry.type !== 'Point') return;
        const coordinates = feature.geometry.coordinates.slice() as [number, number];
        const name = String(feature.properties?.name ?? 'Node');
        const details = String(feature.properties?.details ?? '');
        popup
          .setLngLat(coordinates)
          .setHTML(`<div class="node-popup"><div class="node-popup__title">${name}</div><div class="node-popup__meta">${details}</div></div>`)
          .addTo(map);
      });

      applyViewport();
      requestAnimationFrame(applyViewport);
    });

    const resizeObserver = new ResizeObserver(() => {
      applyViewport();
    });
    resizeObserver.observe(containerRef.current);

    return () => {
      resizeObserver.disconnect();
      popup.remove();
      map.remove();
      void EMPTY; // silence unused warning
    };
  // Re-create map whenever data changes (static display map, re-creation is fine)
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [ownerCoord, peers, allPoints]);

  return <div ref={containerRef} className="owner-map" />;
};

export const OwnerPortalPage: React.FC = () => {
  const [mqttUsername, setMqttUsername] = useState('');
  const [mqttPassword, setMqttPassword] = useState('');
  const [loading, setLoading] = useState(true);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [dashboard, setDashboard] = useState<OwnerDashboard | null>(null);
  const [selectedNodeId, setSelectedNodeId] = useState<string>('');
  const [live, setLive] = useState<OwnerLiveResponse | null>(null);
  const [liveError, setLiveError] = useState<string | null>(null);
  const [lastHopStrength, setLastHopStrength] = useState<LastHopStrengthPoint[]>([]);

  useEffect(() => {
    let cancelled = false;
    fetch('/api/owner/session', { cache: 'no-store' })
      .then(async (res) => {
        if (!res.ok) return null;
        return (await res.json()) as OwnerSessionResponse;
      })
      .then((json) => {
        if (cancelled) return;
        setDashboard(json?.dashboard ?? null);
        publishOwnerSession(json?.mqttUsername ?? null);
        if (json?.dashboard?.nodes?.[0]?.node_id) {
          setSelectedNodeId(json.dashboard.nodes[0].node_id);
        }
        setLoading(false);
      })
      .catch(() => {
        if (cancelled) return;
        setDashboard(null);
        publishOwnerSession(null);
        setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const handleLogin = (event: FormEvent) => {
    event.preventDefault();
    if (!mqttUsername.trim() || !mqttPassword.trim()) {
      setError('Enter your MQTT username and password.');
      return;
    }
    setSubmitting(true);
    setError(null);
    fetch('/api/owner/login', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        mqttUsername: mqttUsername.trim(),
        mqttPassword: mqttPassword.trim(),
      }),
    })
      .then(async (res) => {
        const body = await res.json().catch(() => ({}));
        if (!res.ok) {
          throw new Error(String(body.error ?? `HTTP ${res.status}`));
        }
        return body as OwnerSessionResponse;
      })
      .then((json) => {
        setDashboard(json.dashboard);
        publishOwnerSession(json.mqttUsername ?? mqttUsername.trim());
        if (json.dashboard.nodes[0]?.node_id) {
          setSelectedNodeId(json.dashboard.nodes[0].node_id);
        }
        setMqttUsername('');
        setMqttPassword('');
      })
      .catch((err: Error) => {
        setError(err.message);
      })
      .finally(() => setSubmitting(false));
  };

  const handleLogout = () => {
    fetch('/api/owner/logout', { method: 'POST' })
      .finally(() => {
        setDashboard(null);
        setLive(null);
        setLastHopStrength([]);
        setError(null);
        publishOwnerSession(null);
      });
  };

  useEffect(() => {
    if (!dashboard || !selectedNodeId) return;
    let cancelled = false;

    const load = () => {
      fetch(`/api/owner/live?nodeId=${encodeURIComponent(selectedNodeId)}`, { cache: 'no-store' })
        .then(async (res) => {
          const json = await res.json().catch(() => ({}));
          if (!res.ok) throw new Error(String(json.error ?? `HTTP ${res.status}`));
          return json as OwnerLiveResponse;
        })
        .then((json) => {
          if (cancelled) return;
          setLive(json);
          setLiveError(null);
        })
        .catch((err: Error) => {
          if (cancelled) return;
          setLiveError(err.message);
        });
    };

    load();
    const timer = setInterval(load, 10_000);
    return () => {
      cancelled = true;
      clearInterval(timer);
    };
  }, [dashboard, selectedNodeId]);

  useEffect(() => {
    if (!dashboard || !selectedNodeId) return;
    let cancelled = false;

    const load = () => {
      fetch(`/api/owner/live-last-hop?nodeId=${encodeURIComponent(selectedNodeId)}`, { cache: 'no-store' })
        .then(async (res) => {
          const json = await res.json().catch(() => ({}));
          if (!res.ok) throw new Error(String(json.error ?? `HTTP ${res.status}`));
          return json as OwnerLastHopStrengthResponse;
        })
        .then((json) => {
          if (cancelled) return;
          setLastHopStrength(json.points ?? []);
        })
        .catch(() => {
          if (cancelled) return;
        });
    };

    setLastHopStrength([]);
    load();
    const timer = setInterval(load, 60_000);
    return () => {
      cancelled = true;
      clearInterval(timer);
    };
  }, [dashboard, selectedNodeId]);

  const mapPoints = useMemo(() => {
    const ownerNode = live?.ownerNode;
    const points: Array<{ lat: number; lon: number }> = [];
    if (isValidMapCoord(ownerNode?.lat ?? null, ownerNode?.lon ?? null)) {
      points.push({ lat: ownerNode!.lat as number, lon: ownerNode!.lon as number });
    }
    for (const peer of live?.incomingPeers ?? []) {
      if (!isValidMapCoord(peer.lat, peer.lon)) continue;
      points.push({ lat: peer.lat as number, lon: peer.lon as number });
    }
    return points;
  }, [live]);

  const ownerCoord = useMemo(() => {
    const ownerNode = live?.ownerNode;
    const lat = ownerNode?.lat ?? null;
    const lon = ownerNode?.lon ?? null;
    if (!isValidMapCoord(lat, lon)) return null;
    return { lat: lat as number, lon: lon as number };
  }, [live]);

  const mapPeers = useMemo<MappedPeer[]>(
    () => (live?.incomingPeers ?? [])
      .filter((peer) => isValidMapCoord(peer.lat, peer.lon))
      .map((peer) => ({ ...peer, lat: peer.lat as number, lon: peer.lon as number })),
    [live],
  );

  const strongestLink = useMemo(() => {
    const links = live?.linkHealth ?? [];
    return links
      .filter((link) => link.itm_path_loss_db != null)
      .sort((a, b) => (a.itm_path_loss_db ?? Number.POSITIVE_INFINITY) - (b.itm_path_loss_db ?? Number.POSITIVE_INFINITY))[0] ?? null;
  }, [live]);

  const viableLinkCount = useMemo(
    () => (live?.linkHealth ?? []).filter((link) => link.itm_viable || link.force_viable).length,
    [live],
  );

  const latestTelemetry = useMemo(() => {
    const points = live?.telemetry24h ?? [];
    return points.length > 0 ? points[points.length - 1]! : null;
  }, [live]);

  return (
    <>
      <section className="site-page-hero">
        <div className="site-content">
          <h1 className="site-page-hero__title">Repeater Owner Portal</h1>
          <p className="site-page-hero__sub">
            Login with your MQTT username and password. Sessions are kept in an encrypted cookie.
          </p>
        </div>
      </section>

      <div className="site-content site-prose site-prose--wide">
        {loading ? <p className="prose-note">Checking login session...</p> : null}
        {!loading && !dashboard ? (
          <section className="prose-section owner-login">
            <h2>Login</h2>
            <p className="prose-note">
              Enter the MQTT credentials associated with your repeater observer.
            </p>
            <form className="owner-login__form" onSubmit={handleLogin}>
              <label className="owner-login__label" htmlFor="owner-username">MQTT username</label>
              <input
                id="owner-username"
                className="owner-login__input"
                type="text"
                autoComplete="username"
                value={mqttUsername}
                onChange={(e) => setMqttUsername(e.target.value)}
                placeholder="Enter username"
                maxLength={128}
              />
              <label className="owner-login__label" htmlFor="owner-key">MQTT password</label>
              <input
                id="owner-key"
                className="owner-login__input"
                type="password"
                autoComplete="current-password"
                value={mqttPassword}
                onChange={(e) => setMqttPassword(e.target.value)}
                placeholder="Enter password"
                maxLength={256}
              />
              <button className="site-btn site-btn--primary owner-login__button" type="submit" disabled={submitting}>
                {submitting ? 'Logging in...' : 'Login'}
              </button>
            </form>
            {error ? <p className="prose-note owner-login__error">{error}</p> : null}
          </section>
        ) : null}

        {!loading && dashboard ? (
          <>
            <section className="prose-section">
              <div className="owner-head">
                <h2>Dashboard</h2>
                <button type="button" className="site-btn site-btn--ghost" onClick={handleLogout}>
                  Logout
                </button>
              </div>
              {dashboard.nodes.length > 1 ? (
                <div className="owner-select">
                  <label htmlFor="owner-node-select">Node</label>
                  <select
                    id="owner-node-select"
                    className="owner-select__input"
                    value={selectedNodeId}
                    onChange={(e) => setSelectedNodeId(e.target.value)}
                  >
                    {dashboard.nodes.map((node) => (
                      <option key={node.node_id} value={node.node_id}>
                        {node.name ?? node.node_id}
                      </option>
                    ))}
                  </select>
                </div>
              ) : null}
              <div className="site-stats-grid site-stats-grid--6 owner-summary-grid">
                <div className="site-stat"><span className="site-stat__value">{live?.ownerNode.name ?? 'Unnamed'}</span><span className="site-stat__label">{nodeRoleLabel(live?.ownerNode.role ?? null)}</span></div>
                <div className="site-stat"><span className="site-stat__value">{live?.ownerNode.network ?? '-'}</span><span className="site-stat__label">Network</span></div>
                <div className="site-stat"><span className="site-stat__value">{live?.ownerNode.iata ?? '-'}</span><span className="site-stat__label">IATA</span></div>
                <div className="site-stat"><span className="site-stat__value">{live?.ownerNode.advert_count ?? 0}</span><span className="site-stat__label">Adverts</span></div>
                <div className="site-stat"><span className="site-stat__value">{fmtTs(live?.ownerNode.last_seen ?? null)}</span><span className="site-stat__label">Last Seen</span></div>
                <div className="site-stat"><span className="site-stat__value">{live?.incomingPeers.length ?? 0}</span><span className="site-stat__label">Direct Senders (24h)</span></div>
                <div className="site-stat"><span className="site-stat__value">{viableLinkCount}</span><span className="site-stat__label">Viable Links</span></div>
                <div className="site-stat"><span className="site-stat__value">{strongestLink?.peer_name ?? '-'}</span><span className="site-stat__label">Strongest Link</span></div>
                <div className="site-stat"><span className="site-stat__value">{formatPathLoss(strongestLink?.itm_path_loss_db ?? null)}</span><span className="site-stat__label">Best Path Loss</span></div>
                <div className="site-stat"><span className="site-stat__value">{(live?.advertTrend24h ?? []).reduce((sum, point) => sum + point.adverts, 0)}</span><span className="site-stat__label">Adverts (24h)</span></div>
                <div className="site-stat"><span className="site-stat__value">{dashboard.totals.packets24h}</span><span className="site-stat__label">Packets Sent (24h)</span></div>
                <div className="site-stat"><span className="site-stat__value">{dashboard.totals.packetsReceived24h}</span><span className="site-stat__label">Packets Received (24h)</span></div>
              </div>
              {liveError ? <p className="prose-note owner-login__error">Live data error: {liveError}</p> : null}
            </section>

            <section className="owner-panel owner-telemetry-panel">
              <div className="owner-panel__head">
                <div>
                  <h2>Node Telemetry</h2>
                  <p className="prose-note">Battery level and rolling radio utilisation from MQTT status samples over the last 24 hours.</p>
                </div>
              </div>
              <div className="owner-telemetry-strip">
                {TELEMETRY_SERIES.map((series) => (
                  <TelemetryMiniChart
                    key={series.key}
                    title={series.title}
                    stroke={series.stroke}
                    suffix={series.suffix}
                    points={live?.telemetry24h ?? []}
                    metric={series.key}
                    meta={series.meta}
                  />
                ))}
                <TelemetryStatCard
                  title="Uptime"
                  value={formatUptime(latestTelemetry?.uptimeSecs ?? null)}
                  meta={latestTelemetry?.uptimeSecs == null ? 'No telemetry yet' : `${latestTelemetry.uptimeSecs}s reported`}
                />
                <LastHopStrengthChart points={lastHopStrength} />
              </div>
            </section>

            <div className="owner-dashboard-grid">
              <section className="prose-section owner-panel owner-panel--map">
                <div className="owner-panel__head">
                  <div><h2>Direct Sender Map</h2></div>
                </div>
                <div className="owner-map-wrap">
                  <OwnerMapView ownerCoord={ownerCoord} peers={mapPeers} allPoints={mapPoints} />
                </div>
              </section>

              <section className="prose-section owner-panel owner-panel--alerts">
                <div className="owner-panel__head"><h2>Alerts</h2></div>
                <div className="owner-alerts">
                  {(live?.alerts ?? []).map((alert, idx) => (
                    <article key={`${alert.level}-${idx}`} className={`owner-alert owner-alert--${alert.level}`}>
                      <strong>{alert.level.toUpperCase()}</strong>
                      <span>{alert.message}</span>
                    </article>
                  ))}
                </div>
              </section>

              <section className="prose-section owner-panel owner-panel--trend">
                <div className="owner-panel__head"><h2>Advert Trend</h2></div>
                <TrendBars points={live?.advertTrend24h ?? []} />
              </section>

              <section className="prose-section owner-panel owner-panel--links">
                <div className="owner-panel__head"><h2>RF Link Health</h2></div>
                <div className="owner-list">
                  {(live?.linkHealth ?? []).slice(0, 8).map((link) => (
                    <article key={link.peer_node_id} className="owner-list__row">
                      <div className="owner-list__primary">
                        <strong>{link.peer_name ?? link.peer_node_id}</strong>
                        <span>{link.peer_network ?? '-'}</span>
                      </div>
                      <div className="owner-list__metrics">
                        <span>{linkBadge(link)}</span>
                        <span>{formatPathLoss(link.itm_path_loss_db)}</span>
                        <span>{link.owner_to_peer}/{link.peer_to_owner}</span>
                        <span>{link.observed_count} obs</span>
                      </div>
                    </article>
                  ))}
                  {(live?.linkHealth ?? []).length === 0 ? (
                    <p className="prose-note">No link health data has been calculated for this node yet.</p>
                  ) : null}
                </div>
              </section>

              <section className="prose-section owner-panel owner-panel--senders">
                <div className="owner-panel__head"><h2>Direct Senders</h2></div>
                <div className="owner-list">
                  {(live?.incomingPeers ?? []).slice(0, 8).map((peer) => (
                    <article key={peer.node_id} className="owner-list__row">
                      <div className="owner-list__primary">
                        <strong>{peer.name ?? peer.node_id}</strong>
                        <span>{peer.network ?? '-'} · {peer.iata ?? '-'}</span>
                      </div>
                      <div className="owner-list__metrics">
                        <span>{peer.packets_24h} / 24h</span>
                        <span>{formatCompactTs(peer.last_seen)}</span>
                      </div>
                    </article>
                  ))}
                  {(live?.incomingPeers ?? []).length === 0 ? (
                    <p className="prose-note">No direct sender nodes found in the last 24 hours.</p>
                  ) : null}
                </div>
              </section>

              <section className="prose-section owner-panel owner-panel--packets">
                <div className="owner-panel__head"><h2>Live Packets Received By {nodeRoleLabel(live?.ownerNode.role ?? null)}</h2></div>
                <div className="owner-packets">
                  {(live?.recentPackets ?? []).map((packet, idx) => (
                    <article key={`${packet.time}-${packet.packet_hash ?? `row-${idx}`}`} className="owner-packet">
                      <div className="owner-packet__head">
                        <strong>{PACKET_LABELS[Number(packet.packet_type ?? -1)] ?? `Type ${packet.packet_type ?? '?'}`}</strong>
                        <span>{fmtTs(packet.time)}</span>
                      </div>
                      <div className="owner-packet__meta">
                        <span>From: {packet.src_node_name ?? packet.src_node_id ?? '-'}</span>
                        <span>Sender: {packet.sender ?? '-'}</span>
                        <span>Hops: {packet.hop_count ?? '-'}</span>
                        <span>Route: {ROUTE_LABELS[Number(packet.route_type ?? -1)] ?? (packet.route_type ?? '-')}</span>
                      </div>
                      {cleanPacketBody(packet) ? <p className="owner-packet__body">{cleanPacketBody(packet)}</p> : null}
                    </article>
                  ))}
                  {(live?.recentPackets ?? []).length === 0 ? (
                    <p className="prose-note">No packets received by this node yet.</p>
                  ) : null}
                </div>
              </section>
            </div>

          </>
        ) : null}
      </div>
    </>
  );
};
