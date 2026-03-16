import React, { useCallback, useEffect, useRef, useState } from 'react';
import { extractPacketSummary } from '../../hooks/packetFeed.js';
import '../../styles/teesside-dashboard.css';

// ── Types ────────────────────────────────────────────────────────────────────

type ConnectivityData = {
  inbound: boolean;
  outbound: boolean;
  lastInbound: string | null;
  lastOutbound: string | null;
  windowHours: number;
  checkedAt: string;
};

type StatsData = {
  mqttNodes: number;
  nodesDay: number;
  packetsDay: number;
};

type NodeStatusRow = {
  node_id: string;
  name: string | null;
  uptime_secs: number | null;
  channel_utilization: number | null;
  time: string | null;
};

type RadioSample = {
  batt_percent?: number | null;
  batt_milli_volts?: number | null;
  last_rssi?: number | null;
  last_snr?: number | null;
  noise_floor?: number | null;
  total_up_time_secs?: number | null;
};

type RadioMonitor = {
  id: string;
  nodeName: string;
  pollMinutes: number;
  lastSuccessAt: string | null;
  lastError: string | null;
  lastSample: RadioSample | null;
};

// Actual shape returned by /api/radio-stats → radio bot /state
type RadioStatsData = {
  connected: boolean;
  monitors: RadioMonitor[];
};

type HistorySample = {
  time: string;
  batteryPercent: number | null;
};

type ObserverActivity = {
  node_id: string;
  name: string | null;
  rx_24h: number;
  tx_24h: number;
};

type RecentPacket = {
  time: string;
  packet_hash?: string | null;
  src_node_id?: string | null;
  rx_node_id?: string | null;
  observer_node_ids?: string[] | null;
  hop_count?: number | null;
  packet_type?: number | null;
  summary?: string | null;
  payload?: Record<string, unknown> | null;
};

// ── Helpers ──────────────────────────────────────────────────────────────────

const TYPE_LABELS: Record<number, string> = {
  0: 'REQ', 1: 'RSP', 2: 'DM', 3: 'ACK', 4: 'ADV',
  5: 'GRP', 6: 'DAT', 7: 'ANON', 8: 'PATH', 9: 'TRC', 11: 'CTL',
};

function timeAgo(ts?: string | null): string {
  if (!ts) return 'never';
  const sec = Math.max(0, Math.floor((Date.now() - Date.parse(ts)) / 1000));
  if (sec < 60) return `${sec}s ago`;
  if (sec < 3600) return `${Math.floor(sec / 60)}m ago`;
  if (sec < 86400) return `${Math.floor(sec / 3600)}h ago`;
  return `${Math.floor(sec / 86400)}d ago`;
}

function fmtUptime(secs?: number | null): string {
  if (secs == null || secs < 0) return '—';
  const d = Math.floor(secs / 86400);
  const h = Math.floor((secs % 86400) / 3600);
  const m = Math.floor((secs % 3600) / 60);
  if (d > 0) return `${d}d ${h}h`;
  if (h > 0) return `${h}h ${m}m`;
  return `${m}m`;
}

function isOnline(monitor: RadioMonitor): boolean {
  if (!monitor.lastSuccessAt) return false;
  const ageMs = Date.now() - Date.parse(monitor.lastSuccessAt);
  return ageMs < monitor.pollMinutes * 2 * 60 * 1000;
}

function battClass(pct: number | null): string {
  if (pct == null) return '';
  if (pct >= 50) return 'td-batt-good';
  if (pct >= 20) return 'td-batt-warn';
  return 'td-batt-low';
}

function battBarColor(pct: number | null): string {
  if (pct == null) return 'var(--td-text-muted)';
  if (pct >= 50) return 'var(--td-batt-good)';
  if (pct >= 20) return 'var(--td-batt-warn)';
  return 'var(--td-batt-low)';
}

// ── Data fetching hook ───────────────────────────────────────────────────────

function usePolled<T>(url: string, intervalMs: number): { data: T | null; error: boolean } {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState(false);

  const fetchData = useCallback(async () => {
    try {
      const res = await fetch(`${url}${url.includes('?') ? '&' : '?'}_ts=${Date.now()}`);
      if (!res.ok) { setError(true); return; }
      setData(await res.json() as T);
      setError(false);
    } catch {
      setError(true);
    }
  }, [url]);

  useEffect(() => {
    void fetchData();
    const id = setInterval(() => void fetchData(), intervalMs);
    return () => clearInterval(id);
  }, [fetchData, intervalMs]);

  return { data, error };
}

// ── Sub-components ────────────────────────────────────────────────────────────

function ConnectivityBanner({ data, error }: { data: ConnectivityData | null; error: boolean }) {
  if (error) return <div className="td-error">Unable to check connectivity</div>;
  if (!data) return <div className="td-loading">Checking connectivity…</div>;

  const both   = data.inbound && data.outbound;
  const either = data.inbound || data.outbound;

  const lightEmoji   = both ? '🟢' : either ? '🟡' : '🔴';
  const labelText    = both ? 'Connected to UK Mesh' : either ? 'One-way communication' : 'No mesh connectivity';
  const borderClass  = both ? 'td-connectivity--green' : either ? 'td-connectivity--amber' : 'td-connectivity--red';

  return (
    <div className={`td-connectivity ${borderClass}`}>
      <div className="td-connectivity__light">{lightEmoji}</div>
      <div>
        <p className="td-connectivity__label">{labelText}</p>
        <div className="td-connectivity__meta">
          <span>Inbound: {data.inbound ? `last ${timeAgo(data.lastInbound)}` : `none in ${data.windowHours}h`}</span>
          <span>Outbound: {data.outbound ? `last ${timeAgo(data.lastOutbound)}` : `none in ${data.windowHours}h`}</span>
          <span className="td-muted">Checked {timeAgo(data.checkedAt)}</span>
        </div>
      </div>
    </div>
  );
}

function StatsRow({ data, error }: { data: StatsData | null; error: boolean }) {
  if (error) return <div className="td-error">Stats unavailable</div>;
  const cards: { label: string; value: number | string }[] = [
    { label: 'Active MQTT repeaters', value: data?.mqttNodes ?? '—' },
    { label: 'Heard / 24h',           value: data?.nodesDay  ?? '—' },
    { label: 'Packets / 24h',         value: data?.packetsDay ?? '—' },
  ];
  return (
    <div className="td-stats-row">
      {cards.map(c => (
        <div className="td-stat-card" key={c.label}>
          <p className="td-stat-card__value">{c.value}</p>
          <p className="td-stat-card__label">{c.label}</p>
        </div>
      ))}
    </div>
  );
}

const SEVEN_DAYS_MS = 7 * 24 * 60 * 60 * 1000;

function BatterySparkline({ samples }: { samples: HistorySample[] }) {
  const usable = samples
    .filter(s => s.batteryPercent != null && Number.isFinite(s.batteryPercent))
    .map(s => ({ t: Date.parse(s.time), pct: s.batteryPercent as number }))
    .filter(s => Date.now() - s.t <= SEVEN_DAYS_MS)
    .sort((a, b) => a.t - b.t);

  if (usable.length < 2) return null;

  const W = 200, H = 44;
  const tMin = usable[0].t, tMax = usable[usable.length - 1].t;
  const tRange = tMax - tMin || 1;

  const px = (s: { t: number; pct: number }) => ((s.t - tMin) / tRange) * W;
  const py = (s: { t: number; pct: number }) => H - (s.pct / 100) * H;

  const linePts = usable.map(s => `${px(s)},${py(s)}`).join(' ');
  const areaPts = [
    `0,${H}`,
    ...usable.map(s => `${px(s)},${py(s)}`),
    `${W},${H}`,
  ].join(' ');

  const last = usable[usable.length - 1];
  const strokeColor = last.pct >= 50 ? 'var(--td-batt-good)' : last.pct >= 20 ? 'var(--td-batt-warn)' : 'var(--td-batt-low)';
  const fillColor   = last.pct >= 50 ? 'rgba(34,197,94,0.12)' : last.pct >= 20 ? 'rgba(245,158,11,0.12)' : 'rgba(239,68,68,0.12)';

  const y50 = H - (50 / 100) * H;
  const y20 = H - (20 / 100) * H;

  return (
    <svg viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none"
      style={{ width: '100%', height: '44px', display: 'block', margin: '8px 0 4px' }}>
      {/* threshold lines */}
      <line x1="0" y1={y50} x2={W} y2={y50} stroke="var(--td-border)" strokeWidth="0.75" strokeDasharray="3,3" />
      <line x1="0" y1={y20} x2={W} y2={y20} stroke="var(--td-danger)" strokeWidth="0.75" strokeDasharray="3,3" opacity="0.4" />
      {/* area fill */}
      <polygon points={areaPts} fill={fillColor} />
      {/* line */}
      <polyline points={linePts} fill="none" stroke={strokeColor} strokeWidth="1.5" strokeLinejoin="round" />
      {/* latest dot */}
      <circle cx={px(last)} cy={py(last)} r="2.5" fill={strokeColor} />
    </svg>
  );
}

function RepeaterGrid({
  radioData,
  radioError,
  batteryHistory,
}: {
  radioData: RadioStatsData | null;
  radioError: boolean;
  batteryHistory: Record<string, HistorySample[]>;
}) {
  if (radioError) return <div className="td-error">Repeater data unavailable</div>;
  if (!radioData) return <div className="td-loading">Loading repeaters…</div>;

  const monitors = [...radioData.monitors].sort((a, b) => {
    const ao = isOnline(a), bo = isOnline(b);
    if (ao !== bo) return ao ? -1 : 1;
    return (b.lastSuccessAt ?? '').localeCompare(a.lastSuccessAt ?? '');
  });

  return (
    <div className="td-repeater-grid">
      {monitors.map(monitor => {
        const online = isOnline(monitor);
        const sample = monitor.lastSample;
        const pct = sample?.batt_percent != null
          ? Math.max(0, Math.min(100, Math.round(sample.batt_percent)))
          : null;

        return (
          <div key={monitor.id} className={`td-repeater-card${online ? '' : ' td-repeater-card--offline'}`}>
            <div className="td-repeater-card__header">
              <div className={`td-repeater-card__dot td-repeater-card__dot--${online ? 'online' : 'offline'}`} />
              <div className="td-repeater-card__name">{monitor.nodeName}</div>
            </div>

            {pct != null && (
              <div className="td-batt-bar">
                <div className="td-batt-bar__fill" style={{ width: `${pct}%`, background: battBarColor(pct) }} />
              </div>
            )}

            <BatterySparkline samples={batteryHistory[monitor.id] ?? []} />

            <div className="td-repeater-card__rows">
              <div className="td-repeater-card__row">
                <span className="td-repeater-card__row-label">Battery</span>
                <span className={battClass(pct)}>{pct != null ? `${pct}%` : '—'}</span>
              </div>
              <div className="td-repeater-card__row">
                <span className="td-repeater-card__row-label">Last polled</span>
                <span className="td-muted">{timeAgo(monitor.lastSuccessAt)}</span>
              </div>
            </div>
          </div>
        );
      })}
    </div>
  );
}

function ObserverGrid({
  activity,
  mqttNodes,
  error,
}: {
  activity: ObserverActivity[] | null;
  mqttNodes: NodeStatusRow[] | null;
  error: boolean;
}) {
  if (error) return <div className="td-error">Observer data unavailable</div>;
  if (!activity) return <div className="td-loading">Loading observers…</div>;

  // Merge activity counts into the mqtt node list; fall back to activity-only rows
  const merged = activity.map(a => {
    const telemetry = mqttNodes?.find(n => n.node_id.toLowerCase() === a.node_id.toLowerCase());
    const lastSeen = telemetry?.time ?? null;
    const online = lastSeen ? (Date.now() - Date.parse(lastSeen)) < 10 * 60 * 1000 : false;
    return { ...a, uptime_secs: telemetry?.uptime_secs ?? null, lastSeen, online };
  }).sort((a, b) => (b.online ? 1 : 0) - (a.online ? 1 : 0) || b.rx_24h - a.rx_24h);

  return (
    <div className="td-repeater-grid">
      {merged.map(node => (
        <div key={node.node_id} className={`td-repeater-card${node.online ? '' : ' td-repeater-card--offline'}`}>
          <div className="td-repeater-card__header">
            <div className={`td-repeater-card__dot td-repeater-card__dot--${node.online ? 'online' : 'offline'}`} />
            <div className="td-repeater-card__name">{node.name ?? node.node_id.slice(0, 12)}</div>
          </div>
          <div className="td-repeater-card__rows">
            <div className="td-repeater-card__row">
              <span className="td-repeater-card__row-label">Received / 24h</span>
              <span>{node.rx_24h.toLocaleString()}</span>
            </div>
            <div className="td-repeater-card__row">
              <span className="td-repeater-card__row-label">Sent / 24h</span>
              <span>{node.tx_24h.toLocaleString()}</span>
            </div>
            {node.uptime_secs != null && (
              <div className="td-repeater-card__row">
                <span className="td-repeater-card__row-label">Uptime</span>
                <span>{fmtUptime(node.uptime_secs)}</span>
              </div>
            )}
            <div className="td-repeater-card__row">
              <span className="td-repeater-card__row-label">Last seen</span>
              <span className="td-muted">{timeAgo(node.lastSeen)}</span>
            </div>
          </div>
        </div>
      ))}
    </div>
  );
}

function PacketFeed({
  data,
  error,
  nodeNames,
}: {
  data: RecentPacket[] | null;
  error: boolean;
  nodeNames: Map<string, string>;
}) {
  if (error) return <div className="td-error">Packet feed unavailable</div>;
  if (!data) return <div className="td-loading">Loading packets…</div>;
  if (data.length === 0) return <div className="td-muted" style={{ padding: '12px 0' }}>No recent packets</div>;

  function resolveName(id?: string | null): string {
    if (!id) return '—';
    return nodeNames.get(id.toLowerCase()) ?? id.slice(0, 8);
  }

  function resolveObservers(p: RecentPacket): string {
    const ids = p.observer_node_ids?.length ? p.observer_node_ids : (p.rx_node_id ? [p.rx_node_id] : []);
    if (ids.length === 0) return '—';
    return ids.map(id => resolveName(id)).join(', ');
  }

  function packetContent(p: RecentPacket): string | null {
    const text = p.summary ?? extractPacketSummary(p.payload ?? undefined);
    if (!text) return null;
    if (text.includes('🚫')) return '[redacted]';
    return text;
  }

  return (
    <table className="td-packet-table">
      <thead>
        <tr>
          <th>Time</th>
          <th>Source</th>
          <th>Heard by</th>
          <th>Type</th>
          <th>Hops</th>
          <th>Content</th>
        </tr>
      </thead>
      <tbody>
        {data.map((p, i) => {
          const content = packetContent(p);
          return (
            <tr key={p.packet_hash ?? i}>
              <td className="td-muted">{timeAgo(p.time)}</td>
              <td>{resolveName(p.src_node_id)}</td>
              <td>{resolveObservers(p)}</td>
              <td>{p.packet_type != null ? (TYPE_LABELS[p.packet_type] ?? String(p.packet_type)) : '—'}</td>
              <td>{p.hop_count ?? '—'}</td>
              <td className={content ? 'td-packet-content' : 'td-muted'}>{content ?? '—'}</td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

// ── Main component ────────────────────────────────────────────────────────────

export const TeessieDashboard: React.FC = () => {
  const [lastUpdated, setLastUpdated] = useState<Date>(new Date());
  const tickRef = useRef<ReturnType<typeof setInterval> | null>(null);

  useEffect(() => {
    tickRef.current = setInterval(() => setLastUpdated(new Date()), 20_000);
    // Swap favicon to Teesside amber version
    const link = document.querySelector<HTMLLinkElement>('link[rel~="icon"]');
    if (link) {
      link.href = '/favicon-teesside.svg';
    }
    document.title = 'Teesside Mesh · MME';
    return () => { if (tickRef.current) clearInterval(tickRef.current); };
  }, []);

  const { data: connectivity, error: connError } =
    usePolled<ConnectivityData>('/api/cross-network-connectivity', 60_000);

  const { data: statsRaw, error: statsError } =
    usePolled<Record<string, number>>('/api/stats?network=teesside', 60_000);

  const statsData: StatsData | null = statsRaw
    ? {
        mqttNodes:  statsRaw['mqttNodes']  ?? 0,
        nodesDay:   statsRaw['nodesDay']   ?? 0,
        packetsDay: statsRaw['packetsDay'] ?? 0,
      }
    : null;

  const { data: radioData, error: radioError } =
    usePolled<RadioStatsData>('/api/radio-stats', 30_000);

  // Battery history for sparklines — fetched once per monitor, refreshed every 30 min
  const [batteryHistory, setBatteryHistory] = useState<Record<string, HistorySample[]>>({});
  const monitorIds = radioData?.monitors.map(m => m.id).join(',') ?? '';
  useEffect(() => {
    if (!radioData?.monitors.length) return;
    const fetchAll = async () => {
      const results: Record<string, HistorySample[]> = {};
      await Promise.all(radioData.monitors.map(async (m) => {
        try {
          const res = await fetch(`/api/radio-history?target=${encodeURIComponent(m.nodeName)}&limit=168`);
          if (res.ok) {
            const data = await res.json() as { samples?: HistorySample[] };
            results[m.id] = data.samples ?? [];
          }
        } catch { /* ignore */ }
      }));
      setBatteryHistory(results);
    };
    void fetchAll();
    const id = setInterval(() => void fetchAll(), 30 * 60 * 1000);
    return () => clearInterval(id);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [monitorIds]);

  // MQTT nodes polled to supplement repeater cards with channel utilization
  const { data: mqttNodes } =
    usePolled<NodeStatusRow[]>('/api/node-status/latest?network=teesside', 30_000);

  // All known nodes (broader than mqttNodes) used to resolve names in packet feed
  const { data: allNodes } =
    usePolled<{ node_id: string; name: string | null }[]>('/api/nodes?network=teesside', 120_000);

  const nodeNames = React.useMemo(() => {
    const map = new Map<string, string>();
    for (const n of allNodes ?? []) {
      if (n.name) map.set(n.node_id.toLowerCase(), n.name);
    }
    return map;
  }, [allNodes]);

  const { data: observerActivity, error: observerError } =
    usePolled<ObserverActivity[]>('/api/observer-activity?network=teesside', 60_000);

  const { data: packets, error: packetError } =
    usePolled<RecentPacket[]>('/api/packets/recent?limit=15&network=teesside', 20_000);

  return (
    <div className="teesside-dashboard">
      <header className="td-header">
        <h1 className="td-header__wordmark">Teesside Mesh</h1>
        <span className="td-header__badge">MME</span>
      </header>

      <section className="td-section">
        <p className="td-section__title">UK Mesh Connectivity</p>
        <ConnectivityBanner data={connectivity} error={connError} />
      </section>

      <section className="td-section">
        <p className="td-section__title">Network Overview</p>
        <StatsRow data={statsData} error={statsError} />
      </section>

      <section className="td-section">
        <p className="td-section__title">Repeater Status</p>
        <RepeaterGrid radioData={radioData} radioError={radioError} batteryHistory={batteryHistory} />
      </section>

      <section className="td-section">
        <p className="td-section__title">MQTT Observers</p>
        <ObserverGrid activity={observerActivity} mqttNodes={mqttNodes} error={observerError} />
      </section>

      <section className="td-section">
        <p className="td-section__title">Recent Packets</p>
        <PacketFeed data={packets} error={packetError} nodeNames={nodeNames} />
      </section>

      <section className="td-section">
        <p className="td-section__title">Network Coverage</p>
        <div className="td-local-grid">
          <div className="td-local-card">
            <p className="td-local-card__heading">Coverage area</p>
            <p className="td-local-card__body">
              Middlesbrough · Stockton-on-Tees · Hartlepool · Redcar &amp; Cleveland ·
              Darlington corridor. The network sits between the Cleveland Hills to the
              south and the North Sea coast to the east, using the Tees Valley as a
              natural RF corridor.
            </p>
          </div>
          <div className="td-local-card">
            <p className="td-local-card__heading">Key sites</p>
            <div className="td-local-sites">
              <div className="td-local-site">
                <span className="td-local-site__name">Lordstones-RPT</span>
                <span className="td-local-site__detail">Cleveland Hills · ~380 m ASL</span>
                <span className="td-local-site__note">Elevated above the Tees plain — best LOS node on the network</span>
              </div>
              <div className="td-local-site">
                <span className="td-local-site__name">Hartlepool-RPT</span>
                <span className="td-local-site__detail">North Sea coast · TS24–TS26</span>
                <span className="td-local-site__note">Eastern coastal reach toward the Tees estuary and Seal Sands</span>
              </div>
            </div>
          </div>
          <div className="td-local-card">
            <p className="td-local-card__heading">About MME</p>
            <p className="td-local-card__body">
              MME is the IATA code for{' '}
              <span className="td-local-highlight">Teesside International Airport</span>{' '}
              near Darlington — used as this network's callsign. The airport sits at the
              southern edge of the Tees plain, roughly midpoint between the Cleveland Hills
              and the coast.
            </p>
          </div>
          <div className="td-local-card">
            <p className="td-local-card__heading">RF geography</p>
            <p className="td-local-card__body">
              Roseberry Topping (320 m), the Cleveland Hills escarpment, and the flat
              industrial lowlands around the Tees estuary combine to give the network
              a mix of high-gain hilltop coverage and urban valley fill.
              The TS17 node at Stockton provides deep residential coverage where
              the hilltop sites have ground clutter.
            </p>
          </div>
        </div>
      </section>

      <footer className="td-footer">
        Teesside Mesh Network · MME · Updated {lastUpdated.toLocaleTimeString()}
      </footer>
    </div>
  );
};
