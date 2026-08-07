import React, { useEffect, useState } from 'react';
import { useSearchParams } from 'react-router';
import {
  AreaChart, Area, BarChart, Bar,
  PieChart, Pie, Cell, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer,
} from 'recharts';
import { LoadingIndicator } from '../components/LoadingIndicator.js';
import {
  C_AMBER,
  C_CYAN,
  C_GREEN,
  C_ORANGE,
  C_PURPLE,
  LABEL_COLOR,
  PIE_COLORS,
  STATS_TABS,
  ChartCard,
  CustomTooltip,
  EmptyPacketState,
  StatCard,
  axisProps,
  channelLabel,
  describeSeries,
  formatCount,
  formatRatio,
  formatTimeAgo,
  fmtTrafficPct,
  gridProps,
  isStatsTabId,
  presentDecodedPath,
  type StatsTabId,
} from '../components/stats/StatsPrimitives.js';
import {
  StatsDecodedPathDialog,
  type DecodedPathSelection,
} from '../components/stats/StatsDecodedPathDialog.js';
import { Tab, TabList, TabPanel, Tabs } from '../components/ui/Tabs.js';
import { useStatsPageData } from '../hooks/useStatsPageData.js';
import { useWatchlist } from '../hooks/useWatchlist.js';
import { isStatsPayloadEmpty } from './statsState.js';
import './path-modal.css';
import './stats-page.css';
import { fmtAxisDay, fmtAxisTime } from './statsTimeFormat.js';

// ── Main page ─────────────────────────────────────────────────────────────────
export const StatsPage: React.FC = () => {
  const watchlist = useWatchlist();
  const { data, loading, refreshing, loadError, lastUpdatedAt, reload } = useStatsPageData();
  const [searchParams, setSearchParams] = useSearchParams();
  const [selectedDecodedPath, setSelectedDecodedPath] = useState<DecodedPathSelection | null>(null);
  const requestedTab = searchParams.get('tab');
  const requestedRegion = searchParams.get('region')?.trim().toUpperCase() ?? '';
  const activeTab: StatsTabId = isStatsTabId(requestedTab) ? requestedTab : 'overview';

  const setActiveTab = (tab: StatsTabId) => {
    const next = new URLSearchParams(searchParams);
    if (tab === 'overview') {
      next.delete('tab');
    } else {
      next.set('tab', tab);
    }
    setSearchParams(next, { replace: true });
  };
  const {
    nodes: decodedPathNodes,
    summary: decodedPathSummary,
  } = presentDecodedPath(
    data?.pathHashes.latestFullyDecodedNodes ?? [],
    data?.pathHashes.latestFullyDecodedPath ?? null,
  );
  const {
    nodes: longestDecodedPathNodes,
    summary: longestDecodedPathSummary,
  } = presentDecodedPath(
    data?.pathHashes.longestFullyDecodedNodes ?? [],
    data?.pathHashes.longestFullyDecodedPath ?? null,
  );
  const channelTraffic = data?.channelTraffic ?? [];
  const maxChannelTraffic = Math.max(1, ...channelTraffic.map((channel) => channel.count));
  const routeTypes = data?.routeTypes ?? [];
  const maxRouteTypeCount = Math.max(1, ...routeTypes.map((route) => route.count));
  const transportCodes = data?.transportCodes ?? [];
  const maxTransportCodeCount = Math.max(1, ...transportCodes.map((code) => code.count));
  const dataIsEmpty = data ? isStatsPayloadEmpty(data) : false;

  useEffect(() => {
    if (activeTab !== 'observers' || !requestedRegion || !data) return;
    const frame = window.requestAnimationFrame(() => {
      const card = document.getElementById(`observer-region-${requestedRegion}`);
      card?.scrollIntoView({ behavior: 'smooth', block: 'center' });
      card?.focus({ preventScroll: true });
    });
    return () => window.cancelAnimationFrame(frame);
  }, [activeTab, data, requestedRegion]);

  return (
    <div className="site-layout__inner">
      {/* ── Page hero ─────────────────────────────────────────────────────── */}

      <div className="site-content">

        {loading && (
          <div className="stats-page__loading-shell" aria-label="Loading stats">
            <LoadingIndicator label="Loading stats..." variant="block" />
            <div className="stats-page__summary">
              {Array.from({ length: 6 }, (_, index) => <div key={index} className="stats-page__stat skeleton-shimmer" />)}
            </div>
            <div className="stats-page__row">
              <div className="stats-page__chart skeleton-shimmer" />
              <div className="stats-page__chart skeleton-shimmer" />
            </div>
          </div>
        )}

        {!loading && !data && (
          <div className="stats-page__request-state" role={loadError ? 'alert' : 'status'}>
            <strong>{loadError ? 'Stats are unavailable' : 'No stats response was returned'}</strong>
            <p>{loadError ?? 'Try loading the current network scope again.'}</p>
            <button type="button" onClick={() => { void reload(); }}>Retry</button>
          </div>
        )}

        {data && (
          <div className="stats-page__refresh-state" role={loadError ? 'alert' : 'status'}>
            <span>
              {loadError
                ? `${loadError}. Showing the last successful result.`
                : refreshing
                  ? 'Refreshing stats…'
                  : `Last updated ${lastUpdatedAt ? new Date(lastUpdatedAt).toLocaleString() : 'unknown'}`}
            </span>
            <button type="button" disabled={refreshing} onClick={() => { void reload(); }}>
              {refreshing ? 'Refreshing…' : 'Refresh'}
            </button>
          </div>
        )}

        {data && dataIsEmpty && (
          <div className="stats-page__request-state" role="status">
            <strong>No stats are available for this scope</strong>
            <p>No packet, radio, or observer records were found in the current reporting window.</p>
            <button type="button" onClick={() => { void reload(); }}>Retry</button>
          </div>
        )}

        {data && !dataIsEmpty && (
          <Tabs
            selectedKey={activeTab}
            onSelectionChange={(key) => setActiveTab(key as StatsTabId)}
            className="stats-page__tab-system"
          >
            <TabList className="stats-page__tabs" aria-label="Stats sections">
              {STATS_TABS.map((tab) => (
                <Tab
                  key={tab.id}
                  id={tab.id}
                  className={({ isSelected }) => (
                    `stats-page__tab${isSelected ? ' stats-page__tab--active' : ''}`
                  )}
                >
                  {tab.label}
                </Tab>
              ))}
            </TabList>
            <TabPanel id={activeTab} className="stats-page__tabpanel">

            {activeTab === 'overview' && (
              <>
                <div className="stats-page__summary">
                  <StatCard label="Observed packets (24h)" value={formatCount(data.summary.totalPackets24h)} />
                  <StatCard label="Observed packets (7D)" value={formatCount(data.summary.totalPackets7d)} />
                  <StatCard label="Radios heard (24h)" value={formatCount(data.summary.uniqueRadios24h)} color={C_GREEN} />
                  <StatCard
                    label="Peak hour"
                    value={data.summary.peakHour ?? '—'}
                    sub={data.summary.peakHour ? `${formatCount(data.summary.peakHourCount)} packets` : undefined}
                    color={C_AMBER}
                  />
                  <StatCard
                    label="Avg observers"
                    value={data.observerDiversity.averageObserversPerPacket.toFixed(2)}
                    sub="per packet · 24h"
                    color={C_PURPLE}
                  />
                  <StatCard
                    label="Single-observer packets"
                    value={fmtTrafficPct(data.observerDiversity.singleObserverPct24h)}
                    sub={`${formatCount(data.observerDiversity.singleObserverPackets24h)} packets`}
                    color={C_ORANGE}
                  />
                </div>

                <div className="stats-page__row">
                  <ChartCard
                    title="Observed packets per hour"
                    sub="rolling 1h window · last 24 hours"
                    summary={describeSeries(data.packetsPerHour, (row) => fmtAxisTime(row.hour), (row) => row.count, 'packets')}
                  >
                    <ResponsiveContainer width="100%" height={220}>
                      <AreaChart data={data.packetsPerHour} margin={{ top: 8, right: 8, left: -20, bottom: 0 }}>
                        <defs>
                          <linearGradient id="gCyan" x1="0" y1="0" x2="0" y2="1">
                            <stop offset="5%" stopColor={C_CYAN} stopOpacity={0.3} />
                            <stop offset="95%" stopColor={C_CYAN} stopOpacity={0} />
                          </linearGradient>
                        </defs>
                        <CartesianGrid {...gridProps} />
                        <XAxis dataKey="hour" {...axisProps} interval="preserveStartEnd" tickFormatter={fmtAxisTime} />
                        <YAxis {...axisProps} />
                        <Tooltip content={<CustomTooltip />} labelFormatter={fmtAxisTime} />
                        <Area type="monotone" dataKey="count" name="Packets" stroke={C_CYAN} fill="url(#gCyan)" strokeWidth={2} dot={false} />
                      </AreaChart>
                    </ResponsiveContainer>
                  </ChartCard>

                  <ChartCard
                    title="Observed packets per day"
                    sub="last 7 days"
                    summary={describeSeries(data.packetsPerDay, (row) => fmtAxisDay(row.day), (row) => row.count, 'packets')}
                  >
                    <ResponsiveContainer width="100%" height={220}>
                      <BarChart data={data.packetsPerDay} margin={{ top: 8, right: 8, left: -20, bottom: 0 }}>
                        <CartesianGrid {...gridProps} />
                        <XAxis dataKey="day" {...axisProps} tickFormatter={fmtAxisDay} />
                        <YAxis {...axisProps} />
                        <Tooltip content={<CustomTooltip />} labelFormatter={fmtAxisDay} />
                        <Bar dataKey="count" name="Packets" fill={C_CYAN} fillOpacity={0.8} radius={[3, 3, 0, 0]} />
                      </BarChart>
                    </ResponsiveContainer>
                  </ChartCard>
                </div>

                <div className="stats-page__row">
                  <ChartCard
                    title="Unique radios heard per hour"
                    sub="distinct transmitting nodes · last 24 hours"
                    summary={describeSeries(data.radiosPerHour, (row) => fmtAxisTime(row.hour), (row) => row.count, 'radios')}
                  >
                    <ResponsiveContainer width="100%" height={220}>
                      <AreaChart data={data.radiosPerHour} margin={{ top: 8, right: 8, left: -20, bottom: 0 }}>
                        <defs>
                          <linearGradient id="gGreen" x1="0" y1="0" x2="0" y2="1">
                            <stop offset="5%" stopColor={C_GREEN} stopOpacity={0.3} />
                            <stop offset="95%" stopColor={C_GREEN} stopOpacity={0} />
                          </linearGradient>
                        </defs>
                        <CartesianGrid {...gridProps} />
                        <XAxis dataKey="hour" {...axisProps} interval="preserveStartEnd" tickFormatter={fmtAxisTime} />
                        <YAxis {...axisProps} allowDecimals={false} />
                        <Tooltip content={<CustomTooltip />} labelFormatter={fmtAxisTime} />
                        <Area type="monotone" dataKey="count" name="Radios" stroke={C_GREEN} fill="url(#gGreen)" strokeWidth={2} dot={false} />
                      </AreaChart>
                    </ResponsiveContainer>
                  </ChartCard>

                  <ChartCard
                    title="Unique radios heard per day"
                    sub="distinct transmitting nodes · last 7 days"
                    summary={describeSeries(data.radiosPerDay, (row) => fmtAxisDay(row.day), (row) => row.count, 'radios')}
                  >
                    <ResponsiveContainer width="100%" height={220}>
                      <BarChart data={data.radiosPerDay} margin={{ top: 8, right: 8, left: -20, bottom: 0 }}>
                        <CartesianGrid {...gridProps} />
                        <XAxis dataKey="day" {...axisProps} tickFormatter={fmtAxisDay} />
                        <YAxis {...axisProps} allowDecimals={false} />
                        <Tooltip content={<CustomTooltip />} labelFormatter={fmtAxisDay} />
                        <Bar dataKey="count" name="Radios" fill={C_GREEN} fillOpacity={0.8} radius={[3, 3, 0, 0]} />
                      </BarChart>
                    </ResponsiveContainer>
                  </ChartCard>
                </div>
              </>
            )}

            {activeTab === 'traffic' && (
              <>
                <div className="stats-page__row">
                  <ChartCard
                    title="Packet types"
                    sub="last 24 hours · all observer hits"
                    summary={describeSeries(data.packetTypes, (row) => row.label, (row) => row.count, 'packets')}
                  >
                    {data.packetTypes.length > 0 ? (
                      <div className="stats-page__pie-layout">
                        <ResponsiveContainer width="100%" height={220}>
                          <PieChart>
                            <Pie
                              data={data.packetTypes}
                              dataKey="count"
                              nameKey="label"
                              cx="50%" cy="50%"
                              innerRadius={55} outerRadius={85}
                              paddingAngle={2}
                            >
                              {data.packetTypes.map((_, i) => (
                                <Cell key={i} fill={PIE_COLORS[i % PIE_COLORS.length]} />
                              ))}
                            </Pie>
                            <Tooltip content={<CustomTooltip />} />
                          </PieChart>
                        </ResponsiveContainer>
                        <div className="stats-page__pie-legend">
                          {data.packetTypes.map((t, i) => (
                            <div key={i} className="stats-page__pie-item">
                              <span className="stats-page__pie-dot" style={{ background: PIE_COLORS[i % PIE_COLORS.length] }} />
                              <span className="stats-page__pie-label">{t.label}</span>
                              <span className="stats-page__pie-count">{formatCount(t.count)}</span>
                            </div>
                          ))}
                        </div>
                      </div>
                    ) : (
                      <EmptyPacketState />
                    )}
                  </ChartCard>

                  <ChartCard
                    title="Route types"
                    sub="last 24 hours"
                    summary={describeSeries(data.routeTypes, (row) => row.label, (row) => row.count, 'packets')}
                  >
                    {routeTypes.length > 0 ? (
                      <div className="stats-page__channel-traffic stats-page__channel-traffic--stacked">
                        {routeTypes.map((route, i) => (
                          <div key={route.routeType} className="stats-page__channel-row">
                            <div className="stats-page__channel-head">
                              <span className="stats-page__channel-name">
                                <span className="stats-page__pie-dot" style={{ background: PIE_COLORS[i % PIE_COLORS.length] }} />
                                {route.label}
                              </span>
                              <span className="stats-page__channel-pct">{formatCount(route.count)}</span>
                            </div>
                            <div className="stats-page__channel-track" aria-hidden="true">
                              <span
                                className="stats-page__channel-fill"
                                style={{
                                  width: `${Math.max(3, (route.count / maxRouteTypeCount) * 100)}%`,
                                  background: PIE_COLORS[i % PIE_COLORS.length],
                                }}
                              />
                            </div>
                            <div className="stats-page__channel-meta">
                              <span>{route.description}</span>
                              <span>raw {route.routeType}</span>
                            </div>
                          </div>
                        ))}
                      </div>
                    ) : (
                      <EmptyPacketState />
                    )}
                  </ChartCard>
                </div>

                <div className="stats-page__observer-section">
                  <div className="stats-page__chart-header">
                    <span className="stats-page__chart-title">Channel traffic</span>
                    <span className="stats-page__chart-sub">GroupText packets in the last 24 hours</span>
                  </div>
                  {channelTraffic.length > 0 ? (
                    <div className="stats-page__channel-traffic">
                      {channelTraffic.map((channel, i) => (
                        <div key={channel.channel} className="stats-page__channel-row">
                          <div className="stats-page__channel-head">
                            <span className="stats-page__channel-name">
                              <span className="stats-page__pie-dot" style={{ background: PIE_COLORS[i % PIE_COLORS.length] }} />
                              {channelLabel(channel.channel)}
                            </span>
                            <span className="stats-page__channel-pct">{fmtTrafficPct(channel.pct)}</span>
                          </div>
                          <div className="stats-page__channel-track" aria-hidden="true">
                            <span
                              className="stats-page__channel-fill"
                              style={{
                                width: `${Math.max(3, (channel.count / maxChannelTraffic) * 100)}%`,
                                background: PIE_COLORS[i % PIE_COLORS.length],
                              }}
                            />
                          </div>
                          <div className="stats-page__channel-meta">
                            <span>{formatCount(channel.count)} observed packets</span>
                            <span>{fmtTrafficPct(channel.allPct)} of all packet traffic</span>
                          </div>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <EmptyPacketState />
                  )}
                </div>

                <div className="stats-page__observer-section">
                  <div className="stats-page__chart-header">
                    <span className="stats-page__chart-title">Transport codes</span>
                    <span className="stats-page__chart-sub">decoded from packet route metadata · last 24 hours</span>
                  </div>
                  {transportCodes.length > 0 ? (
                    <div className="stats-page__channel-traffic">
                      {transportCodes.map((code, i) => (
                        <div key={code.raw} className="stats-page__channel-row">
                          <div className="stats-page__channel-head">
                            <span className="stats-page__channel-name">
                              <span className="stats-page__pie-dot" style={{ background: PIE_COLORS[i % PIE_COLORS.length] }} />
                              {code.label}
                            </span>
                            <span className="stats-page__channel-pct">{formatCount(code.count)}</span>
                          </div>
                          <div className="stats-page__channel-track" aria-hidden="true">
                            <span
                              className="stats-page__channel-fill"
                              style={{
                                width: `${Math.max(3, (code.count / maxTransportCodeCount) * 100)}%`,
                                background: PIE_COLORS[i % PIE_COLORS.length],
                              }}
                            />
                          </div>
                          <div className="stats-page__channel-meta">
                            <span>{code.description}</span>
                            <span>raw {code.raw}</span>
                          </div>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <EmptyPacketState label="No transport-code packet data in this window." />
                  )}
                </div>
              </>
            )}

            {activeTab === 'observers' && (
              <>
                <div className="stats-page__summary">
                  <StatCard label="Avg observers per packet" value={data.observerDiversity.averageObserversPerPacket.toFixed(2)} color={C_PURPLE} />
                  <StatCard label="Max observers on one packet" value={formatCount(data.observerDiversity.maxObserversPerPacket)} color={C_GREEN} />
                  <StatCard label="Unique packets measured" value={formatCount(data.observerDiversity.totalPackets24h)} />
                  <StatCard label="Single-observer packets" value={formatCount(data.observerDiversity.singleObserverPackets24h)} color={C_ORANGE} />
                  <StatCard label="Single-observer share" value={fmtTrafficPct(data.observerDiversity.singleObserverPct24h)} color={C_AMBER} />
                </div>

                <div className="stats-page__observer-section">
                  <div className="stats-page__chart-header">
                    <span className="stats-page__chart-title">Observer regions</span>
                    <span className="stats-page__chart-sub">sorted by observed packets over the last 7 days</span>
                  </div>
                  {data.observerRegions.length > 0 ? (
                    <div className="stats-page__observer-grid">
                      {data.observerRegions.map((region) => (
                        <div
                          key={region.iata}
                          id={`observer-region-${region.iata}`}
                          className="stats-page__observer-card"
                          tabIndex={-1}
                        >
                          <div className="stats-page__observer-card-head">
                            <span className="stats-page__observer-iata">{region.iata}</span>
                            <span className={`stats-page__health stats-page__health--${region.health?.status ?? 'poor'}`} title="Weighted from ingest freshness, active observers, packet volume, and observer diversity">
                              {region.health?.score ?? 0}% {region.health?.status ?? 'poor'}
                            </span>
                            <span className="stats-page__observer-last">last packet {formatTimeAgo(region.lastPacketAt)}</span>
                          </div>
                          <div className="stats-page__observer-watch">
                            <button type="button" onClick={() => watchlist.toggle('region', region.iata, `${region.iata} region`)}>{watchlist.isWatched('region', region.iata) ? '★ Region' : '☆ Region'}</button>
                            <button type="button" onClick={() => watchlist.toggle('observer', region.iata, `${region.iata} observers`)}>{watchlist.isWatched('observer', region.iata) ? '★ Observers' : '☆ Observers'}</button>
                          </div>
                          <div className="stats-page__observer-metrics">
                            <div className="stats-page__observer-metric">
                              <span>Packets (7D)</span>
                              <strong>{formatCount(region.packets7d)}</strong>
                            </div>
                            <div className="stats-page__observer-metric">
                              <span>Packets (24h)</span>
                              <strong>{formatCount(region.packets24h)}</strong>
                            </div>
                            <div className="stats-page__observer-metric">
                              <span>Observers</span>
                              <strong>{formatCount(region.activeObservers)}|{formatCount(region.observers)}</strong>
                            </div>
                          </div>
                          <div className="stats-page__observer-chart">
                            <ResponsiveContainer width="100%" height={90}>
                              <AreaChart data={region.series} margin={{ top: 4, right: 0, left: 0, bottom: 0 }}>
                                <defs>
                                  <linearGradient id={`gObserver-${region.iata}`} x1="0" y1="0" x2="0" y2="1">
                                    <stop offset="5%" stopColor={C_CYAN} stopOpacity={0.28} />
                                    <stop offset="95%" stopColor={C_CYAN} stopOpacity={0} />
                                  </linearGradient>
                                </defs>
                                <CartesianGrid vertical={false} {...gridProps} />
                                <XAxis dataKey="day" hide />
                                <YAxis hide />
                                <Tooltip content={<CustomTooltip />} />
                                <Area
                                  type="monotone"
                                  dataKey="count"
                                  name="Packets"
                                  stroke={C_CYAN}
                                  fill={`url(#gObserver-${region.iata})`}
                                  strokeWidth={2}
                                  dot={false}
                                />
                              </AreaChart>
                            </ResponsiveContainer>
                          </div>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <EmptyPacketState />
                  )}
                </div>
              </>
            )}

            {activeTab === 'paths' && (
              <>
                <div className="stats-page__observer-section">
                  <div className="stats-page__chart-header">
                    <span className="stats-page__chart-title">Path hashes</span>
                    <span className="stats-page__chart-sub">observed hop widths across the last 24 hours</span>
                  </div>
                  <div className="site-stats-grid site-stats-grid--4 health-system-grid">
                    <div className="site-stat">
                      <span className="site-stat__value">{formatCount(data.pathHashes.last24hHops.one_byte)}</span>
                      <span className="site-stat__label">1-byte Hops (24h)</span>
                    </div>
                    <div className="site-stat">
                      <span className="site-stat__value">{formatCount(data.pathHashes.last24hHops.two_byte)}</span>
                      <span className="site-stat__label">2-byte Hops (24h)</span>
                    </div>
                    <div className="site-stat">
                      <span className="site-stat__value">{formatCount(data.pathHashes.last24hHops.three_byte)}</span>
                      <span className="site-stat__label">3-byte Hops (24h)</span>
                    </div>
                    <div className="site-stat">
                      <span className="site-stat__value">{formatCount(data.pathHashes.multibytePackets24h)}</span>
                      <span className="site-stat__label">Multibyte Packets (24h)</span>
                    </div>
                    <div className="site-stat">
                      <span className="site-stat__value">{formatCount(data.pathHashes.fullyDecodedMultibyte24h)}</span>
                      <span className="site-stat__label">Fully Decoded (24h)</span>
                      <span className="site-stat__sub">{formatRatio(data.pathHashes.fullyDecodedMultibyte24h, data.pathHashes.multibytePackets24h)} of multibyte packets</span>
                    </div>
                  </div>
                  <div className="health-meta">
                    <div className="health-kv">
                      <span>Latest Multibyte Packet</span>
                      <strong>
                        {data.pathHashes.latestMultibyteHash
                          ? `${data.pathHashes.latestMultibyteHash} · ${formatTimeAgo(data.pathHashes.latestMultibyteAt)}`
                          : 'not seen yet'}
                      </strong>
                    </div>
                    <div className="health-kv">
                      <span>Last Fully Decoded Packet</span>
                      <strong>
                        {data.pathHashes.latestFullyDecodedHash
                          ? `${data.pathHashes.latestFullyDecodedHash} · ${data.pathHashes.latestFullyDecodedHops ?? 0} hops · ${formatTimeAgo(data.pathHashes.latestFullyDecodedAt)}`
                          : 'not decoded yet'}
                      </strong>
                    </div>
                    <div className="health-kv">
                      <span>Decoded Path</span>
                      {decodedPathNodes.length > 1 ? (
                        <button
                          type="button"
                          className="stats-page__path-link"
                          onClick={() => setSelectedDecodedPath({
                            title: 'Last Fully Decoded Path',
                            hash: data.pathHashes.latestFullyDecodedHash,
                            hops: data.pathHashes.latestFullyDecodedHops,
                            nodes: data.pathHashes.latestFullyDecodedNodes,
                          })}
                        >
                          {decodedPathSummary}
                        </button>
                      ) : (
                        <strong>{decodedPathSummary}</strong>
                      )}
                    </div>
                    <div className="health-kv">
                      <span>Longest Decoded Path</span>
                      {longestDecodedPathNodes.length > 1 ? (
                        <button
                          type="button"
                          className="stats-page__path-link"
                          onClick={() => setSelectedDecodedPath({
                            title: 'Longest Fully Decoded Path',
                            hash: data.pathHashes.longestFullyDecodedHash,
                            hops: data.pathHashes.longestFullyDecodedHops,
                            nodes: data.pathHashes.longestFullyDecodedNodes,
                          })}
                        >
                          {`${longestDecodedPathSummary} · ${data.pathHashes.longestFullyDecodedHops ?? 0} hops`}
                        </button>
                      ) : (
                        <strong>
                          {data.pathHashes.longestFullyDecodedHash
                            ? `${longestDecodedPathSummary} · ${data.pathHashes.longestFullyDecodedHops ?? 0} hops`
                            : 'not decoded yet'}
                        </strong>
                      )}
                    </div>
                  </div>
                </div>

                <div className="stats-page__row">
                  <ChartCard
                    title="Hop count distribution"
                    sub="last 7 days · all observer hits"
                    summary={describeSeries(data.hopDistribution, (row) => `${row.hops} hops`, (row) => row.count, 'packets')}
                  >
                    <ResponsiveContainer width="100%" height={220}>
                      <BarChart data={data.hopDistribution} margin={{ top: 8, right: 8, left: -20, bottom: 0 }}>
                        <CartesianGrid {...gridProps} />
                        <XAxis dataKey="hops" {...axisProps} label={{ value: 'hops', position: 'insideBottom', offset: -2, fill: LABEL_COLOR, fontSize: 10 }} />
                        <YAxis {...axisProps} />
                        <Tooltip content={<CustomTooltip labelSuffix=" hops" />} />
                        <Bar dataKey="count" name="Packets" fill={C_AMBER} fillOpacity={0.8} radius={[3, 3, 0, 0]} />
                      </BarChart>
                    </ResponsiveContainer>
                  </ChartCard>

                  <ChartCard
                    title="Multibyte path-hash trend"
                    sub="packet-inferred path hashes · last 7 days"
                    summary={describeSeries(data.pathDecodeTrend, (row) => fmtAxisDay(row.day), (row) => row.multibyte, 'multibyte paths')}
                  >
                    <ResponsiveContainer width="100%" height={220}>
                      <BarChart data={data.pathDecodeTrend} margin={{ top: 8, right: 8, left: -20, bottom: 0 }}>
                        <CartesianGrid {...gridProps} />
                        <XAxis dataKey="day" {...axisProps} tickFormatter={fmtAxisDay} />
                        <YAxis {...axisProps} />
                        <Tooltip content={<CustomTooltip />} labelFormatter={fmtAxisDay} />
                        <Bar dataKey="multibyte" name="Multibyte" fill={C_CYAN} fillOpacity={0.75} radius={[3, 3, 0, 0]} />
                      </BarChart>
                    </ResponsiveContainer>
                  </ChartCard>
                </div>

                <div className="stats-page__row">
                  <ChartCard
                    title="Repeated observed path hashes"
                    sub="Top 10 path-hash values over the last 7 days"
                    summary={describeSeries(data.prefixCollisions, (row) => row.prefix, (row) => row.repeats, 'repeats')}
                    tall
                  >
                    <ResponsiveContainer width="100%" height={220}>
                      <BarChart data={data.prefixCollisions} margin={{ top: 8, right: 8, left: -20, bottom: 0 }}>
                        <CartesianGrid {...gridProps} />
                        <XAxis dataKey="prefix" {...axisProps} />
                        <YAxis {...axisProps} allowDecimals={false} />
                        <Tooltip content={<CustomTooltip />} formatter={(value: number) => [value, 'Repeats']} />
                        <Bar dataKey="repeats" name="Repeats" fill={C_PURPLE} fillOpacity={0.8} radius={[3, 3, 0, 0]} />
                      </BarChart>
                    </ResponsiveContainer>
                  </ChartCard>
                </div>
              </>
            )}

            {activeTab === 'signal' && (
              <>
                <div className="stats-page__summary">
                  <StatCard
                    label="Mean RSSI"
                    value={data.signalSummary.avgRssi == null ? '—' : `${data.signalSummary.avgRssi.toFixed(1)} dBm`}
                    color={C_CYAN}
                  />
                  <StatCard
                    label="Mean SNR"
                    value={data.signalSummary.avgSnr == null ? '—' : `${data.signalSummary.avgSnr.toFixed(1)} dB`}
                    color={C_GREEN}
                  />
                  <StatCard label="RSSI samples" value={formatCount(data.signalSummary.rssiSamples24h)} sub="last 24 hours" />
                  <StatCard label="SNR samples" value={formatCount(data.signalSummary.snrSamples24h)} sub="last 24 hours" />
                </div>
                {data.signalSummary.rssiSamples24h < 1 && data.signalSummary.snrSamples24h < 1 && (
                  <EmptyPacketState label="No RSSI or SNR packet data in this window." />
                )}
              </>
            )}
            </TabPanel>
          </Tabs>
        )}

        <StatsDecodedPathDialog
          selection={selectedDecodedPath}
          onClose={() => setSelectedDecodedPath(null)}
        />
      </div>
    </div>
  );
};
