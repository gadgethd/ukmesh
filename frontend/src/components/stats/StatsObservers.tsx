import type { ChartData } from '../../features/stats/model.js';
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import { chartHeader, C_AMBER, C_CYAN, C_GREEN, C_ORANGE, C_PURPLE, CustomTooltip, EmptyPacketState, StatCard, formatCount, formatTimeAgo, fmtTrafficPct, gridProps } from './StatsPrimitives.js';
import { useWatchlist } from '../../hooks/useWatchlist.js';
import { mergeObserverRegions, type LiveObserverSnapshot } from '../../features/stats/state.js';

export function StatsObservers({ data, liveObservers, observerError }: { data: ChartData; liveObservers: LiveObserverSnapshot | null; observerError: string | null }) {
  const watchlist = useWatchlist();
  const observerRegions = mergeObserverRegions(data.observerRegions, liveObservers);
  return (
             <>
                <p role={observerError ? 'alert' : 'status'}>
                  {observerError ?? (liveObservers
                    ? `Observer activity updated ${formatTimeAgo(liveObservers.generatedAt)} · active in the last ${liveObservers.windowMinutes} minutes. Historical totals and charts use the timestamp above.`
                    : 'Loading current observer activity…')}
                </p>
                <div className="stats-page__summary">
                  <StatCard label="Avg observers per packet" value={data.observerDiversity.averageObserversPerPacket.toFixed(2)} color={C_PURPLE} />
                  <StatCard label="Max observers on one packet" value={formatCount(data.observerDiversity.maxObserversPerPacket)} color={C_GREEN} />
                  <StatCard label="Unique packets measured" value={formatCount(data.observerDiversity.totalPackets24h)} />
                  <StatCard label="Single-observer packets" value={formatCount(data.observerDiversity.singleObserverPackets24h)} color={C_ORANGE} />
                  <StatCard label="Single-observer share" value={fmtTrafficPct(data.observerDiversity.singleObserverPct24h)} color={C_AMBER} />
                </div>

                <div className="stats-page__observer-section">
                  {chartHeader("Observer regions", "sorted by observed packets over the last 7 days")}
                  {observerRegions.length > 0 ? (
                    <div className="stats-page__observer-grid">
                      {observerRegions.map((region) => (
                        <div
                          key={region.iata}
                          id={`observer-region-${region.iata}`}
                          className="stats-page__observer-card"
                          tabIndex={-1}
                        >
                          <div className="stats-page__observer-card-head">
                            <span className="stats-page__observer-iata">{region.iata}</span>
                            <span className={`stats-page__health stats-page__health--${liveObservers ? (region.activeObservers > 0 ? 'healthy' : 'watch') : region.health?.status ?? 'poor'}`} title={liveObservers ? 'Public packet reception in the last 15 minutes' : 'Health in the historical chart snapshot'}>
                              {liveObservers ? (region.activeObservers > 0 ? 'Receiving packets' : 'No recent packets')
                                : `${region.health?.score ?? 0}% ${region.health?.status ?? 'poor'} (snapshot)`}
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
                              <strong>{region.historyAvailable ? formatCount(region.packets7d) : '—'}</strong>
                            </div>
                            <div className="stats-page__observer-metric">
                              <span>Packets (24h)</span>
                              <strong>{region.historyAvailable ? formatCount(region.packets24h) : '—'}</strong>
                            </div>
                            <div className="stats-page__observer-metric">
                              <span>Active observers / 7D snapshot</span>
                              <strong>{formatCount(region.activeObservers)} / {region.historyAvailable ? formatCount(region.observers) : '—'}</strong>
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
  );
}
