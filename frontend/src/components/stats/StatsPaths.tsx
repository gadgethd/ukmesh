import type { ChartData } from '../../features/stats/model.js';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import { chartHeader, C_AMBER, C_CYAN, C_PURPLE, LABEL_COLOR, ChartCard, CustomTooltip, axisProps, describeSeries, formatCount, formatRatio, formatTimeAgo, gridProps, presentDecodedPath } from './StatsPrimitives.js';
import { fmtAxisDay } from '../../features/stats/timeFormat.js';
import type { DecodedPathSelection } from './StatsDecodedPathDialog.js';

function pathStat(label: string, value: string, sub?: string) {
  return <div className="site-stat">
    <span className="site-stat__value">{value}</span>
    <span className="site-stat__label">{label}</span>
    {sub && <span className="site-stat__sub">{sub}</span>}
  </div>;
}

export function StatsPaths({ data, openPath }: { data: ChartData; openPath: (selection: DecodedPathSelection) => void }) {
  const { nodes: decodedPathNodes, summary: decodedPathSummary } = presentDecodedPath(
    data.pathHashes.latestFullyDecodedNodes, data.pathHashes.latestFullyDecodedPath);
  const { nodes: longestDecodedPathNodes, summary: longestDecodedPathSummary } = presentDecodedPath(
    data.pathHashes.longestFullyDecodedNodes, data.pathHashes.longestFullyDecodedPath);
  return (
             <>
                <div className="stats-page__observer-section">
                  {chartHeader("Path hashes", "observed hop widths across the last 24 hours")}
                  <div className="site-stats-grid site-stats-grid--4 health-system-grid">
                    {pathStat("1-byte Hops (24h)", formatCount(data.pathHashes.last24hHops.one_byte))}
                    {pathStat("2-byte Hops (24h)", formatCount(data.pathHashes.last24hHops.two_byte))}
                    {pathStat("3-byte Hops (24h)", formatCount(data.pathHashes.last24hHops.three_byte))}
                    {pathStat("Multibyte Packets (24h)", formatCount(data.pathHashes.multibytePackets24h))}
                    {pathStat("Fully Decoded (24h)", formatCount(data.pathHashes.fullyDecodedMultibyte24h), `${formatRatio(data.pathHashes.fullyDecodedMultibyte24h, data.pathHashes.multibytePackets24h)} of multibyte packets`)}
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
                          onClick={() => openPath({
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
                          onClick={() => openPath({
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
                        <Tooltip content={<CustomTooltip />} />
                        <Bar dataKey="repeats" name="Repeats" fill={C_PURPLE} fillOpacity={0.8} radius={[3, 3, 0, 0]} />
                      </BarChart>
                    </ResponsiveContainer>
                  </ChartCard>
                </div>
              </>
  );
}
