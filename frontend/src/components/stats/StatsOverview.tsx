import type { ChartData } from '../../features/stats/model.js';
import { AreaChart, Area, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import { C_AMBER, C_CYAN, C_GREEN, C_ORANGE, C_PURPLE, ChartCard, CustomTooltip, StatCard, axisProps, describeSeries, formatCount, fmtTrafficPct, gridProps } from './StatsPrimitives.js';
import { fmtAxisDay, fmtAxisTime, fmtPeakHour } from '../../features/stats/timeFormat.js';

export function StatsOverview({ data }: { data: ChartData }) {
  return (
             <>
                <div className="stats-page__summary">
                  <StatCard label="Observed packets (24h)" value={formatCount(data.summary.totalPackets24h)} />
                  <StatCard label="Observed packets (7D)" value={formatCount(data.summary.totalPackets7d)} />
                  <StatCard label="Radios heard (24h)" value={formatCount(data.summary.uniqueRadios24h)} color={C_GREEN} />
                  <StatCard
                    label="Peak hour"
                    value={data.summary.peakHour ? fmtPeakHour(data.summary.peakHour) : '—'}
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
  );
}
