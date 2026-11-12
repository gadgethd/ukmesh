import type { Key } from 'react';
import type { ChartData } from '../../features/stats/model.js';
import { PieChart, Pie, Cell, Tooltip, ResponsiveContainer } from 'recharts';
import { PIE_COLORS, ChartCard, CustomTooltip, EmptyPacketState, channelLabel, describeSeries, formatCount, fmtTrafficPct } from './StatsPrimitives.js';

function trafficRow({ rowKey, index, label, count, maximum, value, description, detail }: {
  rowKey: Key; index: number; label: string; count: number; maximum: number;
  value: string; description: string; detail: string;
}) {
  const color = PIE_COLORS[index % PIE_COLORS.length];
  return <div key={rowKey} className="stats-page__channel-row">
    <div className="stats-page__channel-head">
      <span className="stats-page__channel-name">
        <span className="stats-page__pie-dot" style={{ background: color }} />{label}
      </span>
      <span className="stats-page__channel-pct">{value}</span>
    </div>
    <div className="stats-page__channel-track" aria-hidden="true">
      <span className="stats-page__channel-fill" style={{ width: `${Math.max(3, (count / maximum) * 100)}%`, background: color }} />
    </div>
    <div className="stats-page__channel-meta"><span>{description}</span><span>{detail}</span></div>
  </div>;
}

export function StatsTraffic({ data }: { data: ChartData }) {
  const channelTraffic = data.channelTraffic;
  const maxChannelTraffic = Math.max(1, ...channelTraffic.map(channel => channel.count));
  const routeTypes = data.routeTypes;
  const maxRouteTypeCount = Math.max(1, ...routeTypes.map(route => route.count));
  const transportCodes = data.transportCodes;
  const maxTransportCodeCount = Math.max(1, ...transportCodes.map(code => code.count));
  return (
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
                        {routeTypes.map((route, i) => trafficRow({
                          rowKey: route.routeType, index: i, label: route.label, count: route.count,
                          maximum: maxRouteTypeCount, value: formatCount(route.count),
                          description: route.description, detail: `raw ${route.routeType}`,
                        }))}
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
                      {channelTraffic.map((channel, i) => trafficRow({
                        rowKey: channel.channel, index: i, label: channelLabel(channel.channel), count: channel.count,
                        maximum: maxChannelTraffic, value: fmtTrafficPct(channel.pct),
                        description: `${formatCount(channel.count)} observed packets`, detail: `${fmtTrafficPct(channel.allPct)} of all packet traffic`,
                      }))}
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
                      {transportCodes.map((code, i) => trafficRow({
                        rowKey: code.raw, index: i, label: code.label, count: code.count,
                        maximum: maxTransportCodeCount, value: formatCount(code.count),
                        description: code.description, detail: `raw ${code.raw}`,
                      }))}
                    </div>
                  ) : (
                    <EmptyPacketState label="No transport-code packet data in this window." />
                  )}
                </div>
              </>
  );
}
