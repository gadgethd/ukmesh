import type { ChartData } from '../../features/stats/model.js';
import { C_CYAN, C_GREEN, EmptyPacketState, StatCard, formatCount } from './StatsPrimitives.js';

export function StatsSignal({ data }: { data: ChartData }) {
  return (
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
  );
}
