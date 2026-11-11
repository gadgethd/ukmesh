/** Shared telemetry estimation for owner charts and the public node-status API. */

export const ESTIMATED_AIRTIME_SECONDS_PER_PUBLISH = 0.12;

export type AirUtilisationSample = {
  uptimeSecs: number | null;
  uptimeMs: number | null;
  txAirSecs: number | null;
  rxAirSecs: number | null;
  rxPublishCalls: number | null;
  txPublishCalls: number | null;
  channelUtilization: number | null;
  airUtilTx: number | null;
};

const clampPercent = (value: number): number => Math.min(100, Math.max(0, value));

/** Fill missing utilisation percentages from the cheapest available signal.
 *  Explicit samples always win; airtime counters are preferred over publish
 *  counts because they do not assume a fixed publish duration. */
export function estimateAirUtilisation(
  current: AirUtilisationSample,
  previous: AirUtilisationSample | null,
): { channelUtilPct: number | null; airUtilTxPct: number | null } {
  let channelUtilPct = current.channelUtilization;
  let airUtilTxPct = current.airUtilTx;

  if ((channelUtilPct == null || airUtilTxPct == null) && previous) {
    const uptimeDelta = (current.uptimeSecs ?? 0) - (previous.uptimeSecs ?? 0);
    const txDelta = (current.txAirSecs ?? 0) - (previous.txAirSecs ?? 0);
    const rxDelta = (current.rxAirSecs ?? 0) - (previous.rxAirSecs ?? 0);
    if (uptimeDelta > 0 && txDelta >= 0 && rxDelta >= 0) {
      if (airUtilTxPct == null) airUtilTxPct = clampPercent((txDelta / uptimeDelta) * 100);
      if (channelUtilPct == null) channelUtilPct = clampPercent(((txDelta + rxDelta) / uptimeDelta) * 100);
    }
  }

  if ((channelUtilPct == null || airUtilTxPct == null) && previous) {
    const currentUptimeMs = current.uptimeMs;
    const previousUptimeMs = previous.uptimeMs;
    const deltaUptimeSeconds =
      currentUptimeMs != null && previousUptimeMs != null && currentUptimeMs > previousUptimeMs
        ? (currentUptimeMs - previousUptimeMs) / 1000
        : 0;
    if (deltaUptimeSeconds > 0) {
      const deltaRxCalls = current.rxPublishCalls != null && previous.rxPublishCalls != null
        ? Math.max(0, current.rxPublishCalls - previous.rxPublishCalls)
        : 0;
      const deltaTxCalls = current.txPublishCalls != null && previous.txPublishCalls != null
        ? Math.max(0, current.txPublishCalls - previous.txPublishCalls)
        : 0;
      if (airUtilTxPct == null) {
        airUtilTxPct = clampPercent(
          (deltaTxCalls * ESTIMATED_AIRTIME_SECONDS_PER_PUBLISH / deltaUptimeSeconds) * 100,
        );
      }
      if (channelUtilPct == null) {
        channelUtilPct = clampPercent(
          ((deltaTxCalls + deltaRxCalls) * ESTIMATED_AIRTIME_SECONDS_PER_PUBLISH / deltaUptimeSeconds) * 100,
        );
      }
    }
  }

  return { channelUtilPct, airUtilTxPct };
}
