import { LAZY_SETTLE_MS } from '../../config/timing.js';

export function lazyPathSettleRemainingMs(packetTime: string, nowMs = Date.now()): number {
  const stamp = Date.parse(packetTime);
  const age = Number.isFinite(stamp) ? Math.max(0, nowMs - stamp) : LAZY_SETTLE_MS;
  return Math.max(0, LAZY_SETTLE_MS - age);
}
