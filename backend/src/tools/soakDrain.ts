type Totals = { packets: number; paths: number; facts: number; ledger: number };
type Completion = { before: Totals; expectedDeltas: Totals; count: number;
  metrics: () => Promise<ReadonlyMap<string, number>>; counts: () => Promise<Totals>; seenCount: () => number };
type Clock = { now: () => number; pause: (milliseconds: number) => Promise<void> };

/** SQL, post-persist publication and observed live delivery share one ACK deadline. */
export async function waitForSoakDrain(input: Completion, clock: Clock = {
  now: () => performance.now(), pause: milliseconds => new Promise(resolve => setTimeout(resolve, milliseconds)),
}): Promise<{ drained: boolean; drainElapsedMs: number }> {
  const started = clock.now();
  const deadline = started + 120_000;
  while (clock.now() < deadline) {
    const current = await input.metrics();
    const persisted = await input.counts();
    // Awaited probes can finish after the deadline even when a poll began before it.
    if (clock.now() > deadline) break;
    if ((Object.keys(input.expectedDeltas) as (keyof Totals)[]).every(key =>
      Number(persisted[key]) - Number(input.before[key]) === input.expectedDeltas[key])
      && current.get('meshcore_packet_batch_queue_depth') === 0
      && current.get('meshcore_mqtt_ingest_queue_depth') === 0 && current.get('meshcore_mqtt_ingest_active') === 0
      && current.get('meshcore_mqtt_post_persist_inflight') === 0 && input.seenCount() === input.count
      && clock.now() <= deadline) return { drained: true, drainElapsedMs: clock.now() - started };
    await clock.pause(Math.min(1000, Math.max(0, deadline - clock.now())));
  }
  return { drained: false, drainElapsedMs: clock.now() - started };
}
