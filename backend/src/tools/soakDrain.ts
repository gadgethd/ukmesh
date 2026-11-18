type Completion = {
  count: number;
  metrics: () => Promise<ReadonlyMap<string, number>>;
  verifyCommit: () => Promise<boolean>;
  seenCount: () => number;
};
type Clock = { now: () => number; pause: (milliseconds: number) => Promise<void> };

/** SQL, post-persist publication and observed live delivery share one ACK deadline. */
export async function waitForSoakDrain(input: Completion, clock: Clock = {
  now: () => performance.now(), pause: milliseconds => new Promise(resolve => setTimeout(resolve, milliseconds)),
}): Promise<{ drained: boolean; drainElapsedMs: number }> {
  const started = clock.now();
  const deadline = started + 120_000;
  while (clock.now() < deadline) {
    const current = await input.metrics();
    if (clock.now() >= deadline) break;
    const queuesEmpty = current.get('meshcore_packet_batch_queue_depth') === 0
      && current.get('meshcore_mqtt_ingest_queue_depth') === 0 && current.get('meshcore_mqtt_ingest_active') === 0
      && current.get('meshcore_mqtt_post_persist_inflight') === 0;
    if (queuesEmpty && input.seenCount() === input.count) {
      // Run the exact workset check once cheaper signals show delivery is caught up.
      const committed = await input.verifyCommit();
      if (clock.now() > deadline) break;
      if (committed) return { drained: true, drainElapsedMs: clock.now() - started };
    }
    await clock.pause(Math.min(1000, Math.max(0, deadline - clock.now())));
  }
  return { drained: false, drainElapsedMs: clock.now() - started };
}
