/**
 * Claim arbitration for the in-flight packet dedupe registry.
 *
 * Multiple copies of the same packet can arrive concurrently (broker
 * redeliveries, relay copies fanned across observers, several ingest
 * connections). Only one copy may be mid-persist at a time; everyone else
 * waits for its outcome:
 *
 *  - the attempt succeeds -> every waiter resolves as a duplicate (no write);
 *  - the attempt fails    -> waiters re-arbitrate. The re-check and the claim
 *    registration happen in the SAME microtask inside this function, so
 *    exactly one successor claims; every later waiter queues behind that
 *    successor rather than racing it. This closes the race where two waiters
 *    both fall through after a failure and persist the packet concurrently
 *    (double insert: packets carry no unique constraint).
 *
 * Contract: the caller passes the promise that will settle when its own
 * attempt settles (`claim`). On 'claimed', the caller MUST settle that
 * promise and remove it from the registry when done.
 */
export async function waitForInFlightPacket(
  registry: Map<string, Promise<void>>,
  dedupeKey: string,
  claim: Promise<void>,
): Promise<'claimed' | 'duplicate'> {
  for (;;) {
    const holder = registry.get(dedupeKey);
    if (!holder) {
      registry.set(dedupeKey, claim);
      return 'claimed';
    }
    const persisted = await holder.then(() => true, () => false);
    if (persisted) return 'duplicate';
  }
}
