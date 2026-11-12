import { withDeadline } from '../platform/deadline.js';
import type { MessagePublisher } from './mqttPublisher.js';

/** A destination configuration change retires the previous connection before replacement. */
export function createPublisherCache(closeTimeoutMs: number, cleanupLabel: string) {
  let closed = false;
  const publishers = new Map<string, { publisher: MessagePublisher; fingerprint: string }>();
  const pending = new Map<string, Promise<unknown>>();
  const close = (publisher: MessagePublisher) => withDeadline(
    publisher.close(), closeTimeoutMs, cleanupLabel,
  ).catch(() => undefined);

  function serialize<T>(key: string, work: () => Promise<T>): Promise<T> {
    const previous = pending.get(key) ?? Promise.resolve();
    const current = previous.catch(() => undefined).then(work);
    pending.set(key, current);
    const finished = () => { if (pending.get(key) === current) pending.delete(key); };
    void current.then(finished, finished);
    return current;
  }

  async function discardCurrent(key: string): Promise<void> {
    const entry = publishers.get(key);
    if (!entry) return;
    publishers.delete(key);
    await close(entry.publisher);
  }

  return {
    get(key: string, fingerprint: string, create: () => MessagePublisher): Promise<MessagePublisher> {
      return serialize(key, async () => {
        if (closed) throw new Error('FORWARDING_PUBLISHERS_CLOSED');
        const existing = publishers.get(key);
        if (existing?.fingerprint === fingerprint) return existing.publisher;
        await discardCurrent(key);
        if (closed) throw new Error('FORWARDING_PUBLISHERS_CLOSED');
        const publisher = create();
        publishers.set(key, { publisher, fingerprint });
        return publisher;
      });
    },
    discard: (key: string) => serialize(key, () => discardCurrent(key)),
    async close() {
      closed = true;
      await Promise.allSettled([...pending.values()]);
      const current = [...publishers.values()];
      publishers.clear();
      await Promise.all(current.map(({ publisher }) => close(publisher)));
    },
  };
}
