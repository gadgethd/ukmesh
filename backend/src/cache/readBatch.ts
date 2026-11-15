type PendingRead<T> = {
  value: Promise<T>;
  reject: (reason: unknown) => void;
  timer: ReturnType<typeof setTimeout>;
};
const MAX_PENDING_READ_KEYS = 64;
const READ_ADMISSION_MS = 4;

/** Share only reads not yet dispatched. The projection defines caller ownership:
 * clone mutable values, or retain an immutable representation. */
export function createReadBatcher<T>(copy: (value: T) => T): {
  run: (key: string, load: () => Promise<T>) => Promise<T>;
  close: () => void;
} {
  const pending = new Map<string, PendingRead<T>>();
  let closed = false;
  return {
    run(key, load) {
      if (closed) return Promise.reject(new Error('READ_BATCH_CLOSED'));
      const existing = pending.get(key);
      if (existing) return existing.value.then(copy);
      if (pending.size >= MAX_PENDING_READ_KEYS) {
        try { return load().then(copy); }
        catch (error) { return Promise.reject(error); }
      }
      let resolve!: (value: T | PromiseLike<T>) => void;
      let reject!: (reason: unknown) => void;
      const value = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
      const entry: PendingRead<T> = {
        value,
        reject,
        // Nominal admission delay; event-loop delay remains caller latency.
        timer: setTimeout(() => {
          if (pending.get(key) !== entry) return;
          // Later arrivals must get new SQL, even while this load is unresolved.
          pending.delete(key);
          try { resolve(load()); }
          catch (error) { reject(error); }
        }, READ_ADMISSION_MS),
      };
      pending.set(key, entry);
      return value.then(copy);
    },
    close() {
      closed = true;
      for (const entry of pending.values()) {
        clearTimeout(entry.timer);
        entry.reject(new Error('READ_BATCH_CLOSED'));
      }
      pending.clear();
    },
  };
}
