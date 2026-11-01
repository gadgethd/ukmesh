import type { Redis } from 'ioredis';

export class RedisPublicationUnavailableError extends Error {
  constructor(readonly reason: 'offline' | 'full' | 'oversized') {
    super(`REDIS_PUBLICATION_${reason.toUpperCase()}`);
  }
}

/** Bound outstanding commands as well as ioredis's disconnected command queue. */
export class RedisPublicationGate {
  private pending = 0;
  private pendingBytes = 0;
  private readonly blocked = new WeakSet<object>();

  constructor(private readonly maxPending = 64, private readonly maxBytes = 8 * 1_048_576) {}

  async run<T>(client: Pick<Redis, 'status'> & Partial<Pick<Redis, 'once' | 'disconnect'>>, bytes: number, command: () => Promise<T>): Promise<T> {
    if (client.status !== 'ready' || this.blocked.has(client)) throw new RedisPublicationUnavailableError('offline');
    if (!Number.isSafeInteger(bytes) || bytes < 0 || bytes > this.maxBytes) {
      throw new RedisPublicationUnavailableError('oversized');
    }
    if (this.pending >= this.maxPending || this.pendingBytes + bytes > this.maxBytes) {
      throw new RedisPublicationUnavailableError('full');
    }
    this.pending += 1;
    this.pendingBytes += bytes;
    try {
      return await command();
    } catch (error) {
      // ioredis rejects a timed-out promise but retains its command until a
      // reply or reconnect. Close that stream before releasing admission so
      // repeated timeouts cannot accumulate settled commands in commandQueue.
      if (error instanceof Error && error.message === 'Command timed out'
        && client.once && client.disconnect && !this.blocked.has(client)) {
        this.blocked.add(client);
        client.once('ready', () => this.blocked.delete(client));
        client.disconnect(true);
      }
      throw error;
    } finally {
      this.pending -= 1;
      this.pendingBytes -= bytes;
    }
  }
}
