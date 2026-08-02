import { createHash, randomBytes, timingSafeEqual } from 'node:crypto';

export type OperatorSession = Readonly<{
  id: string;
  csrfToken: string;
  createdAt: number;
  expiresAt: number;
}>;

function digest(value: string): Buffer {
  return createHash('sha256').update(value, 'utf8').digest();
}

export class OperatorSessionStore {
  private sessions = new Map<string, OperatorSession>();

  constructor(
    private readonly ttlMs: number,
    private readonly capacity: number,
    private readonly now: () => number = Date.now,
  ) {}

  private pruneExpired(): void {
    const now = this.now();
    for (const [id, session] of this.sessions) {
      if (session.expiresAt <= now) this.sessions.delete(id);
    }
  }

  private makeRoom(): void {
    this.pruneExpired();
    while (this.sessions.size >= this.capacity) {
      const oldest = this.sessions.keys().next().value as string | undefined;
      if (!oldest) break;
      this.sessions.delete(oldest);
    }
  }

  create(): OperatorSession {
    this.makeRoom();
    const createdAt = this.now();
    const session = Object.freeze({
      id: randomBytes(32).toString('base64url'),
      csrfToken: randomBytes(32).toString('base64url'),
      createdAt,
      expiresAt: createdAt + this.ttlMs,
    });
    this.sessions.set(session.id, session);
    return session;
  }

  get(id: string | null | undefined): OperatorSession | null {
    if (!id || !/^[A-Za-z0-9_-]{43}$/.test(id)) return null;
    const session = this.sessions.get(id);
    if (!session) return null;
    if (session.expiresAt <= this.now()) {
      this.sessions.delete(id);
      return null;
    }
    return session;
  }

  delete(id: string | null | undefined): boolean {
    return id ? this.sessions.delete(id) : false;
  }

  verifyCsrf(session: OperatorSession, provided: unknown): boolean {
    if (typeof provided !== 'string' || !provided) return false;
    return timingSafeEqual(digest(session.csrfToken), digest(provided));
  }

  size(): number {
    this.pruneExpired();
    return this.sessions.size;
  }
}
