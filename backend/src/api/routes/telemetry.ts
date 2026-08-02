import type { Router } from 'express';
import type { QueryResultRow } from 'pg';
import { createHash, createHmac } from 'node:crypto';
import { TELEMETRY_LIMITER } from '../bootstrap/limiters.js';

type QueryFn = <T extends QueryResultRow = QueryResultRow>(
  text: string,
  params?: unknown[],
) => Promise<{ rows: T[] }>;

type TelemetryRouteDeps = {
  query: QueryFn;
};

export function registerTelemetryRoutes(router: Router, deps: TelemetryRouteDeps): void {
  const { query } = deps;

  router.post('/telemetry/frontend-error', TELEMETRY_LIMITER, async (req, res) => {
    try {
      const body = req.body as {
        kind?: string;
        message?: string;
        stack?: string;
        page?: string;
        userAgent?: string;
      };

      const message = String(body.message ?? '').replace(/\s+/g, ' ').trim().slice(0, 500);
      if (!message) {
        res.status(400).json({ error: 'Missing message' });
        return;
      }

      const ALLOWED_KINDS = new Set(['error', 'warning', 'unhandledrejection', 'crash']);
      const kind = ALLOWED_KINDS.has(String(body.kind)) ? String(body.kind) : 'error';
      const page = body.page ? String(body.page).slice(0, 300) : null;
      const stack = body.stack ? String(body.stack).slice(0, 2_000) : null;
      const normalized = `${kind}\n${message
        .replace(/\b0x[0-9a-f]+\b/gi, '<hex>')
        .replace(/\b\d{3,}\b/g, '<number>')}\n${stack?.split('\n')[0] ?? ''}\n${page ?? ''}`;
      const fingerprint = createHash('sha256').update(normalized).digest('hex');
      const sourceSecret = String(process.env['JWT_SECRET'] ?? '');
      if (sourceSecret.length < 32) throw new Error('TELEMETRY_SOURCE_SECRET_INVALID');
      const sourceHash = createHmac('sha256', sourceSecret)
        .update(String(req.ip || req.socket.remoteAddress || 'unknown'))
        .digest('hex');
      const bucketMs = 10 * 60_000;
      const bucketStart = new Date(Math.floor(Date.now() / bucketMs) * bucketMs);

      const contribution = await query<{ count: string }>(
        `SELECT COALESCE(SUM(occurrences), 0)::text AS count
           FROM frontend_error_events
          WHERE source_hash = $1 AND last_seen_at > NOW() - INTERVAL '1 hour'`,
        [sourceHash],
      );
      if (Number(contribution.rows[0]?.count ?? 0) >= 20) {
        res.status(202).json({ ok: true, sampled: true });
        return;
      }

      await query(
        `INSERT INTO frontend_error_events
           (kind, message, stack, page, user_agent, fingerprint, source_hash,
            bucket_start, occurrences, last_seen_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 1, NOW())
         ON CONFLICT (fingerprint, source_hash, bucket_start)
           WHERE fingerprint IS NOT NULL AND source_hash IS NOT NULL AND bucket_start IS NOT NULL
         DO UPDATE SET
           occurrences = LEAST(frontend_error_events.occurrences + 1, 20),
           last_seen_at = NOW()`,
        [
          kind,
          message,
          stack,
          page,
          body.userAgent ? String(body.userAgent).slice(0, 500) : null,
          fingerprint,
          sourceHash,
          bucketStart.toISOString(),
        ],
      );

      res.status(202).json({ ok: true });
    } catch (err) {
      console.error('[api] POST /telemetry/frontend-error', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });
}
