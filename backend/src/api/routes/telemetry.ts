import type { Router } from 'express';
import type { QueryResultRow } from 'pg';

type QueryFn = <T extends QueryResultRow = QueryResultRow>(
  text: string,
  params?: unknown[],
) => Promise<{ rows: T[] }>;

type TelemetryRouteDeps = {
  query: QueryFn;
};

export function registerTelemetryRoutes(router: Router, deps: TelemetryRouteDeps): void {
  const { query } = deps;

  router.post('/telemetry/frontend-error', async (req, res) => {
    try {
      const body = req.body as {
        kind?: string;
        message?: string;
        stack?: string;
        page?: string;
        userAgent?: string;
      };

      const message = String(body.message ?? '').slice(0, 500);
      if (!message) {
        res.status(400).json({ error: 'Missing message' });
        return;
      }

      const ALLOWED_KINDS = new Set(['error', 'warning', 'unhandledrejection', 'crash']);
      const kind = ALLOWED_KINDS.has(String(body.kind)) ? String(body.kind) : 'error';

      await query(
        `INSERT INTO frontend_error_events (kind, message, stack, page, user_agent)
         VALUES ($1, $2, $3, $4, $5)`,
        [
          kind,
          message,
          body.stack ? String(body.stack).slice(0, 4000) : null,
          body.page ? String(body.page).slice(0, 300) : null,
          body.userAgent ? String(body.userAgent).slice(0, 500) : null,
        ],
      );

      res.json({ ok: true });
    } catch (err) {
      console.error('[api] POST /telemetry/frontend-error', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });
}
