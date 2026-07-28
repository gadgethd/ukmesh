import type { Request, Response, Router } from 'express';
import { createOwnerRepository } from '../../owner/ownerRepository.js';
import { createOwnerService } from '../../owner/ownerService.js';
import type { OwnerSession } from '../../owner/ownerSession.js';
import { createCsrfToken, readCookie, requireDoubleSubmitCsrf } from '../../security/operatorAuth.js';

type OwnerDashboard = {
  totals: {
    ownedNodes: number;
  };
};

type OwnerLiveCacheEntry = {
  ts: number;
  data: unknown;
};

type VerifyMqttCredentialsFn = (mqttUsername: string, mqttPassword: string) => Promise<boolean>;
type ResolveOwnerNodeIdsFn = (mqttUsername: string) => Promise<string[]>;
type BuildOwnerDashboardFn = (nodeIds: string[]) => Promise<OwnerDashboard & Record<string, unknown>>;
type EncryptOwnerSessionFn = (payload: OwnerSession) => string;
type IsSecureRequestFn = (req: Request) => boolean;
type GetOwnerSessionFn = (req: Request) => OwnerSession | null;
type RequireOwnerSessionFn = (req: Request, res: Response) => Promise<string[] | null>;
type QueryFn = <T extends import('pg').QueryResultRow = import('pg').QueryResultRow>(text: string, params?: unknown[]) => Promise<{ rows: T[] }>;

type OwnerRouteDeps = {
  ownerCookieName: string;
  ownerLiveCacheTtlMs: number;
  ownerLiveCache: Map<string, OwnerLiveCacheEntry>;
  ownerDashboardCacheTtlMs: number;
  ownerLastHopCacheTtlMs: number;
  ownerSessionTtlMs: number;
  mqttUsernameMaxLen: number;
  mqttPasswordMaxLen: number;
  ownerLoginLimiter: ReturnType<typeof import('express-rate-limit').rateLimit>;
  hasControlChars: (value: string) => boolean;
  verifyMqttCredentials: VerifyMqttCredentialsFn;
  resolveOwnerNodeIds: ResolveOwnerNodeIdsFn;
  autoLinkOwnerNodeIds: ResolveOwnerNodeIdsFn;
  buildOwnerDashboard: BuildOwnerDashboardFn;
  encryptOwnerSession: EncryptOwnerSessionFn;
  isSecureRequest: IsSecureRequestFn;
  getOwnerSession: GetOwnerSessionFn;
  requireOwnerSession: RequireOwnerSessionFn;
  invalidateOwnerNodeIdCache: (mqttUsername: string) => void;
  query: QueryFn;
};

export function registerOwnerRoutes(router: Router, deps: OwnerRouteDeps): void {
  const repository = createOwnerRepository({
    query: deps.query,
  });

  const service = createOwnerService({
    ownerLiveCacheTtlMs: deps.ownerLiveCacheTtlMs,
    ownerLiveCache: deps.ownerLiveCache,
    ownerDashboardCacheTtlMs: deps.ownerDashboardCacheTtlMs,
    ownerLastHopCacheTtlMs: deps.ownerLastHopCacheTtlMs,
    verifyMqttCredentials: deps.verifyMqttCredentials,
    resolveOwnerNodeIds: deps.resolveOwnerNodeIds,
    autoLinkOwnerNodeIds: deps.autoLinkOwnerNodeIds,
    buildOwnerDashboard: deps.buildOwnerDashboard,
    repository,
    invalidateOwnerNodeIdCache: deps.invalidateOwnerNodeIdCache,
  });

  const csrfCookieName = 'meshcore_owner_csrf';
  const csrfProtection = requireDoubleSubmitCsrf(csrfCookieName);
  const setCsrfCookie = (req: Request, res: Response): string => {
    const token = readCookie(req.headers.cookie, csrfCookieName) ?? createCsrfToken();
    res.cookie(csrfCookieName, token, {
      httpOnly: false,
      secure: deps.isSecureRequest(req),
      sameSite: 'strict',
      path: '/',
      maxAge: deps.ownerSessionTtlMs,
    });
    return token;
  };

  router.get('/owner/csrf', (req, res) => {
    res.setHeader('Cache-Control', 'no-store');
    res.json({ csrfToken: setCsrfCookie(req, res) });
  });

  router.post('/owner/login', deps.ownerLoginLimiter, csrfProtection, async (req, res) => {
    try {
      const body = req.body as { mqttUsername?: string; mqttPassword?: string } | undefined;
      const mqttUsername = String(body?.mqttUsername ?? '').trim();
      const mqttPassword = String(body?.mqttPassword ?? '').trim();
      if (!mqttUsername || !mqttPassword) {
        res.status(400).json({ error: 'Missing MQTT username or password' });
        return;
      }
      if (mqttUsername.length > deps.mqttUsernameMaxLen || mqttPassword.length > deps.mqttPasswordMaxLen) {
        res.status(400).json({ error: 'MQTT username or password is too long' });
        return;
      }
      if (deps.hasControlChars(mqttUsername) || deps.hasControlChars(mqttPassword)) {
        res.status(400).json({ error: 'MQTT username or password contains invalid characters' });
        return;
      }
      if (!/^[a-zA-Z0-9_\-.@]+$/.test(mqttUsername)) {
        res.status(400).json({ error: 'Invalid MQTT username format' });
        return;
      }

      const { dashboard } = await service.authenticateOwner(mqttUsername, mqttPassword);
      const token = deps.encryptOwnerSession({
        v: 2,
        exp: Date.now() + deps.ownerSessionTtlMs,
        mqttUsername,
      });
      res.cookie(deps.ownerCookieName, token, {
        httpOnly: true,
        secure: deps.isSecureRequest(req),
        sameSite: 'strict',
        path: '/',
        maxAge: deps.ownerSessionTtlMs,
      });
      res.json({ ok: true, dashboard });
    } catch (err) {
      if ((err as Error).message === 'INVALID_MQTT_CREDENTIALS') {
        res.status(403).json({ error: 'Invalid MQTT credentials' });
        return;
      }
      if ((err as Error).message === 'NO_ACTIVE_OWNER_NODE') {
        res.status(403).json({ error: 'No active node found for this MQTT username yet' });
        return;
      }
      console.error('[api] POST /owner/login', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.get('/owner/session', async (req, res) => {
    try {
      setCsrfCookie(req, res);
      const session = deps.getOwnerSession(req);
      if (!session) {
        res.clearCookie(deps.ownerCookieName, { path: '/' });
        res.status(401).json({ error: 'Not logged in' });
        return;
      }

      const { dashboard } = await service.getSessionDashboard(session);
      if (session.legacy) {
        res.cookie(deps.ownerCookieName, deps.encryptOwnerSession({
          v: 2,
          mqttUsername: session.mqttUsername,
          exp: session.exp,
        }), {
          httpOnly: true,
          secure: deps.isSecureRequest(req),
          sameSite: 'strict',
          path: '/',
          maxAge: Math.max(0, session.exp - Date.now()),
        });
      }
      res.json({ ok: true, dashboard, mqttUsername: session.mqttUsername });
    } catch (err) {
      if ((err as Error).message === 'NO_ACTIVE_OWNER_NODE') {
        res.clearCookie(deps.ownerCookieName, { path: '/' });
        res.status(401).json({ error: 'No active node found for this MQTT username yet' });
        return;
      }
      console.error('[api] GET /owner/session', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.get('/owner/live', async (req, res) => {
    try {
      const ownedNodeIds = await deps.requireOwnerSession(req, res);
      if (!ownedNodeIds) return;
      const requestedNodeId = String(req.query['nodeId'] ?? '').trim().toUpperCase() || undefined;
      res.json(await service.getOwnerLiveData(ownedNodeIds, requestedNodeId));
    } catch (err) {
      const message = (err as Error).message;
      if (message === 'NO_OWNED_NODES') {
        res.status(404).json({ error: 'No owned nodes found' });
        return;
      }
      if (message === 'NODE_NOT_OWNED') {
        res.status(403).json({ error: 'Node is not owned by this session' });
        return;
      }
      if (message === 'OWNER_NODE_NOT_FOUND') {
        res.status(404).json({ error: 'Owner node not found' });
        return;
      }
      console.error('[api] GET /owner/live', message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.get('/owner/live-last-hop', async (req, res) => {
    try {
      const ownedNodeIds = await deps.requireOwnerSession(req, res);
      if (!ownedNodeIds) return;
      const requestedNodeId = String(req.query['nodeId'] ?? '').trim().toUpperCase() || undefined;
      res.json(await service.getOwnerLastHopStrength(ownedNodeIds, requestedNodeId));
    } catch (err) {
      const message = (err as Error).message;
      if (message === 'NO_OWNED_NODES') {
        res.status(404).json({ error: 'No owned nodes found' });
        return;
      }
      if (message === 'NODE_NOT_OWNED') {
        res.status(403).json({ error: 'Node is not owned by this session' });
        return;
      }
      console.error('[api] GET /owner/live-last-hop', message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.get('/owner/alert-rules', async (req, res) => {
    const ownedNodeIds = await deps.requireOwnerSession(req, res);
    if (!ownedNodeIds) return;
    const session = deps.getOwnerSession(req);
    const result = await deps.query(
      `SELECT id::text, node_id, rule_type, threshold, channels, enabled, last_triggered_at::text
       FROM owner_alert_rules
       WHERE owner_username = $1 AND node_id = ANY($2::text[])
       ORDER BY created_at`,
      [session!.mqttUsername, ownedNodeIds],
    );
    res.json(result.rows);
  });

  router.post('/owner/alert-rules', csrfProtection, async (req, res) => {
    const ownedNodeIds = await deps.requireOwnerSession(req, res);
    if (!ownedNodeIds) return;
    const session = deps.getOwnerSession(req);
    const body = req.body as { nodeId?: string; ruleType?: string; threshold?: number; webhook?: string; enabled?: boolean };
    const nodeId = String(body.nodeId ?? '').trim().toUpperCase();
    const ruleType = String(body.ruleType ?? '');
    const threshold = Number(body.threshold);
    const webhook = String(body.webhook ?? '').trim();
    if (!ownedNodeIds.includes(nodeId) || !['offline_minutes', 'battery_below_mv', 'link_loss_above_db'].includes(ruleType) || !Number.isFinite(threshold) || threshold <= 0) {
      res.status(400).json({ error: 'Invalid owner alert rule' });
      return;
    }
    if (webhook && !/^https:\/\//i.test(webhook)) {
      res.status(400).json({ error: 'Webhook must use HTTPS' });
      return;
    }
    const result = await deps.query(
      `INSERT INTO owner_alert_rules (owner_username, node_id, rule_type, threshold, channels, enabled)
       VALUES ($1, $2, $3, $4, $5::jsonb, $6)
       ON CONFLICT (owner_username, node_id, rule_type) DO UPDATE SET
         threshold = EXCLUDED.threshold, channels = EXCLUDED.channels,
         enabled = EXCLUDED.enabled, updated_at = NOW()
       RETURNING id::text, node_id, rule_type, threshold, channels, enabled`,
      [session!.mqttUsername, nodeId, ruleType, threshold, JSON.stringify(webhook ? { webhook } : {}), body.enabled !== false],
    );
    res.status(201).json(result.rows[0]);
  });

  router.delete('/owner/alert-rules/:id', csrfProtection, async (req, res) => {
    const ownedNodeIds = await deps.requireOwnerSession(req, res);
    if (!ownedNodeIds) return;
    const session = deps.getOwnerSession(req);
    await deps.query(
      'DELETE FROM owner_alert_rules WHERE id = $1 AND owner_username = $2 AND node_id = ANY($3::text[])',
      [String(req.params['id'] ?? ''), session!.mqttUsername, ownedNodeIds],
    );
    res.status(204).end();
  });

  router.post('/owner/logout', csrfProtection, async (req, res) => {
    const session = deps.getOwnerSession(req);
    if (session) service.clearOwnerSession(session.mqttUsername);
    res.clearCookie(deps.ownerCookieName, {
      path: '/',
      secure: deps.isSecureRequest(req),
      sameSite: 'strict',
    });
    res.clearCookie(csrfCookieName, {
      path: '/',
      secure: deps.isSecureRequest(req),
      sameSite: 'strict',
    });
    res.json({ ok: true });
  });
}
