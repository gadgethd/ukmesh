import type { Request, Response, Router } from 'express';
import { createOwnerRepository } from '../../owner/ownerRepository.js';
import { createOwnerService } from '../../owner/ownerService.js';
import type { OwnerSession } from '../../owner/ownerSession.js';

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
  query: QueryFn;
};

export function registerOwnerRoutes(router: Router, deps: OwnerRouteDeps): void {
  const repository = createOwnerRepository({
    query: deps.query,
  });

  const service = createOwnerService({
    ownerLiveCacheTtlMs: deps.ownerLiveCacheTtlMs,
    ownerLiveCache: deps.ownerLiveCache,
    verifyMqttCredentials: deps.verifyMqttCredentials,
    resolveOwnerNodeIds: deps.resolveOwnerNodeIds,
    autoLinkOwnerNodeIds: deps.autoLinkOwnerNodeIds,
    buildOwnerDashboard: deps.buildOwnerDashboard,
    repository,
  });

  router.post('/owner/login', deps.ownerLoginLimiter, async (req, res) => {
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

      const { dashboard, nodeIds } = await service.authenticateOwner(mqttUsername, mqttPassword);
      const token = deps.encryptOwnerSession({
        nodeIds,
        exp: Date.now() + deps.ownerSessionTtlMs,
        mqttUsername,
      });
      res.cookie(deps.ownerCookieName, token, {
        httpOnly: true,
        secure: deps.isSecureRequest(req),
        sameSite: 'lax',
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
      const session = deps.getOwnerSession(req);
      if (!session) {
        res.clearCookie(deps.ownerCookieName, { path: '/' });
        res.status(401).json({ error: 'Not logged in' });
        return;
      }

      const { dashboard } = await service.getSessionDashboard(session);
      res.json({ ok: true, dashboard, mqttUsername: session.mqttUsername ?? null });
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

  router.post('/owner/logout', async (_req, res) => {
    res.clearCookie(deps.ownerCookieName, { path: '/' });
    res.json({ ok: true });
  });
}
