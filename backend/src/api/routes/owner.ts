import type { Request, Response, Router } from 'express';
import { createOwnerRepository } from '../../owner/ownerRepository.js';
import { createOwnerService } from '../../owner/ownerService.js';
import type { OwnerSession } from '../../owner/ownerSession.js';
import { createCsrfToken, readCookie, requireDoubleSubmitCsrf } from '../../security/operatorAuth.js';
import { resolveWebhookTarget } from '../../security/outboundWebhook.js';
import {
  deliverDueOwnerAlerts,
  queueOwnerTestDelivery,
} from '../../owner/alertRules.js';
import { parseBoundedString } from '../utils/input.js';
import {
  deleteOwnerAlertRule,
  ownerAlertDeliveryRows,
  ownerAlertRuleRows,
  upsertOwnerAlertRule,
} from '../../repositories/ownerAlerts.js';

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
    const requestedNodeId = parseBoundedString(req.query['nodeId'], {
      name: 'nodeId',
      maxLength: 64,
      pattern: /^[0-9a-fA-F]{64}$/,
    })?.toUpperCase();
    try {
      const ownedNodeIds = await deps.requireOwnerSession(req, res);
      if (!ownedNodeIds) return;
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
    const requestedNodeId = parseBoundedString(req.query['nodeId'], {
      name: 'nodeId',
      maxLength: 64,
      pattern: /^[0-9a-fA-F]{64}$/,
    })?.toUpperCase();
    try {
      const ownedNodeIds = await deps.requireOwnerSession(req, res);
      if (!ownedNodeIds) return;
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
    try {
      const ownedNodeIds = await deps.requireOwnerSession(req, res);
      if (!ownedNodeIds) return;
      const session = deps.getOwnerSession(req);
      const result = await ownerAlertRuleRows(
        deps.query,
        session!.mqttUsername,
        ownedNodeIds,
      );
      res.setHeader('Cache-Control', 'no-store');
      res.json(result.rows.map(({ channels, ...rule }) => {
        const webhook = String(channels?.webhook ?? '').trim();
        return {
          ...rule,
          destination: webhook
            ? { configured: true, host: new URL(webhook).hostname.toLowerCase() }
            : { configured: false, host: null },
        };
      }));
    } catch (error) {
      console.error('[api] GET /owner/alert-rules', (error as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.post('/owner/alert-rules', csrfProtection, async (req, res) => {
    try {
      const ownedNodeIds = await deps.requireOwnerSession(req, res);
      if (!ownedNodeIds) return;
      const session = deps.getOwnerSession(req);
      const body = req.body as { nodeId?: string; ruleType?: string; threshold?: number; webhook?: string; enabled?: boolean };
      const nodeId = String(body.nodeId ?? '').trim().toUpperCase();
      const ruleType = String(body.ruleType ?? '');
      const threshold = Number(body.threshold);
      const webhook = String(body.webhook ?? '').trim();
      if (
        !ownedNodeIds.includes(nodeId)
        || !['offline_minutes', 'battery_below_mv', 'link_loss_above_db'].includes(ruleType)
        || !Number.isFinite(threshold)
        || threshold <= 0
        || threshold > 1_000_000
      ) {
        res.status(400).json({ error: 'Invalid owner alert rule' });
        return;
      }
      if (webhook) {
        try {
          await resolveWebhookTarget(webhook);
        } catch {
          res.status(400).json({ error: 'Webhook destination is not permitted' });
          return;
        }
      }
      const result = await upsertOwnerAlertRule(deps.query, {
        ownerUsername: session!.mqttUsername,
        nodeId,
        ruleType,
        threshold,
        webhook,
        enabled: body.enabled !== false,
      });
      res.status(201).json(result.rows[0]);
    } catch (error) {
      console.error('[api] POST /owner/alert-rules', (error as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.get('/owner/alert-deliveries', async (req, res) => {
    try {
      const ownedNodeIds = await deps.requireOwnerSession(req, res);
      if (!ownedNodeIds) return;
      const session = deps.getOwnerSession(req);
      const result = await ownerAlertDeliveryRows(
        deps.query,
        session!.mqttUsername,
        ownedNodeIds,
      );
      res.setHeader('Cache-Control', 'no-store');
      res.json({ deliveries: result.rows });
    } catch (error) {
      console.error('[api] GET /owner/alert-deliveries', (error as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.post('/owner/alert-rules/:id/test', csrfProtection, async (req, res) => {
    const ruleId = parseBoundedString(req.params['id'], {
      name: 'alert rule id',
      required: true,
      maxLength: 20,
      pattern: /^[1-9][0-9]{0,19}$/,
    })!;
    const idempotencyKey = parseBoundedString(req.headers['idempotency-key'], {
      name: 'Idempotency-Key',
      required: true,
      minLength: 16,
      maxLength: 128,
      pattern: /^[A-Za-z0-9._:-]+$/,
    })!;
    try {
      const ownedNodeIds = await deps.requireOwnerSession(req, res);
      if (!ownedNodeIds) return;
      const session = deps.getOwnerSession(req)!;
      const result = await queueOwnerTestDelivery({
        ruleId,
        ownerUsername: session.mqttUsername,
        ownedNodeIds,
        idempotencyKey,
      });
      void deliverDueOwnerAlerts().catch((error: Error) => {
        console.error('[owner-alerts] test delivery dispatch failed', error.message);
      });
      res.status(result.queued ? 202 : 200).json({
        status: result.queued ? 'queued' : 'already_queued',
        eventKey: result.eventKey,
      });
    } catch (error) {
      const message = (error as Error).message;
      if (message === 'OWNER_ALERT_RULE_NOT_FOUND') {
        res.status(404).json({ error: 'Alert rule not found' });
        return;
      }
      if (message === 'OWNER_ALERT_WEBHOOK_NOT_CONFIGURED') {
        res.status(409).json({ error: 'Configure a webhook before sending a test' });
        return;
      }
      console.error('[api] POST /owner/alert-rules/:id/test', message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.delete('/owner/alert-rules/:id', csrfProtection, async (req, res) => {
    const ruleId = parseBoundedString(req.params['id'], {
      name: 'alert rule id',
      required: true,
      maxLength: 20,
      pattern: /^[1-9][0-9]{0,19}$/,
    })!;
    try {
      const ownedNodeIds = await deps.requireOwnerSession(req, res);
      if (!ownedNodeIds) return;
      const session = deps.getOwnerSession(req);
      await deleteOwnerAlertRule(
        deps.query,
        ruleId,
        session!.mqttUsername,
        ownedNodeIds,
      );
      res.status(204).end();
    } catch (error) {
      console.error('[api] DELETE /owner/alert-rules/:id', (error as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
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
