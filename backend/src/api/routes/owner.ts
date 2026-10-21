import type { Request, Response, Router } from 'express';
import { createOwnerRepository } from '../../owner/ownerRepository.js';
import { createOwnerService } from '../../owner/ownerService.js';
import type { OwnerSession } from '../../owner/ownerSession.js';
import { createCsrfToken, readCookie, requireDoubleSubmitCsrf } from '../../security/operatorAuth.js';
import { resolveWebhookTarget } from '../../security/outboundWebhook.js';
import {
  destinationConfigured,
  publicDestinationView,
} from '../../owner/packetShareConfig.js';
import { hasPacketShareEncryptionKey } from '../../owner/packetShareCrypto.js';
import { createPacketShareRepository, type PacketShareTransactionFn } from '../../owner/packetShareRepository.js';
import {
  getPublicFeedConfig,
  PUBLIC_FEED_TOPIC_FILTER,
} from '../../owner/publicFeedConfig.js';
import { createPublicFeedRepository } from '../../owner/publicFeedRepository.js';
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
  nodes: unknown[];
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
type GetOwnerCredentialGenerationFn = (mqttUsername: string) => Promise<number>;
type ReconcileOwnerAuthorizationFn = () => Promise<void>;
type QueryFn = <T extends import('pg').QueryResultRow = import('pg').QueryResultRow>(text: string, params?: unknown[]) => Promise<{ rows: T[] }>;

type OwnerService = ReturnType<typeof createOwnerService>;

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
  getOwnerCredentialGeneration: GetOwnerCredentialGenerationFn;
  invalidateOwnerNodeIdCache: (mqttUsername: string) => void;
  reconcileOwnerAuthorization?: ReconcileOwnerAuthorizationFn;
  query: QueryFn;
  withTransaction?: PacketShareTransactionFn;
};

let registeredOwnerService: OwnerService | null = null;

export function registerOwnerRoutes(router: Router, deps: OwnerRouteDeps): void {
  const repository = createOwnerRepository({
    query: deps.query,
  });
  const packetShareRepository = createPacketShareRepository(deps.query, deps.withTransaction);
  const publicFeedRepository = createPublicFeedRepository(deps.query, deps.withTransaction);

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
  registeredOwnerService = service;

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
      // Passwords must NOT be trimmed — a valid broker password may begin/end
      // with whitespace, and transforming it would make the credential
      // permanently unauthenticatable (username trimming is fine).
      const mqttPassword = String(body?.mqttPassword ?? '');
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
      const gen = await deps.getOwnerCredentialGeneration(mqttUsername);
      const token = deps.encryptOwnerSession({
        v: 3,
        exp: Date.now() + deps.ownerSessionTtlMs,
        mqttUsername,
        gen,
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

      // BUG-010: reject sessions minted under a revoked password (same check as
      // requireOwnerSession) and upgrade legacy cookies to the generationed v3
      // format so future revocations can invalidate them.
      const currentGen = await deps.getOwnerCredentialGeneration(session.mqttUsername);
      if (session.gen !== currentGen) {
        res.clearCookie(deps.ownerCookieName, { path: '/' });
        res.status(401).json({ error: 'Credentials have been rotated — please log in again' });
        return;
      }

      const { dashboard } = await service.getSessionDashboard(session);
      if (session.legacy || session.v !== 3) {
        res.cookie(deps.ownerCookieName, deps.encryptOwnerSession({
          v: 3,
          mqttUsername: session.mqttUsername,
          exp: session.exp,
          gen: session.gen,
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
    const requestedWindow = parseBoundedString(req.query['window'], {
      name: 'window',
      maxLength: 4,
      pattern: /^(7d|30d)$/,
    });
    const windowDays = requestedWindow === '30d' ? 30 : 7;
    try {
      const ownedNodeIds = await deps.requireOwnerSession(req, res);
      if (!ownedNodeIds) return;
      res.json(await service.getOwnerLastHopStrength(ownedNodeIds, requestedNodeId, windowDays));
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

  router.get('/owner/packet-sharing', async (req, res) => {
    const requestedNodeId = parseBoundedString(req.query['nodeId'], {
      name: 'nodeId',
      maxLength: 64,
      pattern: /^[0-9a-fA-F]{64}$/,
    })?.toUpperCase();
    try {
      const ownedNodeIds = await deps.requireOwnerSession(req, res);
      if (!ownedNodeIds) return;
      const session = deps.getOwnerSession(req)!;
      const nodeId = requestedNodeId ?? ownedNodeIds[0];
      if (!nodeId || !ownedNodeIds.includes(nodeId)) {
        res.status(403).json({ error: 'Node is not owned by this session' });
        return;
      }
      const [destinations, selectedIds, lastForwardedAt] = await Promise.all([
        packetShareRepository.listDestinations(session.mqttUsername, nodeId),
        packetShareRepository.listOwnerRules(session.mqttUsername, nodeId),
        packetShareRepository.listLastForwardedAt(session.mqttUsername, nodeId),
      ]);
      const selected = new Set(selectedIds.filter((id) => destinations.some((destination) => destination.destination_id === id)));
      const encryptionKeyAvailable = hasPacketShareEncryptionKey();
      res.setHeader('Cache-Control', 'no-store');
      res.json({
        nodeId,
        featureEnabled: true,
        enabled: selected.size > 0,
        destinations: destinations.map((destination) => publicDestinationView(
          destination,
          selected.has(destination.destination_id),
          encryptionKeyAvailable,
          lastForwardedAt.get(destination.destination_id) ?? null,
        )),
      });
    } catch (error) {
      console.error('[api] GET /owner/packet-sharing', (error as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.post('/owner/packet-sharing', csrfProtection, async (req, res) => {
    try {
      const ownedNodeIds = await deps.requireOwnerSession(req, res);
      if (!ownedNodeIds) return;
      const session = deps.getOwnerSession(req)!;
      const rawBody = req.body as unknown;
      if (!rawBody || typeof rawBody !== 'object' || Array.isArray(rawBody)) {
        res.status(400).json({ error: 'Invalid packet sharing settings' });
        return;
      }
      const body = rawBody as {
        nodeId?: unknown;
        enabled?: unknown;
        destinationIds?: unknown;
      };
      const nodeId = String(body.nodeId ?? '').trim().toUpperCase();
      const rawDestinationIds = body.destinationIds;
      const enabled = body.enabled;
      const destinationIds = Array.isArray(rawDestinationIds) && rawDestinationIds.every((value) => typeof value === 'string')
        ? Array.from(new Set(rawDestinationIds.map((value) => value.trim().toLowerCase())))
        : [];
      if (
        !/^[0-9A-F]{64}$/.test(nodeId)
        || !ownedNodeIds.includes(nodeId)
        || typeof enabled !== 'boolean'
        || !Array.isArray(rawDestinationIds)
        || !rawDestinationIds.every((value) => typeof value === 'string')
        || destinationIds.length > 32
        || destinationIds.some((destinationId) => !/^[a-z0-9][a-z0-9_-]{0,63}$/.test(destinationId))
      ) {
        res.status(400).json({ error: 'Invalid packet sharing settings' });
        return;
      }
      const destinations = await packetShareRepository.listDestinations(session.mqttUsername, nodeId);
      const byId = new Map(destinations.map((destination) => [destination.destination_id, destination]));
      if (destinationIds.some((destinationId) => {
        const destination = byId.get(destinationId);
        return !destination || (enabled && !destinationConfigured(destination, hasPacketShareEncryptionKey()));
      })) {
        res.status(409).json({ error: 'One or more selected destinations are not configured' });
        return;
      }
      const persistedDestinationIds = await packetShareRepository.replaceOwnerRules(
        session.mqttUsername,
        nodeId,
        destinationIds,
        enabled,
      );
      res.status(200).json({
        nodeId,
        enabled: enabled && persistedDestinationIds.length > 0,
        destinationIds: persistedDestinationIds,
      });
    } catch (error) {
      console.error('[api] POST /owner/packet-sharing', (error as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.get('/owner/public-feed', async (req, res) => {
    try {
      const ownedNodeIds = await deps.requireOwnerSession(req, res);
      if (!ownedNodeIds) return;
      const session = deps.getOwnerSession(req)!;
      const status = await publicFeedRepository.accountStatus(
        session.mqttUsername,
        ownedNodeIds,
      );
      const config = getPublicFeedConfig();
      const topicFilters = config.configured ? [PUBLIC_FEED_TOPIC_FILTER] : [];
      res.setHeader('Cache-Control', 'no-store');
      res.json({
        featureEnabled: true,
        configured: config.configured,
        brokerUrl: config.brokerUrl,
        enabled: status.enabled,
        nodeCount: status.nodeCount,
        eligible: status.eligible,
        recentPacketCount: status.recentPacketCount,
        topicFilters,
        lastForwardedAt: status.lastForwardedAt,
      });
    } catch (error) {
      console.error('[api] GET /owner/public-feed', (error as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.post('/owner/public-feed', csrfProtection, async (req, res) => {
    try {
      const ownedNodeIds = await deps.requireOwnerSession(req, res);
      if (!ownedNodeIds) return;
      const session = deps.getOwnerSession(req)!;
      const rawBody = req.body as unknown;
      if (!rawBody || typeof rawBody !== 'object' || Array.isArray(rawBody)) {
        res.status(400).json({ error: 'Invalid public feed settings' });
        return;
      }
      const enabled = (rawBody as { enabled?: unknown }).enabled;
      if (typeof enabled !== 'boolean') {
        res.status(400).json({ error: 'Invalid public feed settings' });
        return;
      }
      const config = getPublicFeedConfig();
      const currentStatus = await publicFeedRepository.accountStatus(
        session.mqttUsername,
        ownedNodeIds,
      );
      if (enabled && !currentStatus.eligible) {
        res.status(409).json({ error: 'Publish at least one eligible packet in the last 24 hours before enabling the public feed' });
        return;
      }
      if (enabled && !config.configured) {
        res.status(409).json({ error: 'Public feed is not configured by the operator' });
        return;
      }
      const previousEnabled = currentStatus.enabled;
      await publicFeedRepository.setEnabled(session.mqttUsername, ownedNodeIds, enabled);
      if (deps.reconcileOwnerAuthorization) {
        try {
          await deps.reconcileOwnerAuthorization();
        } catch (error) {
          console.error('[api] public-feed ACL reconciliation failed', (error as Error).message);
          // The broker is the enforcement point. If the ACL reload failed, put the
          // database back to the state the broker is still enforcing so a disable
          // never reports failure while access remains granted (and an enable never
          // reports failure while remaining ungranted).
          try {
            await publicFeedRepository.setEnabled(session.mqttUsername, ownedNodeIds, previousEnabled);
          } catch (rollbackError) {
            console.error('[api] public-feed state rollback failed', (rollbackError as Error).message);
          }
          res.status(503).json({ error: 'Public feed access could not be updated yet' });
          return;
        }
      }
      const status = await publicFeedRepository.accountStatus(
        session.mqttUsername,
        ownedNodeIds,
      );
      const topicFilters = config.configured ? [PUBLIC_FEED_TOPIC_FILTER] : [];
      res.status(200).json({
        featureEnabled: true,
        configured: config.configured,
        brokerUrl: config.brokerUrl,
        enabled: status.enabled,
        nodeCount: status.nodeCount,
        eligible: status.eligible,
        recentPacketCount: status.recentPacketCount,
        topicFilters,
        lastForwardedAt: status.lastForwardedAt,
      });
    } catch (error) {
      console.error('[api] POST /owner/public-feed', (error as Error).message);
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

// Exposes the registered owner service so the app bootstrap can start background
// work (e.g. the owner last-hop cache prewarm) against the same instance the
// routes use. Returns null when the routes have not been registered yet.
export function getRegisteredOwnerService(): OwnerService | null {
  return registeredOwnerService;
}
