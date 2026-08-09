import { Router, type Request, type Response } from 'express';
import { rateLimit } from 'express-rate-limit';
import { isIP } from 'node:net';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { randomBytes } from 'node:crypto';
import type { QueryResultRow } from 'pg';
import {
  operatorTokenIsConfigured,
  readCookie,
  verifyOperatorToken,
} from '../security/operatorAuth.js';
import { isTrustedProxyPeer } from '../http/trustedProxy.js';
import {
  OperatorSessionStore,
  type OperatorSession,
} from '../security/operatorSession.js';
import {
  actOnObserverRegistration,
  actOnPlannedNodePublication,
  actOnQueueJob,
  listObserverRegistrations,
  listRecentOperatorAudit,
  loadOperationsDashboard,
  operatorActor,
  validateDecisionReason,
  validateIdempotencyKey,
  validatePlannedPublication,
  type ObserverAction,
  type PlannedPublicationAction,
  type QueueAction,
  type QueueName,
} from '../operations/operatorOperations.js';

type QueryFn = <T extends QueryResultRow = QueryResultRow>(
  text: string,
  params?: unknown[],
) => Promise<{ rows: T[] }>;

type BackendSiteDeps = {
  query: QueryFn;
  getHealthOverview?: () => Promise<unknown>;
};

function loadBackendSiteTemplate(name: string): string {
  const directory = path.dirname(fileURLToPath(import.meta.url));
  const candidates = [
    path.join(directory, name),
    path.join(directory, '..', '..', 'src', 'backend-site', name),
  ];
  const templatePath = candidates.find((candidate) => fs.existsSync(candidate));
  if (!templatePath) throw new Error('Backend site template is unavailable');
  return fs.readFileSync(templatePath, 'utf8');
}

const BACKEND_SITE_TEMPLATE = loadBackendSiteTemplate('template.html');
const BACKEND_LOGIN_TEMPLATE = loadBackendSiteTemplate('login.html');
const OPERATOR_COOKIE_NAME = 'meshcore_operator_session';
const OPERATOR_SESSION_TTL_MS = Math.min(
  8 * 60 * 60_000,
  Math.max(5 * 60_000, Number(process.env['OPERATOR_SESSION_TTL_MS'] ?? 30 * 60_000) || 30 * 60_000),
);
const operatorSessions = new OperatorSessionStore(OPERATOR_SESSION_TTL_MS, 128);

function firstHeaderIp(value: unknown): string {
  const raw = Array.isArray(value) ? value[0] : value;
  return String(raw ?? '').split(',')[0]?.trim() ?? '';
}

function normalizeIp(value: string | undefined): string {
  const raw = String(value ?? '').trim();
  if (!raw) return '';
  if (raw.startsWith('::ffff:')) return raw.slice(7);
  return raw;
}

function isPrivateOrLoopback(ip: string): boolean {
  const normalized = normalizeIp(ip);
  if (!normalized) return false;
  if (normalized === 'localhost' || normalized === '::1' || normalized === '127.0.0.1') return true;
  if (normalized.startsWith('10.')) return true;
  if (normalized.startsWith('192.168.')) return true;
  if (/^172\.(1[6-9]|2\d|3[0-1])\./.test(normalized)) return true;
  if (/^(fc|fd)/i.test(normalized)) return true;
  if (/^fe80:/i.test(normalized)) return true;
  return false;
}

function requireBackendSiteLocalOnly(req: Request, res: Response): boolean {
  const forwarded = [
    firstHeaderIp(req.headers['cf-connecting-ip']),
    firstHeaderIp(req.headers['x-forwarded-for']),
    firstHeaderIp(req.headers['x-real-ip']),
  ].filter(Boolean);
  const peer = normalizeIp(req.socket.remoteAddress ?? '');

  // Public proxy traffic carries forwarded client IPs.  Do not allow a private
  // cloudflared/docker socket address to satisfy the local-only check.
  if (
    (forwarded.length > 0 && !isTrustedProxyPeer(peer))
    || forwarded.some((ip) => !isPrivateOrLoopback(ip))
  ) {
    res.status(403).type('text/plain').send('Local access only');
    return false;
  }

  const candidates = [
    peer,
    ...forwarded.map(normalizeIp),
  ].filter(Boolean);

  if (candidates.some((ip) => isPrivateOrLoopback(ip) || (isIP(ip) === 0 && ip === 'localhost'))) {
    return true;
  }

  res.status(403).type('text/plain').send('Local access only');
  return false;
}

type OperatorAuth =
  | { mode: 'session'; session: OperatorSession }
  | { mode: 'automation'; session: null };

function getOperatorAuth(req: Request): OperatorAuth | null {
  const session = operatorSessions.get(readCookie(req.headers.cookie, OPERATOR_COOKIE_NAME));
  if (session) return { mode: 'session', session };
  const expected = process.env['OPERATOR_SITE_TOKEN'];
  if (!operatorTokenIsConfigured(expected)) return null;
  const authorization = String(req.headers.authorization ?? '');
  const bearer = authorization.startsWith('Bearer ') ? authorization.slice(7).trim() : '';
  const basic = authorization.startsWith('Basic ')
    ? Buffer.from(authorization.slice(6), 'base64').toString('utf8').split(':').slice(1).join(':')
    : '';
  const provided = String(req.headers['x-operator-token'] ?? (bearer || basic));
  return verifyOperatorToken(expected, provided)
    ? { mode: 'automation', session: null }
    : null;
}

function isSupportedOperatorTransport(req: Request): boolean {
  const peer = normalizeIp(req.socket.remoteAddress ?? '');
  const forwardedProto = firstHeaderIp(req.headers['x-forwarded-proto']).toLowerCase();
  const host = String(req.hostname ?? '').toLowerCase();
  return (
    req.secure
    || (isTrustedProxyPeer(peer) && forwardedProto === 'https')
    || host === 'localhost'
    || host === '127.0.0.1'
    || host === '[::1]'
  );
}

function localOnly(
  handler: (req: Request, res: Response) => void | Promise<void>,
  options: { auth?: boolean } = {},
) {
  return async (req: Request, res: Response) => {
    if (!requireBackendSiteLocalOnly(req, res)) return;
    if (options.auth !== false) {
      const auth = getOperatorAuth(req);
      if (!auth) {
        res.status(401).json({ error: 'Operator authentication required' });
        return;
      }
      res.locals['operatorAuth'] = auth;
    }
    try {
      await handler(req, res);
    } catch (err) {
      console.error('[backend-site] request failed:', err);
      if (!res.headersSent) {
        res.status(500).json({ error: 'Backend dashboard query failed' });
      }
    }
  };
}

function sendBackendSiteHtml(template: string, res: Response): void {
  const nonce = randomBytes(18).toString('base64url');
  res.setHeader(
    'Content-Security-Policy',
    `default-src 'self'; script-src 'self' 'nonce-${nonce}'; style-src 'self' 'nonce-${nonce}'; img-src 'self' data:; connect-src 'self'; font-src 'self' data:; object-src 'none'; base-uri 'self'; frame-ancestors 'none'`,
  );
  res.type('html').send(template.replaceAll('{{CSP_NONCE}}', nonce));
}

export function createBackendSiteRoutes(deps: BackendSiteDeps): Router {
  const { query } = deps;
  const router = Router();
  const loginLimiter = rateLimit({
    windowMs: 15 * 60_000,
    limit: 5,
    standardHeaders: true,
    legacyHeaders: false,
  });

  const sendOperatorPage = localOnly((req, res) => {
    sendBackendSiteHtml(
      getOperatorAuth(req) ? BACKEND_SITE_TEMPLATE : BACKEND_LOGIN_TEMPLATE,
      res,
    );
  }, { auth: false });
  router.get('/', sendOperatorPage);
  router.get('/operations', sendOperatorPage);
  router.get('/observer-registrations', sendOperatorPage);
  router.get('/backend', localOnly((_req, res) => res.redirect(302, '/'), { auth: false }));

  router.post('/local-api/operator/login', loginLimiter, localOnly((req, res) => {
    const expected = process.env['OPERATOR_SITE_TOKEN'];
    if (!operatorTokenIsConfigured(expected)) {
      res.status(503).json({ error: 'Operator site is not configured' });
      return;
    }
    if (!isSupportedOperatorTransport(req)) {
      res.status(400).json({ error: 'Use local HTTPS or a localhost tunnel' });
      return;
    }
    if (!String(req.headers['content-type'] ?? '').toLowerCase().startsWith('application/json')) {
      res.status(415).json({ error: 'application/json is required' });
      return;
    }
    const token = (req.body as { token?: unknown } | undefined)?.token;
    if (typeof token !== 'string' || token.length > 4_096 || !verifyOperatorToken(expected, token)) {
      console.warn('[operator-auth] login rejected');
      res.status(403).json({ error: 'Invalid operator token' });
      return;
    }
    const session = operatorSessions.create();
    res.cookie(OPERATOR_COOKIE_NAME, session.id, {
      httpOnly: true,
      secure: true,
      sameSite: 'strict',
      path: '/',
      maxAge: OPERATOR_SESSION_TTL_MS,
    });
    res.setHeader('Cache-Control', 'no-store');
    console.log('[operator-auth] browser session created', {
      expiresAt: new Date(session.expiresAt).toISOString(),
    });
    res.json({ ok: true, expiresAt: session.expiresAt });
  }, { auth: false }));

  router.get('/local-api/operator/session', localOnly((_req, res) => {
    const auth = res.locals['operatorAuth'] as OperatorAuth;
    res.setHeader('Cache-Control', 'no-store');
    res.json({
      mode: auth.mode,
      expiresAt: auth.session?.expiresAt ?? null,
      csrfToken: auth.session?.csrfToken ?? null,
    });
  }));

  function requireOperatorMutation(req: Request, res: Response): {
    actor: ReturnType<typeof operatorActor>;
    idempotencyKey: string;
  } | null {
    if (!String(req.headers['content-type'] ?? '').toLowerCase().startsWith('application/json')) {
      res.status(415).json({ error: 'application/json is required' });
      return null;
    }
    const auth = res.locals['operatorAuth'] as OperatorAuth;
    if (
      auth.mode === 'session'
      && !operatorSessions.verifyCsrf(auth.session, req.headers['x-csrf-token'])
    ) {
      res.status(403).json({ error: 'Invalid CSRF token' });
      return null;
    }
    try {
      return {
        actor: operatorActor(auth.mode, auth.session?.id),
        idempotencyKey: validateIdempotencyKey(req.headers['idempotency-key']),
      };
    } catch {
      res.status(400).json({ error: 'A valid Idempotency-Key header is required' });
      return null;
    }
  }

  router.post('/local-api/operator/logout', localOnly((req, res) => {
    const auth = res.locals['operatorAuth'] as OperatorAuth;
    if (
      auth.mode !== 'session'
      || !operatorSessions.verifyCsrf(auth.session, req.headers['x-csrf-token'])
    ) {
      res.status(403).json({ error: 'Invalid CSRF token' });
      return;
    }
    operatorSessions.delete(auth.session.id);
    res.clearCookie(OPERATOR_COOKIE_NAME, {
      httpOnly: true,
      secure: true,
      sameSite: 'strict',
      path: '/',
    });
    console.log('[operator-auth] browser session logged out');
    res.json({ ok: true });
  }));

  router.get('/local-api/observer-registrations', localOnly(async (req, res) => {
    const status = typeof req.query['status'] === 'string' ? req.query['status'] : 'pending';
    const limit = Number(typeof req.query['limit'] === 'string' ? req.query['limit'] : 50);
    res.setHeader('Cache-Control', 'no-store');
    res.json(await listObserverRegistrations(query, status, limit));
  }));

  router.post('/local-api/observer-registrations/:id/action', localOnly(async (req, res) => {
    const mutation = requireOperatorMutation(req, res);
    if (!mutation) return;
    const body = req.body as {
      action?: unknown;
      reason?: unknown;
      duplicateOf?: unknown;
    } | undefined;
    const action = String(body?.action ?? '') as ObserverAction;
    try {
      const result = await actOnObserverRegistration(query, {
        requestId: String(req.params['id'] ?? ''),
        action,
        reason: validateDecisionReason(body?.reason),
        duplicateOf: body?.duplicateOf === undefined || body.duplicateOf === null
          ? undefined
          : String(body.duplicateOf),
        idempotencyKey: mutation.idempotencyKey,
        actor: mutation.actor,
      });
      res.status(result['idempotentReplay'] ? 200 : 202).json(result);
    } catch (error) {
      const code = (error as Error).message;
      if (code.startsWith('INVALID_') || code === 'CONFIRMATION_REQUIRED') {
        res.status(code === 'INVALID_OBSERVER_STATE' ? 409 : 400).json({ error: code });
        return;
      }
      if (code === 'IDEMPOTENCY_KEY_REUSED') {
        res.status(409).json({ error: code });
        return;
      }
      throw error;
    }
  }));

  router.get('/local-api/operations', localOnly(async (_req, res) => {
    res.setHeader('Cache-Control', 'no-store');
    res.json(await loadOperationsDashboard(query));
  }));

  router.get('/local-api/operator/audit', localOnly(async (req, res) => {
    const limit = Number(typeof req.query['limit'] === 'string' ? req.query['limit'] : 50);
    res.setHeader('Cache-Control', 'no-store');
    res.json({
      generatedAt: new Date().toISOString(),
      events: await listRecentOperatorAudit(query, limit),
    });
  }));

  router.post('/local-api/jobs/:queue/:jobId/:action', localOnly(async (req, res) => {
    const mutation = requireOperatorMutation(req, res);
    if (!mutation) return;
    const queue = String(req.params['queue'] ?? '') as QueueName;
    const action = String(req.params['action'] ?? '') as QueueAction;
    const body = req.body as { confirmation?: unknown } | undefined;
    try {
      const result = await actOnQueueJob(query, {
        queue,
        jobId: String(req.params['jobId'] ?? ''),
        action,
        confirmation: typeof body?.confirmation === 'string' ? body.confirmation : undefined,
        idempotencyKey: mutation.idempotencyKey,
        actor: mutation.actor,
      });
      res.status(result['idempotentReplay'] ? 200 : 202).json(result);
    } catch (error) {
      const code = (error as Error).message;
      if (
        code.startsWith('INVALID_')
        || code === 'CONFIRMATION_REQUIRED'
        || code === 'JOB_NOT_DEAD'
        || code === 'QUEUE_CAPACITY_EXCEEDED'
        || code === 'IDEMPOTENCY_KEY_REUSED'
      ) {
        res.status(
          code === 'JOB_NOT_DEAD'
          || code === 'QUEUE_CAPACITY_EXCEEDED'
          || code === 'IDEMPOTENCY_KEY_REUSED'
            ? 409
            : 400,
        ).json({ error: code });
        return;
      }
      throw error;
    }
  }));

  router.post('/local-api/planned-nodes/:id/publication', localOnly(async (req, res) => {
    const mutation = requireOperatorMutation(req, res);
    if (!mutation) return;
    const body = req.body as {
      action?: unknown;
      reason?: unknown;
      publicName?: unknown;
      publicLat?: unknown;
      publicLon?: unknown;
      publicHeightM?: unknown;
      region?: unknown;
      expiresAt?: unknown;
    } | undefined;
    const action = String(body?.action ?? '') as PlannedPublicationAction;
    try {
      const result = await actOnPlannedNodePublication(query, {
        plannedNodeId: String(req.params['id'] ?? ''),
        action,
        reason: validateDecisionReason(body?.reason),
        publication: action === 'publish' ? validatePlannedPublication(body) : undefined,
        idempotencyKey: mutation.idempotencyKey,
        actor: mutation.actor,
      });
      res.status(result['idempotentReplay'] ? 200 : 202).json(result);
    } catch (error) {
      const code = (error as Error).message;
      if (code.startsWith('INVALID_')) {
        res.status(400).json({ error: code });
        return;
      }
      if (code === 'IDEMPOTENCY_KEY_REUSED' || code === 'PLANNED_PUBLICATION_UNCHANGED') {
        res.status(409).json({ error: code });
        return;
      }
      throw error;
    }
  }));

  if (deps.getHealthOverview) {
    router.get('/local-api/health', localOnly(async (_req, res) => {
      res.setHeader('Cache-Control', 'no-store');
      res.json(await deps.getHealthOverview!());
    }));
  }

  return router;
}
