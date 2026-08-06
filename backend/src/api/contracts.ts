import type { Router } from 'express';
import { listRegisteredRoutes } from './routeRegistry.js';

export type ContractAccess = 'public' | 'test' | 'owner' | 'operator';
export type HttpMethod = 'GET' | 'POST' | 'DELETE';

export type ApiContract = {
  method: HttpMethod;
  path: string;
  access: ContractAccess;
  summary: string;
  requestSchema?: Record<string, unknown>;
  responseSchema?: Record<string, unknown>;
  queryParameters?: readonly Record<string, unknown>[];
  additionalResponses?: Readonly<Record<string, {
    description: string;
    responseSchema: Record<string, unknown>;
  }>>;
};

const ERROR_SCHEMA = {
  type: 'object',
  required: ['error'],
  properties: { error: { type: 'string' } },
  additionalProperties: false,
};

const PLANNED_NODE_PAGE_SCHEMA = {
  type: 'object',
  required: ['items', 'nextCursor'],
  properties: {
    items: {
      type: 'array',
      items: {
        type: 'object',
        required: ['id', 'name', 'lat', 'lon', 'publishedAt', 'expiresAt'],
        properties: {
          id: { type: 'string', format: 'uuid' },
          name: { type: 'string', maxLength: 100 },
          lat: { type: 'number', minimum: -90, maximum: 90 },
          lon: { type: 'number', minimum: -180, maximum: 180 },
          heightM: { type: ['number', 'null'], minimum: 0, maximum: 500 },
          region: { type: ['string', 'null'], pattern: '^[A-Z0-9]{2,8}$' },
          publishedAt: { type: 'string', format: 'date-time' },
          expiresAt: { type: 'string', format: 'date-time' },
        },
        additionalProperties: false,
      },
    },
    nextCursor: { type: ['string', 'null'] },
  },
  additionalProperties: false,
};

const SLOW_MODE_STATUS_SCHEMA = {
  type: 'object',
  required: ['enabled', 'windowMs', 'pending', 'pendingMax'],
  properties: {
    enabled: { type: 'boolean' },
    windowMs: { type: 'integer', minimum: 0 },
    pending: { type: 'integer', minimum: 0 },
    pendingMax: { type: 'integer', minimum: 1 },
  },
  additionalProperties: false,
};

const SLOW_MODE_PENDING_SCHEMA = {
  type: 'object',
  required: ['status', 'remainingMs', 'windowMs'],
  properties: {
    status: { type: 'string', const: 'pending' },
    remainingMs: { type: 'integer', minimum: 0 },
    windowMs: { type: 'integer', minimum: 0 },
  },
  additionalProperties: false,
};

const FEED_MESSAGE_HISTORY_SCHEMA = {
  type: 'array',
  maxItems: 50,
  items: {
    type: 'object',
    required: ['time', 'packet_hash', 'packet_type'],
    properties: {
      time: { type: 'string', format: 'date-time' },
      packet_hash: { type: 'string' },
      packet_type: { type: 'integer', const: 5 },
      summary: { type: ['string', 'null'] },
    },
    additionalProperties: true,
  },
};

function humanize(path: string): string {
  return path
    .replace(/^\/v1\//, '')
    .replace(/[/:.-]+/g, ' ')
    .replace(/\b(id|api)\b/gi, (word) => word.toUpperCase())
    .trim();
}

function contracts(
  method: HttpMethod,
  paths: readonly string[],
  access: ContractAccess,
): ApiContract[] {
  return paths.map((path) => ({
    method,
    path,
    access,
    summary: `${method === 'GET' ? 'Read' : method === 'POST' ? 'Create or run' : 'Delete'} ${humanize(path)}`,
  }));
}

const PUBLIC_GET = [
  '/activity/timeline',
  '/companion-activity',
  '/coverage',
  '/coverage/:nodeId',
  '/coverage/planned/:planId',
  '/feed/messages',
  '/health',
  '/inferred-nodes',
  '/links/:id/history',
  '/mqtt-nodes',
  '/node-status/history',
  '/node-status/latest',
  '/nodes',
  '/nodes/:id/adverts',
  '/nodes/:id/history',
  '/nodes/:id/links',
  '/nodes/map',
  '/observer-activity',
  '/observers/health',
  '/packets/:hash',
  '/packets/recent',
  '/path-beta/history',
  '/path-beta/multibyte-paths',
  '/path-beta/resolve',
  '/path-beta/resolve-multi',
  '/path-beta/slow-mode',
  '/path-lazy/resolve',
  '/path-learning',
  '/planned-nodes',
  '/radio-history',
  '/radio-stats',
  '/repeaters/firmware',
  '/rf-validation',
  '/runtime-config',
  '/spam/messages/incidents',
  '/spam/messages/incidents/:id',
  '/spam/messages/status',
  '/spam/observers',
  '/spam/packet/:id/observers',
  '/spam/suspects',
  '/stats',
  '/stats/charts',
  '/topology',
  '/v1',
  '/v1/exports/nodes.:format',
  '/v1/exports/path.gpx',
  '/v1/openapi.yaml',
] as const;

const OWNER_GET = [
  '/owner/alert-deliveries',
  '/owner/alert-rules',
  '/owner/csrf',
  '/owner/live',
  '/owner/live-last-hop',
  '/owner/session',
] as const;

export const API_CONTRACTS: readonly ApiContract[] = [
  ...contracts('GET', PUBLIC_GET, 'public'),
  ...contracts('GET', ['/local/test-diagnostics'], 'test'),
  ...contracts('GET', OWNER_GET, 'owner'),
  ...contracts('POST', ['/coverage/planned', '/observers/register', '/telemetry/frontend-error'], 'public'),
  ...contracts('POST', [
    '/owner/alert-rules',
    '/owner/alert-rules/:id/test',
    '/owner/login',
    '/owner/logout',
  ], 'owner'),
  ...contracts('DELETE', ['/coverage/planned/:planId'], 'public'),
  ...contracts('DELETE', ['/owner/alert-rules/:id'], 'owner'),
].map((contract) => {
  if (contract.path === '/path-beta/slow-mode') {
    return {
      ...contract,
      summary: 'Read slow-mode path resolution scheduler status',
      responseSchema: SLOW_MODE_STATUS_SCHEMA,
    };
  }
  if (contract.path === '/path-beta/resolve-multi') {
    return {
      ...contract,
      queryParameters: [{
        name: 'mode',
        in: 'query',
        required: false,
        schema: { type: 'string', enum: ['fast', 'slow'] },
      }],
      additionalResponses: {
        '202': {
          description: 'Slow-mode propagation window is still pending',
          responseSchema: SLOW_MODE_PENDING_SCHEMA,
        },
      },
    };
  }
  if (contract.path === '/planned-nodes') {
    return { ...contract, summary: 'List explicitly published planned nodes', responseSchema: PLANNED_NODE_PAGE_SCHEMA };
  }
  if (contract.path === '/feed/messages') {
    return {
      ...contract,
      summary: 'Read historical messages for one channel',
      responseSchema: FEED_MESSAGE_HISTORY_SCHEMA,
      queryParameters: [
        {
          name: 'channel',
          in: 'query',
          required: true,
          schema: { type: 'string', pattern: '^[A-Za-z0-9][A-Za-z0-9_-]*$', maxLength: 64 },
        },
        {
          name: 'limit',
          in: 'query',
          required: false,
          schema: { type: 'integer', minimum: 1, maximum: 50, default: 50 },
        },
      ],
    };
  }
  if (contract.path === '/observers/register') {
    return {
      ...contract,
      summary: 'Submit an observer registration for operator review',
      requestSchema: {
        type: 'object',
        required: ['publicKey', 'iata', 'contact'],
        properties: {
          publicKey: { type: 'string', pattern: '^[0-9A-Fa-f]{64}$' },
          iata: { type: 'string', pattern: '^[A-Za-z0-9]{2,8}$' },
          name: { type: 'string', maxLength: 100 },
          contact: { type: 'string', maxLength: 200 },
        },
        additionalProperties: false,
      },
    };
  }
  return contract;
});

export const OPERATOR_CONTRACTS: readonly ApiContract[] = [
  ...contracts('POST', ['/local-api/operator/login'], 'operator'),
  ...contracts('GET', [
    '/local-api/operator/session',
    '/local-api/observer-registrations',
    '/local-api/operations',
    '/local-api/operator/audit',
    '/local-api/ml-path-learner',
    '/local-api/health',
  ], 'operator'),
  ...contracts('POST', [
    '/local-api/operator/logout',
    '/local-api/observer-registrations/:id/action',
    '/local-api/jobs/:queue/:jobId/:action',
    '/local-api/planned-nodes/:id/publication',
  ], 'operator'),
];

export function contractKey(contract: Pick<ApiContract, 'method' | 'path'>): string {
  return `${contract.method} ${contract.path}`;
}

export function assertContractCoverage(router: Router): void {
  const registered = new Set(listRegisteredRoutes(router));
  const contracted = new Set(API_CONTRACTS.map(contractKey));
  const duplicateContracts = API_CONTRACTS
    .map(contractKey)
    .filter((key, index, keys) => keys.indexOf(key) !== index);
  const missing = [...registered].filter((key) => !contracted.has(key)).sort();
  const stale = [...contracted].filter((key) => !registered.has(key)).sort();
  if (duplicateContracts.length || missing.length || stale.length) {
    throw new Error([
      duplicateContracts.length ? `duplicate contracts: ${[...new Set(duplicateContracts)].join(', ')}` : '',
      missing.length ? `routes without contracts: ${missing.join(', ')}` : '',
      stale.length ? `contracts without routes: ${stale.join(', ')}` : '',
    ].filter(Boolean).join('; '));
  }
}

export const COMMON_ERROR_SCHEMA = ERROR_SCHEMA;
