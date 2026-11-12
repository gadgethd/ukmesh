import { frontendCls, frontendInp, frontendLcp } from '../metrics/frontend.js';

export const PUBLIC_VITAL_ROUTES = ['/', '/docs', '/stats', '/feed', '/regions', '/topology'] as const;
export const WEB_VITAL_SCHEMA = {
  type: 'object', required: ['route', 'metric', 'value'], additionalProperties: false,
  properties: {
    route: { type: 'string', enum: PUBLIC_VITAL_ROUTES },
    metric: { type: 'string', enum: ['CLS', 'LCP', 'INP'] },
    value: { type: 'number', minimum: 0, maximum: 600_000 },
  },
  allOf: [{ if: { properties: { metric: { const: 'CLS' } } }, then: { properties: { value: { maximum: 10 } } } }],
};

export function recordWebVital(body: unknown): boolean {
  if (!body || typeof body !== 'object' || Array.isArray(body)) return false;
  const row = body as Record<string, unknown>;
  if (Object.keys(row).length !== 3 || Object.keys(row).some(key => !['route', 'metric', 'value'].includes(key))) return false;
  const { route, metric, value } = row;
  if (typeof route !== 'string' || !(PUBLIC_VITAL_ROUTES as readonly string[]).includes(route)) return false;
  if (typeof value !== 'number' || !Number.isFinite(value) || value < 0) return false;
  if (metric === 'CLS' && value <= 10) frontendCls.observe({ route }, value);
  else if (metric === 'LCP' && value <= 600_000) frontendLcp.observe({ route }, value / 1000);
  else if (metric === 'INP' && value <= 600_000) frontendInp.observe({ route }, value / 1000);
  else return false;
  return true;
}
