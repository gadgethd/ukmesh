import { createHash } from 'node:crypto';
import { parseCsv } from './sampleData.js';
import { LOOKUP_RESPONSE_MAX_BYTES } from './lookupResponseBody.js';
import http from 'node:http';
import policy from './operationPairPolicy.json' with { type: 'json' };

export const OPERATION_CASES = [
  { name: 'summary', route: '/api/stats?network=ukmesh', format: 'summary' },
  { name: 'charts', route: '/api/stats/charts?network=ukmesh', format: 'charts' },
  { name: 'nodes-csv', route: '/api/v1/exports/nodes.csv?network=ukmesh&limit=5000', format: 'csv' },
  { name: 'nodes-geojson', route: '/api/v1/exports/nodes.geojson?network=ukmesh&limit=5000', format: 'geojson' },
] as const;
export type OperationFormat = typeof OPERATION_CASES[number]['format'];
export type OperationSnapshot = { generatedAt: string; visibilityGeneration: number; maxAgeMs: number };
export function readOperation(port: number, route: typeof OPERATION_CASES[number]['route'], agent: http.Agent,
  deadlineMs = policy.responseDeadlineMs): Promise<{ body: Buffer; status: number; type: string; ms: number;
    transport: { localAddress: string | undefined; reusedSocket: boolean; socketAssignedMs: number; connectedMs: number; headersMs: number; endMs: number } }> {
  if (!Number.isInteger(port) || port < 1 || port > 65535 || !Number.isFinite(deadlineMs) || deadlineMs <= 0 || deadlineMs > policy.responseDeadlineMs) throw new Error('invalid operation transport');
  const started = performance.now();
  return new Promise((resolve, reject) => {
    let timer: ReturnType<typeof setTimeout>;
    const fail = (error: Error) => { clearTimeout(timer); reject(error); };
    let socketAssignedMs = 0, connectedMs = 0;
    const request = http.get({ hostname: '127.0.0.1', port, path: route, agent }, response => {
      const headersMs = performance.now() - started;
      const localAddress = response.socket.localAddress;
      const chunks: Buffer[] = []; let bytes = 0;
      response.on('data', (chunk: Buffer) => {
        bytes += chunk.length;
        if (bytes > LOOKUP_RESPONSE_MAX_BYTES) response.destroy(new Error('operation response byte budget'));
        else chunks.push(chunk);
      });
      response.once('error', fail);
      response.once('end', () => {
        clearTimeout(timer);
        const body = Buffer.concat(chunks);
        const endMs = performance.now() - started;
        resolve({ body, status: response.statusCode ?? 0,
          type: response.headers['content-type'] ?? '', ms: endMs,
          transport: { localAddress, reusedSocket: request.reusedSocket, socketAssignedMs, connectedMs, headersMs, endMs } });
      });
    });
    request.once('error', fail);
    request.once('socket', socket => {
      socketAssignedMs = performance.now() - started;
      if (socket.connecting) socket.once('connect', () => { connectedMs = performance.now() - started; });
      else connectedMs = socketAssignedMs;
    });
    timer = setTimeout(() => request.destroy(new Error('operation response deadline')), deadlineMs);
  });
}
const hash = (value: string | Buffer) => createHash('sha256').update(value).digest('hex');
function canonical(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonical);
  if (value && typeof value === 'object') return Object.fromEntries(Object.entries(value)
    .sort(([a], [b]) => a.localeCompare(b)).map(([key, entry]) => [key, canonical(entry)]));
  return value;
}

export function validateOperationResponse(body: Buffer, format: OperationFormat, status: number, contentType: string,
  expectedSnapshot?: OperationSnapshot) {
  if (status !== 200) throw new Error(`operation HTTP ${status}`);
  if (!body.length || body.length > LOOKUP_RESPONSE_MAX_BYTES) throw new Error('operation response byte budget');
  let normalized: unknown, ids: string[] = [], entries: number | null = null, snapshot: Record<string, unknown> | null = null;
  if (format === 'csv') {
    if (!contentType.includes('text/csv')) throw new Error('CSV content type');
    const { headers, rows } = parseCsv(body.toString());
    const id = headers.indexOf('node_id');
    if (id < 0 || !rows.length || rows.some(row => row.length !== headers.length)) throw new Error('invalid populated CSV');
    ids = rows.map(row => row[id]!); entries = rows.length; normalized = { headers, rows };
  } else {
    if (!contentType.includes(format === 'geojson' ? 'application/geo+json' : 'application/json')) throw new Error('JSON content type');
    const value = JSON.parse(body.toString());
    if (!value || typeof value !== 'object' || Array.isArray(value) || !Object.keys(value).length) throw new Error('empty operation response');
    if (format === 'summary') {
      for (const key of ['totalNodes', 'mapNodes', 'packetsDay']) {
        if (!Number.isFinite(value[key]) || value[key] < 0) throw new Error('incomplete statistics summary');
      }
      if (value.totalNodes < 1) throw new Error('unpopulated statistics fixture');
    } else if (format === 'charts') {
      if (!value.snapshot || !value.summary || !Array.isArray(value.packetsPerHour)
        || !Array.isArray(value.packetTypes)) throw new Error('incomplete charts response');
      const ageMs = Date.now() - Date.parse(value.snapshot.generatedAt);
      if (!expectedSnapshot || value.snapshot.status !== 'complete' || value.snapshot.scope !== 'ukmesh'
        || value.snapshot.generatedAt !== expectedSnapshot.generatedAt
        || value.snapshot.visibilityGeneration !== expectedSnapshot.visibilityGeneration
        || !Number.isFinite(ageMs) || ageMs > expectedSnapshot.maxAgeMs) throw new Error('charts snapshot freshness or generation differs');
      snapshot = { ...value.snapshot, ageMs, maxAgeMs: expectedSnapshot.maxAgeMs };
    } else {
      if (value.type !== 'FeatureCollection' || !Array.isArray(value.features) || !value.features.length) throw new Error('invalid populated GeoJSON');
      ids = value.features.map((feature: any) => {
        if (feature.type !== 'Feature' || feature.geometry?.type !== 'Point'
          || !Array.isArray(feature.geometry.coordinates) || feature.geometry.coordinates.length !== 2
          || !feature.geometry.coordinates.every(Number.isFinite)) throw new Error('invalid GeoJSON point');
        return feature.properties?.node_id;
      });
      entries = ids.length;
    }
    normalized = format === 'geojson' ? { ...value, generatedAt: '[response time]' } : value;
  }
  if (entries !== null && (entries > 5000 || ids.some(id => typeof id !== 'string' || !/^[a-f0-9]{64}$/i.test(id))
    || new Set(ids.map(id => id.toLowerCase())).size !== ids.length)) throw new Error('invalid or duplicate export identity');
  return { bytes: body.length, entries, digest: hash(body), dataDigest: hash(JSON.stringify(canonical(normalized))),
    identityDigest: entries === null ? null : hash(JSON.stringify(ids.map(id => id.toLowerCase()).sort())), snapshot };
}
