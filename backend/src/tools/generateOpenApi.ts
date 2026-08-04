import fs from 'node:fs';
import path from 'node:path';
import {
  API_CONTRACTS,
  COMMON_ERROR_SCHEMA,
  OPERATOR_CONTRACTS,
  type ApiContract,
} from '../api/contracts.js';

function openApiPath(pathname: string, apiPrefix: boolean): string {
  const converted = pathname.replace(/:([A-Za-z][A-Za-z0-9_]*)/g, '{$1}');
  return apiPrefix ? `/api${converted}` : converted;
}

function pathParameters(pathname: string): Array<Record<string, unknown>> {
  return [...pathname.matchAll(/:([A-Za-z][A-Za-z0-9_]*)/g)].map((match) => ({
    name: match[1],
    in: 'path',
    required: true,
    schema: { type: 'string', minLength: 1, maxLength: 200 },
  }));
}

function successStatus(contract: ApiContract): string {
  if (contract.method === 'DELETE') return '204';
  if (
    contract.path === '/observers/register'
    || contract.path === '/coverage/planned'
    || contract.path.includes('/test')
    || contract.access === 'operator' && contract.method === 'POST'
  ) return '202';
  return '200';
}

function security(contract: ApiContract): Array<Record<string, string[]>> | undefined {
  if (contract.access === 'owner') return [{ ownerSession: [] }];
  if (contract.access === 'test') return [{ operatorToken: [] }];
  if (contract.access === 'operator' && contract.path !== '/local-api/operator/login') {
    return [{ operatorSession: [] }, { operatorToken: [] }];
  }
  return undefined;
}

function operation(contract: ApiContract): Record<string, unknown> {
  const status = successStatus(contract);
  const responseSchema = contract.responseSchema ?? {
    type: 'object',
    additionalProperties: true,
  };
  const result: Record<string, unknown> = {
    summary: contract.summary,
    operationId: `${contract.method.toLowerCase()}-${contract.path}`
      .replace(/[^A-Za-z0-9]+/g, '-')
      .replace(/^-|-$/g, ''),
    tags: [contract.access],
    'x-access': contract.access,
    parameters: [
      ...pathParameters(contract.path),
      ...(contract.queryParameters ?? []),
    ],
    responses: {
      [status]: status === '204'
        ? { description: 'Deleted' }
        : {
            description: 'Successful response',
            content: { 'application/json': { schema: responseSchema } },
          },
      '400': {
        description: 'Invalid or out-of-bounds request',
        content: { 'application/json': { schema: COMMON_ERROR_SCHEMA } },
      },
      '429': {
        description: 'Rate limit exceeded',
        content: { 'application/json': { schema: COMMON_ERROR_SCHEMA } },
      },
      '500': {
        description: 'Internal server error',
        content: { 'application/json': { schema: COMMON_ERROR_SCHEMA } },
      },
    },
  };
  for (const [additionalStatus, response] of Object.entries(contract.additionalResponses ?? {})) {
    (result['responses'] as Record<string, unknown>)[additionalStatus] = {
      description: response.description,
      content: { 'application/json': { schema: response.responseSchema } },
    };
  }
  if (contract.access === 'operator') {
    result['servers'] = [{
      url: 'http://127.0.0.1:3000',
      description: 'Local operator interface; use an SSH tunnel when remote access is required',
    }];
  }
  const auth = security(contract);
  if (auth) {
    result['security'] = auth;
    (result['responses'] as Record<string, unknown>)['401'] = {
      description: 'Authentication required',
      content: { 'application/json': { schema: COMMON_ERROR_SCHEMA } },
    };
    (result['responses'] as Record<string, unknown>)['403'] = {
      description: 'Authentication or CSRF validation failed',
      content: { 'application/json': { schema: COMMON_ERROR_SCHEMA } },
    };
  }
  if (contract.requestSchema) {
    result['requestBody'] = {
      required: true,
      content: { 'application/json': { schema: contract.requestSchema } },
    };
  }
  return result;
}

function buildPaths(): Record<string, Record<string, unknown>> {
  const paths: Record<string, Record<string, unknown>> = {};
  for (const [contract, apiPrefix] of [
    ...API_CONTRACTS.map((entry) => [entry, true] as const),
    ...OPERATOR_CONTRACTS.map((entry) => [entry, false] as const),
  ]) {
    const pathname = openApiPath(contract.path, apiPrefix);
    paths[pathname] ??= {};
    paths[pathname]![contract.method.toLowerCase()] = operation(contract);
  }
  return Object.fromEntries(Object.entries(paths).sort(([a], [b]) => a.localeCompare(b)));
}

function buildDocument(): string {
  return `${JSON.stringify({
    openapi: '3.1.0',
    info: {
      title: 'MeshCore Analytics API',
      version: '2.0.0',
      description: 'Runtime-validated contracts for privacy-filtered public, test, owner, and local operator endpoints.',
    },
    servers: [{ url: 'https://ukmesh.com' }],
    tags: [
      { name: 'public', description: 'Anonymous privacy-filtered endpoints' },
      { name: 'test', description: 'Local authenticated diagnostics' },
      { name: 'owner', description: 'Owner-session scoped endpoints' },
      { name: 'operator', description: 'Local-only operator endpoints' },
    ],
    paths: buildPaths(),
    components: {
      securitySchemes: {
        ownerSession: { type: 'apiKey', in: 'cookie', name: 'meshcore_owner_session' },
        operatorSession: { type: 'apiKey', in: 'cookie', name: 'meshcore_operator_session' },
        operatorToken: { type: 'http', scheme: 'bearer' },
      },
      schemas: { Error: COMMON_ERROR_SCHEMA },
    },
  }, null, 2)}\n`;
}

const outputPath = path.resolve(process.cwd(), '..', 'docs', 'openapi.yaml');
const generated = buildDocument();
if (process.argv.includes('--check')) {
  const current = fs.existsSync(outputPath) ? fs.readFileSync(outputPath, 'utf8') : '';
  if (current !== generated) {
    console.error('docs/openapi.yaml is stale; run npm run contract:generate');
    process.exitCode = 1;
  } else {
    console.log(`OpenAPI contract current: ${API_CONTRACTS.length} API and ${OPERATOR_CONTRACTS.length} operator routes`);
  }
} else {
  fs.writeFileSync(outputPath, generated);
  console.log(`Wrote ${outputPath}`);
}
