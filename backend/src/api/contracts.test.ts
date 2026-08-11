import assert from 'node:assert/strict';
import test from 'node:test';
import { Router } from 'express';
import {
  API_CONTRACTS,
  assertContractCoverage,
  contractKey,
} from './contracts.js';

function routerFromContracts() {
  const router = Router();
  for (const contract of API_CONTRACTS) {
    router[contract.method.toLowerCase() as 'get' | 'post' | 'delete'](
      contract.path,
      (_req, res) => res.end(),
    );
  }
  return router;
}

test('API contracts are unique and cover their registered runtime routes exactly', () => {
  assert.equal(new Set(API_CONTRACTS.map(contractKey)).size, API_CONTRACTS.length);
  assert.doesNotThrow(() => assertContractCoverage(routerFromContracts()));
});

test('API contract coverage fails for missing and stale registrations', () => {
  const missing = routerFromContracts();
  missing.get('/uncontracted', (_req, res) => res.end());
  assert.throws(() => assertContractCoverage(missing), /routes without contracts: GET \/uncontracted/);

  const stale = Router();
  stale.get('/health', (_req, res) => res.end());
  assert.throws(() => assertContractCoverage(stale), /contracts without routes/);
});

test('planned-node public response contract has a closed privacy allowlist', () => {
  const contract = API_CONTRACTS.find((entry) => entry.path === '/planned-nodes');
  const serialized = JSON.stringify(contract?.responseSchema);
  assert.match(serialized, /publishedAt/);
  assert.doesNotMatch(serialized, /owner_pubkey|notes|published_by/);
});

test('slow path contracts cover status, mode query, and pending response', () => {
  const status = API_CONTRACTS.find((entry) => entry.path === '/path-beta/slow-mode');
  assert.deepEqual(
    (status?.responseSchema?.['required'] as string[] | undefined)?.sort(),
    ['enabled', 'pending', 'pendingMax', 'windowMs'],
  );

  const resolveMulti = API_CONTRACTS.find((entry) => entry.path === '/path-beta/resolve-multi');
  assert.deepEqual(
    resolveMulti?.queryParameters?.[0]?.['schema'],
    { type: 'string', enum: ['fast', 'slow'] },
  );
  assert.equal(
    resolveMulti?.additionalResponses?.['202']?.responseSchema['properties'] != null,
    true,
  );
});
