import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import {
  decideOwnerBaselineAction,
  parseRefreshValidationSummary,
  refreshValidationPassed,
  readOwnerBaselineDecision,
} from './autoRebaseline.js';
import { OWNER_READINESS_FORMAT, publicReadinessPayload } from './ownerReadiness.js';

test('current coarse readiness does not require the removed public owner fields', () => {
  const checks = {
    database: true,
    mqtt: { state: 'connected' },
    get ownerAuthorization(): never { throw new Error('stale public field access'); },
  };
  assert.deepEqual(decideOwnerBaselineAction(200, { status: 'ready', checks, ts: 0 }), {
    action: 'skip', reason: 'backend-ready', mismatches: [],
  });
  assert.deepEqual(decideOwnerBaselineAction(503, { status: 'degraded', checks, ts: 0 }), {
    action: 'abort', reason: 'owner-baseline-diagnostics-required', mismatches: [],
  });
});

function readyz(options: {
  ok?: boolean;
  mismatches?: string[];
  aclMode?: string;
  desired?: string | null;
  rendered?: string | null;
  applied?: string | null;
  lastError?: string | null;
} = {}): unknown {
  return {
    format: OWNER_READINESS_FORMAT,
      ownerAuthorization: {
        aclMode: options.aclMode ?? 'apply',
        desiredGeneration: options.desired ?? 'gen-2',
        renderedGeneration: options.rendered ?? 'gen-2',
        appliedGeneration: options.applied ?? 'gen-2',
        lastError: options.lastError ?? null,
        inventoryBaseline: {
          required: true,
          ok: options.ok ?? true,
          mismatches: options.mismatches ?? [],
        },
      },
  };
}

function decideWithDiagnostics(status: number, diagnostics: unknown) {
  return decideOwnerBaselineAction(status, publicReadinessPayload(status === 200, true, 'connected', 0), diagnostics);
}

test('a current baseline is skipped even when another readiness check is degraded', () => {
  assert.deepEqual(decideWithDiagnostics(503, readyz()), {
    action: 'skip',
    reason: 'owner-baseline-current',
    mismatches: [],
  });
});

test('a settled owner ACL desired-generation mismatch is eligible for refresh', () => {
  assert.deepEqual(decideWithDiagnostics(503, readyz({
    ok: false,
    mismatches: [
      'aclDesiredGeneration',
      'aclRenderedGeneration',
      'aclAppliedGeneration',
      'aclReadback',
    ],
  })), {
    action: 'rebaseline',
    reason: 'settled-owner-acl-generation-drift',
    mismatches: [
      'aclDesiredGeneration',
      'aclRenderedGeneration',
      'aclAppliedGeneration',
      'aclReadback',
    ],
  });
});

test('unrelated inventory mismatches abort instead of broadening the refresh trigger', () => {
  const result = decideWithDiagnostics(503, readyz({
    ok: false,
    mismatches: ['aclDesiredGeneration', 'activeGrants'],
  }));
  assert.equal(result.action, 'abort');
  assert.equal(result.reason, 'unexpected-owner-baseline-mismatch');
});

test('unsettled ACL state, ACL errors, or shadow mode abort', () => {
  const unsettled = decideWithDiagnostics(503, readyz({
    ok: false,
    mismatches: ['aclDesiredGeneration'],
    applied: 'gen-1',
  }));
  assert.equal(unsettled.action, 'abort');
  assert.equal(unsettled.reason, 'owner-acl-generation-is-not-settled');

  const errored = decideWithDiagnostics(503, readyz({
    ok: false,
    mismatches: ['aclDesiredGeneration'],
    lastError: 'reconcile failed',
  }));
  assert.equal(errored.action, 'abort');

  const shadow = decideWithDiagnostics(503, readyz({
    ok: false,
    mismatches: ['aclDesiredGeneration'],
    aclMode: 'shadow',
  }));
  assert.equal(shadow.action, 'abort');
});

test('missing and inconsistent readiness data aborts', () => {
  assert.equal(decideOwnerBaselineAction(500, {}).action, 'abort');
  assert.equal(decideOwnerBaselineAction(503, { checks: {} }).action, 'abort');
  assert.equal(decideWithDiagnostics(503, readyz({ ok: false })).reason,
    'owner-baseline-failed-without-mismatch');
  assert.equal(decideWithDiagnostics(200, readyz({
    ok: false,
    mismatches: ['aclDesiredGeneration'],
  })).reason, 'owner-baseline-drift-without-degraded-readiness');
});

test('consumer follows the actual public producer contract and authenticates only diagnostics', async () => {
  const token = 'synthetic-operator-token-for-component-test';
  const calls: Array<{ url: string; init?: RequestInit }> = [];
  const request: typeof fetch = async (url, init) => {
    calls.push({ url: String(url), init });
    return calls.length === 1
      ? Response.json(publicReadinessPayload(false, true, 'connected', 0), { status: 503 })
      : Response.json(readyz({ ok: false, mismatches: ['aclDesiredGeneration'] }));
  };
  assert.equal((await readOwnerBaselineDecision('http://127.0.0.1:3000/readyz', token, request)).action, 'rebaseline');
  assert.deepEqual(calls.map((call) => call.url), [
    'http://127.0.0.1:3000/readyz', 'http://127.0.0.1:3000/local-api/owner-authorization',
  ]);
  assert.equal(calls[0].init?.headers, undefined);
  assert.equal(new Headers(calls[1].init?.headers).get('authorization'), `Bearer ${token}`);
  assert.ok(calls.every((call) => call.init?.redirect === 'error'));
});

test('healthy readiness needs no credentials; missing credentials and denied diagnostics cannot refresh', async () => {
  let calls = 0;
  const healthy: typeof fetch = async () => { calls++; return Response.json(publicReadinessPayload(true, true, 'connected')); };
  assert.equal((await readOwnerBaselineDecision('http://localhost/readyz', undefined, healthy)).action, 'skip');
  assert.equal(calls, 1);
  const degraded: typeof fetch = async () => Response.json(publicReadinessPayload(false, true, 'connected'), { status: 503 });
  assert.equal((await readOwnerBaselineDecision('http://localhost/readyz', undefined, degraded)).reason,
    'owner-baseline-operator-token-required');
  const denied: typeof fetch = async (url) => String(url).endsWith('/readyz')
    ? degraded(url) : new Response(null, { status: 401 });
  assert.equal((await readOwnerBaselineDecision('http://localhost/readyz', 'a'.repeat(32), denied)).reason,
    'owner-baseline-diagnostics-http-401');
  assert.equal(decideWithDiagnostics(503, { ...readyz() as object, format: 'wrong' }).action, 'abort');
});

test('refresh validation parses the refresh script summary and requires exit code zero', () => {
  // Mirrors the summary emitted by scripts/refresh-owner-baseline.sh; fixture values are synthetic.
  const output = readFileSync(new URL('./fixtures/refresh-owner-baseline-output.txt', import.meta.url), 'utf8');
  const summary = parseRefreshValidationSummary(output);
  assert.deepEqual(summary, {
    exportPath: '/tmp/owner-baseline-validation-abc123/owner-grants-20260922T120000Z-rebaseline.json',
    generatedAt: '2026-09-22T12:00:00.000Z',
    counts: {
      activeAccounts: 2,
      activeGrants: 2,
      operatorConfig: 1,
      operatorDatabase: 1,
      legacyOrNullMethod: 0,
      configuredGrants: 2,
      aclGrants: 2,
    },
    configuredGeneration: '0123456789ab',
    aclDesiredGeneration: 'abcdef012345',
    contentSha256: 'fedcba987654',
    baselineOk: true,
    mismatches: [],
  });
  assert.equal(refreshValidationPassed(0, summary), true);
  assert.equal(refreshValidationPassed(1, summary), false);
  assert.equal(refreshValidationPassed(0, { baselineOk: false }), false);
  assert.equal(refreshValidationPassed(0, null), false);
  assert.equal(parseRefreshValidationSummary('no machine-readable summary'), null);
});
