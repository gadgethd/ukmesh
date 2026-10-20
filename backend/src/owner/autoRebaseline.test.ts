import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import {
  decideOwnerBaselineAction,
  parseRefreshValidationSummary,
  refreshValidationPassed,
} from './autoRebaseline.js';

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
    status: options.ok === false ? 'degraded' : 'ready',
    checks: {
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
    },
  };
}

test('a current baseline is skipped even when another readiness check is degraded', () => {
  assert.deepEqual(decideOwnerBaselineAction(503, readyz()), {
    action: 'skip',
    reason: 'owner-baseline-current',
    mismatches: [],
  });
});

test('a settled owner ACL desired-generation mismatch is eligible for refresh', () => {
  assert.deepEqual(decideOwnerBaselineAction(503, readyz({
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
  const result = decideOwnerBaselineAction(503, readyz({
    ok: false,
    mismatches: ['aclDesiredGeneration', 'activeGrants'],
  }));
  assert.equal(result.action, 'abort');
  assert.equal(result.reason, 'unexpected-owner-baseline-mismatch');
});

test('unsettled ACL state, ACL errors, or shadow mode abort', () => {
  const unsettled = decideOwnerBaselineAction(503, readyz({
    ok: false,
    mismatches: ['aclDesiredGeneration'],
    applied: 'gen-1',
  }));
  assert.equal(unsettled.action, 'abort');
  assert.equal(unsettled.reason, 'owner-acl-generation-is-not-settled');

  const errored = decideOwnerBaselineAction(503, readyz({
    ok: false,
    mismatches: ['aclDesiredGeneration'],
    lastError: 'reconcile failed',
  }));
  assert.equal(errored.action, 'abort');

  const shadow = decideOwnerBaselineAction(503, readyz({
    ok: false,
    mismatches: ['aclDesiredGeneration'],
    aclMode: 'shadow',
  }));
  assert.equal(shadow.action, 'abort');
});

test('missing and inconsistent readiness data aborts', () => {
  assert.equal(decideOwnerBaselineAction(500, {}).action, 'abort');
  assert.equal(decideOwnerBaselineAction(503, { checks: {} }).action, 'abort');
  assert.equal(decideOwnerBaselineAction(503, readyz({ ok: false })).reason,
    'owner-baseline-failed-without-mismatch');
  assert.equal(decideOwnerBaselineAction(200, readyz({
    ok: false,
    mismatches: ['aclDesiredGeneration'],
  })).reason, 'owner-baseline-drift-without-degraded-readiness');
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
