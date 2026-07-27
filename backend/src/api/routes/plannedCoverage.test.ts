import assert from 'node:assert/strict';
import test from 'node:test';
import { plannedCoverageAdmissionDecision, plannedCoverageHandleDigest } from './plannedCoverage.js';

test('planned coverage capabilities use full-entropy SHA-256 handles', () => {
  const handle = `planv2_${'ab'.repeat(32)}`;
  const digest = plannedCoverageHandleDigest(handle);
  assert.equal(digest?.algorithm, 'sha256');
  assert.match(digest?.hash ?? '', /^[0-9a-f]{64}$/);
  assert.notEqual(digest?.hash, plannedCoverageHandleDigest(`planv2_${'cd'.repeat(32)}`)?.hash);
});

test('legacy plan capabilities remain dual-readable only in their exact format', () => {
  assert.equal(plannedCoverageHandleDigest('plan_0123456789abcdef')?.algorithm, 'md5');
  assert.equal(plannedCoverageHandleDigest('plan_0123456789abcde'), null);
  assert.equal(plannedCoverageHandleDigest('plannedv2_0123456789abcdef'), null);
  assert.equal(plannedCoverageHandleDigest('planv2_not-hex'), null);
});

test('planned coverage admission enforces every durable bound at its edge', () => {
  const base = {
    creatingJob: false,
    outstandingJobs: 1,
    outstandingHandles: 2,
    handlesForJob: 2,
    maxJobs: 3,
    maxHandles: 4,
    maxHandlesPerJob: 3,
  };
  assert.equal(plannedCoverageAdmissionDecision(base), null);
  assert.equal(plannedCoverageAdmissionDecision({
    ...base,
    creatingJob: true,
    outstandingJobs: 3,
  }), 'jobs');
  assert.equal(plannedCoverageAdmissionDecision({
    ...base,
    outstandingHandles: 4,
  }), 'handles');
  assert.equal(plannedCoverageAdmissionDecision({
    ...base,
    handlesForJob: 3,
  }), 'job_handles');
});
