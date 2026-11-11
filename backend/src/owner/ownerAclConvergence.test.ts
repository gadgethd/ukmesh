import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import test from 'node:test';
import {
  OWNER_ACL_RENDERER_VERSION, renderOwnerAcl, type OwnerAclGrant,
} from '../mqtt/aclManager.js';
import { convergeOwnerAcl, createOwnerAclReadinessRecovery, createOwnerAclReconciliation, type OwnerAclConvergencePorts } from './ownerAclConvergence.js';
import { buildOwnerInventoryBaseline, validateOwnerInventoryBaseline, type OwnerAclGenerationState } from './ownerInventoryBaseline.js';
import { isOwnerAclGenerationSettled, validateOwnerInventoryReadiness, type AppliedOwnerAclArtifact } from './ownerInventoryReadiness.js';

const NODE = 'A'.repeat(64);
const USER = 'owner';

function fixture() {
  let grants: OwnerAclGrant[] = [{ mqttUsername: USER, nodeIds: [NODE] }];
  const initial = renderOwnerAcl('', grants, []);
  let content = initial.content;
  let state: OwnerAclGenerationState = {
    desiredGeneration: initial.generation,
    renderedGeneration: initial.generation,
    appliedGeneration: initial.generation,
    lastError: null,
  };
  const artifacts = new Map<string, AppliedOwnerAclArtifact>([[initial.generation, {
    ...initial, rendererVersion: OWNER_ACL_RENDERER_VERSION,
  }]]);
  const calls = { state: 0, save: 0, write: 0, shadow: 0, reload: 0 };
  const ports: OwnerAclConvergencePorts = {
    readState: async () => ({ ...state }),
    loadAppliedArtifact: async (generation) => artifacts.get(generation) ?? null,
    updateState: async (update) => { calls.state++; state = { ...state, ...update }; },
    saveArtifact: async (artifact) => {
      calls.save++;
      if (artifact.applied) artifacts.set(artifact.generation, artifact);
    },
    readAcl: () => content,
    writeAcl: (value) => { calls.write++; content = value; },
    writeShadow: () => { calls.shadow++; },
    reload: async () => { calls.reload++; },
  };
  const inventory = () => buildOwnerInventoryBaseline({
    accounts: [{
      mqttUsername: USER, isActive: true, nodeId: NODE,
      verificationMethod: 'operator-database', verifiedAt: '2026-10-01', revokedAt: null,
    }],
    configuredGrants: [], aclContent: content, aclState: { ...state },
  });
  return {
    initial, calls, ports, artifacts, inventory,
    get content() { return content; },
    set content(value: string) { content = value; },
    get state() { return state; },
    set state(value: OwnerAclGenerationState) { state = value; },
    set grants(value: OwnerAclGrant[]) { grants = value; },
    run: (mode: 'apply' | 'shadow' = 'apply', allowEmptyCutover = false) => convergeOwnerAcl({
      mode, currentContent: content,
      rendered: renderOwnerAcl(content, grants, [], [USER]), allowEmptyCutover,
    }, ports),
    ready: (baseline: ReturnType<typeof inventory>) => validateOwnerInventoryReadiness(baseline, inventory(), {
      aclContent: content, loadAppliedArtifact: ports.loadAppliedArtifact,
    }),
  };
}

test('repeated authoritative passes leave a verified policy, artifacts and broker untouched', async () => {
  const f = fixture();
  const baseline = f.inventory();
  for (let index = 0; index < 3; index++) assert.equal(await f.run(), 'unchanged');
  assert.deepEqual(f.calls, { state: 0, save: 0, write: 0, shadow: 0, reload: 0 });
  assert.equal(f.content, f.initial.content);
  assert.equal(validateOwnerInventoryBaseline(baseline, f.inventory()).ok, true);
});

test('a toggle after input capture waits for a trailing pass that observes the new membership', async () => {
  const f = fixture();
  let enabled = true;
  let passes = 0;
  let entered!: () => void;
  let release!: () => void;
  const captured = new Promise<void>((resolve) => { entered = resolve; });
  const blocked = new Promise<void>((resolve) => { release = resolve; });
  f.ports.reload = async () => { f.calls.reload++; if (passes === 1) { entered(); await blocked; } };
  const reconciler = createOwnerAclReconciliation(async () => {
    passes++;
    // This dependency read models the repository's committed reader query.
    f.grants = [{ mqttUsername: USER, nodeIds: [NODE], ...(enabled ? { publicFeedRead: true } : {}) }];
    await f.run();
  });
  const older = reconciler.reconcile();
  await captured;
  enabled = false;
  let acknowledged = false;
  const afterWrite = reconciler.reconcile().then(() => { acknowledged = true; });
  await Promise.resolve();
  assert.equal(acknowledged, false);
  release();
  await Promise.all([older, afterWrite]);
  assert.equal(passes, 2);
  assert.doesNotMatch(f.content, /topic read public\/\+\/\+\/packets/);
  assert.equal(isOwnerAclGenerationSettled(f.state), true);
  await reconciler.waitForIdle();
});

test('bursts coalesce, writes during a trailing pass cause another pass, and failed drains retry', async () => {
  let passes = 0;
  let release!: () => void;
  let entered!: () => void;
  let fail = false;
  const captured = new Promise<void>((resolve) => { entered = resolve; });
  const blocked = new Promise<void>((resolve) => { release = resolve; });
  const reconciler = createOwnerAclReconciliation(async () => {
    passes++;
    if (passes === 1) { entered(); await blocked; }
    if (passes === 2) void reconciler.reconcile();
    if (fail) throw new Error('apply unavailable');
  });
  const first = reconciler.reconcile();
  const beforeCapture = reconciler.reconcile();
  await captured;
  assert.equal(passes, 1);
  const duringCapture = reconciler.reconcile();
  release();
  await Promise.all([first, beforeCapture, duringCapture]);
  assert.equal(passes, 3);
  fail = true;
  await assert.rejects(reconciler.reconcile(), /apply unavailable/);
  fail = false;
  await reconciler.reconcile();
  assert.equal(passes, 5);
  await reconciler.waitForIdle();
});

test('a failed trailing pass never acknowledges the write that requested it', async () => {
  let entered!: () => void;
  let release!: () => void;
  const captured = new Promise<void>((resolve) => { entered = resolve; });
  const blocked = new Promise<void>((resolve) => { release = resolve; });
  let passes = 0;
  const reconciler = createOwnerAclReconciliation(async () => {
    if (++passes === 1) { entered(); await blocked; }
    else throw new Error('trailing apply failed');
  });
  const first = reconciler.reconcile();
  await captured;
  const postWrite = reconciler.reconcile();
  const rejected = [assert.rejects(first, /trailing apply failed/), assert.rejects(postWrite, /trailing apply failed/)];
  release();
  await Promise.all(rejected);
});

test('an out-of-band desired/rendered bump closes the gate, self-heals and stays converged', async (t) => {
  const f = fixture();
  const baseline = f.inventory();
  f.state = { ...f.state, desiredGeneration: 'spurious', renderedGeneration: 'spurious' };
  assert.equal(isOwnerAclGenerationSettled(f.state), false);
  assert.equal((await f.ready(baseline)).ok, false);
  const recover = createOwnerAclReadinessRecovery({
    readState: f.ports.readState,
    reconcile: async () => { assert.equal(await f.run(), 'recovered'); },
    reportError: (error) => { throw error; },
  });
  assert.equal(isOwnerAclGenerationSettled(await recover()), true);
  assert.equal((await f.ready(baseline)).ok, true);
  assert.equal(await f.run(), 'unchanged');
  assert.deepEqual(f.calls, { state: 1, save: 0, write: 0, shadow: 0, reload: 0 });
  t.diagnostic('STATE-ONLY BUMP: gate=false -> automatic recovery -> gate=true; broker reloads=0; repeat=unchanged');
});

test('a spurious settled generation and file marker return to the authoritative policy on startup pass', async () => {
  const f = fixture();
  const canonical = f.inventory();
  const spurious = 'f'.repeat(64);
  f.state = { desiredGeneration: spurious, renderedGeneration: spurious, appliedGeneration: spurious, lastError: null };
  f.content = f.content.replaceAll(f.initial.generation, spurious);
  f.artifacts.set(spurious, {
    generation: spurious, rendererVersion: OWNER_ACL_RENDERER_VERSION,
    content: f.content, contentSha256: createHash('sha256').update(f.content).digest('hex'),
  });
  const driftedBaseline = f.inventory();
  assert.equal(validateOwnerInventoryBaseline(canonical, driftedBaseline).ok, false);
  assert.equal(await f.run(), 'applied');
  assert.equal(f.content, f.initial.content);
  assert.equal(validateOwnerInventoryBaseline(canonical, f.inventory()).ok, true);
  // Even a baseline recorded before canonical repair can converge without
  // rewriting its file: both durable artifacts prove identical permissions.
  assert.equal((await f.ready(driftedBaseline)).ok, true);
  assert.equal(await f.run(), 'unchanged');
  assert.equal(f.calls.reload, 1);
});

test('enable/disable cycles converge automatically while non-feed permission changes stay fenced', async () => {
  const f = fixture();
  const baseline = f.inventory();
  f.grants = [{ mqttUsername: USER, nodeIds: [NODE], publicFeedRead: true }];
  assert.equal(await f.run(), 'applied');
  const enabled = f.inventory();
  assert.equal((await f.ready(baseline)).ok, true);
  assert.equal(await f.run(), 'unchanged');
  f.grants = [{ mqttUsername: USER, nodeIds: [NODE] }];
  assert.equal(await f.run(), 'applied');
  assert.equal((await f.ready(enabled)).ok, true);
  assert.equal(f.state.appliedGeneration, baseline.aclState.appliedGeneration);
  assert.equal(await f.run(), 'unchanged');
  assert.equal(f.calls.reload, 2);

  f.grants = [{ mqttUsername: USER, nodeIds: [NODE, 'B'.repeat(64)] }];
  assert.equal(await f.run(), 'applied');
  assert.equal((await f.ready(baseline)).ok, false);
});

test('missing, corrupt or incompatible applied evidence requires verified application', async () => {
  for (const damage of ['missing', 'hash', 'content', 'renderer', 'generation']) {
    const f = fixture();
    const artifact = f.artifacts.get(f.initial.generation)!;
    if (damage === 'missing') f.artifacts.clear();
    else f.artifacts.set(f.initial.generation, {
      ...artifact,
      ...(damage === 'hash' ? { contentSha256: 'corrupt' } : {}),
      ...(damage === 'content' ? { content: 'unverified' } : {}),
      ...(damage === 'renderer' ? { rendererVersion: 'different' } : {}),
      ...(damage === 'generation' ? { generation: 'different' } : {}),
    });
    assert.equal(await f.run(), 'applied', damage);
    assert.equal(f.calls.reload, 1, damage);
    assert.equal(await f.run(), 'unchanged', damage);
  }
});

test('a failed or interrupted apply stays fenced until reload and readback succeed', async () => {
  const f = fixture();
  const baseline = f.inventory();
  f.state = { ...f.state, lastError: 'OWNER_ACL_APPLY_PENDING' };
  f.ports.reload = async () => { throw new Error('reload unavailable'); };
  await assert.rejects(f.run(), /reload unavailable/);
  assert.equal(f.content, f.initial.content);
  assert.equal(isOwnerAclGenerationSettled(f.state), false);
  assert.equal((await f.ready(baseline)).ok, false);
  f.ports.reload = async () => { f.calls.reload++; };
  assert.equal(await f.run(), 'applied');
  assert.equal((await f.ready(baseline)).ok, true);
  assert.equal(await f.run(), 'unchanged');
});

test('readback failure before or after reload rolls back the file and never certifies the new generation', async () => {
  for (const timing of ['before', 'after']) {
    const f = fixture();
    const oldGeneration = f.state.appliedGeneration;
    f.grants = [{ mqttUsername: USER, nodeIds: [NODE], publicFeedRead: true }];
    if (timing === 'before') f.ports.readAcl = () => 'tampered';
    else f.ports.reload = async () => { f.content = 'tampered'; };
    await assert.rejects(f.run(), /READBACK_HASH_MISMATCH/);
    assert.equal(f.content, f.initial.content);
    assert.equal(f.state.appliedGeneration, oldGeneration);
    assert.equal(isOwnerAclGenerationSettled(f.state), false);
    assert.equal(f.artifacts.size, 1);
  }
});

test('a no-op rechecks the file before declaring convergence', async () => {
  const f = fixture();
  f.ports.readAcl = () => 'changed during observation';
  await assert.rejects(f.run(), /READBACK_HASH_MISMATCH/);
  assert.deepEqual(f.calls, { state: 0, save: 0, write: 0, shadow: 0, reload: 0 });
});

test('invalid/empty cutovers remain blocked and shadow never writes or reloads the broker ACL', async () => {
  const f = fixture();
  f.grants = [{ mqttUsername: USER, nodeIds: [] }];
  await assert.rejects(f.run(), /EMPTY_CUTOVER_BLOCKED/);
  assert.equal(f.calls.write, 0);
  assert.equal(await f.run('shadow'), 'shadow');
  assert.equal(f.calls.write, 0);
  assert.equal(f.calls.reload, 0);
  assert.equal(f.state.appliedGeneration, f.initial.generation);
  assert.equal(f.content, f.initial.content);
});

test('readiness coalesces simultaneous recovery requests and rate-limits failures before retrying', async () => {
  const f = fixture();
  f.state = { ...f.state, desiredGeneration: 'spurious' };
  let clock = 1;
  let attempts = 0;
  let errors = 0;
  let fail = true;
  let release!: () => void;
  const blocked = new Promise<void>((resolve) => { release = resolve; });
  const recover = createOwnerAclReadinessRecovery({
    readState: f.ports.readState,
    reconcile: async () => {
      attempts++;
      await blocked;
      if (fail) throw new Error('temporarily unavailable');
      await f.run();
    },
    reportError: () => { errors++; },
    now: () => clock,
  });
  const pending = [recover(), recover(), recover()];
  await new Promise<void>((resolve) => setImmediate(resolve));
  assert.equal(attempts, 1);
  release();
  for (const state of await Promise.all(pending)) assert.equal(isOwnerAclGenerationSettled(state), false);
  assert.equal(errors, 1);
  assert.equal(isOwnerAclGenerationSettled(await recover()), false);
  assert.equal(attempts, 1);
  clock += 10_000;
  fail = false;
  assert.equal(isOwnerAclGenerationSettled(await recover()), true);
  assert.equal(attempts, 2);
  assert.equal(isOwnerAclGenerationSettled(await recover()), true);
  assert.equal(attempts, 2);
});
