import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import fs from 'node:fs';
import test from 'node:test';
import { OWNER_ACL_RENDERER_VERSION, renderOwnerAcl, type OwnerAclGrant } from '../mqtt/aclManager.js';
import { buildOwnerInventoryBaseline, loadOwnerInventoryBaseline, validateOwnerInventoryBaseline } from './ownerInventoryBaseline.js';
import {
  classifyRenderedOwnerAclDelta,
  validateOwnerInventoryReadiness,
  type AppliedOwnerAclArtifact,
} from './ownerInventoryReadiness.js';

const NODE = 'A'.repeat(64);
const OTHER_NODE = 'B'.repeat(64);
const USER = 'arbitrary.owner@example';
const feed = 'topic read public/+/+/packets';

function fixture(options: { feedRead?: boolean; systemRead?: boolean; nodeIds?: string[]; existing?: string } = {}) {
  const grants: OwnerAclGrant[] = [{
    mqttUsername: USER,
    nodeIds: options.nodeIds ?? [NODE],
    publicFeedRead: options.feedRead,
  }];
  const rendered = renderOwnerAcl(options.existing ?? '', grants, ['unmanaged'], [USER], options.systemRead ? [USER] : []);
  assert.equal(rendered.validation.ok, true);
  const baselineInput = {
    accounts: [{
      mqttUsername: USER, isActive: true, nodeId: NODE,
      verificationMethod: 'operator-database', verifiedAt: '2026-10-01', revokedAt: null,
    }],
    configuredGrants: [],
    aclContent: rendered.content,
    aclState: {
      desiredGeneration: rendered.generation,
      renderedGeneration: rendered.generation,
      appliedGeneration: rendered.generation,
      lastError: null,
    },
  };
  const inventory = buildOwnerInventoryBaseline(baselineInput);
  const artifact: AppliedOwnerAclArtifact = {
    generation: rendered.generation,
    rendererVersion: OWNER_ACL_RENDERER_VERSION,
    content: rendered.content,
    contentSha256: rendered.contentSha256,
  };
  return { inventory, artifact, baselineInput };
}

function check(previous: ReturnType<typeof fixture>, current: ReturnType<typeof fixture>) {
  const artifacts = new Map([previous.artifact, current.artifact].map((artifact) => [artifact.generation, artifact]));
  return validateOwnerInventoryReadiness(previous.inventory, current.inventory, {
    aclContent: current.artifact.content,
    loadAppliedArtifact: async (generation) => artifacts.get(generation) ?? null,
  });
}

test('feed enable and disable pass readiness while strict inventory comparison still rejects drift', async () => {
  const withoutFeed = fixture();
  const withFeed = fixture({ feedRead: true });
  for (const [previous, current] of [[withoutFeed, withFeed], [withFeed, withoutFeed]] as const) {
    assert.deepEqual(validateOwnerInventoryBaseline(previous.inventory, current.inventory), {
      ok: false,
      mismatches: ['aclDesiredGeneration', 'aclRenderedGeneration', 'aclAppliedGeneration'],
    });
    assert.deepEqual(await check(previous, current), {
      ok: true, mismatches: [], reason: 'only-public-feed-read-grants-changed',
    });
  }
});

test('a checksummed baseline can retain the missing historical applied certificate', async () => {
  const previous = fixture();
  const current = fixture({ feedRead: true });
  const baseline = buildOwnerInventoryBaseline({ ...previous.baselineInput, appliedAclArtifact: previous.artifact });
  const result = await validateOwnerInventoryReadiness(baseline, current.inventory, {
    aclContent: current.artifact.content,
    loadAppliedArtifact: async (generation) => generation === current.artifact.generation ? current.artifact : null,
  });
  assert.equal(result.ok, true);
  assert.equal(result.reason, 'only-public-feed-read-grants-changed');
});

test('legacy missing-history repair is deterministic and does not open readiness', async () => {
  const previous = fixture();
  const current = fixture({ feedRead: true });
  const result = await validateOwnerInventoryReadiness(previous.inventory, current.inventory, {
    aclContent: current.artifact.content,
    loadAppliedArtifact: async (generation) => generation === current.artifact.generation ? current.artifact : null,
  });
  assert.equal(result.ok, false);
  assert.equal(result.reason, 'applied-acl-artifact-missing');
  assert.deepEqual(result.repair, {
    kind: 'restore-owner-acl-history', generation: previous.artifact.generation,
    baselineSha256: previous.inventory.contentSha256,
    currentGeneration: current.artifact.generation, reviewRequired: true,
  });
});

test('retained history cannot hide corrupt live evidence, unsafe deltas or baseline tampering', async () => {
  const previous = fixture();
  const baseline = buildOwnerInventoryBaseline({ ...previous.baselineInput, appliedAclArtifact: previous.artifact });
  const current = fixture({ feedRead: true });
  const check = (snapshot = baseline, latest = current.artifact, historical: AppliedOwnerAclArtifact | null = null) =>
    validateOwnerInventoryReadiness(snapshot, current.inventory, {
      aclContent: current.artifact.content,
      loadAppliedArtifact: async (generation) => generation === current.artifact.generation ? latest : historical,
    });
  assert.equal((await check({ ...baseline, contentSha256: '0'.repeat(64) })).reason, 'owner-baseline-evidence-invalid');
  assert.equal((await check(baseline, { ...current.artifact, contentSha256: 'bad' })).ok, false);
  assert.equal((await check(baseline, current.artifact, { ...previous.artifact, contentSha256: 'bad' })).ok, false);
  for (const artifact of [
    { ...previous.artifact, generation: 'wrong' },
    { ...previous.artifact, contentSha256: 'bad' },
    { ...previous.artifact, rendererVersion: 'older-renderer' },
  ]) {
    const { contentSha256: _checksum, ...content } = baseline;
    const changed = { ...content, appliedAclArtifact: artifact };
    const snapshot = { ...changed, contentSha256: createHash('sha256').update(JSON.stringify(changed)).digest('hex') };
    assert.equal((await check(snapshot)).ok, false);
  }
  assert.equal((await validateOwnerInventoryReadiness(baseline, current.inventory, {
    aclContent: current.artifact.content, loadAppliedArtifact: async () => null,
  })).ok, false);
  const nonFeed = fixture({ feedRead: true, systemRead: true });
  const changed = await validateOwnerInventoryReadiness(baseline, nonFeed.inventory, {
    aclContent: nonFeed.artifact.content,
    loadAppliedArtifact: async (generation) => generation === nonFeed.artifact.generation ? nonFeed.artifact : null,
  });
  assert.equal(changed.ok, false);
  assert.equal(changed.reason, 'non-feed-user-directive-changed');
});

test('baseline export rejects unverified bytes and its file checksum protects retained evidence', (t) => {
  const previous = fixture();
  for (const artifact of [
    { ...previous.artifact, generation: 'wrong' },
    { ...previous.artifact, contentSha256: 'wrong' },
    { ...previous.artifact, rendererVersion: 'unverified-renderer' },
    { ...previous.artifact, content: 'not-the-applied-file' },
  ]) assert.throws(() => buildOwnerInventoryBaseline({ ...previous.baselineInput, appliedAclArtifact: artifact }),
    /OWNER_INVENTORY_APPLIED_ARTIFACT_INVALID/);
  assert.throws(() => buildOwnerInventoryBaseline({
    ...previous.baselineInput, aclState: { ...previous.baselineInput.aclState, lastError: 'apply pending' },
    appliedAclArtifact: previous.artifact,
  }), /OWNER_INVENTORY_APPLIED_ARTIFACT_INVALID/);
  const baseline = buildOwnerInventoryBaseline({ ...previous.baselineInput, appliedAclArtifact: previous.artifact });
  let serialized = JSON.stringify(baseline);
  t.mock.method(fs, 'readFileSync', () => serialized);
  assert.deepEqual(loadOwnerInventoryBaseline('in-memory-fixture'), baseline);
  serialized = JSON.stringify({ ...baseline, appliedAclArtifact: { ...previous.artifact, content: 'tampered' } });
  assert.throws(() => loadOwnerInventoryBaseline('in-memory-fixture'), /CHECKSUM_INVALID/);
  serialized = JSON.stringify(previous.inventory);
  assert.deepEqual(loadOwnerInventoryBaseline('in-memory-fixture'), previous.inventory);
});

test('an empty delta passes without requiring historical artifacts', async () => {
  const current = fixture();
  const result = await validateOwnerInventoryReadiness(current.inventory, current.inventory, {
    aclContent: current.artifact.content,
    loadAppliedArtifact: async () => { throw new Error('must not load artifacts'); },
  });
  assert.deepEqual(result, { ok: true, mismatches: [], reason: 'owner-baseline-current' });
  assert.equal(classifyRenderedOwnerAclDelta(current.artifact.content, current.artifact.content).kind, 'empty');
});

test('spurious out-of-band ACL generation converges when verified applied policy is unchanged', async (t) => {
  const previous = fixture();
  const current = fixture();
  const bumpedGeneration = 'f'.repeat(64);
  current.inventory.aclState = {
    desiredGeneration: bumpedGeneration,
    renderedGeneration: bumpedGeneration,
    appliedGeneration: bumpedGeneration,
    lastError: null,
  };
  current.artifact = {
    ...current.artifact,
    generation: bumpedGeneration,
    content: current.artifact.content.replaceAll(previous.artifact.generation, bumpedGeneration),
  };
  current.artifact.contentSha256 = createHash('sha256').update(current.artifact.content).digest('hex');
  assert.deepEqual(validateOwnerInventoryBaseline(previous.inventory, current.inventory), {
    ok: false,
    mismatches: ['aclDesiredGeneration', 'aclRenderedGeneration', 'aclAppliedGeneration'],
  });
  const result = await check(previous, current);
  t.diagnostic(`OUT-OF-BAND GENERATION: strict baseline=false; readiness=${result.ok}; reason=${result.reason}`);
  assert.deepEqual(result, { ok: true, mismatches: [], reason: 'rendered-acl-policy-unchanged' });
});

test('feed changes mixed with non-feed permissions fail closed, including permissions missed by node readback', async () => {
  const previous = fixture({ existing: 'user unmanaged\ntopic read private/#\n' });
  const current = fixture({ feedRead: true, existing: 'user unmanaged\ntopic readwrite private/#\n' });
  const result = await check(previous, current);
  assert.equal(result.ok, false);
  assert.equal(result.reason, 'non-feed-user-directive-changed');
  assert.deepEqual(result.mismatches, ['aclDesiredGeneration', 'aclRenderedGeneration', 'aclAppliedGeneration']);
  assert.equal((await check(fixture(), fixture({ feedRead: true, nodeIds: [NODE, OTHER_NODE] }))).ok, false);
});

test('revoking a non-feed grant fails even when the owner node still appears in readback', async () => {
  const previous = fixture({ systemRead: true });
  const current = fixture();
  assert.deepEqual(previous.inventory.aclGrants, current.inventory.aclGrants);
  const result = await check(previous, current);
  assert.equal(result.ok, false);
  assert.equal(result.reason, 'non-feed-user-directive-changed');
  assert.equal((await check(fixture({ feedRead: true }), fixture({ nodeIds: [] }))).ok, false);

  const oldAcl = fixture().artifact.content;
  const newAcl = oldAcl.replace(`topic write meshcore-test/+/${NODE}/status\n`, '');
  assert.equal(classifyRenderedOwnerAclDelta(oldAcl, newAcl).kind, 'other');
});

test('scope comparison binds feed reads to their user and permits pure reader stanzas', () => {
  assert.equal(classifyRenderedOwnerAclDelta(
    `user first\n${feed}\nuser second\n`,
    `user first\nuser second\n${feed}\n`,
  ).kind, 'public-feed-only');
  for (const [before, after] of [
    ['', `user subscriber\n${feed}\n`],
    [`user subscriber\n${feed}\n`, ''],
    [`user subscriber\n`, `user subscriber\n${feed}\n`],
  ]) assert.equal(classifyRenderedOwnerAclDelta(before!, after!).kind, 'public-feed-only');

  for (const directive of [
    'topic readwrite public/+/+/packets', 'topic write public/+/+/packets',
    'topic read public/#', 'topic read meshcore/+/+/packets', 'pattern read public/%u/#',
  ]) {
    assert.equal(classifyRenderedOwnerAclDelta('user owner\n', `user owner\n${feed}\n${directive}\n`).kind, 'other');
  }
  assert.equal(classifyRenderedOwnerAclDelta('', `${feed}\n`).kind, 'other');
  assert.equal(classifyRenderedOwnerAclDelta('user owner\n', `user owner\n${feed}\nuser empty\n`).kind, 'other');
});

test('malformed or duplicate stanzas never qualify as feed-only changes', () => {
  for (const content of [
    `user owner\n${feed}\nuser owner\n`,
    `user owner\n${feed}\ninvalid directive\n`,
    `# BEGIN MESHCORE OWNER ACL\nuser owner\n${feed}\n`,
  ]) assert.equal(classifyRenderedOwnerAclDelta('user owner\n', content).kind, 'other');
});

test('every non-generation inventory mismatch remains fail closed alongside feed changes', async () => {
  const previous = fixture();
  const current = fixture({ feedRead: true });
  const mutations = [
    { counts: { ...current.inventory.counts, activeGrants: 0 } },
    { activeGrants: [] },
    { configuredGeneration: 'different' },
    { aclGrants: [] },
    { aclState: { ...current.inventory.aclState, lastError: 'reload failed' } },
  ];
  for (const mutation of mutations) {
    const modified = { ...current, inventory: { ...current.inventory, ...mutation } };
    assert.equal((await check(previous, modified)).ok, false);
  }
});

test('unsettled baseline or current generations cannot use the readiness exception', async () => {
  for (const side of ['baseline', 'current']) {
    for (const field of ['desiredGeneration', 'renderedGeneration', 'appliedGeneration'] as const) {
      const previous = fixture();
      const current = fixture({ feedRead: true });
      const target = side === 'baseline' ? previous : current;
      target.inventory.aclState[field] = 'unsettled';
      const result = await check(previous, current);
      assert.equal(result.ok, false);
      assert.equal(result.reason, 'owner-acl-generation-not-settled');
    }
  }
});

test('missing, corrupt, or unreadable applied artifacts leave generation mismatches intact', async () => {
  const previous = fixture();
  const current = fixture({ feedRead: true });
  for (const loadAppliedArtifact of [
    async () => null,
    async () => { throw new Error('database unavailable'); },
    async () => ({ ...previous.artifact, contentSha256: 'invalid' }),
    async () => ({ ...current.artifact, generation: previous.artifact.generation }),
  ]) {
    const result = await validateOwnerInventoryReadiness(previous.inventory, current.inventory, {
      aclContent: current.artifact.content, loadAppliedArtifact,
    });
    assert.equal(result.ok, false);
    assert.deepEqual(result.mismatches, ['aclDesiredGeneration', 'aclRenderedGeneration', 'aclAppliedGeneration']);
  }
});

test('readback tampering and renderer changes fail closed; proven equivalent policies converge', async () => {
  const previous = fixture();
  const current = fixture({ feedRead: true });
  assert.equal((await validateOwnerInventoryReadiness(previous.inventory, current.inventory, {
    aclContent: current.artifact.content.replace(feed, 'topic read public/#'),
    loadAppliedArtifact: async (generation) => generation === previous.artifact.generation
      ? previous.artifact : current.artifact,
  })).reason, 'applied-acl-readback-mismatch');

  current.artifact.rendererVersion = 'other-renderer';
  assert.equal((await check(previous, current)).reason, 'owner-acl-renderer-changed');
  current.artifact.rendererVersion = previous.artifact.rendererVersion;

  // A different generation with identical, verified applied permissions is safe.
  current.artifact.content = previous.artifact.content.replaceAll(previous.artifact.generation, current.artifact.generation);
  current.artifact.contentSha256 = createHash('sha256').update(current.artifact.content).digest('hex');
  assert.deepEqual(await check(previous, current), {
    ok: true,
    mismatches: [],
    reason: 'rendered-acl-policy-unchanged',
  });
});
