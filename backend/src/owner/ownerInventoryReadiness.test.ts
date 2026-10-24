import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import test from 'node:test';
import { OWNER_ACL_RENDERER_VERSION, renderOwnerAcl, type OwnerAclGrant } from '../mqtt/aclManager.js';
import { buildOwnerInventoryBaseline, validateOwnerInventoryBaseline } from './ownerInventoryBaseline.js';
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
  const inventory = buildOwnerInventoryBaseline({
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
  });
  const artifact: AppliedOwnerAclArtifact = {
    generation: rendered.generation,
    rendererVersion: OWNER_ACL_RENDERER_VERSION,
    content: rendered.content,
    contentSha256: rendered.contentSha256,
  };
  return { inventory, artifact };
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

test('an empty delta passes without requiring historical artifacts', async () => {
  const current = fixture();
  const result = await validateOwnerInventoryReadiness(current.inventory, current.inventory, {
    aclContent: current.artifact.content,
    loadAppliedArtifact: async () => { throw new Error('must not load artifacts'); },
  });
  assert.deepEqual(result, { ok: true, mismatches: [], reason: 'owner-baseline-current' });
  assert.equal(classifyRenderedOwnerAclDelta(current.artifact.content, current.artifact.content).kind, 'empty');
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

test('readback tampering, renderer changes, and unexplained generation changes fail closed', async () => {
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

  // The generation changes but the policy is identical: no feed delta proves an exception.
  current.artifact.content = previous.artifact.content.replaceAll(previous.artifact.generation, current.artifact.generation);
  current.artifact.contentSha256 = createHash('sha256').update(current.artifact.content).digest('hex');
  assert.deepEqual(await check(previous, current), {
    ok: false,
    mismatches: ['aclDesiredGeneration', 'aclRenderedGeneration', 'aclAppliedGeneration'],
    reason: 'rendered-acl-policy-unchanged',
  });
});
