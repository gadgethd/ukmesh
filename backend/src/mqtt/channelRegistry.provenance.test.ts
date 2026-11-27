import assert from 'node:assert/strict';
import test from 'node:test';
import { createHash } from 'node:crypto';
import { buildChannelEntries, PUBLIC_CHANNELS } from './channelRegistry.js';

/**
 * Two MeshCore channels ship with fixed PSKs rather than a key derived from
 * their name. They are protocol defaults, public by construction.
 */
const PROTOCOL_DEFAULT_CHANNELS = new Set(['Public', 'public2']);

/**
 * `test2` predates the `#name` derivation used by the other community keys and
 * is the legacy key for the `test` channel (sha256("test"), not sha256("#test2")).
 * Keeping the upstream name here makes the exception explicit instead of
 * weakening the check for every entry.
 */
const LEGACY_NAME_DERIVATIONS = new Map([['test2', 'test']]);

function channelKey(name: string): string {
  return createHash('sha256').update(`#${name}`).digest('hex').slice(0, 32);
}

test('every committed channel key is derived from its published name', () => {
  assert.equal(PUBLIC_CHANNELS.length, 41, 'registry entries must stay accounted for');
  const seen = new Set<string>();

  for (const { name, secret } of PUBLIC_CHANNELS) {
    assert.match(secret, /^[0-9a-f]{32}$/, `${name} must be a 32-character lowercase hex key`);
    assert.equal(seen.has(secret), false, `${name} duplicates another channel key`);
    seen.add(secret);

    if (PROTOCOL_DEFAULT_CHANNELS.has(name)) continue;

    const legacyName = LEGACY_NAME_DERIVATIONS.get(name);
    const expected = legacyName
      ? createHash('sha256').update(legacyName).digest('hex').slice(0, 32)
      : channelKey(name);
    assert.equal(
      secret,
      expected,
      legacyName
        ? `${name} must remain the legacy sha256("${legacyName}") channel key`
        : `${name} must equal sha256("#${name}")[:32]`,
    );
  }
});

test('an environment-supplied 32-hex channel key is confidential', () => {
  const envKey = createHash('sha256').update('provenance-test-private-channel').digest('hex').slice(0, 32);
  const entries = buildChannelEntries(`private-region:${envKey}`);
  const entry = entries.find((candidate) => candidate.name === 'private-region');

  assert.ok(entry, 'the environment channel must be registered');
  assert.equal(entry.classification, 'confidential');
  assert.equal(entry.secret, envKey);
});
