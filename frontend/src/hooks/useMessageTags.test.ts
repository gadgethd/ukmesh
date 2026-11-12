import assert from 'node:assert/strict';
import test from 'node:test';
import { isTaggedMessages } from './useMessageTags.js';

test('tag enrichment validates nested labels/confidence and bounded rows', () => {
  const row = { packet_hash: 'INVENTED', tags: { kind: 'coordination', safety: 0.9 }, confidence: { kind: 0.8 } };
  assert.equal(isTaggedMessages([row]), true);
  for (const bad of [{ ...row, tags: { kind: {} } }, { ...row, confidence: { kind: 'certain' } },
    { ...row, tagged_at: 'invalid' }, { ...row, packet_hash: 'x'.repeat(129) }]) assert.equal(isTaggedMessages([bad]), false);
  assert.equal(isTaggedMessages(Array.from({ length: 301 }, () => row)), false);
});
