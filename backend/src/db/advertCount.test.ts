import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const dbSource = readFileSync(new URL('./index.ts', import.meta.url), 'utf8');
const mqttSource = readFileSync(new URL('../mqtt/client.ts', import.meta.url), 'utf8');

test('advert count is folded into node UPSERT with durable canonical-hash deduplication', () => {
  assert.match(dbSource, /WITH advert_once AS/);
  assert.match(dbSource, /ON CONFLICT \(canonical_advert_hash\) DO NOTHING/);
  assert.match(dbSource, /advert_count\s+= nodes\.advert_count \+ \(SELECT COUNT\(\*\) FROM advert_once\)/);
  assert.doesNotMatch(dbSource, /function incrementAdvertCount/);
  assert.match(mqttSource, /advertHash: canonicalPacketId/);
  assert.doesNotMatch(mqttSource, /tryCountAdvert|countedAdvertHashes/);
});
