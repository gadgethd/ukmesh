import assert from 'node:assert/strict';
import test from 'node:test';
import { validateRamRedisHeadroom } from './memory.js';

test('F09 startup refuses unlimited, evicting, full or undersized Redis', () => {
  const info = (max: number, used: number, policy='noeviction') =>
    `used_memory:${used}\r\nmaxmemory:${max}\r\nmaxmemory_policy:${policy}\r\n`;
  assert.doesNotThrow(()=>validateRamRedisHeadroom(info(96*1048576,1048576),32*1048576));
  for (const value of [info(0,0),info(48*1048576,0),info(96*1048576,80*1048576),info(96*1048576,0,'allkeys-lru'),'']) {
    assert.throws(()=>validateRamRedisHeadroom(value,32*1048576),/RAM_REDIS_HEADROOM_UNSAFE/);
  }
});
