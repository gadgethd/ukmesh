import { getRamQueueMaxBytes, RAM_REDIS_HEADROOM_BYTES } from './protocol.js';

export function validateRamRedisHeadroom(info: string, queueBudget = getRamQueueMaxBytes()): void {
  const number = (name: string) => Number(info.match(new RegExp(`(?:^|\\n)${name}:(\\d+)`))?.[1]);
  const max = number('maxmemory'), used = number('used_memory');
  const policy = info.match(/(?:^|\n)maxmemory_policy:([^\s]+)/)?.[1];
  if (!Number.isSafeInteger(max) || max <= 0 || !Number.isSafeInteger(used) || used < 0
    || policy !== 'noeviction' || queueBudget + RAM_REDIS_HEADROOM_BYTES > max
    || used + RAM_REDIS_HEADROOM_BYTES >= max) {
    throw new Error('RAM_REDIS_HEADROOM_UNSAFE: bounded noeviction Redis and queue/recovery headroom required');
  }
}
