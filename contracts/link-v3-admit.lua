
local existing = redis.call('HGET', KEYS[6], ARGV[2])
if existing then
  local existing_state = redis.call('HGET', KEYS[3], existing)
  if existing == ARGV[1] and existing_state == 'complete' then
    return {'duplicate', existing}
  end
  if existing_state == 'queued' or existing_state == 'in_flight' or existing_state == 'dead' then
    return {'coalesced', existing}
  end
end
local payload_bytes = tonumber(ARGV[4])
if payload_bytes > tonumber(ARGV[7]) then
  return {'oversized', ''}
end
local count = tonumber(redis.call('HGET', KEYS[10], 'count') or '0')
local bytes = tonumber(redis.call('HGET', KEYS[10], 'bytes') or '0')
if count + 1 > tonumber(ARGV[5]) or bytes + payload_bytes > tonumber(ARGV[6]) then
  return {'full', ''}
end
redis.call('HSET', KEYS[2], ARGV[1], ARGV[3])
redis.call('HSET', KEYS[3], ARGV[1], 'queued')
redis.call('HSET', KEYS[4], ARGV[1], '0')
redis.call('HSET', KEYS[5], ARGV[1], tostring(payload_bytes))
redis.call('HSET', KEYS[6], ARGV[2], ARGV[1])
redis.call('HSET', KEYS[7], ARGV[1], ARGV[2])
redis.call('HINCRBY', KEYS[10], 'count', 1)
redis.call('HINCRBY', KEYS[10], 'bytes', payload_bytes)
redis.call('ZADD', KEYS[12], ARGV[9], ARGV[1])
if ARGV[8] == '' and redis.call('EXISTS', KEYS[9]) == 1 then
  redis.call('LPUSH', KEYS[8], ARGV[1])
else
  redis.call('LPUSH', KEYS[1], ARGV[1])
end
redis.call('LPUSH', KEYS[11], 'admit')
redis.call('LTRIM', KEYS[11], 0, 255)
redis.call('LPUSH', KEYS[13], '1')
redis.call('LTRIM', KEYS[13], 0, 0)
return {'accepted', ARGV[1]}
