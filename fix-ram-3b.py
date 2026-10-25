import os
os.chdir(os.path.expanduser('~/ukmesh/.agent-worktrees/ram-ingest-20261011/backend'))
p = 'src/db/packetBatch.test.ts'
s = open(p).read()
lines = s.split('\n')
target = None
for i, line in enumerate(lines):
    if 'gen_random_uuid' in line and 'assert.match' in line:
        target = i
        break
assert target is not None, 'no assert.match line with gen_random_uuid found'
line = lines[target]
print('BEFORE:', repr(line))
start = line.find('/gen_random_uuid')
assert start != -1, f'start marker not found in {line!r}'
end = line.find('AS observation_id/', start)
assert end != -1, f'end marker not found in {line!r}'
end += len('AS observation_id/')
new_regex = "/COALESCE\\(i\\.observation_id, gen_random_uuid\\(\\)\\) AS observation_id/"
lines[target] = line[:start] + new_regex + line[end:]
print('AFTER: ', repr(lines[target]))
open(p, 'w').write('\n'.join(lines))
print('packetBatch.test.ts updated')
