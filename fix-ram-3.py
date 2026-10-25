import os
os.chdir(os.path.expanduser('~/ukmesh/.agent-worktrees/ram-ingest-20261011/backend'))
p = 'src/db/packetBatch.test.ts'
s = open(p).read()
old = "assert.match(statements[1]!, /gen_random_uuid() AS observation_id/);"
new = "assert.match(statements[1]!, /COALESCE\\(i\\.observation_id, gen_random_uuid\\(\\)\\) AS observation_id/);"
assert s.count(old) == 1, f"anchor count={s.count(old)}"
s = s.replace(old, new, 1)
open(p, 'w').write(s)
print('packetBatch.test.ts updated')
