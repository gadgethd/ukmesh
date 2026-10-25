import os
os.chdir(os.path.expanduser('~/ukmesh/.agent-worktrees/ram-ingest-20261011/backend'))

p = 'src/api/routes/misc.ts'
s = open(p).read()
old = """        : await mergePendingPacketsIntoRecentRows(
            await getRecentPackets(limit, network, observer, fields),
            { limit, network, observer },
          );"""
new = """        : await mergePendingPacketsIntoRecentRows(
            (await getRecentPackets(limit, network, observer, fields)) as Record<string, unknown>[],
            { limit, network, observer },
          );"""
assert s.count(old) == 1, f"misc anchor count={s.count(old)}"
s = s.replace(old, new, 1)
open(p, 'w').write(s)
print('misc.ts ok')

p = 'src/index.ts'
s = open(p).read()
old = "from './ram/writer.js'"
assert s.count(old) == 1, f"index anchor count={s.count(old)}"
s = s.replace(old, "from './ram/writerRuntime.js'", 1)
open(p, 'w').write(s)
print('index.ts ok')
