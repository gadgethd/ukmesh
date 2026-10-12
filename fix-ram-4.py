import os
os.chdir(os.path.expanduser('~/ukmesh/.agent-worktrees/ram-ingest-20261011/backend'))

p = 'src/db/packetBatch.ts'
s = open(p).read()

old = "         row_id, observation_id, time, packet_hash, rx_node_id, src_node_id, topic, topic_prefix,"
new = "         row_id, observation_id_in, time, packet_hash, rx_node_id, src_node_id, topic, topic_prefix,"
assert s.count(old) == 1, f"incoming anchor count={s.count(old)}"
s = s.replace(old, new, 1)

old = "           COALESCE(i.observation_id, gen_random_uuid()) AS observation_id,"
new = "           COALESCE(i.observation_id_in, gen_random_uuid()) AS observation_id,"
assert s.count(old) == 1, f"coalesce anchor count={s.count(old)}"
s = s.replace(old, new, 1)

open(p, 'w').write(s)
print('packetBatch.ts SQL ambiguity fixed (observation_id_in)')

p = 'src/db/packetBatch.test.ts'
s = open(p).read()
old = r"/COALESCE\(i\.observation_id, gen_random_uuid\(\)\) AS observation_id/"
new = r"/COALESCE\(i\.observation_id_in, gen_random_uuid\(\)\) AS observation_id/"
assert s.count(old) == 1, f"test anchor count={s.count(old)}"
s = s.replace(old, new, 1)
open(p, 'w').write(s)
print('packetBatch.test.ts updated')
