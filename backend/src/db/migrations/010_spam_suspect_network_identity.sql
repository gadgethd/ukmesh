-- A source identifier may legitimately occur on more than one network. Keep
-- recomputation and public reads from crossing those trust boundaries.
ALTER TABLE spam_suspects DROP CONSTRAINT IF EXISTS spam_suspects_pkey;
ALTER TABLE spam_suspects ADD PRIMARY KEY (network, src_node_id);
CREATE INDEX IF NOT EXISTS spam_suspects_src_node_idx ON spam_suspects (src_node_id);
