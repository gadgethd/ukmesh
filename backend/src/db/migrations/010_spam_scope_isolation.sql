DO $$
DECLARE
  primary_key_name TEXT;
BEGIN
  SELECT conname INTO primary_key_name
  FROM pg_constraint
  WHERE conrelid = 'spam_suspects'::regclass
    AND contype = 'p';

  IF primary_key_name IS NOT NULL THEN
    EXECUTE format('ALTER TABLE spam_suspects DROP CONSTRAINT %I', primary_key_name);
  END IF;
END $$;

ALTER TABLE spam_suspects
  ADD PRIMARY KEY (network, src_node_id);

DELETE FROM path_prefix_priors WHERE network = 'all';
DELETE FROM path_transition_priors WHERE network = 'all';
DELETE FROM path_edge_priors WHERE network = 'all';
DELETE FROM path_motif_priors WHERE network = 'all';
DELETE FROM path_model_calibration WHERE network = 'all';
