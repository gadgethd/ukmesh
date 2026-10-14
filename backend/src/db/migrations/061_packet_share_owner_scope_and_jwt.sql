-- NULL scopes preserve existing destinations available to every owner.
ALTER TABLE owner_packet_share_destinations
  ADD COLUMN allowed_owner_username TEXT CHECK (char_length(allowed_owner_username) BETWEEN 1 AND 128),
  ADD COLUMN allowed_node_id TEXT CHECK (allowed_node_id ~ '^[0-9A-F]{64}$'),
  ADD COLUMN auth_mode TEXT NOT NULL DEFAULT 'password' CHECK (auth_mode IN ('password', 'meshcore_jwt')),
  ADD COLUMN auth_audience TEXT CHECK (char_length(auth_audience) BETWEEN 1 AND 253),
  ADD COLUMN tx_mode TEXT NOT NULL DEFAULT 'on' CHECK (tx_mode IN ('on', 'advert'));

-- Provision restricted destinations only after deploying the scoped backend.
-- The preceding backend lists all rows and cannot enforce these scopes.
