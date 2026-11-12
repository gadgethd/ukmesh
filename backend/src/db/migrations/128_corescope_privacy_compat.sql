-- 128_corescope_privacy_compat.sql
-- CoreScope's public-projection query still uses pre-move names.
-- Read-only compatibility views; no writes are accepted through them.
CREATE OR REPLACE VIEW public.packet_visibility_materialization_state AS
  SELECT * FROM ops.packet_visibility_materialization_state;

CREATE OR REPLACE VIEW public.private_node_prefixes AS
  SELECT * FROM core.private_node_prefixes;
