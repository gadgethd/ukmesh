-- 117_move_core_identity_views.sql
-- P1-4 core domain: identity projection views move to core.
-- Public read-only compatibility views keep existing external readers working.
SET LOCAL lock_timeout = '10s';
SET LOCAL statement_timeout = '60s';

ALTER VIEW public.node_identity_nodes SET SCHEMA core;
ALTER VIEW public.node_identity_packets SET SCHEMA core;
ALTER VIEW public.node_identity_status_samples SET SCHEMA core;

CREATE VIEW public.node_identity_nodes AS SELECT * FROM core.node_identity_nodes;
CREATE VIEW public.node_identity_packets AS SELECT * FROM core.node_identity_packets;
CREATE VIEW public.node_identity_status_samples AS SELECT * FROM core.node_identity_status_samples;
