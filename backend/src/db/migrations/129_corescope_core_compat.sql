-- 129_corescope_core_compat.sql
-- Read-only compatibility views for the external CoreScope reader after the P1 core moves.
CREATE OR REPLACE VIEW public.nodes AS SELECT * FROM core.nodes;
CREATE OR REPLACE VIEW public.node_links AS SELECT * FROM core.node_links;
CREATE OR REPLACE VIEW public.node_status_samples AS SELECT * FROM core.node_status_samples;
CREATE OR REPLACE VIEW public.observers AS SELECT * FROM core.observers;
