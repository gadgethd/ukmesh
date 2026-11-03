-- 108_compat_visibility_views.sql
-- CoreScope connects as meshcore and still reads the pre-087 unqualified
-- privacy relations. Keep read-only compatibility views in public; the
-- canonical tables remain in ops.
CREATE OR REPLACE VIEW public.public_visibility_state AS
  SELECT * FROM ops.public_visibility_state;

CREATE OR REPLACE VIEW public.privacy_rematerialization_queue AS
  SELECT * FROM ops.privacy_rematerialization_queue;
