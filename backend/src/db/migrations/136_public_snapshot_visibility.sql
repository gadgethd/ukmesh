-- Existing region snapshots lack authority provenance. Leave them uncertified;
-- regions:rebuild publishes a fresh snapshot for the current privacy generation.
ALTER TABLE core.region_scope_run
  ADD COLUMN visibility_generation BIGINT,
  ADD COLUMN network TEXT NOT NULL DEFAULT 'ukmesh';

-- Incident JSON contains derived geometry as well as member identifiers. An
-- EXISTS on one public member cannot certify all of that historical geometry.
ALTER TABLE analytics.spam_message_incidents
  ADD COLUMN visibility_generation BIGINT;
