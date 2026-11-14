\set ON_ERROR_STOP on
-- Operator bootstrap for the owned local cluster, after schema migrations.
-- Existing object/function owners and their security settings stay unchanged.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

DO $roles$
DECLARE role_name text;
BEGIN
  FOREACH role_name IN ARRAY ARRAY['meshcore_api','meshcore_ingest',
    'meshcore_worker','meshcore_owner','meshcore_migrate','meshcore_monitor','corescope_ro'] LOOP
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname=role_name) THEN
      EXECUTE format('CREATE ROLE %I LOGIN', role_name);
    END IF;
    EXECUTE format('ALTER ROLE %I LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD %L',
      role_name, 'local-full-dev-only');
  END LOOP;
  IF EXISTS (
    SELECT FROM pg_auth_members membership
    JOIN pg_roles member ON member.oid=membership.member
    JOIN pg_roles granted ON granted.oid=membership.roleid
    WHERE member.rolname IN ('meshcore_api','meshcore_ingest','meshcore_worker',
      'meshcore_owner','meshcore_migrate','meshcore_monitor','corescope_ro')
      AND NOT (member.rolname='meshcore_monitor' AND granted.rolname='pg_monitor')
  ) THEN RAISE EXCEPTION 'Unexpected local runtime role membership'; END IF;
END
$roles$;
GRANT pg_monitor TO meshcore_monitor WITH INHERIT TRUE;

-- Logins are cluster-wide. A target switch must withdraw access to previously
-- selected stores, including PUBLIC's default CONNECT, before granting this pair.
DO $databases$
DECLARE database_record record;
BEGIN
  FOR database_record IN SELECT oid, datname FROM pg_database LOOP
    IF database_record.datname NOT IN ('meshcore','meshcore_owner_auth','postgres','template0','template1') THEN
      IF database_record.datname !~ '^lean_(verify|fixture|1x|10x|soak|owner)_[a-z0-9_]{1,38}$'
        OR shobj_description(database_record.oid,'pg_database') IS DISTINCT FROM
          'ukmesh-owned-fixture:' || regexp_replace(database_record.datname,
            '^lean_(verify|fixture|1x|10x|soak|owner)_', '') THEN
        RAISE EXCEPTION 'Unexpected or unowned database in local UKMesh cluster';
      END IF;
    END IF;
    EXECUTE format('REVOKE ALL ON DATABASE %I FROM PUBLIC, meshcore_api, meshcore_ingest, meshcore_worker, meshcore_owner, meshcore_migrate, meshcore_monitor, corescope_ro',
      database_record.datname);
  END LOOP;
END
$databases$;

REVOKE ALL ON DATABASE :"ukmesh_database" FROM PUBLIC;
REVOKE ALL ON DATABASE :"ukmesh_database" FROM meshcore_api, meshcore_ingest,
  meshcore_worker, meshcore_owner, meshcore_migrate, meshcore_monitor, corescope_ro;
GRANT CONNECT ON DATABASE :"ukmesh_database" TO meshcore_api, meshcore_ingest,
  meshcore_worker, meshcore_migrate, meshcore_monitor, corescope_ro;
-- deltaPublication stages a complete learning generation in a session-local
-- table before its atomic publish. This grants no persistent schema DDL.
GRANT TEMPORARY ON DATABASE :"ukmesh_database" TO meshcore_worker;

-- Revocation is deliberately exhaustive; grants below are named allowlists.
REVOKE ALL ON ALL TABLES IN SCHEMA core, mesh, analytics, ops, archive, public
  FROM meshcore_api, meshcore_ingest, meshcore_worker, meshcore_owner, meshcore_migrate, meshcore_monitor;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA core, mesh, analytics, ops, archive, public
  FROM meshcore_api, meshcore_ingest, meshcore_worker, meshcore_owner, meshcore_migrate, meshcore_monitor;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA core, mesh, analytics, ops, archive, public
  FROM meshcore_api, meshcore_ingest, meshcore_worker, meshcore_owner, meshcore_migrate, meshcore_monitor;
GRANT USAGE ON SCHEMA core, mesh, analytics, ops, public TO meshcore_api, meshcore_worker;
GRANT USAGE ON SCHEMA core, mesh, ops, public TO meshcore_ingest;
GRANT USAGE, CREATE ON SCHEMA core, mesh, analytics, ops, archive, public TO meshcore_migrate;
ALTER ROLE meshcore_api IN DATABASE :"ukmesh_database" SET search_path = pg_catalog, core, mesh, analytics, ops, public;
ALTER ROLE meshcore_ingest IN DATABASE :"ukmesh_database" SET search_path = pg_catalog, core, mesh, public;
ALTER ROLE meshcore_worker IN DATABASE :"ukmesh_database" SET search_path = pg_catalog, core, mesh, analytics, ops, public, pg_temp;

-- API/repository reads and worker model/health inputs. View owners retain their
-- existing projection privileges; neither app role reads ops.node_privacy or
-- ops.channel_blocklist directly. Owner authentication lives in another DB.
GRANT SELECT ON
  core.nodes, core.node_counted_adverts, core.node_coverage,
  core.node_identity_aliases, core.node_identity_match_evidence, core.node_identity_refresh_state,
  core.node_identity_nodes, core.node_identity_links, core.node_identity_packets,
  core.node_identity_sightings, core.node_identity_status_samples, core.node_identity_link_radio_reports,
  core.node_links, core.node_map_suppressions, core.node_name_history,
  core.node_neighbor_samples, core.node_status_samples, core.node_network_sightings,
  core.observer_region_observer_sightings, core.observer_region_packet_sightings,
  core.observer_registration_requests, core.observers, core.private_node_prefixes,
  core.owner_alert_rules, core.owner_alert_deliveries, core.owner_alert_delivery_attempts,
  core.owner_packet_share_rules, core.owner_packet_share_deliveries,
  core.owner_public_feed_nodes, core.owner_public_feed_settings,
  core.planned_node_publications, core.region_scope_members, core.region_scope_run,
  core.region_scope_stats, core.region_scope_unattributed, core.repeater_scope_status,
  mesh.packets, mesh.packet_paths, mesh.packet_decryptions,
  mesh.packet_daily_stats, mesh.packet_hourly_stats,
  mesh.multibyte_path_facts, mesh.multibyte_path_fact_state, mesh.message_tags,
  mesh.ingest_observation_ledger, mesh.ingest_observation_history,
  mesh.ingest_observation_ledger_coverage, mesh.packet_forwarding_outbox,
  analytics.analysis_runs, analytics.analysis_workload_state, analytics.ml_model_versions,
  analytics.ml_model_variant_runs, analytics.ml_model_variant_packet_results,
  analytics.path_corridor_priors, analytics.path_edge_priors, analytics.path_model_calibration,
  analytics.path_motif_priors, analytics.path_position_prefix_priors,
  analytics.path_position_transition_priors, analytics.path_prefix_priors,
  analytics.path_slow_resolutions, analytics.path_transition_priors,
  analytics.spam_suspects, analytics.spam_message_incidents, analytics.spam_message_members,
  analytics.stats_chart_snapshots,
  ops.frontend_error_events, ops.operational_check_results, ops.operator_audit_events,
  ops.packet_visibility_materialization_state, ops.privacy_rematerialization_queue,
  ops.public_visibility_state, ops.worker_health_current, ops.worker_health_snapshots,
  ops.planned_nodes, ops.planned_coverage_jobs, ops.planned_coverage_handles,
  ops.public_feed_deliveries, ops.link_job_commits, ops.link_rebuild_runs,
  public.ml_learner_state
TO meshcore_api, meshcore_worker;
GRANT SELECT ON core.owner_packet_share_destinations TO meshcore_api;

-- These business writes run through the existing HTTP executor and still rely
-- on application actor/scope checks. API never inserts packet observations.
GRANT INSERT, UPDATE, DELETE ON
  core.node_map_suppressions, core.observer_registration_requests, core.observers,
  core.owner_alert_rules, core.owner_alert_deliveries, core.owner_alert_delivery_attempts,
  core.owner_packet_share_rules, core.owner_packet_share_deliveries,
  core.owner_public_feed_nodes, core.owner_public_feed_settings,
  core.planned_node_publications,
  analytics.spam_suspects, analytics.spam_message_incidents, analytics.spam_message_members,
  ops.planned_nodes, ops.planned_coverage_jobs, ops.planned_coverage_handles, ops.public_feed_deliveries
TO meshcore_api;
GRANT INSERT, UPDATE ON analytics.stats_chart_snapshots, analytics.path_slow_resolutions,
  ops.frontend_error_events, ops.ingest_rejections, ops.operator_audit_events TO meshcore_api;
GRANT SELECT ON ops.ingest_rejections TO meshcore_api;
GRANT SELECT, UPDATE, DELETE ON mesh.packet_forwarding_outbox TO meshcore_api;
GRANT UPDATE (last_predicted_online_at, last_path_evidence_at) ON core.nodes TO meshcore_api;
-- PostgreSQL requires an UPDATE privilege for SELECT FOR SHARE/UPDATE fences.
GRANT UPDATE (updated_at) ON ops.public_visibility_state TO meshcore_api;
GRANT USAGE ON SEQUENCE
  core.observer_registration_requests_id_seq, core.owner_alert_rules_id_seq,
  core.owner_alert_deliveries_id_seq, core.owner_alert_delivery_attempts_id_seq,
  core.owner_packet_share_deliveries_id_seq, ops.public_feed_deliveries_id_seq,
  ops.frontend_error_events_id_seq, ops.operator_audit_events_id_seq
TO meshcore_api;

-- Ingest SQL and invoker-security trigger followups: node history, privacy
-- prefixes/queues, observation tombstones and multibyte facts are real writes.
GRANT SELECT ON
  core.nodes, core.node_counted_adverts, core.node_coverage, core.node_status_samples,
  core.node_neighbor_samples, core.node_name_history, core.node_identity_aliases,
  core.node_identity_nodes, core.private_node_prefixes,
  core.node_network_sightings, core.observer_region_observer_sightings,
  core.observer_region_packet_sightings,
  core.owner_packet_share_rules, core.owner_public_feed_nodes, core.owner_public_feed_settings,
  mesh.packets, mesh.packet_paths, mesh.packet_daily_stats, mesh.packet_hourly_stats,
  mesh.ingest_observation_ledger, mesh.ingest_observation_tombstones,
  mesh.ingest_observation_archive, mesh.ingest_observation_ledger_coverage,
  mesh.multibyte_path_facts, mesh.packet_forwarding_outbox,
  ops.public_visibility_state, ops.packet_visibility_materialization_state,
  ops.privacy_rematerialization_queue
TO meshcore_ingest;
GRANT INSERT, UPDATE ON
  core.nodes, core.node_counted_adverts, core.node_coverage, core.node_name_history,
  core.node_status_samples, core.node_neighbor_samples, core.node_network_sightings,
  core.observer_region_observer_sightings, core.observer_region_packet_sightings,
  mesh.packets, mesh.packet_paths, mesh.packet_daily_stats, mesh.packet_hourly_stats,
  mesh.ingest_observation_ledger, mesh.ingest_observation_tombstones,
  mesh.multibyte_path_facts, mesh.packet_forwarding_outbox,
  ops.public_visibility_state, ops.privacy_rematerialization_queue
TO meshcore_ingest;
GRANT DELETE ON core.node_coverage TO meshcore_ingest;
GRANT INSERT, UPDATE, DELETE ON core.private_node_prefixes TO meshcore_ingest;
GRANT USAGE ON SEQUENCE mesh.ingest_observation_ledger_seq_seq,
  ops.privacy_rematerialization_queue_id_seq TO meshcore_ingest;

-- Shared worker role is the union of active worker capabilities, not ownership.
GRANT INSERT, UPDATE, DELETE ON
  core.node_identity_aliases, core.node_identity_match_evidence, core.node_identity_refresh_state,
  core.node_links, core.node_coverage, core.node_map_suppressions,
  core.node_network_sightings, core.observer_region_observer_sightings,
  core.region_scope_members, core.region_scope_run, core.region_scope_stats,
  core.region_scope_unattributed, core.repeater_scope_status, core.private_node_prefixes,
  analytics.analysis_runs, analytics.analysis_workload_state,
  analytics.path_corridor_priors, analytics.path_edge_priors, analytics.path_model_calibration,
  analytics.path_motif_priors, analytics.path_position_prefix_priors,
  analytics.path_position_transition_priors, analytics.path_prefix_priors,
  analytics.path_transition_priors, analytics.spam_message_incidents, analytics.spam_message_members,
  ops.public_visibility_state, ops.packet_visibility_materialization_state,
  ops.privacy_rematerialization_queue, ops.worker_health_current, ops.worker_health_snapshots,
  ops.tagger_state, ops.tagger_retry, ops.link_job_commits, ops.link_rebuild_runs,
  core.owner_alert_rules, core.owner_alert_deliveries, core.owner_alert_delivery_attempts
TO meshcore_worker;
GRANT UPDATE, DELETE ON core.nodes TO meshcore_worker;
GRANT INSERT, UPDATE ON core.node_name_history, mesh.message_tags,
  ops.operational_check_results, ops.maintenance_removed_records TO meshcore_worker;
GRANT UPDATE ON core.observer_registration_requests TO meshcore_worker;
GRANT UPDATE ON mesh.packets, mesh.packet_paths TO meshcore_worker;
GRANT DELETE ON ops.frontend_error_events, ops.operational_check_results,
  core.observer_region_packet_sightings, core.owner_packet_share_deliveries,
  ops.public_feed_deliveries, analytics.ml_model_variant_packet_results,
  mesh.packet_decryptions, core.observer_registration_requests, ops.operator_audit_events
TO meshcore_worker;
GRANT SELECT ON ops.tagger_state, ops.tagger_retry, ops.maintenance_removed_records TO meshcore_worker;
GRANT USAGE ON SEQUENCE ops.privacy_rematerialization_queue_id_seq,
  ops.maintenance_removed_records_archive_id_seq,
  core.owner_alert_deliveries_id_seq, core.owner_alert_delivery_attempts_id_seq TO meshcore_worker;

-- Runtime roles get no persistent schema CREATE, table ownership, default future-table
-- grants, security-definer rewrites, or reader permissions on raw measurements.
COMMIT;

\connect :owner_database
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';
REVOKE ALL ON DATABASE :"owner_database" FROM PUBLIC;
REVOKE ALL ON DATABASE :"owner_database" FROM meshcore_api, meshcore_ingest,
  meshcore_worker, meshcore_owner, meshcore_migrate, meshcore_monitor;
GRANT CONNECT ON DATABASE :"owner_database" TO meshcore_owner, meshcore_migrate;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA core, public FROM meshcore_api, meshcore_ingest,
  meshcore_worker, meshcore_owner, meshcore_migrate, meshcore_monitor;
REVOKE ALL ON SCHEMA core, public FROM meshcore_api, meshcore_ingest,
  meshcore_worker, meshcore_owner, meshcore_migrate, meshcore_monitor;
GRANT USAGE ON SCHEMA core, public TO meshcore_owner;
GRANT USAGE, CREATE ON SCHEMA core, public TO meshcore_migrate;
GRANT SELECT, INSERT, UPDATE, DELETE ON core.owner_accounts, core.owner_account_nodes,
  core.mqtt_node_logins, core.owner_grant_audit, core.owner_acl_state,
  core.owner_acl_artifacts, public.owner_sessions TO meshcore_owner;
ALTER ROLE meshcore_owner IN DATABASE :"owner_database" SET search_path = pg_catalog, core, public;
COMMIT;
