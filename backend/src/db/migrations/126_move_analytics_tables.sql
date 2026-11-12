-- 126_move_analytics_tables.sql
SET LOCAL lock_timeout = '15s';
SET LOCAL statement_timeout = '180s';

ALTER TABLE public.analysis_runs SET SCHEMA analytics;
ALTER TABLE public.analysis_workload_state SET SCHEMA analytics;
ALTER TABLE public.ml_gold_paths SET SCHEMA analytics;
ALTER TABLE public.ml_model_variant_packet_results SET SCHEMA analytics;
ALTER TABLE public.ml_model_variant_runs SET SCHEMA analytics;
ALTER TABLE public.ml_model_versions SET SCHEMA analytics;
ALTER TABLE public.ml_path_prefix_scores SET SCHEMA analytics;
ALTER TABLE public.path_corridor_priors SET SCHEMA analytics;
ALTER TABLE public.path_edge_priors SET SCHEMA analytics;
ALTER TABLE public.path_model_calibration SET SCHEMA analytics;
ALTER TABLE public.path_motif_priors SET SCHEMA analytics;
ALTER TABLE public.path_position_prefix_priors SET SCHEMA analytics;
ALTER TABLE public.path_position_transition_priors SET SCHEMA analytics;
ALTER TABLE public.path_prefix_priors SET SCHEMA analytics;
ALTER TABLE public.path_slow_resolutions SET SCHEMA analytics;
ALTER TABLE public.path_transition_priors SET SCHEMA analytics;
ALTER TABLE public.spam_message_incidents SET SCHEMA analytics;
ALTER TABLE public.spam_message_members SET SCHEMA analytics;
ALTER TABLE public.spam_suspects SET SCHEMA analytics;
ALTER TABLE public.stats_chart_snapshots SET SCHEMA analytics;
