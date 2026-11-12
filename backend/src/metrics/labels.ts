

const METRIC_NETWORKS = new Set(['ukmesh', 'teesside', 'test']);

const METRIC_ANALYSIS_WORKLOADS = new Set(['spam-analysis', 'path-learning']);

const METRIC_CACHES = new Set([
  'api_stats',
  'api_inferred_nodes',
  'api_node_links',
  'api_charts',
  'topology_dto',
  'api_activity_timeline',
  'api_owner_live',
  'mqtt_channels',
  'owner_auth',
  'owner_nodes',
  'owner_dashboard',
  'mqtt_onboarding',
  'owner_last_hop',
  'path_context',
  'path_resolution',
  'path_invalidation',
  'path_sticky_nodes',
  'stats_channel',
  'stats_observer',
  'stats_observer_live',
  'ws_initial_state',
  'ws_viable_links',
]);

export function boundedNetworkMetricLabel(network: string | null | undefined): string {
  const normalized = String(network ?? '').trim().toLowerCase();
  return METRIC_NETWORKS.has(normalized) ? normalized : 'other';
}

export function boundedAnalysisWorkloadLabel(workload: string): string {
  const normalized = workload.trim().toLowerCase();
  return METRIC_ANALYSIS_WORKLOADS.has(normalized) ? normalized : 'other';
}

export function boundedCacheMetricLabel(cache: string | undefined): string {
  const normalized = String(cache ?? '').trim().toLowerCase();
  return METRIC_CACHES.has(normalized) ? normalized : 'other';
}

export function boundedLabel(value: string, allowed: Set<string>): string {
  const normalized = value.trim().toLowerCase();
  return allowed.has(normalized) ? normalized : 'other';
}
