export type ApiScope = {
  network?: string;
  observer?: string;
};

export function withScopeParams(path: string, scope: ApiScope = {}): string {
  const params = new URLSearchParams();
  if (scope.network) params.set('network', scope.network);
  if (scope.observer) params.set('observer', scope.observer);
  const query = params.toString();
  if (!query) return path;
  const sep = path.includes('?') ? '&' : '?';
  return `${path}${sep}${query}`;
}

export function withNetworkParam(path: string, network?: string): string {
  return withScopeParams(path, { network });
}

export function statsEndpoint(scope: ApiScope = {}): string {
  return withScopeParams('/api/stats', scope);
}

export function chartStatsEndpoint(scope: ApiScope = {}): string {
  return withScopeParams('/api/stats/charts', scope);
}

export function uncachedEndpoint(url: string): string {
  const sep = url.includes('?') ? '&' : '?';
  return `${url}${sep}_ts=${Date.now()}`;
}
