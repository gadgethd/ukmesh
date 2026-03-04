export function withNetworkParam(path: string, network?: string): string {
  if (!network) return path;
  const sep = path.includes('?') ? '&' : '?';
  return `${path}${sep}network=${encodeURIComponent(network)}`;
}

export function statsEndpoint(network?: string): string {
  return withNetworkParam('/api/stats', network);
}

export function chartStatsEndpoint(network?: string): string {
  return withNetworkParam('/api/stats/charts', network);
}

export function uncachedEndpoint(url: string): string {
  const sep = url.includes('?') ? '&' : '?';
  return `${url}${sep}_ts=${Date.now()}`;
}
