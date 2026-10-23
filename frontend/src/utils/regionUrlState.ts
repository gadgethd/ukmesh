/** Region names are case-sensitive; # is only a display/transport prefix. */
export function validRegionName(value: string): boolean {
  return /^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$/.test(value);
}

export function regionFromHash(hash: string): string | undefined {
  const params = new URLSearchParams(hash.replace(/^#\??/, ''));
  const values = params.getAll('region');
  const name = values[0]?.replace(/^#/, '');
  return values.length === 1 && name && validRegionName(name) ? name : undefined;
}

export function hashWithRegion(hash: string, region?: string): string {
  const params = new URLSearchParams(hash.replace(/^#\??/, ''));
  params.delete('region');
  if (region && validRegionName(region)) params.set('region', region);
  const query = params.toString();
  return query ? `#${query}` : '';
}
