/** Relative-time formatting shared by the dashboard, feed, stats and pages. */
export function timeAgo(
  value: string | number | Date | null | undefined,
  options: { now?: number; empty?: string } = {},
): string {
  if (value === null || value === undefined || value === '') return options.empty ?? '';
  const timestamp = value instanceof Date
    ? value.getTime()
    : typeof value === 'number' ? value : Date.parse(value);
  if (!Number.isFinite(timestamp)) return options.empty ?? 'Unknown';
  const secs = Math.max(0, Math.floor(((options.now ?? Date.now()) - timestamp) / 1000));
  if (secs < 60) return `${secs}s ago`;
  if (secs < 3600) return `${Math.floor(secs / 60)}m ago`;
  if (secs < 86400) return `${Math.floor(secs / 3600)}h ago`;
  return `${Math.floor(secs / 86400)}d ago`;
}
