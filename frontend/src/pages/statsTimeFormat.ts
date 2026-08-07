/**
 * Local-time formatting for chart axis/tooltip labels.
 *
 * The backend returns machine-readable ISO timestamps for all time-bucketed
 * chart series; every label is formatted here in the viewer's local timezone
 * (never server-side). Unparseable inputs (e.g. legacy cached chart snapshots
 * still holding "HH:MM" strings) fall back to the raw value so charts never
 * render "Invalid Date".
 */

/** Format an ISO/UTC timestamp as a short local-time label (e.g. "10:30"). */
export function fmtAxisTime(value: unknown): string {
  const d = new Date(String(value));
  return Number.isFinite(d.getTime())
    ? d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
    : String(value);
}

/** Format an ISO/UTC day in the viewer's local timezone (e.g. "Fri 7 Aug"). */
export function fmtAxisDay(value: unknown): string {
  const d = new Date(String(value));
  return Number.isFinite(d.getTime())
    ? d.toLocaleDateString('en-GB', { weekday: 'short', month: 'short', day: 'numeric' })
    : String(value);
}
