export type TropoBounds = {
  north: number;
  south: number;
  west: number;
  east: number;
};

export type TropoPeak = {
  lat: number;
  lon: number;
};

export type TropoStep = {
  index: number;
  forecast_hour: number;
  valid: string;
  file: string;
  max_index: number;
  peak_duct: number;
  pct_strong: number;
  pct_ducting: number;
  peak: TropoPeak;
};

export type TropoIndex = {
  generated_at: string;
  run: string;
  cycle: string;
  model: string;
  bounds: TropoBounds;
  width: number;
  height: number;
  steps: TropoStep[];
  source: string;
  attribution: string;
};

export type TropoTier = {
  min: number;
  max: number;
  label: string;
  shortLabel: string;
  color: string;
  description: string;
};

export const TROPO_TIERS: readonly TropoTier[] = [
  { min: 0, max: 39, label: 'Normal', shortLabel: 'Normal', color: '#334155', description: 'No meaningful enhancement' },
  { min: 40, max: 54, label: 'Slight enhancement', shortLabel: 'Slight', color: '#00c4ff', description: 'A little extra bending' },
  { min: 55, max: 69, label: 'Fair enhancement', shortLabel: 'Fair', color: '#22c55e', description: 'Scattered enhancement' },
  { min: 70, max: 84, label: 'Moderate enhancement', shortLabel: 'Moderate', color: '#fbbf24', description: 'Ducts becoming organised' },
  { min: 85, max: 99, label: 'Strong enhancement', shortLabel: 'Strong', color: '#ff9f43', description: 'Broad enhancement, short ducts possible' },
  { min: 100, max: 114, label: 'Ducting', shortLabel: 'Ducting', color: '#ef4444', description: 'dM/dz at or below zero' },
  { min: 115, max: 150, label: 'Intense ducting', shortLabel: 'Intense', color: '#d946ef', description: 'Strong, sustained duct' },
];

const VALID_FORMATTER = new Intl.DateTimeFormat('en-GB', {
  weekday: 'short',
  day: '2-digit',
  month: 'short',
  hour: '2-digit',
  minute: '2-digit',
  hour12: false,
  timeZone: 'UTC',
});

const RUN_FORMATTER = new Intl.DateTimeFormat('en-GB', {
  weekday: 'short',
  day: '2-digit',
  month: 'short',
  timeZone: 'UTC',
});

export function tierFor(index: number): TropoTier {
  const value = Number.isFinite(index) ? index : 0;
  return TROPO_TIERS.find((tier) => value >= tier.min && value < tier.max + 1) ?? TROPO_TIERS[TROPO_TIERS.length - 1];
}

export function formatValid(iso: string): string {
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return 'Unknown time';
  return `${VALID_FORMATTER.format(date).replace(',', ' ·')} UTC`;
}

export function formatValidShort(iso: string): string {
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return '—';
  return VALID_FORMATTER.format(date).replace(',', ' ·');
}

export function formatRun(iso: string): string {
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return 'Unknown run';
  return RUN_FORMATTER.format(date);
}

export function formatGenerated(iso: string): string {
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return 'unknown';
  return `${VALID_FORMATTER.format(date).replace(',', ' ·')} UTC`;
}

export function stalenessHours(iso: string, now = Date.now()): number {
  const date = new Date(iso).getTime();
  if (Number.isNaN(date)) return Number.POSITIVE_INFINITY;
  return Math.max(0, (now - date) / 3_600_000);
}

export function bestStep(data: TropoIndex | null): TropoStep | null {
  if (!data || data.steps.length === 0) return null;
  return data.steps.reduce((best, step) => (step.max_index > best.max_index ? step : best), data.steps[0]);
}

export function isQuiet(data: TropoIndex | null): boolean {
  return Boolean(data && data.steps.every((step) => step.max_index < 40));
}

export function initialStepIndex(data: TropoIndex): number {
  if (data.steps.length === 0) return 0;
  const now = Date.now();
  const future = data.steps.findIndex((step) => new Date(step.valid).getTime() >= now);
  return future >= 0 ? future : 0;
}
