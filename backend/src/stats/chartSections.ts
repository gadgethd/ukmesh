import { CHART_SECTIONS } from '../contracts/publicData.generated.js';
export { CHART_SECTIONS } from '../contracts/publicData.generated.js';

export type ChartSection = keyof typeof CHART_SECTIONS;

export function parseChartSections(value: unknown): ChartSection[] | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== 'string' || value.length > 80) throw new Error('INVALID_CHART_SECTIONS');
  const parts = value.split(',');
  if (!parts.length || parts.length > 5 || parts.some(part => !Object.hasOwn(CHART_SECTIONS, part))
    || new Set(parts).size !== parts.length) throw new Error('INVALID_CHART_SECTIONS');
  return (Object.keys(CHART_SECTIONS) as ChartSection[]).filter(section => parts.includes(section));
}

export function projectChartSections(payload: unknown, sections: ChartSection[] | undefined,
  scope: { network: string; observer: string | null }, generation: number): unknown {
  if (!sections) return payload; // Preserve the existing default response, including legacy fields.
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) throw new Error('INVALID_CHART_SNAPSHOT');
  const full = payload as Record<string, unknown>;
  const snapshot = full['snapshot'] as { visibilityGeneration?: unknown } | undefined;
  if (snapshot?.visibilityGeneration !== generation) throw new Error('STALE_CHART_SNAPSHOT');
  const selected: Record<string, unknown> = { snapshot, summary: full['summary'],
    representation: { sections, scope, visibilityGeneration: generation } };
  for (const section of sections) {
    for (const field of CHART_SECTIONS[section]) {
      if (!Object.hasOwn(full, field)) throw new Error('INCOMPLETE_CHART_SNAPSHOT');
      selected[field] = full[field];
    }
  }
  return selected;
}
