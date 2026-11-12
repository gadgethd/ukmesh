import type { ChartData } from '../../../src/components/stats/StatsPrimitives.js';
export const chartStats: ChartData & {
  snapshot: { status: 'complete'; generatedAt: string; scope: string; visibilityGeneration: number };
};
