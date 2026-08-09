import { query } from '../db/index.js';

export type SyntheticCheckResult = {
  name: string;
  status: 'ok' | 'failed';
  latencyMs: number;
  detail: string;
};

type QueryFn = (
  text: string,
  params?: unknown[],
) => Promise<unknown>;

export async function persistSyntheticCheckResults(
  results: readonly SyntheticCheckResult[],
  execute: QueryFn = query,
): Promise<void> {
  await execute(
    `INSERT INTO operational_check_results (check_name, status, latency_ms, detail)
     SELECT result.name, result.status, result.latency_ms, result.detail
       FROM jsonb_to_recordset($1::jsonb) AS result(
         name text,
         status text,
         latency_ms integer,
         detail text
       )`,
    [JSON.stringify(results.map((result) => ({
      name: result.name,
      status: result.status,
      latency_ms: result.latencyMs,
      detail: result.detail,
    })))],
  );
}
