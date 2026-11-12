export type WorkerSnapshot = {
  worker_name: string;
  status: string;
  queue_depth: number;
  processed_1h: number;
  last_activity_at: string | null;
  cpu_load_1m: number;
  cpu_usage_pct: number;
  mem_used_pct: number;
  disk_used_pct: number;
  queue_bytes?: number;
  dead_jobs?: number;
  retries?: number;
  active_leases?: number;
  oldest_age_s?: number;
};


export type OperationalCheckRow = {
  check_name: string;
  status: string;
  latency_ms: number;
  detail: string | null;
  ts: string;
};
