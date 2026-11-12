import fs from 'node:fs';

function toPct(num: number): number {
  return Math.round(num * 1000) / 10;
}

type ProcessCpuSample = {
  usageMicros: number;
  atNanos: bigint;
};

function readFiniteNumber(path: string): number | null {
  try {
    const value = fs.readFileSync(path, 'utf8').trim();
    if (!/^\d+$/.test(value)) return null;
    const parsed = Number(value);
    return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : null;
  } catch {
    return null;
  }
}

export function measureDirectoryBytes(
  root: string,
  maxEntries = 100_000,
): { bytes: number | null; entries: number; complete: boolean } {
  const pending = [root];
  let bytes = 0;
  let entries = 0;
  try {
    while (pending.length > 0) {
      const directory = pending.pop()!;
      for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
        entries += 1;
        if (entries > maxEntries) return { bytes: null, entries, complete: false };
        const entryPath = `${directory}/${entry.name}`;
        if (entry.isDirectory()) pending.push(entryPath);
        else if (entry.isFile()) bytes += fs.statSync(entryPath).size;
      }
    }
    return { bytes, entries, complete: true };
  } catch {
    return { bytes: null, entries, complete: false };
  }
}

type VolumeConfig = {
  name: string;
  path: string;
  budgetBytes: number;
};

function volumeConfigs(raw = process.env['HEALTH_VOLUME_PATHS'] ?? ''): VolumeConfig[] {
  return raw.split(',')
    .map((entry) => entry.trim())
    .filter(Boolean)
    .flatMap((entry) => {
      const [name, path, budget] = entry.split('=');
      const budgetBytes = Number(budget);
      return name && path && /^[a-z][a-z0-9_-]{0,31}$/i.test(name)
        && path.startsWith('/') && Number.isSafeInteger(budgetBytes) && budgetBytes > 0
        ? [{ name, path, budgetBytes }]
        : [];
    });
}

export function measureConfiguredVolumes(raw: string): Record<string, {
  path: string;
  used_bytes: number | null;
  budget_bytes: number;
  used_pct: number | null;
  complete: boolean;
}> {
  return Object.fromEntries(volumeConfigs(raw).map((config) => {
    const measured = measureDirectoryBytes(config.path);
    return [config.name, {
      path: config.path,
      used_bytes: measured.bytes,
      budget_bytes: config.budgetBytes,
      used_pct: measured.bytes == null
        ? null
        : toPct(measured.bytes / config.budgetBytes),
      complete: measured.complete,
    }];
  }));
}

export function createSystemProbe() {
let lastProcessCpuSample: ProcessCpuSample | null = null;

function processCpuUsagePct(): number {
  const usage = process.cpuUsage();
  const current = {
    usageMicros: usage.user + usage.system,
    atNanos: process.hrtime.bigint(),
  };
  const previous = lastProcessCpuSample;
  lastProcessCpuSample = current;
  if (!previous) return 0;
  const elapsedMicros = Number(current.atNanos - previous.atNanos) / 1_000;
  if (elapsedMicros <= 0) return 0;
  return Math.round(
    (Math.max(0, current.usageMicros - previous.usageMicros) / elapsedMicros) * 10_000,
  ) / 100;
}

let cachedVolumes: {
  expiresAt: number;
  value: Record<string, {
    path: string;
    used_bytes: number | null;
    budget_bytes: number;
    used_pct: number | null;
    complete: boolean;
  }>;
} | null = null;

function volumeStats(): Record<string, {
  path: string;
  used_bytes: number | null;
  budget_bytes: number;
  used_pct: number | null;
  complete: boolean;
}> {
  if (cachedVolumes && cachedVolumes.expiresAt > Date.now()) return cachedVolumes.value;
  const value = measureConfiguredVolumes(process.env['HEALTH_VOLUME_PATHS'] ?? '');
  cachedVolumes = { expiresAt: Date.now() + 5 * 60_000, value };
  return value;
}

function systemStats() {
  const usagePct = processCpuUsagePct();
  const cgroupCurrent = readFiniteNumber('/sys/fs/cgroup/memory.current');
  const cgroupMax = readFiniteNumber('/sys/fs/cgroup/memory.max');
  const rss = process.memoryUsage().rss;
  const memoryUsed = cgroupCurrent ?? rss;
  const memoryTotal = cgroupMax;
  const volumes = volumeStats();
  const knownVolumePcts = Object.values(volumes)
    .map((volume) => volume.used_pct)
    .filter((value): value is number => value != null);
  const maxVolumePct = knownVolumePcts.length > 0 ? Math.max(...knownVolumePcts) : null;

  return {
    generated_at: new Date().toISOString(),
    cpu: {
      scope: 'process',
      load_1m: usagePct,
      usage_pct: usagePct,
    },
    memory: {
      scope: cgroupCurrent != null && cgroupMax != null ? 'cgroup' : 'process',
      total_mb: memoryTotal == null ? null : Math.round(memoryTotal / 1_048_576),
      used_mb: Math.round(memoryUsed / 1_048_576),
      used_pct: memoryTotal == null ? null : toPct(memoryUsed / memoryTotal),
    },
    disk: {
      scope: 'configured_volumes',
      used_pct: maxVolumePct,
      volumes,
    },
    runtime: {
      scope: 'process',
      uptime_s: Math.round(process.uptime()),
    },
  };
}

  return systemStats;
}
