import policy from './operationPairPolicy.json' with { type: 'json' };

/** CSV and GeoJSON share each real client's export bucket across all trials. */
export function createExportPacer(actors: readonly string[], now = () => performance.now(),
  sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms))) {
  const completed = new Map<string, number>();
  const owned = new Set(actors);
  const requireActor = (actor: string) => {
    if (!owned.has(actor)) throw new Error('unknown operation export actor');
  };
  return {
    async wait(actor: string) {
      requireActor(actor);
      while (completed.has(actor)) {
        const remaining = completed.get(actor)! + policy.exportSpacingMs - now();
        if (remaining <= 0) return;
        await sleep(remaining);
      }
    },
    complete(actor: string) { requireActor(actor); completed.set(actor, now()); },
  };
}

export function exportIdentityChecks(records: readonly Record<string, unknown>[]) {
  return Array.from({ length: policy.trials }, (_, trial) => {
    const csv = records.filter(row => row['trial'] === trial && row['name'] === 'nodes-csv');
    const geo = records.filter(row => row['trial'] === trial && row['name'] === 'nodes-geojson');
    const identity = csv[0]?.['identityDigest'];
    return { trial, csvSamples: csv.length, geojsonSamples: geo.length,
      passed: csv.length === policy.exportSamples && geo.length === policy.exportSamples
        && typeof identity === 'string' && /^[a-f0-9]{64}$/.test(identity)
        && [...csv, ...geo].every(row => row['identityDigest'] === identity) };
  });
}
