export function createStatusReader(targets, probe, now = Date.now) {
  let cached;
  let expiresAt = 0;
  let refresh;
  return async function status() {
    if (cached && now() < expiresAt) return cached;
    refresh ??= Promise.all(targets.map(async ({ id, name, port, group, note, upstream, probe: path }) => ({
      id, name, port, group, note,
      ...await probe(upstream + path),
    }))).then((results) => {
      const finished = now();
      cached = { at: new Date(finished).toISOString(), targets: results };
      expiresAt = finished + 5000;
      return cached;
    }).finally(() => { refresh = undefined; });
    return refresh;
  };
}
