import fs from 'node:fs';
import path from 'node:path';
import { createRequire } from 'node:module';

/** Rebundle the exact, integrity-locked upstream sources, rather than nesting
 * MapLibre's already bundled dependencies inside another application bundle.
 * Upstream ships these sources in its source maps, including generated shaders.
 * Keep this intentionally version-bound: an upgrade must qualify its source
 * inventory and emitted worker again before it can enter a release.
 */
export function prepareMapLibreSource(): string {
  const require = createRequire(import.meta.url);
  const packageDir = path.dirname(require.resolve('maplibre-gl/package.json'));
  const metadata = JSON.parse(fs.readFileSync(path.join(packageDir, 'package.json'), 'utf8'));
  if (metadata.version !== '6.11.2') throw new Error('Qualify MapLibre source/worker inventory before changing version');
  const target = path.resolve('node_modules/.cache/ukmesh-maplibre-source/6.11.2');
  const stamp = path.join(target, '.complete-v1');
  if (fs.existsSync(stamp)) return target;
  fs.mkdirSync(target, { recursive: true });
  fs.copyFileSync(path.join(packageDir, 'package.json'), path.join(target, 'package.json'));
  const written = new Set<string>();
  for (const name of ['maplibre-gl', 'maplibre-gl-shared', 'maplibre-gl-worker',
    'maplibre-gl-dev', 'maplibre-gl-shared-dev', 'maplibre-gl-worker-dev']) {
    const map = JSON.parse(fs.readFileSync(path.join(packageDir, 'dist', `${name}.mjs.map`), 'utf8')) as {
      sources: string[]; sourcesContent: Array<string | null>;
    };
    for (let i = 0; i < map.sources.length; i += 1) {
      const source = map.sources[i];
      if (!source.startsWith('../src/') || written.has(source)) continue;
      const destination = path.resolve(target, source.slice(3));
      if (!destination.startsWith(`${target}${path.sep}`) || typeof map.sourcesContent[i] !== 'string') {
        throw new Error(`Incomplete MapLibre source inventory: ${source}`);
      }
      fs.mkdirSync(path.dirname(destination), { recursive: true });
      fs.writeFileSync(destination, map.sourcesContent[i]!);
      written.add(source);
    }
  }
  // Rollup removes this upstream barrel from source maps because it has no
  // executable statements. Preserve the v6.11.2 barrel's three re-exports.
  fs.writeFileSync(path.join(target, 'src/data/index_array_type.ts'),
    "export { LineIndexArray, TriangleIndexArray, LineStripIndexArray } from './array_types.g.ts';\n");
  for (const required of ['src/index.ts', 'src/source/worker.ts', 'src/util/actor_messages.ts']) {
    if (!fs.existsSync(path.join(target, required))) throw new Error(`Missing MapLibre source: ${required}`);
  }
  fs.writeFileSync(stamp, 'maplibre-6.11.2-source-map-inventory-v1\n');
  return target;
}
