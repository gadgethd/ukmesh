import fs from 'node:fs';
import path from 'node:path';
import { createRequire } from 'node:module';

const MAPLIBRE_VERSION = '6.19.0';
/** Upstream stopped emitting source maps for the `-shared` bundles in 6.19.0.
 * Their sources are a subset of the renderer/worker maps, so they are optional;
 * the four remaining maps are required. */
const REQUIRED_MAPS = ['maplibre-gl', 'maplibre-gl-worker',
  'maplibre-gl-dev', 'maplibre-gl-worker-dev'];
const OPTIONAL_MAPS = ['maplibre-gl-shared', 'maplibre-gl-shared-dev'];
/** Entrypoints the rebundled graph and its emitted worker cannot work without. */
const REQUIRED_SOURCES = ['src/index.ts', 'src/source/worker.ts', 'src/util/actor_messages.ts'];

function assertSourceInventory(target: string): void {
  for (const required of REQUIRED_SOURCES) {
    const file = path.join(target, required);
    if (!fs.existsSync(file) || fs.statSync(file).size === 0) {
      throw new Error(`Missing MapLibre source: ${required}`);
    }
  }
}

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
  if (metadata.version !== MAPLIBRE_VERSION) throw new Error('Qualify MapLibre source/worker inventory before changing version');
  const target = path.resolve(`node_modules/.cache/ukmesh-maplibre-source/${MAPLIBRE_VERSION}`);
  const stamp = path.join(target, '.complete-v1');
  if (fs.existsSync(stamp)) {
    assertSourceInventory(target);
    return target;
  }
  fs.mkdirSync(target, { recursive: true });
  fs.copyFileSync(path.join(packageDir, 'package.json'), path.join(target, 'package.json'));
  const written = new Set<string>();
  const recover = (name: string): void => {
    const mapPath = path.join(packageDir, 'dist', `${name}.mjs.map`);
    if (!fs.existsSync(mapPath)) throw new Error(`Missing MapLibre source map: ${name}`);
    const map = JSON.parse(fs.readFileSync(mapPath, 'utf8')) as {
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
  };
  for (const name of REQUIRED_MAPS) recover(name);
  // 6.19.0 no longer ships the `-shared` maps. When present they can only add
  // sources, so recover them when available but never require them.
  for (const name of OPTIONAL_MAPS) {
    if (!fs.existsSync(path.join(packageDir, 'dist', `${name}.mjs.map`))) continue;
    recover(name);
  }
  // Rollup removes this upstream barrel from source maps because it has no
  // executable statements. Preserve the v6.19.0 barrel's three re-exports.
  fs.writeFileSync(path.join(target, 'src/data/index_array_type.ts'),
    "export { LineIndexArray, TriangleIndexArray, LineStripIndexArray } from './array_types.g.ts';\n");
  assertSourceInventory(target);
  fs.writeFileSync(stamp, `maplibre-${MAPLIBRE_VERSION}-source-map-inventory-v1\n`);
  return target;
}
