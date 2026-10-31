import { defineConfig, transformWithOxc } from 'vite';
import react from '@vitejs/plugin-react';
import path from 'node:path';
import fs from 'node:fs';
import { minify as minifyCharts } from 'terser';
import { prepareMapLibreSource } from './src/plugins/maplibre-source.js';
const mapLibreSource = prepareMapLibreSource();
import viteSeoPlugin from './src/plugins/vite-seo.js';

const productionTarget = process.env.VITE_BUILD_TARGET || (
  process.env.VITE_SITE === 'ukmesh' && process.env.VITE_NETWORK === 'ukmesh' ? 'app'
    : process.env.VITE_SITE === 'dev' || process.env.VITE_NETWORK === 'test' ? 'dev-website' : 'public-website'
);
export default defineConfig(({ command }) => ({
  define: command === 'build' ? { 'import.meta.env.VITE_BUILD_TARGET': JSON.stringify(productionTarget) } : {},
  plugins: [{
    name: 'maplibre-upstream-typescript-semantics', enforce: 'pre',
    async transform(code, id) {
      const filename = id.split('?', 1)[0];
      if (!filename.startsWith(mapLibreSource) || !filename.endsWith('.ts')) return;
      return transformWithOxc(code, filename, {
        lang: 'ts', target: 'es2020', tsconfig: false,
        assumptions: { setPublicClassFields: true },
        typescript: { removeClassFieldsWithoutInitializer: true },
      });
    },
  }, {
    name: 'compact-chart-chunk',
    renderChunk: { order: 'post', async handler(code, chunk) {
      if (chunk.name !== 'recharts') return null;
      const result = await minifyCharts(code, { module: true, compress: { passes: 2 }, mangle: true, format: { comments: /@license|@preserve|^!/ } });
      if (!result.code) throw new Error('Chart minification produced no output');
      return { code: result.code, map: null };
    } },
  }, react(), viteSeoPlugin(), {
    name: 'maplibre-shared-module-worker',
    apply: 'build',
    resolveId(id) { if (id === 'virtual:maplibre-worker-url') return '\0maplibre-worker-url'; },
    load(id) {
      if (id !== '\0maplibre-worker-url') return;
      const entry = path.join(mapLibreSource, 'src/source/worker.ts');
      const reference = this.emitFile({ type: 'chunk', id: entry, name: 'maplibre-gl-worker' });
      return `export default import.meta.ROLLUP_FILE_URL_${reference};`;
    },
    generateBundle() {
      this.emitFile({ type: 'asset', fileName: 'build-target.json', source: JSON.stringify({ target: productionTarget }) });
      this.emitFile({ type: 'asset', fileName: 'assets/maplibre-LICENSE.txt', source: fs.readFileSync(path.resolve('node_modules/maplibre-gl/LICENSE.txt'), 'utf8') });
    },
  }, {
    name: 'maplibre-worker-dev', apply: 'serve',
    resolveId(id) { if (id === 'virtual:maplibre-worker-url') return '\0maplibre-worker-dev'; },
    load(id) { if (id === '\0maplibre-worker-dev') return 'import url from "maplibre-gl/dist/maplibre-gl-worker.mjs?worker&url";export default url;'; },
  }],
  resolve: { alias: [{ find: /^maplibre-gl$/, replacement: path.join(mapLibreSource, 'src/index.ts') }] },
  optimizeDeps: {
    // maplibre-gl v6 loads its worker via new URL(..., import.meta.url) —
    // vite's dep pre-bundling rewrites that to node_modules/.vite/deps/maplibre-gl-worker.mjs
    // which does not exist, killing the worker (and custom raster protocols) in dev.
    exclude: ['maplibre-gl'],
    // The excluded upstream source graph reaches CommonJS packages directly.
    // Prebundle these in dev so their default exports have the required interop.
    include: ['murmurhash-js', 'earcut', '@maplibre/mlt'],
  },
  server: {
    proxy: {
      '/api': 'http://localhost:3000',
      '/ws': { target: 'ws://localhost:3000', ws: true },
    },
  },
  build: {
    rollupOptions: {
      output: {
        entryFileNames: 'assets/[name]-[hash].js',
        chunkFileNames: 'assets/[name]-[hash].js',
        assetFileNames: 'assets/[name]-[hash].[ext]',
        codeSplitting: {
          includeDependenciesRecursively: false,
          groups: [
            { name: 'map-math', test: /\/node_modules\/(?:gl-matrix|earcut|@mapbox|pbf|quickselect|tinyqueue|kdbush|@maplibre)\//, priority: 50, includeDependenciesRecursively: true },
            { name: 'maplibre', test: /ukmesh-maplibre-source/, priority: 20 },
            { name: 'deck', test: /\/node_modules\/(?:@deck\.gl|@luma\.gl|@math\.gl|@loaders\.gl)\//, priority: 18, includeDependenciesRecursively: true },
            { name: 'recharts', test: /\/node_modules\/recharts\//, priority: 15, includeDependenciesRecursively: true },
            { name: 'vite-preload', test: /vite\/preload-helper/, priority: 100 },
            { name: 'react', test: /\/node_modules\/(?:react|react-dom)\//, priority: 40 },
          ],
        },
      },
    },
  },
}));
