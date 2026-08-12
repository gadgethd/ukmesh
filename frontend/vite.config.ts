import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { defineConfig, type Plugin } from 'vite';
import react from '@vitejs/plugin-react';
import viteSeoPlugin from './src/plugins/vite-seo.js';

/**
 * maplibre-gl v6 loads its WebWorker via new URL(<var>, import.meta.url) —
 * the URL is built dynamically, so Rollup cannot statically rewrite it and no
 * worker asset is ever emitted. At runtime the browser requests
 * /assets/maplibre-gl-worker.mjs, which 404s (nginx SPA fallback serves HTML),
 * and the map renders dead (no tiles, no node dots). Emit the worker file
 * explicitly so the runtime request resolves.
 */
function maplibreWorkerPlugin(): Plugin {
  return {
    name: 'maplibre-worker-emit',
    generateBundle() {
      const workerPath = fileURLToPath(
        new URL('./node_modules/maplibre-gl/dist/maplibre-gl-worker.mjs', import.meta.url),
      );
      this.emitFile({
        type: 'asset',
        fileName: 'assets/maplibre-gl-worker.mjs',
        source: readFileSync(workerPath),
      });
    },
  };
}

export default defineConfig({
  plugins: [react(), viteSeoPlugin(), maplibreWorkerPlugin()],
  optimizeDeps: {
    // maplibre-gl v6 loads its worker via new URL(..., import.meta.url) —
    // vite's dep pre-bundling rewrites that to node_modules/.vite/deps/maplibre-gl-worker.mjs
    // which does not exist, killing the worker (and custom raster protocols) in dev.
    exclude: ['maplibre-gl'],
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
        manualChunks(id) {
          // Keep Vite's dynamic-import helper out of the large Deck chunk so
          // public-site routes do not preload Deck just to load a lazy route.
          if (id === '\0vite/preload-helper.js') return 'vite-preload';
          if (/\/node_modules\/(?:react|react-dom)\//.test(id)) return 'react';
          return undefined;
        },
      },
    },
  },
});
