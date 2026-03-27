import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import viteSeoPlugin from './src/plugins/vite-seo.js';

const buildTime = Date.now();

export default defineConfig({
  plugins: [react(), viteSeoPlugin()],
  server: {
    proxy: {
      '/api': 'http://localhost:3000',
      '/ws': { target: 'ws://localhost:3000', ws: true },
    },
  },
  build: {
    rollupOptions: {
      output: {
        entryFileNames: `assets/[name]-[hash]-${buildTime}.js`,
        chunkFileNames: `assets/[name]-[hash]-${buildTime}.js`,
        assetFileNames: `assets/[name]-[hash]-${buildTime}.[ext]`,
        manualChunks: {
          'deck': ['@deck.gl/core', '@deck.gl/layers', '@deck.gl/geo-layers', '@deck.gl/mapbox'],
          'maplibre': ['maplibre-gl'],
          'react': ['react', 'react-dom'],
        },
      },
    },
  },
});
