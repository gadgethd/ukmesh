import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import viteSeoPlugin from './src/plugins/vite-seo.js';

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
        entryFileNames: 'assets/[name]-[hash].js',
        chunkFileNames: 'assets/[name]-[hash].js',
        assetFileNames: 'assets/[name]-[hash].[ext]',
        manualChunks: {
          // Keep Vite's dynamic-import helper out of the large Deck chunk so
          // public-site routes do not preload Deck just to load a lazy route.
          'vite-preload': ['\0vite/preload-helper.js'],
          'react': ['react', 'react-dom'],
        },
      },
    },
  },
});
