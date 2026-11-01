import { defineConfig, mergeConfig } from 'vite';
import baseConfig from './vite.config.js';
import { portBase } from './test/e2e/localServers.js';

// The three servers optimize lazy imports independently. Sharing .vite/deps
// lets one optimizer replace files while another server is serving them.
const role = process.env['VITE_SITE'] === 'dev' ? 'dev'
  : process.env['VITE_APP_HOSTNAME'] === 'app.invalid' ? 'public' : 'dashboard';

export default defineConfig(env => mergeConfig(typeof baseConfig === 'function' ? baseConfig(env) : baseConfig, {
  cacheDir: `node_modules/.vite-playwright/${portBase}/${role}`,
  server: {
    proxy: {
      '/api': 'http://127.0.0.1:1',
      '/ws': { target: 'ws://127.0.0.1:1', ws: true },
    },
  },
}));
