import { mergeConfig } from 'vite';
import baseConfig from './vite.config.js';
import { portBase } from './test/e2e/localServers.js';

// The three servers optimize lazy imports independently. Sharing .vite/deps
// lets one optimizer replace files while another server is serving them.
const role = process.env['VITE_SITE'] === 'dev' ? 'dev'
  : process.env['VITE_APP_HOSTNAME'] === 'app.invalid' ? 'public' : 'dashboard';

export default mergeConfig(baseConfig, {
  cacheDir: `node_modules/.vite-playwright/${portBase}/${role}`,
});
