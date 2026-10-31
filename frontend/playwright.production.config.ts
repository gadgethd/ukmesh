import {defineConfig} from '@playwright/test';
import base from './playwright.config.js';
// Lifecycle assertions qualify the emitted app and website, as deployed.
export default defineConfig({...base,fullyParallel:false,workers:1,timeout:90000,expect:{timeout:20000},
use:{...base.use,launchOptions:{executablePath:process.env['PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH'],args:['--use-gl=angle','--use-angle=swiftshader','--enable-unsafe-swiftshader']}},
projects:base.projects?.filter(p=>p.name!=='mobile-regression'),
webServer:[
 {command:'VITE_BUILD_TARGET=public-website VITE_APP_HOSTNAME=app.ukmesh.com VITE_SITE=ukmesh npm run build -- --config vite.e2e.config.ts --outDir dist/e2e-public && npm run preview -- --config vite.e2e.config.ts --host 127.0.0.1 --port 4173 --strictPort --outDir dist/e2e-public',port:4173,reuseExistingServer:false,timeout:120000},
 {command:'VITE_BUILD_TARGET=app VITE_APP_HOSTNAME=app.ukmesh.com VITE_SITE=ukmesh VITE_NETWORK=ukmesh VITE_RF_COVERAGE_ENABLED=true npm run build -- --config vite.e2e.config.ts --outDir dist/e2e-app && npm run preview -- --config vite.e2e.config.ts --host 127.0.0.1 --port 4174 --strictPort --outDir dist/e2e-app',port:4174,reuseExistingServer:false,timeout:120000},
]});
