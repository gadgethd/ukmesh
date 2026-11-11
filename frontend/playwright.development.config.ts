import {defineConfig,devices} from '@playwright/test';
export default defineConfig({testDir:'./test/e2e',testMatch:/development\.spec\.ts/,workers:1,timeout:90000,expect:{timeout:20000},
use:{baseURL:'http://127.0.0.1:4182',serviceWorkers:'block',trace:'retain-on-failure',screenshot:'only-on-failure',launchOptions:{executablePath:process.env['PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH'],args:['--use-gl=angle','--use-angle=swiftshader','--enable-unsafe-swiftshader']}},
projects:[{name:'development-desktop',use:{...devices['Desktop Chrome']}},{name:'development-mobile',use:{...devices['Pixel 7'],deviceScaleFactor:1}}],
webServer:{command:'VITE_BUILD_TARGET=combined VITE_APP_HOSTNAME=127.0.0.1 VITE_SITE=ukmesh VITE_NETWORK=ukmesh VITE_RF_COVERAGE_ENABLED=true npm run dev -- --config vite.e2e.config.ts --host 127.0.0.1 --port 4182 --strictPort',port:4182,reuseExistingServer:false,timeout:120000}});
