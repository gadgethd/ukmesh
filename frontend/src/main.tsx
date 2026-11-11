import React, { Suspense, lazy } from 'react';
import ReactDOM from 'react-dom/client';
import { LoadingIndicator } from './components/LoadingIndicator.js';
import { AppErrorBoundary } from './components/app/AppErrorBoundary.js';
import { ServiceWorkerUpdatePrompt } from './components/ServiceWorkerUpdatePrompt.js';
import {
  initializeRuntimeFeatures,
  startRuntimeFeaturePolling,
} from './config/runtimeFeatures.js';
import './styles/tokens.css';
import './styles/globals.css';
import { registerServiceWorker } from './serviceWorkerUpdates.js';
import { installClientErrorReporting } from './telemetry/clientErrors.js';

// Dedicated release targets eliminate routes belonging to the other host.
// An unset target retains the combined local/development behaviour.
const appOnly = import.meta.env.VITE_BUILD_TARGET === 'app';
const websiteOnly = import.meta.env.VITE_BUILD_TARGET === 'public-website'
  || import.meta.env.VITE_BUILD_TARGET === 'dev-website';
const App = websiteOnly ? null : lazy(() => import('./App.js').then(({ App: Component }) => ({ default: Component })));
const PublicWebsite = appOnly ? null : lazy(() => import('./components/app/PublicWebsite.js')
  .then(({ PublicWebsite: Component }) => ({ default: Component })));
const root = document.getElementById('root')!;
const hostname = window.location.hostname.toLowerCase();
const appHostname = String(import.meta.env['VITE_APP_HOSTNAME'] ?? '').trim().toLowerCase();
const buildSite = String(import.meta.env['VITE_SITE'] ?? '').trim().toLowerCase();
const buildNetwork = String(import.meta.env['VITE_NETWORK'] ?? '').trim().toLowerCase();
const isLocalhost = hostname === 'localhost' || hostname === '127.0.0.1' || hostname === '[::1]';
// Dockerfile.app supplies the ukmesh pair; the public website leaves
// VITE_NETWORK blank and the test website uses test. This keeps localhost:3003
// on the dashboard without turning either website container into the dashboard.
const isDashboardBuild = buildSite === 'ukmesh' && buildNetwork === 'ukmesh';
const isAppDomain = appOnly || (!websiteOnly && (!appHostname || hostname === appHostname || (isLocalhost && isDashboardBuild)));

// Title is managed per-route by SeoHead; only set a fallback for the app domain
if (isAppDomain) document.title = 'MeshCore Analytics';

async function bootstrap(): Promise<void> {
  // Client error capture must be installed before any module can throw.
  installClientErrorReporting();
  // The map is not mounted until its same-origin kill switches have resolved.
  // initializeRuntimeFeatures handles timeout/malformed/offline failures by
  // publishing the all-disabled snapshot.
  await initializeRuntimeFeatures();
  startRuntimeFeaturePolling();

  ReactDOM.createRoot(root).render(
    <React.StrictMode>
      <AppErrorBoundary>
        <ServiceWorkerUpdatePrompt />
        <Suspense fallback={<LoadingIndicator label="Loading..." variant="overlay" />}>
          {isAppDomain && App ? <App /> : PublicWebsite ? <PublicWebsite /> : null}
        </Suspense>
      </AppErrorBoundary>
    </React.StrictMode>,
  );
  registerServiceWorker();
}

void bootstrap();
