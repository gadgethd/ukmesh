import React, { Suspense, lazy } from 'react';
import ReactDOM from 'react-dom/client';
import { BrowserRouter, Routes, Route, Navigate } from 'react-router';
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
import {
  PUBLIC_ROUTES,
  type PublicRouteComponent,
} from './config/publicRoutes.js';
import { installClientErrorReporting } from './telemetry/clientErrors.js';

const App = lazy(() => import('./App.js').then(({ App: Component }) => ({ default: Component })));
const OpenSourcePage = lazy(() => import('./pages/OpenSourcePage.js').then(({ OpenSourcePage: Component }) => ({ default: Component })));
const StatsPage = lazy(() => import('./pages/StatsPage.js').then(({ StatsPage: Component }) => ({ default: Component })));
const ContactPage = lazy(() => import('./pages/ContactPage.js').then(({ ContactPage: Component }) => ({ default: Component })));
const PrivacyPage = lazy(() => import('./pages/PrivacyPage.js').then(({ PrivacyPage: Component }) => ({ default: Component })));
const OwnerPortalPage = lazy(() => import('./pages/OwnerPortalPage.js').then(({ OwnerPortalPage: Component }) => ({ default: Component })));
const UKLayout = lazy(() => import('./pages/ukmesh/UKLayout.js').then(({ UKLayout: Component }) => ({ default: Component })));
const UKHomePage = lazy(() => import('./pages/ukmesh/UKHomePage.js').then(({ UKHomePage: Component }) => ({ default: Component })));
const UKInstallPage = lazy(() => import('./pages/ukmesh/UKInstallPage.js').then(({ UKInstallPage: Component }) => ({ default: Component })));
const UKFeedPage = lazy(() => import('./pages/ukmesh/UKFeedPage.js').then(({ UKFeedPage: Component }) => ({ default: Component })));
const UKRepeaterSearchPage = lazy(() => import('./pages/ukmesh/UKRepeaterSearchPage.js').then(({ UKRepeaterSearchPage: Component }) => ({ default: Component })));
const UKCompanionPage = lazy(() => import('./pages/ukmesh/UKCompanionPage.js').then(({ UKCompanionPage: Component }) => ({ default: Component })));
const UKBestPracticePage = lazy(() => import('./pages/ukmesh/UKBestPracticePage.js').then(({ UKBestPracticePage: Component }) => ({ default: Component })));
const SpamPage = lazy(() => import('./pages/SpamTransparencyPage.js').then(({ SpamPage: Component }) => ({ default: Component })));
const TopologyPage = lazy(() => import('./pages/TopologyPage.js').then(({ TopologyPage: Component }) => ({ default: Component })));
const NotFoundPage = lazy(() => import('./pages/NotFoundPage.js').then(({ NotFoundPage: Component }) => ({ default: Component })));

const PUBLIC_ROUTE_ELEMENTS: Record<PublicRouteComponent, React.ReactElement> = {
  home: <UKHomePage />,
  feed: <UKFeedPage />,
  repeater: <UKRepeaterSearchPage />,
  companion: <UKCompanionPage />,
  install: <UKInstallPage />,
  docs: <UKBestPracticePage />,
  owner: <OwnerPortalPage />,
  'open-source': <OpenSourcePage />,
  stats: <StatsPage />,
  spam: <SpamPage />,
  topology: <TopologyPage />,
  contact: <ContactPage />,
  privacy: <PrivacyPage />,
};

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
const isAppDomain = !appHostname || hostname === appHostname || (isLocalhost && isDashboardBuild);

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
          {isAppDomain ? (
            <App />
          ) : (
            <BrowserRouter>
              <Routes>
              <Route element={<UKLayout />}>
                {PUBLIC_ROUTES.map((route) => {
                  const element = route.redirectTo
                    ? <Navigate to={route.redirectTo} replace />
                    : route.component
                      ? PUBLIC_ROUTE_ELEMENTS[route.component]
                      : null;
                  return route.path === '/'
                    ? <Route key={route.path} index element={element} />
                    : <Route key={route.path} path={route.path.slice(1)} element={element} />;
                })}
                <Route path="*" element={<NotFoundPage />} />
              </Route>
              </Routes>
            </BrowserRouter>
          )}
        </Suspense>
      </AppErrorBoundary>
    </React.StrictMode>,
  );
  registerServiceWorker();
}

void bootstrap();
