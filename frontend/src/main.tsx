import React, { Suspense, lazy } from 'react';
import ReactDOM from 'react-dom/client';
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { LoadingIndicator } from './components/LoadingIndicator.js';
import { AppErrorBoundary } from './components/app/AppErrorBoundary.js';
import './styles/tokens.css';
import './styles/globals.css';

const App = lazy(() => import('./App.js').then(({ App: Component }) => ({ default: Component })));
const OpenSourcePage = lazy(() => import('./pages/OpenSourcePage.js').then(({ OpenSourcePage: Component }) => ({ default: Component })));
const StatsPage = lazy(() => import('./pages/StatsPage.js').then(({ StatsPage: Component }) => ({ default: Component })));
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
const StatusPage = lazy(() => import('./pages/StatusPage.js').then(({ StatusPage: Component }) => ({ default: Component })));

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

if ('serviceWorker' in navigator) {
  let refreshing = false;
  const reloadForUpdate = () => {
    if (refreshing) return;
    refreshing = true;
    window.location.reload();
  };
  navigator.serviceWorker.addEventListener('controllerchange', reloadForUpdate);
  navigator.serviceWorker.register('/sw.js')
    .then((registration) => {
      registration.addEventListener('updatefound', () => {
        const installing = registration.installing;
        if (!installing) return;
        installing.addEventListener('statechange', () => {
          if (installing.state === 'installed' && navigator.serviceWorker.controller) {
            reloadForUpdate();
          }
        });
      });
    })
    .catch(() => {});
}

// Title is managed per-route by SeoHead; only set a fallback for the app domain
if (isAppDomain) document.title = 'MeshCore Analytics';

ReactDOM.createRoot(root).render(
  <React.StrictMode>
    <AppErrorBoundary>
      <Suspense fallback={<LoadingIndicator label="Loading..." variant="overlay" />}>
        {isAppDomain ? (
          <App />
        ) : (
          <BrowserRouter>
            <Routes>
            <Route element={<UKLayout />}>
              <Route index element={<UKHomePage />} />
              <Route path="feed" element={<UKFeedPage />} />
              <Route path="repeater" element={<UKRepeaterSearchPage />} />
              <Route path="companion" element={<UKCompanionPage />} />
              <Route path="regions" element={<Navigate to="/" replace />} />
              <Route path="about" element={<Navigate to="/" replace />} />
              <Route path="install" element={<UKInstallPage />} />
              <Route path="docs" element={<UKBestPracticePage />} />
              <Route path="mqtt" element={<Navigate to="/install" replace />} />
              <Route path="health" element={<StatusPage />} />
              <Route path="status" element={<Navigate to="/health" replace />} />
              <Route path="login" element={<OwnerPortalPage />} />
              <Route path="open-source" element={<OpenSourcePage />} />
              <Route path="stats" element={<StatsPage />} />
              <Route path="spam" element={<SpamPage />} />
              <Route path="topology" element={<TopologyPage />} />
            </Route>
            </Routes>
          </BrowserRouter>
        )}
      </Suspense>
    </AppErrorBoundary>
  </React.StrictMode>
);
