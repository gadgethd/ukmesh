import React from 'react';
import ReactDOM from 'react-dom/client';
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { App } from './App.js';
import { Layout } from './pages/Layout.js';
import { HomePage } from './pages/HomePage.js';
import { InstallPage } from './pages/InstallPage.js';
import { OpenSourcePage } from './pages/OpenSourcePage.js';
import { StatsPage } from './pages/StatsPage.js';
import { PacketsPage } from './pages/PacketsPage.js';
import { OwnerPortalPage } from './pages/OwnerPortalPage.js';
import { UKLayout } from './pages/ukmesh/UKLayout.js';
import { UKHomePage } from './pages/ukmesh/UKHomePage.js';
import { UKInstallPage } from './pages/ukmesh/UKInstallPage.js';
import { UKFeedPage } from './pages/ukmesh/UKFeedPage.js';
import { DevLayout } from './pages/dev/DevLayout.js';
import { DevHomePage } from './pages/dev/DevHomePage.js';
import { getCurrentSite } from './config/site.js';
import './styles/globals.css';

const root = document.getElementById('root')!;
const { hostname } = window.location;
const APP_HOSTNAME = import.meta.env['VITE_APP_HOSTNAME'];
const site = getCurrentSite();
const isAppDomain  = !APP_HOSTNAME || hostname === APP_HOSTNAME;

document.title = isAppDomain
  ? 'MeshCore Analytics'
  : site.footerName;

ReactDOM.createRoot(root).render(
  <React.StrictMode>
    {isAppDomain ? (
      <App />
    ) : site.id === 'dev' ? (
      <BrowserRouter>
        <Routes>
          <Route element={<DevLayout />}>
            <Route index element={<DevHomePage />} />
            <Route path="*" element={<Navigate to="/" replace />} />
          </Route>
        </Routes>
      </BrowserRouter>
    ) : site.id === 'ukmesh' ? (
      <BrowserRouter>
        <Routes>
          <Route element={<UKLayout />}>
            <Route index element={<UKHomePage />} />
            <Route path="feed" element={<UKFeedPage />} />
            <Route path="about" element={<Navigate to="/" replace />} />
            <Route path="install" element={<UKInstallPage />} />
            <Route path="mqtt" element={<Navigate to="/install" replace />} />
            <Route path="health" element={<Navigate to="/stats" replace />} />
            <Route path="login" element={<OwnerPortalPage />} />
            <Route path="open-source" element={<OpenSourcePage />} />
            <Route path="stats" element={<StatsPage />} />
          </Route>
        </Routes>
      </BrowserRouter>
    ) : (
      <BrowserRouter>
        <Routes>
          <Route element={<Layout />}>
            <Route index element={<HomePage />} />
            <Route path="about" element={<Navigate to="/" replace />} />
            <Route path="install" element={<InstallPage />} />
            <Route path="mqtt" element={<Navigate to="/install" replace />} />
            <Route path="health" element={<Navigate to="/stats" replace />} />
            <Route path="login" element={<OwnerPortalPage />} />
            <Route path="packets" element={<PacketsPage />} />
            <Route path="open-source" element={<OpenSourcePage />} />
            <Route path="stats" element={<StatsPage />} />
          </Route>
        </Routes>
      </BrowserRouter>
    )}
  </React.StrictMode>
);
