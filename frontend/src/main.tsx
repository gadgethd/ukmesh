import React from 'react';
import ReactDOM from 'react-dom/client';
import { BrowserRouter, Routes, Route } from 'react-router-dom';
import { App } from './App.js';
import { Layout } from './pages/Layout.js';
import { HomePage } from './pages/HomePage.js';
import { AboutPage } from './pages/AboutPage.js';
import { InstallPage } from './pages/InstallPage.js';
import { OpenSourcePage } from './pages/OpenSourcePage.js';
import { MqttPage } from './pages/MqttPage.js';
import { StatsPage } from './pages/StatsPage.js';
import { PacketsPage } from './pages/PacketsPage.js';
import { HealthPage } from './pages/HealthPage.js';
import { OwnerPortalPage } from './pages/OwnerPortalPage.js';
import { UKLayout } from './pages/ukmesh/UKLayout.js';
import { UKHomePage } from './pages/ukmesh/UKHomePage.js';
import { UKInstallPage } from './pages/ukmesh/UKInstallPage.js';
import { UKMqttPage } from './pages/ukmesh/UKMqttPage.js';
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
    ) : site.id === 'ukmesh' ? (
      <BrowserRouter>
        <Routes>
          <Route element={<UKLayout />}>
            <Route index element={<UKHomePage />} />
            <Route path="about" element={<AboutPage />} />
            <Route path="install" element={<UKInstallPage />} />
            <Route path="mqtt" element={<UKMqttPage />} />
            <Route path="health" element={<HealthPage />} />
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
            <Route path="about" element={<AboutPage />} />
            <Route path="install" element={<InstallPage />} />
            <Route path="mqtt" element={<MqttPage />} />
            <Route path="health" element={<HealthPage />} />
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
