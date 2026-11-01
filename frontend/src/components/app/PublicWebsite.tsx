import React, { lazy } from 'react';
import { BrowserRouter, Routes, Route, Navigate } from 'react-router';
import { PUBLIC_ROUTES, type PublicRouteComponent } from '../../config/publicRoutes.js';

const OpenSourcePage = lazy(() => import('../../pages/OpenSourcePage.js').then(({ OpenSourcePage: Component }) => ({ default: Component })));
const StatsPage = lazy(() => import('../../pages/StatsPage.js').then(({ StatsPage: Component }) => ({ default: Component })));
const ContactPage = lazy(() => import('../../pages/ContactPage.js').then(({ ContactPage: Component }) => ({ default: Component })));
const PrivacyPage = lazy(() => import('../../pages/PrivacyPage.js').then(({ PrivacyPage: Component }) => ({ default: Component })));
const OwnerPortalPage = lazy(() => import('../../pages/OwnerPortalPage.js').then(({ OwnerPortalPage: Component }) => ({ default: Component })));
const UKLayout = lazy(() => import('../../pages/ukmesh/UKLayout.js').then(({ UKLayout: Component }) => ({ default: Component })));
const UKHomePage = lazy(() => import('../../pages/ukmesh/UKHomePage.js').then(({ UKHomePage: Component }) => ({ default: Component })));
const UKInstallPage = lazy(() => import('../../pages/ukmesh/UKInstallPage.js').then(({ UKInstallPage: Component }) => ({ default: Component })));
const UKFeedPage = lazy(() => import('../../pages/ukmesh/UKFeedPage.js').then(({ UKFeedPage: Component }) => ({ default: Component })));
const UKRepeaterSearchPage = lazy(() => import('../../pages/ukmesh/UKRepeaterSearchPage.js').then(({ UKRepeaterSearchPage: Component }) => ({ default: Component })));
const UKCompanionPage = lazy(() => import('../../pages/ukmesh/UKCompanionPage.js').then(({ UKCompanionPage: Component }) => ({ default: Component })));
const UKBestPracticePage = lazy(() => import('../../pages/ukmesh/UKBestPracticePage.js').then(({ UKBestPracticePage: Component }) => ({ default: Component })));
const SpamPage = lazy(() => import('../../pages/SpamTransparencyPage.js').then(({ SpamPage: Component }) => ({ default: Component })));
const TopologyPage = lazy(() => import('../../pages/TopologyPage.js').then(({ TopologyPage: Component }) => ({ default: Component })));
const RegionsPage = lazy(() => import('../../pages/RegionsPage.js').then(({ RegionsPage: Component }) => ({ default: Component })));
const NotFoundPage = lazy(() => import('../../pages/NotFoundPage.js').then(({ NotFoundPage: Component }) => ({ default: Component })));

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
  regions: <RegionsPage />,
  contact: <ContactPage />,
  privacy: <PrivacyPage />,
};

export function PublicWebsite() {
  return <BrowserRouter><Routes>
    <Route element={<UKLayout />}>
      {PUBLIC_ROUTES.map((route) => {
        const element = route.redirectTo
          ? <Navigate to={route.redirectTo} replace />
          : route.component ? PUBLIC_ROUTE_ELEMENTS[route.component] : null;
        return route.path === '/'
          ? <Route key={route.path} index element={element} />
          : <Route key={route.path} path={route.path.slice(1)} element={element} />;
      })}
      <Route path="*" element={<NotFoundPage />} />
    </Route>
  </Routes></BrowserRouter>;
}
