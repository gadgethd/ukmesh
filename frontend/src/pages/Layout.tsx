import React from 'react';
import { getCurrentSite } from '../config/site.js';
import { SiteLayout } from './shared/SiteLayout.js';
import { SeoHead } from '../components/SeoHead.js';
import { JsonLd } from '../components/JsonLd.js';

export const Layout: React.FC = () => {
  const site = getCurrentSite();
  return (
    <>
    <SeoHead />
    <JsonLd />
    <SiteLayout
      brandName={site.displayName}
      footerName={site.footerName}
      appUrl={site.appUrl}
      showAbout={false}
      showMqtt={false}
      showHealth={false}
      showPackets
      showStats
    />
    </>
  );
};
