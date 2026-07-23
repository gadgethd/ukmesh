import React from 'react';
import { getCurrentSite } from '../../config/site.js';
import { SiteLayout } from '../shared/SiteLayout.js';
import { SeoHead } from '../../components/SeoHead.js';
import { JsonLd } from '../../components/JsonLd.js';
import '../site-shell.css';
import '../site-content.css';
import '../docs-pages.css';

export const UKLayout: React.FC = () => {
  const site = getCurrentSite();
  return (
    <>
    <SeoHead />
    <JsonLd />
    <SiteLayout
      brandName={site.displayName}
      footerName={site.footerName}
      appUrl={site.appUrl}
      showFeed
      showRepeaterSearch
      showCompanion
      showRegions={false}
      showTopology
      showAbout={false}
      showInstall
      showBestPractice
      showMqtt={false}
      showHealth
      showPackets={false}
      showOpenSource={false}
      showStats={false}
    />
    </>
  );
};
