import React from 'react';
import { getCurrentSite } from '../../config/site.js';
import { SiteLayout } from '../shared/SiteLayout.js';
import { SeoHead } from '../../components/SeoHead.js';
import { JsonLd } from '../../components/JsonLd.js';

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
      showAbout={false}
      showMqtt={false}
      showHealth={false}
      showPackets={false}
      showStats
    />
    </>
  );
};
