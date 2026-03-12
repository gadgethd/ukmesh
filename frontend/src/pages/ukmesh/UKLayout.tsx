import React from 'react';
import { getCurrentSite } from '../../config/site.js';
import { SiteLayout } from '../shared/SiteLayout.js';

export const UKLayout: React.FC = () => {
  const site = getCurrentSite();
  return (
    <SiteLayout
      brandName={site.displayName}
      footerName={site.footerName}
      appUrl={site.appUrl}
      showFeed
      showAbout={false}
      showMqtt={false}
      showHealth={false}
      showPackets={false}
      showStats
    />
  );
};
