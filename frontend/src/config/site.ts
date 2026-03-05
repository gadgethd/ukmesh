export type SiteId = 'teesside' | 'ukmesh';

type SiteConfig = {
  id: SiteId;
  displayName: string;
  footerName: string;
  network: SiteId;
  appUrl: string;
  appHomeUrl: string;
  mapHomeUrl: string;
};

const SITE_CONFIGS: Record<SiteId, SiteConfig> = {
  teesside: {
    id: 'teesside',
    displayName: 'Teesside Mesh',
    footerName: 'Teesside Mesh Network',
    network: 'teesside',
    appUrl: 'https://app.teessidemesh.com',
    appHomeUrl: 'https://www.teessidemesh.com',
    mapHomeUrl: 'https://app.teessidemesh.com',
  },
  ukmesh: {
    id: 'ukmesh',
    displayName: 'UK Mesh',
    footerName: 'UK Mesh Network',
    network: 'ukmesh',
    appUrl: 'https://app.ukmesh.com',
    appHomeUrl: 'https://www.ukmesh.com',
    mapHomeUrl: 'https://app.ukmesh.com',
  },
};

export function getCurrentSite(): SiteConfig {
  const siteEnv = import.meta.env['VITE_SITE'];
  if (siteEnv === 'ukmesh') return SITE_CONFIGS.ukmesh;
  if (siteEnv === 'teesside') return SITE_CONFIGS.teesside;

  const networkEnv = import.meta.env['VITE_NETWORK'];
  if (networkEnv === 'ukmesh') return SITE_CONFIGS.ukmesh;
  if (networkEnv === 'teesside') return SITE_CONFIGS.teesside;

  if (typeof window !== 'undefined') {
    const host = window.location.hostname.toLowerCase();
    if (host.includes('ukmesh.com')) return SITE_CONFIGS.ukmesh;
    if (host.includes('teessidemesh.com')) return SITE_CONFIGS.teesside;
  }

  return SITE_CONFIGS.teesside;
}
