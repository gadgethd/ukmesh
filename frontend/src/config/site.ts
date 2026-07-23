export type NetworkId = 'ukmesh' | 'test';
export type SiteId = 'ukmesh' | 'dev';

export type SiteConfig = {
  id: SiteId;
  displayName: string;
  footerName: string;
  network: NetworkId;
  networkFilter?: NetworkId;
  observerId?: string;
  appUrl: string;
  appHomeUrl: string;
  mapHomeUrl: string;
};

function envValue(key: keyof ImportMetaEnv, fallback: string): string {
  const value = String(import.meta.env[key] ?? '').trim();
  return value || fallback;
}

const DEV_OBSERVER_ID = String(import.meta.env['VITE_OBSERVER_ID'] ?? '').trim().toLowerCase() || undefined;

const SITE_CONFIGS: Record<SiteId, SiteConfig> = {
  ukmesh: {
    id: 'ukmesh',
    displayName: 'UK Mesh',
    footerName: 'UK Mesh Network',
    network: 'ukmesh',
    networkFilter: 'ukmesh',
    appUrl: 'https://app.ukmesh.com',
    appHomeUrl: 'https://ukmesh.com',
    mapHomeUrl: 'https://app.ukmesh.com',
  },
  dev: {
    id: 'dev',
    displayName: envValue('VITE_SITE_DISPLAY_NAME', 'UK Mesh Test'),
    footerName: envValue('VITE_SITE_FOOTER_NAME', 'UK Mesh Test'),
    network: 'test',
    networkFilter: 'test',
    observerId: DEV_OBSERVER_ID,
    appUrl: envValue('VITE_SITE_APP_URL', 'https://test.ukmesh.com'),
    appHomeUrl: envValue('VITE_SITE_HOME_URL', 'https://test.ukmesh.com'),
    mapHomeUrl: envValue('VITE_SITE_APP_URL', 'https://test.ukmesh.com'),
  },
};

export function getCurrentSite(): SiteConfig {
  const siteEnv = String(import.meta.env['VITE_SITE'] ?? '').trim().toLowerCase();
  if (siteEnv === 'dev') return SITE_CONFIGS.dev;
  if (siteEnv === 'ukmesh') return SITE_CONFIGS.ukmesh;

  const networkEnv = String(import.meta.env['VITE_NETWORK'] ?? '').trim().toLowerCase();
  if (networkEnv === 'test') return SITE_CONFIGS.dev;
  if (networkEnv === 'ukmesh') return SITE_CONFIGS.ukmesh;

  if (typeof window !== 'undefined') {
    const host = window.location.hostname.toLowerCase();
    if (host === 'test.ukmesh.com' || host.includes('app-dev.ukmesh.com') || host === 'dev.ukmesh.com') {
      return SITE_CONFIGS.dev;
    }
  }

  return SITE_CONFIGS.ukmesh;
}
