export type NetworkId = 'ukmesh' | 'test';
export type SiteId = 'ukmesh' | 'dev';
export const HEALTH_CHECK_URL = 'https://healthcheck.ukmesh.com';

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
  healthCheckUrl: string;
  embeddedTools?: { mapUrl: string; healthCheckUrl: string; feedUrl: string };
};

function localLinks() {
  // DEV images retain the loopback app build setting when served through the LAN gateway.
  if (!String(import.meta.env['VITE_APP_HOSTNAME'] ?? '').startsWith('127.0.0.1:') || typeof window === 'undefined') return;
  const { hostname, port, protocol } = window.location;
  const loopback = hostname === 'localhost' || hostname === '127.0.0.1' || hostname === '[::1]';
  const lan = /^(?:10\.\d+|192\.168|172\.(?:1[6-9]|2\d|3[01]))\.\d+\.\d+$/.test(hostname);
  const useLan = (lan || loopback) && ['8081', '8083', '8087'].includes(port);
  if (protocol !== 'http:' || (!useLan && !(loopback && ['3003', '3004', '3005', '5173'].includes(port)))) return;
  const base = (target: number) => `http://${hostname}:${target}`;
  const map = base(useLan ? 8081 : 3003);
  const embeddedTools = useLan && port === '8083' ? {
    mapUrl: `${base(8082)}/#/live?embed=1&theme=dark&lat=54.5&lon=-3.5&zoom=6`,
    healthCheckUrl: `${base(8089)}/app`,
    feedUrl: `${base(8084)}/channels.html`,
  } : undefined;
  return { appUrl: map, mapHomeUrl: map, appHomeUrl: base(useLan ? 8083 : 5173), healthCheckUrl: base(useLan ? 8089 : 3090), embeddedTools };
}

const devLinks = localLinks();

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
    healthCheckUrl: HEALTH_CHECK_URL,
    ...devLinks,
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
    healthCheckUrl: HEALTH_CHECK_URL,
    ...devLinks,
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
