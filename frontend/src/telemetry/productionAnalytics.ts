import { isTelemetryDisabled } from './clientErrors.js';

const PRODUCTION_HOSTS = ['ukmesh.com', 'www.ukmesh.com', 'app.ukmesh.com'];
const WEBSITE_ID = '8f076103-e8d0-4642-88a7-9eb8bbd58f35';

export function installProductionAnalytics(): void {
  const { hostname, origin, protocol } = window.location;
  if (protocol !== 'https:' || !PRODUCTION_HOSTS.includes(hostname)
    || navigator.doNotTrack === '1' || isTelemetryDisabled()) return;

  const script = document.createElement('script');
  script.src = '/_stats/script.js';
  script.referrerPolicy = 'no-referrer';
  script.dataset['websiteId'] = WEBSITE_ID;
  script.dataset['domains'] = PRODUCTION_HOSTS.join(',');
  script.dataset['hostUrl'] = `${origin}/_stats`;
  script.dataset['doNotTrack'] = 'true';
  script.dataset['excludeSearch'] = 'true';
  script.dataset['excludeHash'] = 'true';
  document.head.append(script);
}
