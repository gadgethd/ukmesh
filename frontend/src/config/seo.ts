import type { SiteId } from './site.js';

export type RouteMeta = {
  title: string;
  description: string;
};

/** Per-site, per-route SEO metadata. Used by both the Vite build plugin and the runtime SeoHead component. */
// Dev/test builds are deliberately no-index and have no public metadata.
export const SEO_META: Partial<Record<SiteId, Record<string, RouteMeta>>> = {
  ukmesh: {
    '/': {
      title: 'UK Mesh Network — MeshCore LoRa Coverage & Live Map',
      description:
        'Real-time analytics for the UK MeshCore LoRa mesh network. Live packet feed, repeater coverage maps, network statistics, and install guides.',
    },
    '/install': {
      title: 'Install MeshCore — UK Mesh Network',
      description:
        'Step-by-step guide to flash MeshCore firmware on a LoRa device and join the UK mesh network. No soldering, no special tools — just a browser and a USB cable.',
    },
    '/stats': {
      title: 'Network Statistics — UK Mesh Network',
      description:
        'Live statistics for the UK MeshCore network: active nodes, packet counts, repeater uptime, and coverage trends.',
    },
    '/feed': {
      title: 'Live Packet Feed — UK Mesh Network',
      description:
        'Real-time decoded LoRa packet stream from UK MeshCore observers. Watch adverts, messages, and traceroutes as they arrive.',
    },
    '/repeater': {
      title: 'Repeater Search — UK Mesh Network',
      description:
        'Search and browse MeshCore repeater nodes across the UK network. View coverage, uptime, and connection details.',
    },
    '/regions': {
      title: 'Regions — UK Mesh Network',
      description:
        'Browse MeshCore mesh network regions across the UK.',
    },
    '/docs': {
      title: 'Docs — UK Mesh Network',
      description:
        'Radio settings, repeater placement, network etiquette, and troubleshooting guidance for UK MeshCore operators.',
    },
    '/open-source': {
      title: 'Open Source — UK Mesh Network',
      description:
        'Libraries and open-source technologies powering the UK Mesh analytics platform.',
    },
    '/spam': {
      title: 'Spam Watch — UK Mesh Network',
      description:
        'Live detection of suspected message-spam clusters on the UK MeshCore network: repeated near-duplicate messages, rotating sender names, and coarse origin estimates. Sanitized, privacy-safe abuse mitigation.',
    },
    '/topology': {
      title: 'Repeater Topology — UK Mesh Network',
      description:
        'Explore recent viable repeater relationships, highly connected relay hubs, and observed MeshCore network topology across the UK.',
    },
    '/health': {
      title: 'Platform Status — UK Mesh Network',
      description: 'Public platform health, packet-ingest freshness, synthetic journeys, and background-worker status for UKMesh analytics.',
    },
    '/login': {
      title: 'Repeater Owner Portal — UK Mesh Network',
      description:
        'Log in to manage your MeshCore repeater node on the UK Mesh network.',
    },
  },
};

/** Site-level defaults used for OG tags and the base index.html. */
export const SITE_SEO_DEFAULTS: Partial<Record<SiteId, { siteName: string; baseUrl: string; themeColor: string }>> = {
  ukmesh: {
    siteName: 'UK Mesh Network',
    baseUrl: 'https://ukmesh.com',
    themeColor: '#0a1628',
  },
};

/** Routes to include in the sitemap for each site. Order = priority (descending). */
export const SITEMAP_ROUTES: Partial<Record<SiteId, string[]>> = {
  ukmesh: ['/', '/install', '/docs', '/feed', '/repeater', '/topology', '/health', '/regions', '/stats', '/spam', '/open-source'],
};
