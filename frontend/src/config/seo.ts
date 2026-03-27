import type { SiteId } from './site.js';

export type RouteMeta = {
  title: string;
  description: string;
};

/** Per-site, per-route SEO metadata. Used by both the Vite build plugin and the runtime SeoHead component. */
export const SEO_META: Record<SiteId, Record<string, RouteMeta>> = {
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
    '/open-source': {
      title: 'Open Source — UK Mesh Network',
      description:
        'Libraries and open-source technologies powering the UK Mesh analytics platform.',
    },
    '/login': {
      title: 'Repeater Owner Portal — UK Mesh Network',
      description:
        'Log in to manage your MeshCore repeater node on the UK Mesh network.',
    },
  },
  teesside: {
    '/': {
      title: 'Teesside Mesh — MeshCore LoRa Network Dashboard',
      description:
        'Live dashboard for the Teesside MeshCore LoRa mesh network. Real-time node map, packet feed, network statistics, and install guides.',
    },
    '/install': {
      title: 'Install MeshCore — Teesside Mesh',
      description:
        'Get a companion node on the air in about 10 minutes. Flash MeshCore firmware on a LoRa board and join the Teesside mesh network.',
    },
    '/stats': {
      title: 'Network Statistics — Teesside Mesh',
      description:
        'Live statistics for the Teesside MeshCore network: active nodes, packet counts, repeater uptime, and coverage trends.',
    },
    '/packets': {
      title: 'Packet Types — Teesside Mesh',
      description:
        'Reference guide to MeshCore packet types: adverts, messages, traceroutes, and more. Understand what flows through the mesh.',
    },
    '/open-source': {
      title: 'Open Source — Teesside Mesh',
      description:
        'Libraries and open-source technologies powering the Teesside Mesh analytics platform.',
    },
    '/login': {
      title: 'Repeater Owner Portal — Teesside Mesh',
      description:
        'Log in to manage your MeshCore repeater node on the Teesside mesh network.',
    },
  },
  dev: {
    '/': {
      title: 'UK Mesh Test — Development Environment',
      description: 'Development and testing environment for the UK Mesh network analytics platform.',
    },
  },
};

/** Site-level defaults used for OG tags and the base index.html. */
export const SITE_SEO_DEFAULTS: Record<SiteId, { siteName: string; baseUrl: string; themeColor: string }> = {
  ukmesh: {
    siteName: 'UK Mesh Network',
    baseUrl: 'https://ukmesh.com',
    themeColor: '#0a1628',
  },
  teesside: {
    siteName: 'Teesside Mesh',
    baseUrl: 'https://www.teessidemesh.com',
    themeColor: '#0a1628',
  },
  dev: {
    siteName: 'UK Mesh Test',
    baseUrl: 'https://test.ukmesh.com',
    themeColor: '#0a1628',
  },
};

/** Routes to include in the sitemap for each site. Order = priority (descending). */
export const SITEMAP_ROUTES: Record<SiteId, string[]> = {
  ukmesh: ['/', '/install', '/stats', '/feed', '/repeater', '/open-source'],
  teesside: ['/', '/install', '/stats', '/packets', '/open-source'],
  dev: ['/'],
};
