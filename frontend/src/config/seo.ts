import type { SiteId } from './site.js';
import { PUBLIC_CONTENT_ROUTES } from './publicRoutes.js';

export type RouteMeta = {
  title: string;
  description: string;
};

/** Per-site, per-route SEO metadata. Used by both the Vite build plugin and the runtime SeoHead component. */
// Dev/test builds are deliberately no-index and have no public metadata.
export const SEO_META: Partial<Record<SiteId, Record<string, RouteMeta>>> = {
  ukmesh: Object.fromEntries(PUBLIC_CONTENT_ROUTES.map((route) => [
    route.path,
    { title: route.title, description: route.description },
  ])),
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
  ukmesh: PUBLIC_CONTENT_ROUTES.filter((route) => route.sitemap).map((route) => route.path),
};
