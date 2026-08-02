import { type Plugin } from 'vite';
import fs from 'node:fs';
import path from 'node:path';
import { PUBLIC_CONTENT_ROUTES } from '../config/publicRoutes.js';

type SiteId = 'ukmesh';

type RouteMeta = { title: string; description: string };

const SEO_META: Record<SiteId, Record<string, RouteMeta>> = {
  ukmesh: Object.fromEntries(PUBLIC_CONTENT_ROUTES.map((route) => [
    route.path,
    { title: route.title, description: route.description },
  ])),
};

const SITE_DEFAULTS: Record<SiteId, { siteName: string; baseUrl: string; themeColor: string }> = {
  ukmesh: { siteName: 'UK Mesh Network', baseUrl: 'https://ukmesh.com', themeColor: '#0a1628' },
};

const SITEMAP_ROUTES: Record<SiteId, string[]> = {
  ukmesh: PUBLIC_CONTENT_ROUTES.filter((route) => route.sitemap).map((route) => route.path),
};

function getSiteId(): SiteId {
  return 'ukmesh';
}

type BuildTarget = 'app' | 'public-website' | 'dev-website';

function buildTarget(): BuildTarget {
  const explicitTarget = String(process.env['VITE_BUILD_TARGET'] ?? '').trim().toLowerCase();
  if (
    explicitTarget === 'app'
    || explicitTarget === 'public-website'
    || explicitTarget === 'dev-website'
  ) {
    return explicitTarget;
  }

  const site = String(process.env['VITE_SITE'] ?? '').trim().toLowerCase();
  const network = String(process.env['VITE_NETWORK'] ?? '').trim().toLowerCase();

  // Preserve direct/local build compatibility, but release Dockerfiles always
  // supply VITE_BUILD_TARGET so cached layers cannot change the artifact type.
  if (site === 'ukmesh' && network === 'ukmesh') return 'app';
  if (site === 'dev' || network === 'test') return 'dev-website';
  return 'public-website';
}

function buildMetaTags(meta: RouteMeta, routePath: string, site: SiteId): string {
  const defaults = SITE_DEFAULTS[site];
  const url = `${defaults.baseUrl}${routePath === '/' ? '' : routePath}`;
  const ogImage = `${defaults.baseUrl}/og-image.png`;

  return [
    `<title>${meta.title}</title>`,
    `<meta name="description" content="${meta.description}">`,
    `<link rel="canonical" href="${url}">`,
    `<meta name="theme-color" content="${defaults.themeColor}">`,
    `<meta property="og:type" content="website">`,
    `<meta property="og:site_name" content="${defaults.siteName}">`,
    `<meta property="og:title" content="${meta.title}">`,
    `<meta property="og:description" content="${meta.description}">`,
    `<meta property="og:url" content="${url}">`,
    `<meta property="og:image" content="${ogImage}">`,
    `<meta name="twitter:card" content="summary_large_image">`,
    `<meta name="twitter:title" content="${meta.title}">`,
    `<meta name="twitter:description" content="${meta.description}">`,
    `<meta name="twitter:image" content="${ogImage}">`,
  ].join('\n    ');
}

function generateRobotsTxt(site: SiteId, isApp: boolean): string {
  if (isApp) {
    return 'User-agent: *\nDisallow: /\n';
  }
  const baseUrl = SITE_DEFAULTS[site].baseUrl;
  return `User-agent: *\nAllow: /\n\nSitemap: ${baseUrl}/sitemap.xml\n`;
}

function generateSitemapXml(site: SiteId): string {
  const baseUrl = SITE_DEFAULTS[site].baseUrl;
  const routes = SITEMAP_ROUTES[site];
  const totalRoutes = routes.length;

  const urls = routes.map((route, i) => {
    const loc = route === '/' ? baseUrl + '/' : baseUrl + route;
    const priority = Math.max(0.4, 1.0 - (i / totalRoutes) * 0.6).toFixed(1);
    return `  <url>\n    <loc>${loc}</loc>\n    <priority>${priority}</priority>\n  </url>`;
  });

  return [
    '<?xml version="1.0" encoding="UTF-8"?>',
    '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
    ...urls,
    '</urlset>',
    '',
  ].join('\n');
}

function generateManifestJson(site: SiteId): string {
  const defaults = SITE_DEFAULTS[site];
  return JSON.stringify({
    name: defaults.siteName,
    short_name: defaults.siteName,
    start_url: '/',
    display: 'standalone',
    theme_color: defaults.themeColor,
    background_color: defaults.themeColor,
    icons: [
      { src: '/favicon.svg', type: 'image/svg+xml', sizes: 'any' },
    ],
  }, null, 2);
}

export default function viteSeoPlugin(): Plugin {
  const site = getSiteId();
  const target = buildTarget();
  const isPublicWebsite = target === 'public-website';
  const homeMeta = SEO_META[site]['/'];
  let resolvedOutDir = path.resolve(process.cwd(), 'dist');

  return {
    name: 'vite-seo',

    configResolved(config) {
      resolvedOutDir = path.resolve(config.root, config.build.outDir);
    },

    transformIndexHtml(html) {
      if (!isPublicWebsite || !homeMeta) {
        // Only public website builds emit a manifest. Otherwise nginx's SPA
        // fallback turns a missing manifest into HTML with a 200 response.
        return html.replace(/\s*<link rel="manifest" href="\/manifest\.json">/, '');
      }

      const metaTags = buildMetaTags(homeMeta, '/', site);

      // Replace the static <title> with our SEO tags
      html = html.replace(
        /<title>.*?<\/title>/,
        metaTags,
      );

      // Add a manifest only when the base template does not already have one.
      if (!html.includes('rel="manifest"')) {
        html = html.replace(
          /(<link rel="icon"[^>]*>)/,
          '$1\n    <link rel="manifest" href="/manifest.json">',
        );
      }

      return html;
    },

    closeBundle() {
      const outDir = resolvedOutDir;
      if (!fs.existsSync(outDir)) return;

      // Dashboard and dev/test builds must not be indexed. Public website
      // builds get the sitemap, manifest, and route-specific static HTML.
      fs.writeFileSync(path.join(outDir, 'robots.txt'), generateRobotsTxt(site, !isPublicWebsite));

      if (!isPublicWebsite) return;

      // Write sitemap.xml
      fs.writeFileSync(path.join(outDir, 'sitemap.xml'), generateSitemapXml(site));

      // Write manifest.json
      fs.writeFileSync(path.join(outDir, 'manifest.json'), generateManifestJson(site));

      // Generate per-route index.html variants
      const baseHtml = fs.readFileSync(path.join(outDir, 'index.html'), 'utf-8');
      const routes = Object.keys(SEO_META[site]);

      for (const route of routes) {
        if (route === '/') continue; // Homepage already has correct meta from transformIndexHtml
        const meta = SEO_META[site][route];
        if (!meta) continue;

        // Replace the homepage meta tags with route-specific ones
        let routeHtml = baseHtml;

        // Replace title
        routeHtml = routeHtml.replace(/<title>.*?<\/title>/, `<title>${meta.title}</title>`);

        // Replace meta description
        routeHtml = routeHtml.replace(
          /<meta name="description" content="[^"]*">/,
          `<meta name="description" content="${meta.description}">`,
        );

        // Replace canonical
        const baseUrl = SITE_DEFAULTS[site].baseUrl;
        const routeUrl = `${baseUrl}${route}`;
        routeHtml = routeHtml.replace(
          /<link rel="canonical" href="[^"]*">/,
          `<link rel="canonical" href="${routeUrl}">`,
        );

        // Replace OG tags
        routeHtml = routeHtml.replace(/(<meta property="og:title" content=")[^"]*(")/,  `$1${meta.title}$2`);
        routeHtml = routeHtml.replace(/(<meta property="og:description" content=")[^"]*(")/,  `$1${meta.description}$2`);
        routeHtml = routeHtml.replace(/(<meta property="og:url" content=")[^"]*(")/,  `$1${routeUrl}$2`);

        // Replace Twitter tags
        routeHtml = routeHtml.replace(/(<meta name="twitter:title" content=")[^"]*(")/,  `$1${meta.title}$2`);
        routeHtml = routeHtml.replace(/(<meta name="twitter:description" content=")[^"]*(")/,  `$1${meta.description}$2`);

        // Write to route subdirectory
        const routeDir = path.join(outDir, route.slice(1)); // Remove leading /
        fs.mkdirSync(routeDir, { recursive: true });
        fs.writeFileSync(path.join(routeDir, 'index.html'), routeHtml);
      }
    },
  };
}
