import { type Plugin } from 'vite';
import fs from 'node:fs';
import path from 'node:path';

type SiteId = 'teesside' | 'ukmesh' | 'dev';

type RouteMeta = { title: string; description: string };

const SEO_META: Record<SiteId, Record<string, RouteMeta>> = {
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
      description: 'Log in to manage your MeshCore repeater node on the UK Mesh network.',
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
      description: 'Log in to manage your MeshCore repeater node on the Teesside mesh network.',
    },
  },
  dev: {
    '/': {
      title: 'UK Mesh Test — Development Environment',
      description: 'Development and testing environment for the UK Mesh network analytics platform.',
    },
  },
};

const SITE_DEFAULTS: Record<SiteId, { siteName: string; baseUrl: string; themeColor: string }> = {
  ukmesh: { siteName: 'UK Mesh Network', baseUrl: 'https://ukmesh.com', themeColor: '#0a1628' },
  teesside: { siteName: 'Teesside Mesh', baseUrl: 'https://www.teessidemesh.com', themeColor: '#0a1628' },
  dev: { siteName: 'UK Mesh Test', baseUrl: 'https://test.ukmesh.com', themeColor: '#0a1628' },
};

const SITEMAP_ROUTES: Record<SiteId, string[]> = {
  ukmesh: ['/', '/install', '/stats', '/feed', '/repeater', '/open-source'],
  teesside: ['/', '/install', '/stats', '/packets', '/open-source'],
  dev: ['/'],
};

function getSiteId(): SiteId {
  const env = process.env['VITE_SITE'] ?? '';
  if (env === 'ukmesh' || env === 'teesside' || env === 'dev') return env;
  const net = process.env['VITE_NETWORK'] ?? '';
  if (net === 'ukmesh') return 'ukmesh';
  if (net === 'teesside') return 'teesside';
  if (net === 'test') return 'dev';
  return 'teesside';
}

function isAppBuild(): boolean {
  const hostname = process.env['VITE_APP_HOSTNAME'] ?? '';
  // App builds set VITE_APP_HOSTNAME to the app domain; website builds leave it empty or set VITE_SITE
  return hostname !== '';
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
  const isApp = isAppBuild();
  const homeMeta = SEO_META[site]['/'];

  return {
    name: 'vite-seo',

    transformIndexHtml(html) {
      if (isApp || !homeMeta) return html;

      const metaTags = buildMetaTags(homeMeta, '/', site);

      // Replace the static <title> with our SEO tags
      html = html.replace(
        /<title>.*?<\/title>/,
        metaTags,
      );

      // Add manifest link after favicon
      html = html.replace(
        /(<link rel="icon"[^>]*>)/,
        '$1\n    <link rel="manifest" href="/manifest.json">',
      );

      return html;
    },

    closeBundle() {
      const outDir = path.resolve(process.cwd(), 'dist');
      if (!fs.existsSync(outDir)) return;

      // Write robots.txt
      fs.writeFileSync(path.join(outDir, 'robots.txt'), generateRobotsTxt(site, isApp));

      if (isApp) return; // App builds don't need sitemap or per-route HTML

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
