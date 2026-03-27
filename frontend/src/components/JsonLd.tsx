import React from 'react';
import { useLocation } from 'react-router-dom';
import { getCurrentSite } from '../config/site.js';
import { SITE_SEO_DEFAULTS, SEO_META } from '../config/seo.js';

function getStructuredData(siteId: string, pathname: string): object | null {
  const defaults = SITE_SEO_DEFAULTS[siteId as keyof typeof SITE_SEO_DEFAULTS];
  const meta = SEO_META[siteId as keyof typeof SEO_META]?.[pathname];
  if (!defaults || !meta) return null;

  if (pathname === '/') {
    return {
      '@context': 'https://schema.org',
      '@type': 'WebSite',
      name: defaults.siteName,
      url: defaults.baseUrl,
      description: meta.description,
    };
  }

  return {
    '@context': 'https://schema.org',
    '@type': 'WebPage',
    name: meta.title,
    url: `${defaults.baseUrl}${pathname}`,
    description: meta.description,
    isPartOf: {
      '@type': 'WebSite',
      name: defaults.siteName,
      url: defaults.baseUrl,
    },
  };
}

export const JsonLd: React.FC = () => {
  const { pathname } = useLocation();
  const site = getCurrentSite();
  const data = getStructuredData(site.id, pathname);

  if (!data) return null;

  return (
    <script
      type="application/ld+json"
      dangerouslySetInnerHTML={{ __html: JSON.stringify(data) }}
    />
  );
};
