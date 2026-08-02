import React from 'react';
import { useLocation } from 'react-router';
import { getCurrentSite } from '../config/site.js';
import { SEO_META, SITE_SEO_DEFAULTS } from '../config/seo.js';
import { useDocumentMeta } from '../hooks/useDocumentMeta.js';

export const SeoHead: React.FC = () => {
  const { pathname } = useLocation();
  const site = getCurrentSite();
  const siteMeta = SEO_META[site.id];
  const siteDefaults = SITE_SEO_DEFAULTS[site.id];

  const meta = siteMeta?.[pathname];
  const canonicalUrl = siteDefaults && meta
    ? `${siteDefaults.baseUrl}${pathname === '/' ? '' : pathname}`
    : undefined;

  useDocumentMeta({
    title: meta?.title ?? `Page not found — ${site.footerName}`,
    description: meta?.description ?? 'The requested UK Mesh page could not be found.',
    canonicalUrl,
  });

  return null;
};
