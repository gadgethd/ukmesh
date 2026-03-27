import React from 'react';
import { useLocation } from 'react-router-dom';
import { getCurrentSite } from '../config/site.js';
import { SEO_META, SITE_SEO_DEFAULTS } from '../config/seo.js';
import { useDocumentMeta } from '../hooks/useDocumentMeta.js';

export const SeoHead: React.FC = () => {
  const { pathname } = useLocation();
  const site = getCurrentSite();
  const siteMeta = SEO_META[site.id];
  const siteDefaults = SITE_SEO_DEFAULTS[site.id];

  const meta = siteMeta?.[pathname] ?? siteMeta?.['/'];
  const canonicalUrl = siteDefaults
    ? `${siteDefaults.baseUrl}${pathname === '/' ? '' : pathname}`
    : undefined;

  useDocumentMeta({
    title: meta?.title ?? site.footerName,
    description: meta?.description ?? '',
    canonicalUrl,
  });

  return null;
};
