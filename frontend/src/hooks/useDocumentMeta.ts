import { useEffect } from 'react';

type MetaConfig = {
  title: string;
  description: string;
  canonicalUrl?: string;
};

function setMetaTag(attr: 'name' | 'property', key: string, content: string): void {
  let el = document.querySelector<HTMLMetaElement>(`meta[${attr}="${key}"]`);
  if (!el) {
    el = document.createElement('meta');
    el.setAttribute(attr, key);
    document.head.appendChild(el);
  }
  el.setAttribute('content', content);
}

function setCanonical(href: string): void {
  let el = document.querySelector<HTMLLinkElement>('link[rel="canonical"]');
  if (!el) {
    el = document.createElement('link');
    el.setAttribute('rel', 'canonical');
    document.head.appendChild(el);
  }
  el.setAttribute('href', href);
}

function removeMetaTag(attr: 'name' | 'property', key: string): void {
  document.querySelector<HTMLMetaElement>(`meta[${attr}="${key}"]`)?.remove();
}

export function useDocumentMeta({ title, description, canonicalUrl }: MetaConfig): void {
  useEffect(() => {
    document.title = title;
    setMetaTag('name', 'description', description);
    setMetaTag('property', 'og:title', title);
    setMetaTag('property', 'og:description', description);
    setMetaTag('name', 'twitter:title', title);
    setMetaTag('name', 'twitter:description', description);
    if (canonicalUrl) {
      setCanonical(canonicalUrl);
      setMetaTag('property', 'og:url', canonicalUrl);
    } else {
      document.querySelector<HTMLLinkElement>('link[rel="canonical"]')?.remove();
      removeMetaTag('property', 'og:url');
    }
  }, [title, description, canonicalUrl]);
}
