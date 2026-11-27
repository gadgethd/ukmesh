import './docs-pages.css';
import { useEffect, useRef, useState } from 'react';
import { boundedResponseText } from '../utils/api.js';

const tags = new Set('A BR CODE DIV EM H1 H2 H3 LI OL P PRE SECTION SPAN STRONG UL'.split(' '));
const attributes = new Set(['aria-label', 'class', 'tabindex', 'href', 'target', 'rel']);

async function loadDocument(url: string, signal: AbortSignal) {
  const response = await fetch(url, { redirect: 'error', signal: AbortSignal.any([signal, AbortSignal.timeout(8_000)]) });
  if (!response.ok) throw new Error('Document unavailable');
  const document = new DOMParser().parseFromString(await boundedResponseText(response, 64 * 1024), 'text/html');
  const template = document.querySelector<HTMLTemplateElement>('template#ukmesh-document');
  if (!template) throw new Error('Document malformed');
  for (const node of template.content.querySelectorAll('*')) {
    if (!tags.has(node.tagName) || node.getAttributeNames().some(name => !attributes.has(name))) throw new Error('Invalid document markup');
    const href = node.getAttribute('href');
    if (href && !['https:', 'http:', 'mailto:'].includes(new URL(href, location.origin).protocol)) {
      throw new Error('Invalid document link');
    }
  }
  return template.content;
}

export function StaticDocument({ url, title }: { url: string; title: string }) {
  const content = useRef<HTMLDivElement>(null);
  const [attempt, setAttempt] = useState(0);
  const [status, setStatus] = useState<'loading' | 'ready' | 'error'>('loading');
  useEffect(() => {
    const controller = new AbortController();
    void loadDocument(url, controller.signal).then(fragment => {
      if (controller.signal.aborted) return;
      content.current?.replaceChildren(fragment);
      setStatus('ready');
    }).catch(() => { if (!controller.signal.aborted) setStatus('error'); });
    return () => controller.abort();
  }, [url, attempt]);
  return <>
    <header className="site-page-hero">
      <div className="site-content"><h1 className="site-page-hero__title">{title}</h1></div>
    </header>
    <div ref={content} />
    {status === 'loading' && <p className="site-content" role="status">Loading documentation…</p>}
    {status === 'error' && <div className="site-content">
      <p role="status">Could not load this page.</p>
      <button type="button" className="site-btn site-btn--ghost" onClick={() => {
        setStatus('loading');
        setAttempt(value => value + 1);
      }}>Try again</button>
    </div>}
  </>;
}
