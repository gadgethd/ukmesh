export function ToolFrame({ src, title }: { src: string; title: string }) {
  return <iframe className="site-tool-frame" src={src} title={title} referrerPolicy="no-referrer"
    sandbox="allow-same-origin allow-scripts allow-forms allow-popups allow-popups-to-escape-sandbox allow-downloads"
    allow="clipboard-write; geolocation; fullscreen" />;
}
