function syncLiveEmbed() {
  const [route, query] = window.location.hash.slice(1).split('?', 2);
  const embedded = window.parent !== window && route === '/live'
    && new URLSearchParams(query).get('embed') === '1';
  document.documentElement.classList.toggle('ukmesh-live-embed', embedded);
}

syncLiveEmbed();
window.addEventListener('hashchange', syncLiveEmbed);
