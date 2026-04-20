// Service Worker voor offline caching (PWA)
const CACHE = 'turfrekening-v1';

self.addEventListener('install', e => {
  e.waitUntil(
    caches.open(CACHE).then(cache => cache.addAll([
      '/', '/rapport', '/ho', '/admin'
    ]))
  );
});

self.addEventListener('fetch', e => {
  // Altijd eerst naar netwerk, dan cache als fallback
  e.respondWith(
    fetch(e.request).catch(() => caches.match(e.request))
  );
});
