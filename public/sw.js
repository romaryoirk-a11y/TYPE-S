const CACHE_NAME = 'messenger-v5';
const STATIC_ASSETS = ['/', '/index.html', '/manifest.json', '/icon.svg'];

self.addEventListener('install', e => {
  e.waitUntil(
    caches.open(CACHE_NAME).then(c => c.addAll(STATIC_ASSETS)).then(() => self.skipWaiting())
  );
});

self.addEventListener('activate', e => {
  e.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.filter(k => k !== CACHE_NAME).map(k => caches.delete(k)))
    ).then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', e => {
  const url = new URL(e.request.url);
  if (url.pathname.startsWith('/socket.io') ||
      url.pathname.startsWith('/upload') ||
      url.pathname.startsWith('/health') ||
      url.pathname.startsWith('/vapid') ||
      url.pathname.startsWith('/subscribe') ||
      url.pathname.startsWith('/unsubscribe') ||
      url.hostname !== self.location.hostname) {
    return;
  }
  if (url.pathname === '/' || url.pathname.endsWith('.html') || url.pathname.endsWith('.json')) {
    e.respondWith(
      fetch(e.request).then(r => {
        const clone = r.clone();
        caches.open(CACHE_NAME).then(c => c.put(e.request, clone));
        return r;
      }).catch(() => caches.match(e.request))
    );
    return;
  }
  e.respondWith(caches.match(e.request).then(cached => cached || fetch(e.request)));
});

self.addEventListener('push', e => {
  let data = {};
  try { data = e.data ? e.data.json() : {}; } catch (err) { data = { title: 'Новое сообщение' }; }
  e.waitUntil((async () => {
    const clientList = await self.clients.matchAll({ type: 'window', includeUncontrolled: true });
    const hasFocused = clientList.some(c => c.focused);
    if (hasFocused) return;
    await self.registration.showNotification(data.title || 'Новое сообщение', {
      body: data.body || '',
      tag: data.tag || 'msg',
      data: data.data || {},
      vibrate: [200, 100, 200],
      renotify: true,
      icon: '/icon.svg',
      badge: '/icon.svg'
    });
  })());
});

self.addEventListener('notificationclick', e => {
  e.notification.close();
  e.waitUntil((async () => {
    const clientList = await self.clients.matchAll({ type: 'window', includeUncontrolled: true });
    for (const c of clientList) {
      if (c.url.includes(self.location.origin) && 'focus' in c) return c.focus();
    }
    if (self.clients.openWindow) return self.clients.openWindow('/');
  })());
});
