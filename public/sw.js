self.addEventListener('install', e => self.skipWaiting());
self.addEventListener('activate', e => e.waitUntil(self.clients.claim()));

self.addEventListener('push', e => {
  const data = e.data ? e.data.json() : {};
  e.waitUntil((async () => {
    const clientList = await self.clients.matchAll({ type: 'window', includeUncontrolled: true });
    const hasFocused = clientList.some(c => c.focused);
    if (hasFocused) return;
    await self.registration.showNotification(data.title || 'Новое сообщение', {
      body: data.body || '',
      tag: data.tag || 'msg',
      data: data.data || {},
      vibrate: [200, 100, 200],
      renotify: true
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
