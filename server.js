// ============================================================
// ИМПОРТЫ
// ============================================================
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const multer = require('multer');
const webpush = require('web-push');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');

console.log('🟢 Запуск сервера...');
console.log('📦 Node version:', process.version);
console.log('🌍 PORT env:', process.env.PORT || '(не задан)');

// ============================================================
// EXPRESS + SOCKET.IO
// ============================================================
const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  maxHttpBufferSize: 30 * 1024 * 1024,
  cors: { origin: '*' },
  transports: ['polling', 'websocket'],
  allowEIO3: true,
  pingTimeout: 60000,
  pingInterval: 25000
});

// ============================================================
// ПУТИ И ПАПКИ
// ============================================================
const UPLOADS_DIR = path.join(__dirname, 'public', 'uploads');
const DATA_DIR = path.join(__dirname, 'data');
const MESSAGES_FILE = path.join(DATA_DIR, 'messages.json');
const USERS_FILE = path.join(DATA_DIR, 'users.json');
const ROOMS_FILE = path.join(DATA_DIR, 'rooms.json');
const SUBS_FILE = path.join(DATA_DIR, 'subscriptions.json');
const VAPID_FILE = path.join(DATA_DIR, 'vapid.json');

try {
  [UPLOADS_DIR, DATA_DIR].forEach(d => fs.mkdirSync(d, { recursive: true }));
  console.log('📁 Папки готовы');
} catch (e) {
  console.error('❌ Ошибка создания папок:', e);
}

// ============================================================
// VAPID ДЛЯ WEB PUSH
// ============================================================
let vapidKeys;
try {
  if (fs.existsSync(VAPID_FILE)) {
    vapidKeys = JSON.parse(fs.readFileSync(VAPID_FILE, 'utf8'));
  } else {
    vapidKeys = webpush.generateVAPIDKeys();
    fs.writeFileSync(VAPID_FILE, JSON.stringify(vapidKeys, null, 2));
    console.log('🔑 Сгенерированы VAPID-ключи');
  }
  webpush.setVapidDetails('mailto:admin@example.com', vapidKeys.publicKey, vapidKeys.privateKey);
} catch (e) {
  console.error('❌ Ошибка VAPID, используем временные ключи:', e.message);
  vapidKeys = webpush.generateVAPIDKeys();
  try {
    webpush.setVapidDetails('mailto:admin@example.com', vapidKeys.publicKey, vapidKeys.privateKey);
  } catch (err) { console.error('Критическая ошибка VAPID:', err); }
}

// ============================================================
// ЗАГРУЗКА ДАННЫХ
// ============================================================
let messages = [];
let knownUsers = {};
let rooms = {};
let subscriptions = {};

try {
  if (fs.existsSync(MESSAGES_FILE)) messages = JSON.parse(fs.readFileSync(MESSAGES_FILE, 'utf8'));
} catch (e) { console.error('messages.json:', e.message); messages = []; }

try {
  if (fs.existsSync(USERS_FILE)) knownUsers = JSON.parse(fs.readFileSync(USERS_FILE, 'utf8'));
} catch (e) { console.error('users.json:', e.message); knownUsers = {}; }

try {
  if (fs.existsSync(ROOMS_FILE)) rooms = JSON.parse(fs.readFileSync(ROOMS_FILE, 'utf8'));
} catch (e) { console.error('rooms.json:', e.message); rooms = {}; }

try {
  if (fs.existsSync(SUBS_FILE)) subscriptions = JSON.parse(fs.readFileSync(SUBS_FILE, 'utf8'));
} catch (e) { console.error('subscriptions.json:', e.message); subscriptions = {}; }

if (!Array.isArray(messages)) messages = [];
if (typeof knownUsers !== 'object' || knownUsers === null || Array.isArray(knownUsers)) knownUsers = {};
if (typeof rooms !== 'object' || rooms === null || Array.isArray(rooms)) rooms = {};
if (typeof subscriptions !== 'object' || subscriptions === null || Array.isArray(subscriptions)) subscriptions = {};

console.log('📊 Загружено:',
  Object.keys(knownUsers).length, 'пользователей,',
  messages.length, 'сообщений,',
  Object.keys(rooms).length, 'комнат');

// ============================================================
// СОХРАНЕНИЕ
// ============================================================
const saveMessages = () => { try { fs.writeFile(MESSAGES_FILE, JSON.stringify(messages), () => {}); } catch (e) {} };
const saveUsers = () => { try { fs.writeFile(USERS_FILE, JSON.stringify(knownUsers), () => {}); } catch (e) {} };
const saveRooms = () => { try { fs.writeFile(ROOMS_FILE, JSON.stringify(rooms), () => {}); } catch (e) {} };
const saveSubs = () => { try { fs.writeFile(SUBS_FILE, JSON.stringify(subscriptions), () => {}); } catch (e) {} };

// ============================================================
// ОНЛАЙН-ПОЛЬЗОВАТЕЛИ
// ============================================================
const online = new Map();

// ============================================================
// MIDDLEWARE
// ============================================================
app.use(express.json({ limit: '1mb' }));

// Отключаем кэширование HTML/JS
app.use((req, res, next) => {
  res.set('Cache-Control', 'no-cache, no-store, must-revalidate');
  res.set('Pragma', 'no-cache');
  res.set('Expires', '0');
  next();
});

// Логирование запросов к socket.io
app.use((req, res, next) => {
  if (req.path.includes('socket.io')) console.log('🌐', req.method, req.path);
  next();
});

// Статика
app.use(express.static(path.join(__dirname, 'public'), { etag: false, lastModified: false }));

// ============================================================
// HTTP ROUTES
// ============================================================
app.get('/health', (req, res) => {
  try {
    res.json({
      ok: true,
      version: '2.2.0',
      uptime: Math.round(process.uptime()),
      memory: Math.round(process.memoryUsage().rss / 1024 / 1024) + ' MB',
      users: Object.keys(knownUsers).length,
      rooms: Object.keys(rooms).length,
      messages: messages.length,
      online: online.size,
      socketio: 'ready'
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.get('/test-socketio', (req, res) => {
  res.send(`<!DOCTYPE html>
<html lang="ru"><head><meta charset="utf-8"><title>Test Socket.IO</title>
<style>
  body { font-family: -apple-system, system-ui, sans-serif; background: #0a0b16; color: #e8eaf6; padding: 40px; max-width: 800px; margin: 0 auto; line-height: 1.6; }
  a { color: #7c5cff; }
  h1 { color: #7c5cff; margin-bottom: 20px; }
  h2 { margin-top: 30px; font-size: 18px; }
  code { background: #1e2340; padding: 3px 8px; border-radius: 4px; color: #b8a6ff; font-size: 13px; }
  .status { padding: 12px 18px; border-radius: 10px; margin: 14px 0; font-weight: 500; }
  .ok { background: rgba(46,204,113,0.15); color: #2ecc71; }
  .err { background: rgba(255,92,124,0.15); color: #ff5c7c; }
  .box { background: #1e2340; padding: 16px; border-radius: 10px; margin: 12px 0; }
</style></head><body>
<h1>✅ Тест Socket.IO</h1>
<div class="status ok">Сервер работает и отдаёт эту страницу</div>

<h2>Проверка 1: socket.io.js</h2>
<div class="box">
  <p>Открой: <a href="/socket.io/socket.io.js" target="_blank">/socket.io/socket.io.js</a></p>
  <p>Должен открыться JS-код (тысячи строк). <b>404 = сломан Socket.IO.</b></p>
</div>

<h2>Проверка 2: health</h2>
<div class="box">
  <p>Открой: <a href="/health" target="_blank">/health</a></p>
  <p>JSON должен содержать <code>"socketio":"ready"</code> и <code>"version":"2.2.0"</code></p>
</div>

<h2>Проверка 3: главное приложение</h2>
<div class="box">
  <p>Открой: <a href="/" target="_blank">/</a></p>
  <p>Если крутится загрузка — жми <code>Ctrl+Shift+R</code> для жёсткого обновления.</p>
</div>

<h2>Проверка 4: клиентская консоль</h2>
<div class="box">
  <p>На главной странице: <code>F12</code> → Console → смотри логи со эмодзи.</p>
  <p>Должно быть: <code>✅ Socket подключён: xxx</code></p>
</div>
</body></html>`);
});

app.get('/vapid-public-key', (req, res) => {
  try {
    res.json({ key: vapidKeys.publicKey });
  } catch (e) {
    res.status(500).json({ error: 'VAPID not ready' });
  }
});

// ============================================================
// ЗАГРУЗКА ФАЙЛОВ
// ============================================================
const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, UPLOADS_DIR),
  filename: (req, file, cb) => {
    const ext = path.extname(file.originalname) || '';
    cb(null, crypto.randomBytes(10).toString('hex') + ext);
  }
});
const upload = multer({ storage, limits: { fileSize: 25 * 1024 * 1024 } });

app.post('/upload', upload.single('file'), (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'Файл не получен' });
    res.json({
      url: '/uploads/' + req.file.filename,
      name: req.file.originalname,
      type: req.file.mimetype,
      size: req.file.size
    });
  } catch (e) {
    console.error('upload error:', e);
    res.status(500).json({ error: 'Ошибка загрузки' });
  }
});

// ============================================================
// PUSH ПОДПИСКИ
// ============================================================
app.post('/subscribe', (req, res) => {
  try {
    const { userId, subscription } = req.body || {};
    if (!userId || !subscription || !subscription.endpoint) {
      return res.status(400).json({ error: 'bad request' });
    }
    if (!subscriptions[userId]) subscriptions[userId] = [];
    if (!subscriptions[userId].some(s => s.endpoint === subscription.endpoint)) {
      subscriptions[userId].push(subscription);
      saveSubs();
    }
    res.json({ ok: true });
  } catch (e) {
    console.error('subscribe error:', e);
    res.status(500).json({ error: 'server error' });
  }
});

app.post('/unsubscribe', (req, res) => {
  try {
    const { userId, endpoint } = req.body || {};
    if (subscriptions[userId]) {
      subscriptions[userId] = subscriptions[userId].filter(s => s.endpoint !== endpoint);
      saveSubs();
    }
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: 'server error' });
  }
});

// ============================================================
// ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
// ============================================================
const safeUsername = (u) => String(u || '').replace(/^@/, '').trim();

function contactListFor(userId) {
  const u = knownUsers[userId];
  if (!u || !Array.isArray(u.contacts)) return [];
  return u.contacts.map(cid => {
    const c = knownUsers[cid];
    if (!c) return null;
    return {
      userId: cid,
      name: c.name,
      username: c.username,
      color: c.color,
      initials: c.initials,
      avatar: c.avatar || null,
      online: online.has(cid)
    };
  }).filter(Boolean);
}

function emitUserListTo(userId) {
  if (!knownUsers[userId]) return;
  io.to('user_' + userId).emit('users', contactListFor(userId));
}

function broadcastUserLists() {
  try {
    Object.keys(knownUsers).forEach(uid => emitUserListTo(uid));
  } catch (e) { console.error('broadcastUserLists:', e); }
}

function allUserIds() {
  return Object.keys(knownUsers);
}

function visibleMessagesFor(userId) {
  return messages.filter(m => {
    if (!m || !m.to) return false;
    if (m.to === 'public') return true;
    if (typeof m.to === 'string' && m.to.startsWith('r_')) {
      const room = rooms[m.to];
      return !!(room && Array.isArray(room.members) && room.members.includes(userId));
    }
    return m.from === userId || m.to === userId;
  });
}

function messageRecipients(m) {
  if (!m || !m.to) return [];
  if (m.to === 'public') return allUserIds();
  if (typeof m.to === 'string' && m.to.startsWith('r_')) {
    return (rooms[m.to] && Array.isArray(rooms[m.to].members)) ? rooms[m.to].members : [];
  }
  return [m.from, m.to];
}

function emitToUsers(userIds, event, data) {
  const seen = new Set();
  userIds.forEach(uid => {
    if (!uid || seen.has(uid)) return;
    seen.add(uid);
    io.to('user_' + uid).emit(event, data);
  });
}

async function sendPush(userId, payload) {
  const subs = subscriptions[userId] || [];
  for (const sub of subs) {
    try {
      await webpush.sendNotification(sub, JSON.stringify(payload));
    } catch (e) {
      if (e.statusCode === 404 || e.statusCode === 410) {
        subscriptions[userId] = subscriptions[userId].filter(s => s.endpoint !== sub.endpoint);
        saveSubs();
      }
    }
  }
}

function broadcastRooms() {
  io.emit('rooms', Object.values(rooms));
}

function ensureContacts(a, b) {
  if (!knownUsers[a] || !knownUsers[b]) return;
  if (!Array.isArray(knownUsers[a].contacts)) knownUsers[a].contacts = [];
  if (!Array.isArray(knownUsers[b].contacts)) knownUsers[b].contacts = [];
  let changed = false;
  if (!knownUsers[a].contacts.includes(b)) { knownUsers[a].contacts.push(b); changed = true; }
  if (!knownUsers[b].contacts.includes(a)) { knownUsers[b].contacts.push(a); changed = true; }
  if (changed) {
    saveUsers();
    emitUserListTo(a);
    emitUserListTo(b);
  }
}

// ============================================================
// SOCKET.IO
// ============================================================
io.on('connection', socket => {
  console.log('🔌 Подключение:', socket.id, '| transport:', socket.conn.transport.name);

  // --- ВХОД ---
  socket.on('join', ({ userId, name, username, color, initials }) => {
    try {
      if (!userId || !name || !username) {
        socket.emit('join-error', { text: 'Не хватает данных' });
        return;
      }
      const cleanUsername = safeUsername(username);
      if (!/^[a-zA-Z][a-zA-Z0-9_]{2,19}$/.test(cleanUsername)) {
        socket.emit('join-error', { text: 'Username: 3–20 символов, латиница/цифры/_, начинается с буквы' });
        return;
      }

      const existingEntry = Object.entries(knownUsers).find(([id, u]) =>
        u && u.username && u.username.toLowerCase() === cleanUsername.toLowerCase()
      );

      let finalUserId;
      let isNewAccount = false;

      if (existingEntry) {
        finalUserId = existingEntry[0];
      } else {
        finalUserId = userId;
        isNewAccount = true;
      }

      socket.userId = finalUserId;
      socket.join('user_' + finalUserId);
      if (!online.has(finalUserId)) online.set(finalUserId, new Set());
      online.get(finalUserId).add(socket.id);

      if (isNewAccount) {
        knownUsers[finalUserId] = {
          name, username: cleanUsername, color, initials,
          avatar: null, contacts: []
        };
      } else {
        const u = knownUsers[finalUserId];
        if (!u.color) u.color = color;
        if (!u.initials) u.initials = initials;
        if (!Array.isArray(u.contacts)) u.contacts = [];
      }
      saveUsers();

      const user = knownUsers[finalUserId];

      socket.emit('history', visibleMessagesFor(finalUserId));
      socket.emit('rooms', Object.values(rooms));
      socket.emit('joined', {
        userId: finalUserId,
        user: {
          userId: finalUserId,
          name: user.name,
          username: user.username,
          color: user.color,
          initials: user.initials,
          avatar: user.avatar || null
        },
        isNewAccount
      });
      broadcastUserLists();
      console.log(`[+] ${user.name} (@${user.username}) ${isNewAccount ? 'зарегистрирован' : 'вошёл'}`);
    } catch (e) {
      console.error('join error:', e);
      socket.emit('join-error', { text: 'Ошибка сервера' });
    }
  });

  // --- ОБНОВЛЕНИЕ ПРОФИЛЯ ---
  socket.on('update-profile', ({ userId, name, color, initials, avatar }) => {
    try {
      if (!userId || !knownUsers[userId]) return;
      knownUsers[userId] = {
        ...knownUsers[userId],
        ...(name && { name }),
        ...(color && { color }),
        ...(initials && { initials }),
        ...(avatar !== undefined && { avatar })
      };
      saveUsers();
      broadcastUserLists();
      io.emit('user-updated', { userId, user: knownUsers[userId] });
    } catch (e) { console.error('update-profile:', e); }
  });

  // --- ПОИСК ПОЛЬЗОВАТЕЛЯ ---
  socket.on('search-user', ({ query, byUserId }) => {
    try {
      const q = safeUsername(query).toLowerCase();
      if (!q) { socket.emit('search-result', { query, results: [] }); return; }
      const results = Object.entries(knownUsers)
        .filter(([id, u]) => id !== byUserId && u.username && u.username.toLowerCase().includes(q))
        .slice(0, 10)
        .map(([id, u]) => ({
          userId: id,
          name: u.name,
          username: u.username,
          color: u.color,
          initials: u.initials,
          avatar: u.avatar || null,
          online: online.has(id)
        }));
      socket.emit('search-result', { query, results });
    } catch (e) { console.error('search-user:', e); }
  });

  // --- ДОБАВИТЬ КОНТАКТ ---
  socket.on('add-contact', ({ userId, contactId }) => {
    try {
      if (!userId || !contactId || userId === contactId) return;
      if (!knownUsers[userId] || !knownUsers[contactId]) return;
      ensureContacts(userId, contactId);
      socket.emit('contact-added', { userId: contactId });
    } catch (e) { console.error('add-contact:', e); }
  });

  // --- УДАЛИТЬ КОНТАКТ ---
  socket.on('remove-contact', ({ userId, contactId }) => {
    try {
      if (!userId || !contactId) return;
      if (knownUsers[userId] && Array.isArray(knownUsers[userId].contacts)) {
        knownUsers[userId].contacts = knownUsers[userId].contacts.filter(x => x !== contactId);
        saveUsers();
        emitUserListTo(userId);
        socket.emit('contact-removed', { userId: contactId });
      }
    } catch (e) { console.error('remove-contact:', e); }
  });

  // --- СООБЩЕНИЕ ---
  socket.on('message', msg => {
    try {
      if (!msg || !msg.id || !msg.from || !msg.to) return;
      if (messages.some(m => m.id === msg.id)) return;

      const isPublic = msg.to === 'public';
      const isRoom = typeof msg.to === 'string' && msg.to.startsWith('r_');
      const room = isRoom ? rooms[msg.to] : null;

      if (isRoom && !room) return;
      if (room && room.type === 'channel' && Array.isArray(room.admins) && !room.admins.includes(msg.from)) {
        socket.emit('error-msg', { text: 'Только администраторы могут публиковать в канал' });
        return;
      }

      if (!isPublic && !isRoom) ensureContacts(msg.from, msg.to);

      let recipients;
      if (isPublic) {
        recipients = allUserIds();
      } else if (isRoom) {
        if (!Array.isArray(room.members) || !room.members.includes(msg.from)) return;
        recipients = room.members.slice();
      } else {
        recipients = [msg.to, msg.from];
      }

      const ts = msg.timestamp || Date.now();
      const full = {
        id: msg.id,
        to: msg.to,
        from: msg.from,
        fromName: msg.fromName,
        text: String(msg.text || '').slice(0, 4000),
        file: msg.file || null,
        timestamp: ts,
        time: new Date(ts).toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' }),
        deliveredTo: [],
        readBy: []
      };

      messages.push(full);
      if (messages.length > 10000) messages = messages.slice(-10000);
      saveMessages();
      emitToUsers(recipients, 'message', full);

      recipients.forEach(uid => {
        if (uid === msg.from) return;
        if (!online.has(uid)) {
          const title = isPublic
            ? '🌐 Общий чат'
            : isRoom
              ? (room.type === 'channel' ? `📢 ${room.name}` : `👥 ${room.name}`)
              : `💬 ${msg.fromName}`;
          const body = msg.file
            ? (msg.text ? msg.text.slice(0, 80) + ' 📎' : '📎 ' + msg.file.name)
            : msg.text.slice(0, 120);
          sendPush(uid, { title, body, data: { roomId: msg.to }, tag: msg.to });
        }
      });
    } catch (e) { console.error('message:', e); }
  });

  // --- ДОСТАВЛЕНО ---
  socket.on('delivered', ({ userId, messageIds }) => {
    try {
      if (!userId || !Array.isArray(messageIds)) return;
      const updated = [];
      messageIds.forEach(id => {
        const m = messages.find(x => x.id === id);
        if (m && Array.isArray(m.deliveredTo) && !m.deliveredTo.includes(userId)) {
          m.deliveredTo.push(userId);
          updated.push(m);
        }
      });
      if (updated.length) {
        saveMessages();
        updated.forEach(m => emitToUsers(messageRecipients(m), 'message-update', m));
      }
    } catch (e) { console.error('delivered:', e); }
  });

  // --- ПРОЧИТАНО ---
  socket.on('read', ({ userId, messageIds }) => {
    try {
      if (!userId || !Array.isArray(messageIds)) return;
      const updated = [];
      messageIds.forEach(id => {
        const m = messages.find(x => x.id === id);
        if (m && Array.isArray(m.readBy) && !m.readBy.includes(userId)) {
          m.readBy.push(userId);
          if (!m.deliveredTo.includes(userId)) m.deliveredTo.push(userId);
          updated.push(m);
        }
      });
      if (updated.length) {
        saveMessages();
        updated.forEach(m => emitToUsers(messageRecipients(m), 'message-update', m));
      }
    } catch (e) { console.error('read:', e); }
  });

  // --- ПЕЧАТАЕТ ---
  socket.on('typing', ({ to, from, fromName }) => {
    try {
      if (!to || !from) return;
      let recipients;
      if (to === 'public') {
        recipients = allUserIds().filter(u => u !== from);
      } else if (typeof to === 'string' && to.startsWith('r_')) {
        recipients = ((rooms[to] && rooms[to].members) || []).filter(u => u !== from);
      } else {
        recipients = [to];
      }
      emitToUsers(recipients, 'typing', { to, from, fromName });
    } catch (e) { console.error('typing:', e); }
  });

  // --- СОЗДАТЬ КОМНАТУ ---
  socket.on('create-room', ({ name, type, description, creator, inviteUsernames }) => {
    try {
      if (!name || !type || !creator) return;
      if (type !== 'group' && type !== 'channel') return;

      const id = 'r_' + crypto.randomBytes(6).toString('hex');
      const COLORS = [
        ['#7c5cff','#b846ff'], ['#ff5c8a','#ff8a5c'], ['#5cffb8','#5c9dff'],
        ['#ffb85c','#ff5c5c'], ['#5cffd9','#5c7cff'], ['#b85cff','#ff5cb8']
      ];
      const c = COLORS[Math.floor(Math.random() * COLORS.length)];

      const members = [creator];
      const admins = [creator];

      if (Array.isArray(inviteUsernames)) {
        inviteUsernames.forEach(un => {
          const clean = safeUsername(un).toLowerCase();
          if (!clean) return;
          const found = Object.entries(knownUsers).find(([id2, u]) =>
            id2 !== creator && u.username && u.username.toLowerCase() === clean
          );
          if (found && !members.includes(found[0])) {
            members.push(found[0]);
            ensureContacts(creator, found[0]);
          }
        });
      }

      rooms[id] = {
        id, type,
        name: name.slice(0, 60),
        description: (description || '').slice(0, 200),
        color: `linear-gradient(135deg,${c[0]},${c[1]})`,
        createdBy: creator,
        createdAt: Date.now(),
        members,
        admins
      };
      saveRooms();
      broadcastRooms();
      socket.emit('room-created', rooms[id]);
    } catch (e) { console.error('create-room:', e); }
  });

  // --- ПРИГЛАСИТЬ В КОМНАТУ ---
  socket.on('invite-to-room', ({ roomId, byUserId, usernames }) => {
    try {
      const room = rooms[roomId];
      if (!room) return;
      if (!Array.isArray(room.admins) || !room.admins.includes(byUserId)) {
        socket.emit('error-msg', { text: 'Только администраторы могут приглашать' });
        return;
      }
      const added = [];
      (usernames || []).forEach(un => {
        const clean = safeUsername(un).toLowerCase();
        if (!clean) return;
        const found = Object.entries(knownUsers).find(([id, u]) =>
          u.username && u.username.toLowerCase() === clean
        );
        if (found && !room.members.includes(found[0])) {
          room.members.push(found[0]);
          added.push('@' + found[1].username);
          ensureContacts(byUserId, found[0]);
        }
      });
      if (added.length) {
        saveRooms();
        broadcastRooms();
        socket.emit('invited', { names: added });
      } else {
        socket.emit('error-msg', { text: 'Никто не добавлен — проверьте @username' });
      }
    } catch (e) { console.error('invite-to-room:', e); }
  });

  // --- ПРИСОЕДИНИТЬСЯ К КОМНАТЕ ---
  socket.on('join-room', ({ roomId, userId }) => {
    try {
      const r = rooms[roomId];
      if (!r || !Array.isArray(r.members) || r.members.includes(userId)) return;
      r.members.push(userId);
      saveRooms();
      broadcastRooms();
    } catch (e) { console.error('join-room:', e); }
  });

  // --- ПОКИНУТЬ КОМНАТУ ---
  socket.on('leave-room', ({ roomId, userId }) => {
    try {
      const r = rooms[roomId];
      if (!r) return;
      r.members = r.members.filter(x => x !== userId);
      r.admins = r.admins.filter(x => x !== userId);
      if (r.members.length === 0) delete rooms[roomId];
      saveRooms();
      broadcastRooms();
    } catch (e) { console.error('leave-room:', e); }
  });

  // --- ОТКЛЮЧЕНИЕ ---
  socket.on('disconnect', (reason) => {
    try {
      console.log('🔌 Отключение:', socket.id, '|', reason);
      if (socket.userId && online.has(socket.userId)) {
        online.get(socket.userId).delete(socket.id);
        if (online.get(socket.userId).size === 0) online.delete(socket.userId);
        broadcastUserLists();
      }
    } catch (e) { console.error('disconnect:', e); }
  });

  socket.on('error', (err) => {
    console.error('Socket error:', err);
  });
});

// ============================================================
// ГЛОБАЛЬНЫЕ ОБРАБОТЧИКИ ОШИБОК
// ============================================================
process.on('uncaughtException', (err) => {
  console.error('❌ uncaughtException:', err);
});
process.on('unhandledRejection', (err) => {
  console.error('❌ unhandledRejection:', err);
});

// ============================================================
// ЗАПУСК
// ============================================================
const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`🚀 Мессенджер запущен на порту ${PORT}`);
  console.log(`🌐 Health: /health`);
  console.log(`🧪 Тест: /test-socketio`);
});
