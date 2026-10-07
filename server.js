const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const multer = require('multer');
const webpush = require('web-push');
const { MongoClient, ObjectId } = require('mongodb');
const bcrypt = require('bcryptjs');
const path = require('path');
const crypto = require('crypto');

console.log('🟢 Старт сервера v5.0.0');
console.log('📦 Node', process.version);

if (!process.env.MONGODB_URI) {
  console.error('❌ MONGODB_URI не задана');
  process.exit(1);
}

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

const mongoClient = new MongoClient(process.env.MONGODB_URI, {
  serverSelectionTimeoutMS: 10000,
  connectTimeoutMS: 10000
});

let db, usersCol, messagesCol, roomsCol, subsCol, configCol, invitesCol;

async function initDB() {
  console.log('🔌 MongoDB...');
  await mongoClient.connect();
  db = mongoClient.db('messenger');
  usersCol = db.collection('users');
  messagesCol = db.collection('messages');
  roomsCol = db.collection('rooms');
  subsCol = db.collection('subscriptions');
  configCol = db.collection('config');
  invitesCol = db.collection('invites');

  await usersCol.createIndex({ username: 1 }, { unique: true, sparse: true });
  await usersCol.createIndex({ userId: 1 }, { unique: true });
  await messagesCol.createIndex({ timestamp: -1 });
  await messagesCol.createIndex({ to: 1, timestamp: -1 });
  await messagesCol.createIndex({ from: 1, timestamp: -1 });
  await messagesCol.createIndex({ id: 1 }, { unique: true, sparse: true });
  await roomsCol.createIndex({ id: 1 }, { unique: true });
  await invitesCol.createIndex({ code: 1 }, { unique: true });

  console.log('✅ MongoDB готова');
}

let messages = [];
let knownUsers = {};
let rooms = {};
let subscriptions = {};

async function loadData() {
  console.log('📥 Загрузка данных...');
  const usersArr = await usersCol.find({}).toArray();
  knownUsers = {};
  usersArr.forEach(u => { knownUsers[u.userId] = u; });
  const roomsArr = await roomsCol.find({}).toArray();
  rooms = {};
  roomsArr.forEach(r => {
    // Миграция: admins → roles
    if (!r.roles && Array.isArray(r.admins)) {
      r.roles = {};
      r.members.forEach(m => { r.roles[m] = r.admins.includes(m) ? 'admin' : 'member'; });
      if (r.createdBy) r.roles[r.createdBy] = 'owner';
    }
    if (!Array.isArray(r.pinnedMessages)) r.pinnedMessages = [];
    rooms[r.id] = r;
  });
  const subsArr = await subsCol.find({}).toArray();
  subscriptions = {};
  subsArr.forEach(s => { subscriptions[s.userId] = s.subscriptions || []; });
  const msgsArr = await messagesCol.find({}).sort({ timestamp: -1 }).limit(10000).toArray();
  messages = msgsArr.map(m => ({
    ...m,
    edited: m.edited || false,
    deleted: m.deleted || false,
    pinned: m.pinned || false,
    reactions: m.reactions || {},
    replyTo: m.replyTo || null
  })).reverse();
  console.log(`✅ ${Object.keys(knownUsers).length} польз., ${messages.length} сообщ., ${Object.keys(rooms).length} комнат`);
}

async function saveUser(userId) {
  try {
    const u = knownUsers[userId];
    if (!u) return;
    await usersCol.updateOne({ userId }, { $set: u }, { upsert: true });
  } catch (e) { console.error('saveUser:', e.message); }
}

async function saveMessage(msg) {
  try {
    await messagesCol.insertOne(msg);
    const count = await messagesCol.countDocuments();
    if (count > 50000) {
      const old = await messagesCol.find({}).sort({ timestamp: 1 }).limit(count - 50000).toArray();
      await messagesCol.deleteMany({ _id: { $in: old.map(o => o._id) } });
    }
  } catch (e) { console.error('saveMessage:', e.message); }
}

async function updateMessage(id, updates) {
  try { await messagesCol.updateOne({ id }, { $set: updates }); } catch (e) {}
}

async function saveRoom(roomId) {
  try {
    const r = rooms[roomId];
    if (!r) return;
    await roomsCol.replaceOne({ id: roomId }, r, { upsert: true });
  } catch (e) {}
}

async function deleteRoom(roomId) {
  try { await roomsCol.deleteOne({ id: roomId }); } catch (e) {}
}

async function saveSubs(userId) {
  try {
    const arr = subscriptions[userId] || [];
    await subsCol.updateOne({ userId }, { $set: { userId, subscriptions: arr } }, { upsert: true });
  } catch (e) {}
}

let vapidKeys;
async function initVapid() {
  try {
    const doc = await configCol.findOne({ _id: 'vapid' });
    if (doc && doc.publicKey) vapidKeys = { publicKey: doc.publicKey, privateKey: doc.privateKey };
    else {
      vapidKeys = webpush.generateVAPIDKeys();
      await configCol.updateOne({ _id: 'vapid' }, { $set: vapidKeys }, { upsert: true });
    }
    webpush.setVapidDetails('mailto:admin@example.com', vapidKeys.publicKey, vapidKeys.privateKey);
  } catch (e) {
    vapidKeys = webpush.generateVAPIDKeys();
  }
}

app.use(express.json({ limit: '30mb' }));
app.use((req, res, next) => {
  res.set('Cache-Control', 'no-cache, no-store, must-revalidate');
  next();
});
app.use(express.static(path.join(__dirname, 'public'), { etag: false, lastModified: false }));

app.get('/health', async (req, res) => {
  try {
    res.json({
      ok: true, version: '5.0.0', storage: 'mongodb-only',
      features: ['pins','mutes','roles','invites','auth','stats','themes','i18n'],
      uptime: Math.round(process.uptime()),
      memory: Math.round(process.memoryUsage().rss / 1024 / 1024) + ' MB',
      users: await usersCol.countDocuments(),
      rooms: await roomsCol.countDocuments(),
      messages: await messagesCol.countDocuments(),
      online: online.size,
      socketio: 'ready'
    });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// Публичная статистика
app.get('/stats', async (req, res) => {
  try {
    const totalUsers = await usersCol.countDocuments();
    const totalMessages = await messagesCol.countDocuments();
    const totalRooms = await roomsCol.countDocuments();
    const totalFileMsgs = messages.filter(m => m.file).length;
    const totalReactions = messages.reduce((sum, m) => sum + Object.keys(m.reactions || {}).length, 0);
    // Последние 30 дней активности
    const since = Date.now() - 30 * 24 * 60 * 60 * 1000;
    const last30 = await messagesCol.countDocuments({ timestamp: { $gte: since } });
    res.json({
      totalUsers, totalMessages, totalRooms, totalFileMsgs, totalReactions, online: online.size, last30
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Приглашение по ссылке
app.get('/join/:code', (req, res) => {
  res.redirect('/?invite=' + req.params.code);
});

app.get('/vapid-public-key', (req, res) => res.json({ key: vapidKeys.publicKey }));

const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 10 * 1024 * 1024 } });

app.post('/upload', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'Файл не получен' });
    const base64 = req.file.buffer.toString('base64');
    res.json({
      url: `data:${req.file.mimetype};base64,${base64}`,
      name: req.file.originalname,
      type: req.file.mimetype,
      size: req.file.size
    });
  } catch (e) { res.status(500).json({ error: 'Ошибка' }); }
});

app.post('/subscribe', async (req, res) => {
  try {
    const { userId, subscription } = req.body || {};
    if (!userId || !subscription?.endpoint) return res.status(400).json({ error: 'bad' });
    if (!subscriptions[userId]) subscriptions[userId] = [];
    if (!subscriptions[userId].some(s => s.endpoint === subscription.endpoint)) {
      subscriptions[userId].push(subscription);
      await saveSubs(userId);
    }
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: 'err' }); }
});

app.post('/unsubscribe', async (req, res) => {
  try {
    const { userId, endpoint } = req.body || {};
    if (subscriptions[userId]) {
      subscriptions[userId] = subscriptions[userId].filter(s => s.endpoint !== endpoint);
      await saveSubs(userId);
    }
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: 'err' }); }
});

const online = new Map();
const safeUsername = (u) => String(u || '').replace(/^@/, '').trim();

function contactListFor(userId) {
  const u = knownUsers[userId];
  if (!u || !Array.isArray(u.contacts)) return [];
  return u.contacts.map(cid => {
    const c = knownUsers[cid];
    if (!c) return null;
    return {
      userId: cid, name: c.name, username: c.username,
      color: c.color, initials: c.initials, avatar: c.avatar || null,
      online: online.has(cid)
    };
  }).filter(Boolean);
}

function emitUserListTo(userId) {
  if (!knownUsers[userId]) return;
  io.to('user_' + userId).emit('users', contactListFor(userId));
}

function broadcastUserLists() {
  try { Object.keys(knownUsers).forEach(uid => emitUserListTo(uid)); } catch (e) {}
}

const allUserIds = () => Object.keys(knownUsers);

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
  // Проверка мута: если чат замьючен — не отправлять push
  const user = knownUsers[userId];
  if (user && Array.isArray(user.mutedChats) && payload.data && user.mutedChats.includes(payload.data.roomId)) {
    return;
  }
  const subs = subscriptions[userId] || [];
  for (const sub of subs) {
    try { await webpush.sendNotification(sub, JSON.stringify(payload)); }
    catch (e) {
      if (e.statusCode === 404 || e.statusCode === 410) {
        subscriptions[userId] = subscriptions[userId].filter(s => s.endpoint !== sub.endpoint);
        await saveSubs(userId);
      }
    }
  }
}

function broadcastRooms() { io.emit('rooms', Object.values(rooms)); }

function getUserRole(room, userId) {
  if (!room) return null;
  if (room.roles && room.roles[userId]) return room.roles[userId];
  if (room.createdBy === userId) return 'owner';
  if (Array.isArray(room.admins) && room.admins.includes(userId)) return 'admin';
  return 'member';
}

function canManage(role) { return role === 'owner' || role === 'admin'; }
function canPin(role) { return canManage(role); }
function canKick(role) { return canManage(role); }

async function ensureContacts(a, b) {
  if (!knownUsers[a] || !knownUsers[b]) return;
  if (!Array.isArray(knownUsers[a].contacts)) knownUsers[a].contacts = [];
  if (!Array.isArray(knownUsers[b].contacts)) knownUsers[b].contacts = [];
  let changed = false;
  if (!knownUsers[a].contacts.includes(b)) { knownUsers[a].contacts.push(b); changed = true; }
  if (!knownUsers[b].contacts.includes(a)) { knownUsers[b].contacts.push(a); changed = true; }
  if (changed) {
    await Promise.all([saveUser(a), saveUser(b)]);
    emitUserListTo(a); emitUserListTo(b);
  }
}

io.on('connection', socket => {
  console.log('🔌 Подключение:', socket.id);

  // ============================================================
  // АВТОРИЗАЦИЯ
  // ============================================================
  socket.on('register', async ({ name, username, password, color, initials, token }) => {
    try {
      if (!name || !username) return socket.emit('auth-error', { text: 'Не хватает данных' });
      const cleanUsername = safeUsername(username);
      if (!/^[a-zA-Z][a-zA-Z0-9_]{2,19}$/.test(cleanUsername)) {
        return socket.emit('auth-error', { text: 'Username: 3-20 символов, латиница/цифры/_, начинается с буквы' });
      }
      if (!password || String(password).length < 4) {
        return socket.emit('auth-error', { text: 'Пароль минимум 4 символа' });
      }
      const existingEntry = Object.entries(knownUsers).find(([id, u]) =>
        u && u.username && u.username.toLowerCase() === cleanUsername.toLowerCase()
      );
      if (existingEntry) return socket.emit('auth-error', { text: 'Этот @username уже занят' });

      const userId = 'u_' + crypto.randomBytes(8).toString('hex');
      const passwordHash = await bcrypt.hash(String(password), 10);
      const sessionToken = crypto.randomBytes(24).toString('hex');
      knownUsers[userId] = {
        userId, name, username: cleanUsername, passwordHash,
        color, initials, avatar: null,
        contacts: [], mutedChats: [],
        settings: { theme: 'dark', accent: '#7c5cff', language: 'ru' },
        tokens: [sessionToken],
        createdAt: Date.now()
      };
      await saveUser(userId);

      socket.userId = userId;
      socket.join('user_' + userId);
      if (!online.has(userId)) online.set(userId, new Set());
      online.get(userId).add(socket.id);

      const user = knownUsers[userId];
      socket.emit('history', visibleMessagesFor(userId));
      socket.emit('rooms', Object.values(rooms));
      socket.emit('joined', {
        userId,
        user: sanitizeUser(user),
        token: sessionToken,
        isNewAccount: true
      });
      broadcastUserLists();
      console.log(`[+] ${name} (@${cleanUsername}) зарегистрирован`);
    } catch (e) {
      console.error('register error:', e);
      socket.emit('auth-error', { text: 'Ошибка регистрации' });
    }
  });

  socket.on('login', async ({ username, password }) => {
    try {
      const cleanUsername = safeUsername(username);
      const entry = Object.entries(knownUsers).find(([id, u]) =>
        u && u.username && u.username.toLowerCase() === cleanUsername.toLowerCase()
      );
      if (!entry) return socket.emit('auth-error', { text: 'Аккаунт не найден' });
      const [userId, user] = entry;
      if (!user.passwordHash) return socket.emit('auth-error', { text: 'Аккаунт без пароля, войдите через регистрацию' });
      const ok = await bcrypt.compare(String(password), user.passwordHash);
      if (!ok) return socket.emit('auth-error', { text: 'Неверный пароль' });

      const sessionToken = crypto.randomBytes(24).toString('hex');
      if (!Array.isArray(user.tokens)) user.tokens = [];
      user.tokens.push(sessionToken);
      if (user.tokens.length > 10) user.tokens = user.tokens.slice(-10);
      await saveUser(userId);

      socket.userId = userId;
      socket.join('user_' + userId);
      if (!online.has(userId)) online.set(userId, new Set());
      online.get(userId).add(socket.id);

      socket.emit('history', visibleMessagesFor(userId));
      socket.emit('rooms', Object.values(rooms));
      socket.emit('joined', {
        userId,
        user: sanitizeUser(user),
        token: sessionToken,
        isNewAccount: false
      });
      broadcastUserLists();
      console.log(`[+] ${user.name} (@${user.username}) вошёл`);
    } catch (e) {
      console.error('login error:', e);
      socket.emit('auth-error', { text: 'Ошибка входа' });
    }
  });

  socket.on('auto-login', async ({ userId, token }) => {
    try {
      if (!userId || !token) return socket.emit('auth-error', { text: 'Нужен вход' });
      const user = knownUsers[userId];
      if (!user) return socket.emit('auth-error', { text: 'Аккаунт не найден' });
      if (!Array.isArray(user.tokens) || !user.tokens.includes(token)) {
        return socket.emit('auth-error', { text: 'Сессия истекла, войдите заново' });
      }
      socket.userId = userId;
      socket.join('user_' + userId);
      if (!online.has(userId)) online.set(userId, new Set());
      online.get(userId).add(socket.id);

      socket.emit('history', visibleMessagesFor(userId));
      socket.emit('rooms', Object.values(rooms));
      socket.emit('joined', {
        userId,
        user: sanitizeUser(user),
        token,
        isNewAccount: false
      });
      broadcastUserLists();
      console.log(`[+] ${user.name} (@${user.username}) auto-login`);
    } catch (e) { socket.emit('auth-error', { text: 'Ошибка' }); }
  });

  function sanitizeUser(u) {
    const { passwordHash, tokens, ...rest } = u;
    return rest;
  }

  // ============================================================
  // ПРОФИЛЬ, НАСТРОЙКИ
  // ============================================================
  socket.on('update-profile', async ({ userId, name, color, initials, avatar }) => {
    try {
      if (!userId || !knownUsers[userId]) return;
      knownUsers[userId] = {
        ...knownUsers[userId],
        ...(name && { name }),
        ...(color && { color }),
        ...(initials && { initials }),
        ...(avatar !== undefined && { avatar })
      };
      await saveUser(userId);
      broadcastUserLists();
      io.emit('user-updated', { userId, user: knownUsers[userId] });
    } catch (e) {}
  });

  socket.on('update-settings', async ({ userId, settings }) => {
    try {
      if (!userId || !knownUsers[userId]) return;
      knownUsers[userId].settings = {
        ...(knownUsers[userId].settings || {}),
        ...settings
      };
      await saveUser(userId);
      socket.emit('settings-updated', { settings: knownUsers[userId].settings });
    } catch (e) {}
  });

  socket.on('change-password', async ({ userId, oldPassword, newPassword }) => {
    try {
      if (!userId || !knownUsers[userId]) return;
      if (!newPassword || String(newPassword).length < 4) {
        return socket.emit('error-msg', { text: 'Пароль минимум 4 символа' });
      }
      const user = knownUsers[userId];
      if (user.passwordHash) {
        const ok = await bcrypt.compare(String(oldPassword || ''), user.passwordHash);
        if (!ok) return socket.emit('error-msg', { text: 'Старый пароль неверный' });
      }
      user.passwordHash = await bcrypt.hash(String(newPassword), 10);
      await saveUser(userId);
      socket.emit('toast-msg', { text: '✅ Пароль изменён' });
    } catch (e) {}
  });

  // ============================================================
  // ПОИСК, КОНТАКТЫ
  // ============================================================
  socket.on('search-user', ({ query, byUserId }) => {
    try {
      const q = safeUsername(query).toLowerCase();
      if (!q) return socket.emit('search-result', { query, results: [] });
      const results = Object.entries(knownUsers)
        .filter(([id, u]) => id !== byUserId && u.username && u.username.toLowerCase().includes(q))
        .slice(0, 10)
        .map(([id, u]) => ({
          userId: id, name: u.name, username: u.username,
          color: u.color, initials: u.initials, avatar: u.avatar || null,
          online: online.has(id)
        }));
      socket.emit('search-result', { query, results });
    } catch (e) {}
  });

  socket.on('add-contact', async ({ userId, contactId }) => {
    try {
      if (!userId || !contactId || userId === contactId) return;
      if (!knownUsers[userId] || !knownUsers[contactId]) return;
      await ensureContacts(userId, contactId);
      socket.emit('contact-added', { userId: contactId });
    } catch (e) {}
  });

  socket.on('remove-contact', async ({ userId, contactId }) => {
    try {
      if (!userId || !contactId) return;
      if (knownUsers[userId] && Array.isArray(knownUsers[userId].contacts)) {
        knownUsers[userId].contacts = knownUsers[userId].contacts.filter(x => x !== contactId);
        await saveUser(userId);
        emitUserListTo(userId);
        socket.emit('contact-removed', { userId: contactId });
      }
    } catch (e) {}
  });

  // ============================================================
  // СООБЩЕНИЯ
  // ============================================================
  socket.on('message', async msg => {
    try {
      if (!msg || !msg.id || !msg.from || !msg.to) return;
      if (messages.some(m => m.id === msg.id)) return;
      const isPublic = msg.to === 'public';
      const isRoom = typeof msg.to === 'string' && msg.to.startsWith('r_');
      const room = isRoom ? rooms[msg.to] : null;
      if (isRoom && !room) return;
      if (room && room.type === 'channel') {
        const role = getUserRole(room, msg.from);
        if (!canManage(role)) return socket.emit('error-msg', { text: 'Только админы и владелец могут писать в канал' });
      }
      if (!isPublic && !isRoom) await ensureContacts(msg.from, msg.to);

      let recipients;
      if (isPublic) recipients = allUserIds();
      else if (isRoom) {
        if (!Array.isArray(room.members) || !room.members.includes(msg.from)) return;
        recipients = room.members.slice();
      } else recipients = [msg.to, msg.from];

      const ts = msg.timestamp || Date.now();
      const full = {
        id: msg.id, to: msg.to, from: msg.from, fromName: msg.fromName,
        text: String(msg.text || '').slice(0, 4000),
        file: msg.file || null,
        replyTo: msg.replyTo || null,
        reactions: {}, edited: false, deleted: false, pinned: false,
        timestamp: ts,
        time: new Date(ts).toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' }),
        deliveredTo: [], readBy: []
      };
      messages.push(full);
      if (messages.length > 10000) messages = messages.slice(-10000);
      saveMessage(full);
      emitToUsers(recipients, 'message', full);
      recipients.forEach(uid => {
        if (uid === msg.from) return;
        if (!online.has(uid)) {
          const title = isPublic ? '🌐 Общий чат'
            : isRoom ? (room.type === 'channel' ? `📢 ${room.name}` : `👥 ${room.name}`)
            : `💬 ${msg.fromName}`;
          const body = msg.file ? (msg.text ? msg.text.slice(0, 80) + ' 📎' : '📎 ' + msg.file.name) : msg.text.slice(0, 120);
          sendPush(uid, { title, body, data: { roomId: msg.to }, tag: msg.to });
        }
      });
    } catch (e) { console.error('message:', e); }
  });

  socket.on('edit-message', async ({ messageId, userId, newText }) => {
    try {
      const m = messages.find(x => x.id === messageId);
      if (!m || m.from !== userId || m.deleted) return;
      if (Date.now() - m.timestamp > 24 * 60 * 60 * 1000) return socket.emit('error-msg', { text: 'Можно редактировать в течение 24 часов' });
      const text = String(newText || '').slice(0, 4000);
      if (!text) return;
      m.text = text; m.edited = true; m.editedAt = Date.now();
      await updateMessage(m.id, { text: m.text, edited: true, editedAt: m.editedAt });
      emitToUsers(messageRecipients(m), 'message-update', m);
    } catch (e) {}
  });

  socket.on('delete-message', async ({ messageId, userId }) => {
    try {
      const m = messages.find(x => x.id === messageId);
      if (!m || m.from !== userId || m.deleted) return;
      m.deleted = true; m.text = ''; m.file = null; m.reactions = {};
      await updateMessage(m.id, { deleted: true, text: '', file: null, reactions: {} });
      emitToUsers(messageRecipients(m), 'message-update', m);
    } catch (e) {}
  });

  socket.on('react-message', async ({ messageId, userId, emoji }) => {
    try {
      const m = messages.find(x => x.id === messageId);
      if (!m || m.deleted) return;
      const validEmojis = ['👍','❤️','😂','😮','😢','🔥'];
      if (!validEmojis.includes(emoji)) return;
      if (!m.reactions) m.reactions = {};
      if (!m.reactions[emoji]) m.reactions[emoji] = [];
      const idx = m.reactions[emoji].indexOf(userId);
      if (idx >= 0) m.reactions[emoji].splice(idx, 1);
      else m.reactions[emoji].push(userId);
      if (m.reactions[emoji].length === 0) delete m.reactions[emoji];
      await updateMessage(m.id, { reactions: m.reactions });
      emitToUsers(messageRecipients(m), 'message-update', m);
    } catch (e) {}
  });

  // PIN MESSAGE
  socket.on('pin-message', async ({ messageId, userId }) => {
    try {
      const m = messages.find(x => x.id === messageId);
      if (!m || !m.to) return;
      const isRoom = m.to.startsWith('r_');
      const isPublic = m.to === 'public';
      if (!isRoom && !isPublic) return;

      let room;
      if (isRoom) {
        room = rooms[m.to];
        if (!room) return;
        const role = getUserRole(room, userId);
        if (!canPin(role)) return socket.emit('error-msg', { text: 'Нет прав закреплять' });
      } else {
        // Публичный — только "первый" пользователь? Разрешим всем в тесте
        if (userId !== m.from && userId !== 'admin') {
          // Простая проверка: кто первый зарегистрировался - владелец public
          // Для простоты: разрешим только если этот пользователь не первый раз заходит
        }
      }
      m.pinned = !m.pinned;
      await updateMessage(m.id, { pinned: m.pinned });

      if (isRoom && room) {
        if (!Array.isArray(room.pinnedMessages)) room.pinnedMessages = [];
        if (m.pinned && !room.pinnedMessages.includes(m.id)) {
          room.pinnedMessages.push(m.id);
          if (room.pinnedMessages.length > 20) room.pinnedMessages = room.pinnedMessages.slice(-20);
        } else if (!m.pinned) {
          room.pinnedMessages = room.pinnedMessages.filter(x => x !== m.id);
        }
        await saveRoom(room.id);
        io.emit('rooms', Object.values(rooms));
      }
      emitToUsers(messageRecipients(m), 'message-update', m);
      socket.emit('toast-msg', { text: m.pinned ? '📌 Закреплено' : '📌 Откреплено' });
    } catch (e) { console.error('pin-message:', e); }
  });

  // MUTE CHAT
  socket.on('mute-chat', async ({ userId, chatId, muted }) => {
    try {
      if (!userId || !chatId || !knownUsers[userId]) return;
      if (!Array.isArray(knownUsers[userId].mutedChats)) knownUsers[userId].mutedChats = [];
      if (muted && !knownUsers[userId].mutedChats.includes(chatId)) {
        knownUsers[userId].mutedChats.push(chatId);
      } else if (!muted) {
        knownUsers[userId].mutedChats = knownUsers[userId].mutedChats.filter(x => x !== chatId);
      }
      await saveUser(userId);
      socket.emit('mutes-updated', { mutedChats: knownUsers[userId].mutedChats });
    } catch (e) {}
  });

  // SEARCH MESSAGES
  socket.on('search-messages', ({ userId, query }) => {
    try {
      const q = String(query || '').toLowerCase().trim();
      if (!q || q.length < 2) return socket.emit('search-messages-result', { query, results: [] });
      const visible = visibleMessagesFor(userId);
      const results = visible
        .filter(m => !m.deleted && m.text && m.text.toLowerCase().includes(q))
        .sort((a, b) => b.timestamp - a.timestamp)
        .slice(0, 50);
      socket.emit('search-messages-result', { query, results });
    } catch (e) {}
  });

  socket.on('delivered', async ({ userId, messageIds }) => {
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
        for (const m of updated) await updateMessage(m.id, { deliveredTo: m.deliveredTo });
        updated.forEach(m => emitToUsers(messageRecipients(m), 'message-update', m));
      }
    } catch (e) {}
  });

  socket.on('read', async ({ userId, messageIds }) => {
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
        for (const m of updated) await updateMessage(m.id, { readBy: m.readBy, deliveredTo: m.deliveredTo });
        updated.forEach(m => emitToUsers(messageRecipients(m), 'message-update', m));
      }
    } catch (e) {}
  });

  socket.on('typing', ({ to, from, fromName }) => {
    try {
      if (!to || !from) return;
      let recipients;
      if (to === 'public') recipients = allUserIds().filter(u => u !== from);
      else if (typeof to === 'string' && to.startsWith('r_')) recipients = ((rooms[to] && rooms[to].members) || []).filter(u => u !== from);
      else recipients = [to];
      emitToUsers(recipients, 'typing', { to, from, fromName });
    } catch (e) {}
  });

  // ============================================================
  // КОМНАТЫ: РОЛИ, ПРИГЛАШЕНИЯ
  // ============================================================
  socket.on('create-room', async ({ name, type, description, creator, inviteUsernames }) => {
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
      const roles = { [creator]: 'owner' };
      if (Array.isArray(inviteUsernames)) {
        for (const un of inviteUsernames) {
          const clean = safeUsername(un).toLowerCase();
          if (!clean) continue;
          const found = Object.entries(knownUsers).find(([id2, u]) =>
            id2 !== creator && u.username && u.username.toLowerCase() === clean
          );
          if (found && !members.includes(found[0])) {
            members.push(found[0]);
            roles[found[0]] = 'member';
            await ensureContacts(creator, found[0]);
          }
        }
      }
      rooms[id] = {
        id, type,
        name: name.slice(0, 60),
        description: (description || '').slice(0, 200),
        color: `linear-gradient(135deg,${c[0]},${c[1]})`,
        createdBy: creator, createdAt: Date.now(),
        members, roles,
        pinnedMessages: []
      };
      await saveRoom(id);
      broadcastRooms();
      socket.emit('room-created', rooms[id]);
    } catch (e) {}
  });

  socket.on('invite-to-room', async ({ roomId, byUserId, usernames }) => {
    try {
      const room = rooms[roomId];
      if (!room) return;
      const role = getUserRole(room, byUserId);
      if (!canManage(role)) return socket.emit('error-msg', { text: 'Только админы могут приглашать' });
      const added = [];
      for (const un of (usernames || [])) {
        const clean = safeUsername(un).toLowerCase();
        if (!clean) continue;
        const found = Object.entries(knownUsers).find(([id, u]) => u.username && u.username.toLowerCase() === clean);
        if (found && !room.members.includes(found[0])) {
          room.members.push(found[0]);
          if (!room.roles) room.roles = {};
          room.roles[found[0]] = 'member';
          added.push('@' + found[1].username);
          await ensureContacts(byUserId, found[0]);
        }
      }
      if (added.length) {
        await saveRoom(roomId);
        broadcastRooms();
        socket.emit('invited', { names: added });
      } else {
        socket.emit('error-msg', { text: 'Никто не добавлен' });
      }
    } catch (e) {}
  });

  socket.on('create-invite', async ({ roomId, byUserId }) => {
    try {
      const room = rooms[roomId];
      if (!room) return;
      const role = getUserRole(room, byUserId);
      if (!canManage(role)) return socket.emit('error-msg', { text: 'Нет прав' });
      const code = crypto.randomBytes(5).toString('hex');
      await invitesCol.insertOne({
        code, roomId, createdBy: byUserId,
        createdAt: Date.now(),
        uses: 0
      });
      socket.emit('invite-created', { code, roomId, link: '/join/' + code });
    } catch (e) { console.error('create-invite:', e); }
  });

  socket.on('redeem-invite', async ({ code, userId }) => {
    try {
      const inv = await invitesCol.findOne({ code });
      if (!inv) return socket.emit('invite-error', { text: 'Приглашение не найдено' });
      const room = rooms[inv.roomId];
      if (!room) return socket.emit('invite-error', { text: 'Комната не найдена' });
      if (room.members.includes(userId)) {
        socket.emit('invite-redeemed', { roomId: room.id, alreadyMember: true });
        return;
      }
      room.members.push(userId);
      if (!room.roles) room.roles = {};
      room.roles[userId] = 'member';
      await saveRoom(room.id);
      await invitesCol.updateOne({ code }, { $inc: { uses: 1 } });
      await ensureContacts(inv.createdBy, userId);
      broadcastRooms();
      socket.emit('invite-redeemed', { roomId: room.id, alreadyMember: false });
      socket.emit('toast-msg', { text: '✅ Вы присоединились к "' + room.name + '"' });
    } catch (e) { console.error('redeem-invite:', e); }
  });

  socket.on('set-role', async ({ roomId, byUserId, targetUserId, role }) => {
    try {
      const room = rooms[roomId];
      if (!room) return;
      const myRole = getUserRole(room, byUserId);
      if (myRole !== 'owner') return socket.emit('error-msg', { text: 'Только владелец может менять роли' });
      if (targetUserId === byUserId) return socket.emit('error-msg', { text: 'Нельзя сменить свою роль' });
      if (!room.members.includes(targetUserId)) return;
      if (!['admin', 'member'].includes(role)) return;
      if (!room.roles) room.roles = {};
      room.roles[targetUserId] = role;
      await saveRoom(roomId);
      broadcastRooms();
      socket.emit('toast-msg', { text: '✅ Роль обновлена' });
    } catch (e) {}
  });

  socket.on('kick-user', async ({ roomId, byUserId, targetUserId }) => {
    try {
      const room = rooms[roomId];
      if (!room) return;
      const myRole = getUserRole(room, byUserId);
      if (!canKick(myRole)) return socket.emit('error-msg', { text: 'Нет прав' });
      const targetRole = getUserRole(room, targetUserId);
      if (targetRole === 'owner') return socket.emit('error-msg', { text: 'Нельзя исключить владельца' });
      if (myRole === 'admin' && targetRole === 'admin') return socket.emit('error-msg', { text: 'Админ не может исключить админа' });
      room.members = room.members.filter(x => x !== targetUserId);
      if (room.roles) delete room.roles[targetUserId];
      await saveRoom(roomId);
      broadcastRooms();
      socket.emit('toast-msg', { text: '✅ Пользователь исключён' });
    } catch (e) {}
  });

  socket.on('join-room', async ({ roomId, userId }) => {
    try {
      const r = rooms[roomId];
      if (!r || !Array.isArray(r.members) || r.members.includes(userId)) return;
      r.members.push(userId);
      if (!r.roles) r.roles = {};
      r.roles[userId] = 'member';
      await saveRoom(roomId);
      broadcastRooms();
    } catch (e) {}
  });

  socket.on('leave-room', async ({ roomId, userId }) => {
    try {
      const r = rooms[roomId];
      if (!r) return;
      if (getUserRole(r, userId) === 'owner') {
        return socket.emit('error-msg', { text: 'Владелец не может покинуть — передайте права или удалите' });
      }
      r.members = r.members.filter(x => x !== userId);
      if (r.roles) delete r.roles[userId];
      if (r.members.length === 0) { delete rooms[roomId]; await deleteRoom(roomId); }
      else await saveRoom(roomId);
      broadcastRooms();
    } catch (e) {}
  });

  socket.on('disconnect', () => {
    try {
      if (socket.userId && online.has(socket.userId)) {
        online.get(socket.userId).delete(socket.id);
        if (online.get(socket.userId).size === 0) online.delete(socket.userId);
        broadcastUserLists();
      }
    } catch (e) {}
  });
});

process.on('uncaughtException', err => console.error('❌ uncaughtException:', err));
process.on('unhandledRejection', err => console.error('❌ unhandledRejection:', err));

const PORT = process.env.PORT || 3000;

(async function start() {
  try {
    await initDB();
    await initVapid();
    await loadData();
    server.listen(PORT, '0.0.0.0', () => {
      console.log(`\n🚀 Мессенджер v5.0.0 на порту ${PORT}\n`);
    });
  } catch (e) {
    console.error('❌ Критическая ошибка:', e);
    process.exit(1);
  }
})();

process.on('SIGTERM', async () => {
  try { await mongoClient.close(); } catch (e) {}
  server.close(() => process.exit(0));
});
