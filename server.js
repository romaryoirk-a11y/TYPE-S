'use strict';
const path = require('path');
const fs = require('fs');
// Загрузка переменных из файла .env (если существует) — до чтения process.env ниже
try {
  const envPath = path.join(__dirname, '.env');
  if (fs.existsSync(envPath)) {
    fs.readFileSync(envPath, 'utf8').split(/\r?\n/).forEach(line => {
      const m = line.match(/^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)$/);
      if (m && !(m[1] in process.env)) {
        process.env[m[1]] = m[2].trim().replace(/^["']|["']$/g, '');
      }
    });
  }
} catch (e) { console.warn('[env] не удалось прочитать .env:', e.message); }

const express = require('express');
const http = require('http');
const crypto = require('crypto');
const bcrypt = require('bcryptjs');
const multer = require('multer');
const { Server } = require('socket.io');
const { MongoClient } = require('mongodb');

const PORT = process.env.PORT || 3000;
const DATA_DIR = path.join(__dirname, 'data');
const DATA_FILE = path.join(DATA_DIR, 'db.json'); // старая JSON-БД — только для одноразового переноса в MongoDB
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

const VAPID_PUBLIC = process.env.VAPID_PUBLIC_KEY || '';
const VAPID_PRIVATE = process.env.VAPID_PRIVATE_KEY || '';
let webpush = null;
if (VAPID_PUBLIC && VAPID_PRIVATE) {
  try {
    webpush = require('web-push');
    webpush.setVapidDetails('mailto:admin@example.com', VAPID_PUBLIC, VAPID_PRIVATE);
  } catch (e) { console.warn('[push] disabled:', e.message); }
}

// ---------- MongoDB ----------
// MONGODB_URI — внешняя база (MongoDB Atlas / локальный mongod).
// Если не задана — автоматически поднимается встроенная in-memory MongoDB
// (mongodb-memory-server), чтобы проект работал «из коробки» без установки Mongo.
const MONGODB_URI = process.env.MONGODB_URI || '';
let client = null;
let col = {}; // users, usernames, messages, rooms, invites, subs, hiddenChats, encKeys, sessions

async function startMongo() {
  let uri = MONGODB_URI;
  if (!uri) {
    const { MongoMemoryServer } = require('mongodb-memory-server');
    const mem = await MongoMemoryServer.create({ instance: { dbName: 'pulse' } });
    uri = mem.getUri();
    console.log('[mongo] запущена встроенная in-memory MongoDB:', uri);
  } else {
    console.log('[mongo] подключение к внешней MongoDB...');
  }
  client = new MongoClient(uri);
  await client.connect();
  const dbo = client.db();
  col = {
    users: dbo.collection('users'),
    usernames: dbo.collection('usernames'),
    messages: dbo.collection('messages'),
    rooms: dbo.collection('rooms'),
    invites: dbo.collection('invites'),
    subs: dbo.collection('subs'),
    hiddenChats: dbo.collection('hiddenChats'),
    encKeys: dbo.collection('encKeys'),
    sessions: dbo.collection('sessions')
  };
  await Promise.all([
    col.users.createIndex({ userId: 1 }, { unique: true }),
    col.usernames.createIndex({ username: 1 }, { unique: true }),
    col.messages.createIndex({ id: 1 }, { unique: true }),
    col.messages.createIndex({ to: 1, timestamp: 1 }),
    col.rooms.createIndex({ id: 1 }, { unique: true }),
    col.invites.createIndex({ code: 1 }, { unique: true }),
    col.subs.createIndex({ userId: 1 }, { unique: true }),
    col.hiddenChats.createIndex({ userId: 1 }, { unique: true }),
    col.encKeys.createIndex({ chatId: 1 }, { unique: true }),
    col.sessions.createIndex({ token: 1 }, { unique: true })
  ]);
  await migrateFromJson();
  console.log('[mongo] база готова:', dbo.databaseName);
}

// Одноразовый перенос данных из старого data/db.json (если он непустой и Mongo ещё пуста)
async function migrateFromJson() {
  try {
    if (!fs.existsSync(DATA_FILE)) return;
    const raw = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
    const hasData = Object.keys(raw.users || {}).length || (raw.messages || []).length || Object.keys(raw.rooms || {}).length;
    if (!hasData) return;
    const existing = await col.users.countDocuments();
    if (existing > 0) { console.log('[migrate] MongoDB уже содержит данные — перенос пропущен'); return; }
    const ops = [];
    Object.values(raw.users || {}).forEach(u => ops.push(col.users.replaceOne({ userId: u.userId }, { userId: u.userId, ...u }, { upsert: true })));
    Object.entries(raw.usernames || {}).forEach(([username, userId]) => ops.push(col.usernames.replaceOne({ username }, { username, userId }, { upsert: true })));
    (raw.messages || []).forEach(m => ops.push(col.messages.replaceOne({ id: m.id }, { ...m }, { upsert: true })));
    Object.values(raw.rooms || {}).forEach(r => ops.push(col.rooms.replaceOne({ id: r.id }, { ...r }, { upsert: true })));
    Object.entries(raw.invites || {}).forEach(([code, v]) => ops.push(col.invites.replaceOne({ code }, { code, ...v }, { upsert: true })));
    Object.entries(raw.subs || {}).forEach(([userId, subscription]) => ops.push(col.subs.replaceOne({ userId }, { userId, subscription }, { upsert: true })));
    Object.entries(raw.hiddenChats || {}).forEach(([userId, ids]) => ops.push(col.hiddenChats.replaceOne({ userId }, { userId, ids }, { upsert: true })));
    Object.entries(raw.encKeys || {}).forEach(([chatId, rec]) => ops.push(col.encKeys.replaceOne({ chatId }, { chatId, ...rec }, { upsert: true })));
    await Promise.all(ops.map(p => p.catch(e => console.error('[migrate] op failed:', e.message))));
    const backup = DATA_FILE + '.migrated.bak';
    fs.renameSync(DATA_FILE, backup);
    console.log('[migrate] данные перенесены в MongoDB, старый файл сохранён как', path.basename(backup));
  } catch (e) { console.error('[migrate] failed:', e.message); }
}

// ---------- Sessions / presence ----------
const online = new Map();     // userId -> Set<socketId>  (в реальном времени, не хранится в БД)

async function sessionGet(token) {
  if (!token) return null;
  const s = await col.sessions.findOne({ token });
  return s ? s.userId : null;
}
async function sessionSet(token, userId) {
  await col.sessions.updateOne({ token }, { $set: { token, userId, createdAt: Date.now() } }, { upsert: true });
}

// ---------- Express ----------
const app = express();
const server = http.createServer(app);
const io = new Server(server, { maxHttpBufferSize: 12 * 1024 * 1024 });

app.use(express.json({ limit: '1mb' }));
app.use(express.static(path.join(__dirname, 'public')));

const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 10 * 1024 * 1024 } });
app.post('/upload', upload.single('file'), (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'no file' });
  const b64 = req.file.buffer.toString('base64');
  res.json({
    url: 'data:' + req.file.mimetype + ';base64,' + b64,
    name: req.file.originalname,
    size: req.file.size,
    type: req.file.mimetype
  });
});

app.get('/health', (_req, res) => res.json({ ok: true, time: Date.now(), db: 'mongodb' }));

app.get('/stats', async (_req, res) => {
  try {
    const [totalUsers, totalMessages, totalRooms] = await Promise.all([
      col.users.countDocuments(),
      col.messages.countDocuments({ deleted: { $ne: true } }),
      col.rooms.countDocuments()
    ]);
    const agg = await col.messages.aggregate([
      { $match: { deleted: { $ne: true } } },
      {
        $project: {
          file: { $cond: [{ $ifNull: ['$file', false] }, 1, 0] },
          reactions: {
            $reduce: {
              input: { $ifNull: [{ $objectToArray: { $ifNull: ['$reactions', {}] } }, []] },
              initialValue: 0,
              in: { $add: ['$$value', { $size: '$$this.v' }] }
            }
          }
        }
      },
      { $group: { _id: null, files: { $sum: '$file' }, reactions: { $sum: '$reactions' } } }
    ]).toArray();
    res.json({
      totalUsers,
      online: online.size,
      totalMessages,
      totalRooms,
      totalFileMsgs: agg[0] ? agg[0].files : 0,
      totalReactions: agg[0] ? agg[0].reactions : 0
    });
  } catch (e) {
    console.error('[stats]', e);
    res.status(500).json({ error: 'stats failed' });
  }
});

app.get('/vapid-public-key', (_req, res) => res.json({ key: VAPID_PUBLIC || '' }));

app.post('/subscribe', async (req, res) => {
  const { userId, subscription } = req.body || {};
  if (!userId || !subscription) return res.status(400).json({ error: 'bad request' });
  await col.subs.updateOne({ userId }, { $set: { userId, subscription } }, { upsert: true });
  res.json({ ok: true });
});

app.post('/unsubscribe', async (req, res) => {
  const { userId } = req.body || {};
  if (userId) await col.subs.deleteOne({ userId });
  res.json({ ok: true });
});

app.get('/join/:code', (req, res) => {
  res.redirect('/?invite=' + encodeURIComponent(req.params.code));
});

// ---------- Helpers ----------
const now = () => Date.now();
const uid = (p) => (p || 'x') + '_' + now().toString(36) + Math.random().toString(36).slice(2, 8);
const newToken = () => crypto.randomBytes(24).toString('hex');
function fmtTime(ts) {
  const d = new Date(ts);
  return String(d.getHours()).padStart(2, '0') + ':' + String(d.getMinutes()).padStart(2, '0');
}

function pubUser(u) {
  if (!u) return null;
  return {
    userId: u.userId, name: u.name, username: u.username,
    color: u.color, initials: u.initials, avatar: u.avatar || null,
    online: online.has(u.userId)
  };
}

// Ключи документов (для быстрых map-lookup'ов поверх Mongo)
const docKey = (d) => d._id || d.userId || d.id || d.roomId || d.chatId || d.code;
const indexById = (docs) => { const m = {}; docs.forEach(d => { m[docKey(d)] = d; }); return m; };

async function getUser(userId) { return col.users.findOne({ userId }); }
async function getAllUsers() { return col.users.find({}).toArray(); }
async function getUserIdByUsername(username) {
  const u = await col.usernames.findOne({ username });
  return u ? u.userId : null;
}
async function getRoom(roomId) { return roomId ? col.rooms.findOne({ id: roomId }) : null; }
async function getAllRooms() { return col.rooms.find({}).toArray(); }
async function getMessage(msgId) { return msgId ? col.messages.findOne({ id: msgId }) : null; }
async function getHiddenMap(userId) {
  const h = await col.hiddenChats.findOne({ userId });
  return h ? (h.ids || {}) : {};
}
async function getEncKey(chatId) { return col.encKeys.findOne({ chatId }); }
async function getSub(userId) {
  const s = await col.subs.findOne({ userId });
  return s ? s.subscription : null;
}

// Установка/обновление комнаты с корректной сериализацией ролей (Mongo не принимает "$" и "." в ключах)
async function putRoom(room) {
  const rolesArr = Object.entries(room.roles || {}).map(([userId, role]) => ({ userId, role }));
  const doc = {
    id: room.id, name: room.name, type: room.type, description: room.description || '',
    members: room.members.slice(), rolesArr, createdBy: room.createdBy, color: room.color || '#5288c1'
  };
  await col.rooms.replaceOne({ id: room.id }, doc, { upsert: true });
}
function hydrateRoom(doc) {
  if (!doc) return null;
  const roles = {};
  (doc.rolesArr || []).forEach(r => { roles[r.userId] = r.role; });
  return { ...doc, roles, _id: undefined };
}

function canSeeMessage(m, userId, roomsById) {
  if (m.to === 'public') return true;
  if (m.to === 'saved') return m.from === userId;
  if (typeof m.to === 'string' && m.to.indexOf('r_') === 0) {
    const r = roomsById[m.to];
    return !!(r && r.members && r.members.indexOf(userId) >= 0);
  }
  return m.from === userId || m.to === userId;
}

async function msgsFor(userId) {
  const [messages, roomsArr, hidden] = await Promise.all([
    col.messages.find({}).sort({ timestamp: 1 }).toArray(),
    getAllRooms(),
    getHiddenMap(userId)
  ]);
  const roomsById = indexById(roomsArr.map(hydrateRoom));
  return messages.filter(m => !hidden[m.id] && canSeeMessage(m, userId, roomsById));
}

function isBlockedAny(a, b, usersById) {
  // взаимная блокировка: если хоть один заблокировал другого — контакт невозможен
  const ua = usersById[a], ub = usersById[b];
  if (!ua || !ub) return false;
  const ba = Array.isArray(ua.blocked) ? ua.blocked : [];
  const bb = Array.isArray(ub.blocked) ? ub.blocked : [];
  return ba.indexOf(b) >= 0 || bb.indexOf(a) >= 0;
}

async function usersForViewer(userId) {
  const [users, messages, roomsArr] = await Promise.all([getAllUsers(), col.messages.find({}).toArray(), getAllRooms()]);
  const usersById = indexById(users);
  const roomsById = indexById(roomsArr.map(hydrateRoom));
  const me = usersById[userId];
  const myBlocked = (me && Array.isArray(me.blocked)) ? me.blocked : [];
  const ids = new Set();
  if (me && Array.isArray(me.contacts)) me.contacts.forEach(id => ids.add(id));

  messages.forEach(m => {
    if (m.from === userId) {
      if (m.to !== 'public' && m.to !== 'saved' && String(m.to).indexOf('r_') !== 0) ids.add(m.to);
    } else if (canSeeMessage(m, userId, roomsById)) {
      ids.add(m.from);
    }
  });
  Object.values(roomsById).forEach(r => {
    if (r.members && r.members.indexOf(userId) >= 0) r.members.forEach(id => ids.add(id));
  });
  ids.delete(userId);
  myBlocked.forEach(id => ids.delete(id));

  const out = [];
  ids.forEach(id => { if (usersById[id]) out.push(pubUser(usersById[id])); });
  out.sort((a, b) => a.name.localeCompare(b.name));
  return out;
}

function roomView(r) {
  return {
    id: r.id, name: r.name, type: r.type,
    description: r.description || '',
    members: r.members.slice(),
    roles: Object.assign({}, r.roles),
    createdBy: r.createdBy,
    color: r.color || '#5288c1'
  };
}
async function roomsList() {
  const rooms = await getAllRooms();
  return rooms.map(hydrateRoom).map(roomView);
}

function emitToUser(userId, event, data) {
  const set = online.get(userId);
  if (!set) return;
  set.forEach(sid => io.to(sid).emit(event, data));
}

async function broadcastRoomsToAll() {
  const list = await roomsList();
  io.sockets.sockets.forEach(s => { if (s.data.userId) s.emit('rooms', list); });
}
async function broadcastUsersToAll() {
  io.sockets.sockets.forEach(async s => {
    if (s.data.userId) s.emit('users', await usersForViewer(s.data.userId));
  });
}

function canWriteRoom(userId, room) {
  if (!room) return false;
  const role = (room.roles && room.roles[userId]) || (room.createdBy === userId ? 'owner' : 'member');
  if (room.type === 'channel') return role === 'owner' || role === 'admin';
  return true;
}
function inRoom(userId, room) {
  return !!(room && room.members && room.members.indexOf(userId) >= 0);
}

async function sendPush(userId, title, body) {
  if (!webpush) return;
  const sub = await getSub(userId);
  if (!sub) return;
  webpush.sendNotification(sub, JSON.stringify({ title, body }))
    .catch(() => { col.subs.deleteOne({ userId }).catch(() => {}); });
}

// ---------- Socket.IO ----------
io.on('connection', (socket) => {
  socket.data.userId = null;

  async function attachUser(userId, token, isNewAccount) {
    socket.data.userId = userId;
    if (!online.has(userId)) online.set(userId, new Set());
    online.get(userId).add(socket.id);
    const u = await getUser(userId);
    if (!u) return;
    const safeUser = Object.assign({}, u, { password: undefined });
    socket.emit('joined', { user: safeUser, userId, token, isNewAccount: !!isNewAccount });
    socket.emit('history', await msgsFor(userId));
    socket.emit('rooms', await roomsList());
    socket.emit('users', await usersForViewer(userId));
    io.emit('user-updated', { userId, user: pubUser(u) });
  }

  socket.on('register', async (data = {}) => {
    try {
      const name = String(data.name || '').trim().slice(0, 30);
      const username = String(data.username || '').trim().toLowerCase();
      const password = String(data.password || '');
      if (!name || !/^[a-z][a-z0-9_]{2,19}$/.test(username) || password.length < 4) {
        return socket.emit('auth-error', { text: 'Проверьте поля' });
      }
      if (await getUserIdByUsername(username)) return socket.emit('auth-error', { text: 'Username уже занят' });

      const userId = uid('u');
      const hash = await bcrypt.hash(password, 10);
      const user = {
        userId, name, username, password: hash,
        color: data.color || '#5288c1',
        initials: data.initials || name.slice(0, 2).toUpperCase(),
        avatar: null,
        contacts: [], mutedChats: [], blocked: [],
        settings: { theme: 'light', accent: '#3390ec', language: 'ru' },
        createdAt: now()
      };
      await Promise.all([
        col.users.insertOne(user),
        col.usernames.insertOne({ username, userId })
      ]);
      const tok = newToken();
      await sessionSet(tok, userId);
      attachUser(userId, tok, true);
    } catch (e) {
      console.error('[register]', e);
      socket.emit('auth-error', { text: 'Ошибка регистрации' });
    }
  });

  socket.on('login', async (data = {}) => {
    try {
      const username = String(data.username || '').trim().toLowerCase();
      const password = String(data.password || '');
      const userId = await getUserIdByUsername(username);
      if (!userId) return socket.emit('auth-error', { text: 'Неверный логин или пароль' });
      const u = await getUser(userId);
      const ok = await bcrypt.compare(password, u.password);
      if (!ok) return socket.emit('auth-error', { text: 'Неверный логин или пароль' });
      const tok = newToken();
      await sessionSet(tok, userId);
      attachUser(userId, tok, false);
    } catch (e) {
      console.error('[login]', e);
      socket.emit('auth-error', { text: 'Ошибка входа' });
    }
  });

  socket.on('auto-login', async (data = {}) => {
    try {
      const { userId, token } = data;
      const owner = await sessionGet(token);
      const u = userId ? await getUser(userId) : null;
      if (!userId || !token || !u || owner !== userId) {
        return socket.emit('auth-error', { text: 'Сессия истекла' });
      }
      attachUser(userId, token, false);
    } catch (e) {
      console.error('[auto-login]', e);
      socket.emit('auth-error', { text: 'Ошибка восстановления сессии' });
    }
  });

  socket.on('disconnect', () => {
    const userId = socket.data.userId;
    if (!userId) return;
    const set = online.get(userId);
    if (set) {
      set.delete(socket.id);
      if (set.size === 0) {
        online.delete(userId);
        getUser(userId).then(u => { if (u) io.emit('user-updated', { userId, user: pubUser(u) }); });
      }
    }
  });

  // auth-обёртка: асинхронная, с перехватом ошибок
  const auth = (fn) => async (data, cb) => {
    if (!socket.data.userId) return;
    try { await fn(data || {}, cb); } catch (e) { console.error('[handler]', e); }
  };

  socket.on('message', auth(async (data) => {
    const userId = socket.data.userId;
    const u = await getUser(userId);
    if (!u) return;
    const to = String(data.to || '');
    if (!to) return;
    if (to.indexOf('r_') === 0) {
      const room = await getRoom(to);
      if (!inRoom(userId, room)) return;
      if (!canWriteRoom(userId, room)) return socket.emit('error-msg', { text: 'Только админы могут писать' });
    } else if (to !== 'public' && to !== 'saved') {
      // личный чат: проверка блокировки (в любую сторону)
      const [target, users] = await Promise.all([getUser(to), getAllUsers()]);
      if (!target) return;
      const usersById = indexById(users);
      if (isBlockedAny(userId, to, usersById)) {
        const byMe = !!(u.blocked && u.blocked.indexOf(to) >= 0);
        return socket.emit('error-msg', { text: byMe ? 'Пользователь заблокирован. Разблокируйте его.' : 'Вы заблокированы этим пользователем' });
      }
    }
    const ts = data.timestamp || now();
    const msg = {
      id: data.id || uid('m'),
      from: userId, fromName: u.name,
      to,
      text: String(data.text || '').slice(0, 4000),
      file: data.file || null,
      replyTo: data.replyTo || null,
      timestamp: ts,
      time: fmtTime(ts),
      deliveredTo: [], readBy: [], reactions: {},
      edited: false, deleted: false, pinned: false
    };
    await col.messages.insertOne(msg);

    const [users, roomsArr] = await Promise.all([getAllUsers(), getAllRooms()]);
    const roomsById = indexById(roomsArr.map(hydrateRoom));
    const usersById = indexById(users);
    for (const rid of Object.keys(usersById)) {
      if (!canSeeMessage(msg, rid, roomsById)) continue;
      // блокировка скрывает личные сообщения от обеих сторон (в т.ч. уже отправленные)
      if (msg.to !== 'public' && msg.to !== 'saved' && String(msg.to).indexOf('r_') !== 0) {
        const other = msg.from === rid ? msg.to : msg.from;
        if (isBlockedAny(rid, other, usersById)) continue;
      }
      emitToUser(rid, 'message', msg);
      if (rid !== userId) {
        const me = usersById[rid];
        const key = msg.to === 'public' ? 'public'
          : msg.to === 'saved' ? 'saved'
          : msg.to.indexOf('r_') === 0 ? msg.to
          : (msg.from === rid ? msg.to : msg.from);
        if (!me.mutedChats || me.mutedChats.indexOf(key) < 0) {
          sendPush(rid, u.name, msg.text || 'Файл');
        }
      }
    }
  }));

  socket.on('delivered', auth(async (data) => {
    const userId = socket.data.userId;
    for (const id of (data.messageIds || [])) {
      const m = await getMessage(id);
      if (!m) continue;
      if (m.from === userId) continue;
      await col.messages.updateOne({ id }, { $addToSet: { deliveredTo: userId } });
      const fresh = await getMessage(id);
      emitToUser(m.from, 'message-update', fresh);
    }
  }));

  socket.on('read', auth(async (data) => {
    const userId = socket.data.userId;
    for (const id of (data.messageIds || [])) {
      const m = await getMessage(id);
      if (!m) continue;
      if (m.from === userId) continue;
      await col.messages.updateOne({ id }, { $addToSet: { readBy: userId, deliveredTo: userId } });
      const fresh = await getMessage(id);
      emitToUser(m.from, 'message-update', fresh);
    }
  }));

  socket.on('typing', auth(async (data) => {
    const userId = socket.data.userId;
    const u = await getUser(userId);
    if (!u) return;
    const to = String(data.to || '');
    if (!to || to === 'saved') return;
    const payload = { to, from: userId, fromName: u.name };
    if (to === 'public') socket.broadcast.emit('typing', payload);
    else if (to.indexOf('r_') === 0) {
      const r = await getRoom(to);
      if (!r) return;
      r.members.forEach(mid => { if (mid !== userId) emitToUser(mid, 'typing', payload); });
    } else {
      const [users] = await Promise.all([getAllUsers()]);
      if (isBlockedAny(userId, to, indexById(users))) return;
      emitToUser(to, 'typing', payload);
    }
  }));

  socket.on('react-message', auth(async (data) => {
    const userId = socket.data.userId;
    const m = await getMessage(data.messageId);
    if (!m) return;
    const em = String(data.emoji || '').slice(0, 8);
    if (!em) return;
    const cur = (m.reactions && m.reactions[em]) || [];
    const next = cur.indexOf(userId) >= 0 ? cur.filter(x => x !== userId) : cur.concat(userId);
    const upd = next.length ? { $set: { ['reactions.' + em]: next } } : { $unset: { ['reactions.' + em]: '' } };
    await col.messages.updateOne({ id: m.id }, upd);
    const fresh = await getMessage(m.id);
    const [users, roomsArr] = await Promise.all([getAllUsers(), getAllRooms()]);
    const roomsById = indexById(roomsArr.map(hydrateRoom));
    Object.keys(indexById(users)).forEach(rid => {
      if (canSeeMessage(fresh, rid, roomsById)) emitToUser(rid, 'message-update', fresh);
    });
  }));

  socket.on('pin-message', auth(async (data) => {
    const userId = socket.data.userId;
    const m = await getMessage(data.messageId);
    if (!m) return;
    if (typeof m.to === 'string' && m.to.indexOf('r_') === 0) {
      const r = await getRoom(m.to);
      if (!r) return;
      const role = (r.roles && r.roles[userId]) || (r.createdBy === userId ? 'owner' : 'member');
      if (role !== 'owner' && role !== 'admin') return;
    }
    await col.messages.updateOne({ id: m.id }, { $set: { pinned: !m.pinned } });
    const fresh = await getMessage(m.id);
    const [users, roomsArr] = await Promise.all([getAllUsers(), getAllRooms()]);
    const roomsById = indexById(roomsArr.map(hydrateRoom));
    Object.keys(indexById(users)).forEach(rid => {
      if (canSeeMessage(fresh, rid, roomsById)) emitToUser(rid, 'message-update', fresh);
    });
  }));

  socket.on('edit-message', auth(async (data) => {
    const userId = socket.data.userId;
    const m = await getMessage(data.messageId);
    if (!m || m.from !== userId) return;
    if (now() - m.timestamp > 24 * 60 * 60 * 1000) return socket.emit('error-msg', { text: 'Слишком поздно редактировать' });
    const txt = String(data.newText || '').trim().slice(0, 4000);
    if (!txt) return;
    await col.messages.updateOne({ id: m.id }, { $set: { text: txt, edited: true } });
    const fresh = await getMessage(m.id);
    const [users, roomsArr] = await Promise.all([getAllUsers(), getAllRooms()]);
    const roomsById = indexById(roomsArr.map(hydrateRoom));
    Object.keys(indexById(users)).forEach(rid => {
      if (canSeeMessage(fresh, rid, roomsById)) emitToUser(rid, 'message-update', fresh);
    });
  }));

  socket.on('delete-message', auth(async (data) => {
    const userId = socket.data.userId;
    const m = await getMessage(data.messageId);
    if (!m || m.from !== userId) return;
    await col.messages.updateOne({ id: m.id }, { $set: { deleted: true, text: '', file: null, reactions: {}, pinned: false } });
    const fresh = await getMessage(m.id);
    const [users, roomsArr] = await Promise.all([getAllUsers(), getAllRooms()]);
    const roomsById = indexById(roomsArr.map(hydrateRoom));
    Object.keys(indexById(users)).forEach(rid => {
      if (canSeeMessage(fresh, rid, roomsById)) emitToUser(rid, 'message-update', fresh);
    });
  }));

  socket.on('create-room', auth(async (data) => {
    const userId = socket.data.userId;
    const name = String(data.name || '').trim().slice(0, 60);
    if (!name) return;
    const type = data.type === 'channel' ? 'channel' : 'group';
    const rid = uid('r');
    const room = {
      id: rid, name, type,
      description: String(data.description || '').slice(0, 200),
      members: [userId],
      roles: { [userId]: 'owner' },
      createdBy: userId,
      color: '#5288c1'
    };
    if (Array.isArray(data.inviteUsernames)) {
      for (const un of data.inviteUsernames) {
        const clean = String(un || '').replace(/^@/, '').trim().toLowerCase();
        const iid = await getUserIdByUsername(clean);
        if (iid && iid !== userId && room.members.indexOf(iid) < 0) {
          room.members.push(iid);
          room.roles[iid] = 'member';
          emitToUser(iid, 'invited', { names: [name] });
        }
      }
    }
    await putRoom(room);
    socket.emit('room-created', roomView(room));
    await broadcastRoomsToAll();
    await broadcastUsersToAll();
  }));

  socket.on('invite-to-room', auth(async (data) => {
    const userId = socket.data.userId;
    const r = await getRoom(data.roomId);
    if (!r) return;
    const myRole = r.roles[userId] || (r.createdBy === userId ? 'owner' : 'member');
    if (myRole !== 'owner' && myRole !== 'admin') return;
    const added = [];
    for (const un of (data.usernames || [])) {
      const clean = String(un || '').replace(/^@/, '').trim().toLowerCase();
      const iid = await getUserIdByUsername(clean);
      if (iid && r.members.indexOf(iid) < 0) {
        r.members.push(iid);
        r.roles[iid] = 'member';
        const tu = await getUser(iid);
        if (tu) added.push(tu.name);
        emitToUser(iid, 'invited', { names: [r.name] });
      }
    }
    await putRoom(r);
    socket.emit('toast-msg', { text: added.length ? 'Приглашены: ' + added.join(', ') : 'Никого не добавили' });
    await broadcastRoomsToAll();
    await broadcastUsersToAll();
  }));

  socket.on('join-room', auth(async (data) => {
    const userId = socket.data.userId;
    const r = await getRoom(data.roomId);
    if (!r) return;
    if (r.type === 'channel') return;
    if (r.members.indexOf(userId) < 0) {
      r.members.push(userId);
      r.roles[userId] = 'member';
      await putRoom(r);
      await broadcastRoomsToAll();
      await broadcastUsersToAll();
    }
  }));

  socket.on('leave-room', auth(async (data) => {
    const userId = socket.data.userId;
    const r = await getRoom(data.roomId);
    if (!r) return;
    const i = r.members.indexOf(userId);
    if (i >= 0) r.members.splice(i, 1);
    delete r.roles[userId];
    if (!r.members.length) {
      await col.rooms.deleteOne({ id: r.id });
    } else {
      if (r.createdBy === userId) {
        r.createdBy = r.members[0];
        r.roles[r.createdBy] = 'owner';
      }
      await putRoom(r);
    }
    await broadcastRoomsToAll();
    await broadcastUsersToAll();
  }));

  socket.on('kick-user', auth(async (data) => {
    const userId = socket.data.userId;
    const r = await getRoom(data.roomId);
    if (!r) return;
    const myRole = r.roles[userId] || (r.createdBy === userId ? 'owner' : 'member');
    if (myRole !== 'owner' && myRole !== 'admin') return;
    const target = data.targetUserId;
    if (!target || target === userId) return;
    const tr = r.roles[target] || 'member';
    if (tr === 'owner') return;
    if (myRole === 'admin' && tr === 'admin') return;
    const i = r.members.indexOf(target);
    if (i >= 0) r.members.splice(i, 1);
    delete r.roles[target];
    await putRoom(r);
    await broadcastRoomsToAll();
    await broadcastUsersToAll();
  }));

  socket.on('set-role', auth(async (data) => {
    const userId = socket.data.userId;
    const r = await getRoom(data.roomId);
    if (!r) return;
    if (r.roles[userId] !== 'owner' && r.createdBy !== userId) return;
    const target = data.targetUserId;
    if (!target || target === userId) return;
    if (r.members.indexOf(target) < 0) return;
    r.roles[target] = data.role === 'admin' ? 'admin' : 'member';
    await putRoom(r);
    await broadcastRoomsToAll();
  }));

  socket.on('add-contact', auth(async (data) => {
    const userId = socket.data.userId;
    const cid = data.contactId;
    if (!await getUser(cid) || cid === userId) return;
    await col.users.updateOne({ userId }, { $addToSet: { contacts: cid } });
    socket.emit('contact-added', { userId: cid });
    socket.emit('users', await usersForViewer(userId));
  }));

  socket.on('remove-contact', auth(async (data) => {
    const userId = socket.data.userId;
    const cid = data.contactId;
    await col.users.updateOne({ userId }, { $pull: { contacts: cid } });
    socket.emit('contact-removed', { userId: cid });
    socket.emit('users', await usersForViewer(userId));
  }));

  socket.on('search-user', auth(async (data) => {
    const userId = socket.data.userId;
    const users = await getAllUsers();
    const usersById = indexById(users);
    const meU = usersById[userId];
    const myBlocked = (meU && Array.isArray(meU.blocked)) ? meU.blocked : [];
    const q = String(data.query || '').replace(/^@/, '').toLowerCase();
    if (!q) return socket.emit('search-result', { results: [] });
    const out = [];
    users.forEach(u => {
      if (u.userId === userId) return;
      if (myBlocked.indexOf(u.userId) >= 0) return;                 // я заблокировал — не ищу
      if (Array.isArray(u.blocked) && u.blocked.indexOf(userId) >= 0) return; // он заблокировал меня — невидим
      if (u.username.indexOf(q) >= 0 || u.name.toLowerCase().indexOf(q) >= 0) out.push(pubUser(u));
    });
    out.sort((a, b) => a.username.indexOf(q) - b.username.indexOf(q));
    socket.emit('search-result', { results: out.slice(0, 30) });
  }));

  socket.on('search-messages', auth(async (data) => {
    const userId = socket.data.userId;
    const q = String(data.query || '').toLowerCase().trim();
    if (q.length < 2) return socket.emit('search-messages-result', { query: q, results: [] });
    const [messages, roomsArr] = await Promise.all([
      col.messages.find({ deleted: { $ne: true } }).sort({ timestamp: 1 }).limit(2000).toArray(),
      getAllRooms()
    ]);
    const roomsById = indexById(roomsArr.map(hydrateRoom));
    const results = messages
      .filter(m => canSeeMessage(m, userId, roomsById) && (m.text || '').toLowerCase().indexOf(q) >= 0)
      .slice(-100);
    socket.emit('search-messages-result', { query: q, results });
  }));

  socket.on('create-invite', auth(async (data) => {
    const userId = socket.data.userId;
    const r = await getRoom(data.roomId);
    if (!r) return;
    const myRole = r.roles[userId] || (r.createdBy === userId ? 'owner' : 'member');
    if (myRole !== 'owner' && myRole !== 'admin') return;
    const code = crypto.randomBytes(6).toString('hex');
    await col.invites.updateOne({ code }, { $set: { code, roomId: r.id, createdBy: userId, createdAt: now() } }, { upsert: true });
    socket.emit('invite-created', { link: '/join/' + code });
  }));

  socket.on('redeem-invite', auth(async (data) => {
    const userId = socket.data.userId;
    const inv = await col.invites.findOne({ code: String(data.code || '') });
    if (!inv) return socket.emit('invite-error', { text: 'Ссылка недействительна' });
    const r = await getRoom(inv.roomId);
    if (!r) return socket.emit('invite-error', { text: 'Комната не найдена' });
    const already = r.members.indexOf(userId) >= 0;
    if (!already) {
      r.members.push(userId);
      r.roles[userId] = 'member';
      await putRoom(r);
      await broadcastRoomsToAll();
      await broadcastUsersToAll();
    }
    socket.emit('invite-redeemed', { roomId: r.id, alreadyMember: already });
  }));

  socket.on('mute-chat', auth(async (data) => {
    const userId = socket.data.userId;
    const cid = String(data.chatId || '');
    if (!cid) return;
    if (data.muted) await col.users.updateOne({ userId }, { $addToSet: { mutedChats: cid } });
    else await col.users.updateOne({ userId }, { $pull: { mutedChats: cid } });
    const u = await getUser(userId);
    socket.emit('mutes-updated', { mutedChats: u.mutedChats || [] });
  }));

  // ---------- Блокировка пользователя ----------
  socket.on('block-user', auth(async (data) => {
    const userId = socket.data.userId;
    const target = String(data.targetUserId || '');
    if (!target || target === userId || !await getUser(target)) return;
    await col.users.updateOne({ userId }, { $addToSet: { blocked: target }, $pull: { contacts: target } });
    await col.users.updateOne({ userId: target }, { $pull: { contacts: userId } });
    const u = await getUser(userId);
    socket.emit('blocked-updated', { blocked: (u.blocked || []).slice() });
    socket.emit('users', await usersForViewer(userId));
    socket.emit('history', await msgsFor(userId));
    await broadcastUsersToAll();
    emitToUser(target, 'users', await usersForViewer(target));
    socket.emit('toast-msg', { text: 'Пользователь заблокирован' });
  }));

  socket.on('unblock-user', auth(async (data) => {
    const userId = socket.data.userId;
    const target = String(data.targetUserId || '');
    await col.users.updateOne({ userId }, { $pull: { blocked: target } });
    const u = await getUser(userId);
    socket.emit('blocked-updated', { blocked: (u.blocked || []).slice() });
    socket.emit('users', await usersForViewer(userId));
    socket.emit('history', await msgsFor(userId));
    await broadcastUsersToAll();
    socket.emit('toast-msg', { text: 'Пользователь разблокирован' });
  }));

  // ---------- Удаление чата (только у себя) ----------
  socket.on('delete-chat', auth(async (data) => {
    const userId = socket.data.userId;
    const chatId = String(data.chatId || '');
    if (!chatId) return;
    const [messages, roomsArr] = await Promise.all([col.messages.find({}).toArray(), getAllRooms()]);
    const roomsById = indexById(roomsArr.map(hydrateRoom));
    const hidden = await getHiddenMap(userId);
    const ids = Object.assign({}, hidden);
    messages.forEach(m => {
      if (ids[m.id]) return;
      if (!canSeeMessage(m, userId, roomsById)) return;
      let key;
      if (m.to === 'public') key = 'public';
      else if (m.to === 'saved') key = 'saved';
      else if (String(m.to).indexOf('r_') === 0) key = m.to;
      else key = (m.from === userId ? m.to : m.from);
      if (key === chatId) ids[m.id] = true;
    });
    await col.hiddenChats.updateOne({ userId }, { $set: { userId, ids } }, { upsert: true });
    // если это комната — выйти из неё (чат исчезнет из списка полностью)
    if (chatId.indexOf('r_') === 0) {
      const r = await getRoom(chatId);
      if (r) {
        const mi = r.members.indexOf(userId);
        if (mi >= 0) {
          r.members.splice(mi, 1);
          delete r.roles[userId];
          if (!r.members.length) await col.rooms.deleteOne({ id: r.id });
          else {
            if (r.createdBy === userId) { r.createdBy = r.members[0]; r.roles[r.createdBy] = 'owner'; }
            await putRoom(r);
          }
          await broadcastRoomsToAll();
          await broadcastUsersToAll();
        }
      }
    }
    socket.emit('chat-deleted', { chatId });
    socket.emit('history', await msgsFor(userId));
    socket.emit('toast-msg', { text: 'Чат удалён' });
  }));

  socket.on('update-settings', auth(async (data) => {
    const userId = socket.data.userId;
    if (data.settings && typeof data.settings === 'object') {
      const u = await getUser(userId);
      const merged = Object.assign({}, u.settings, data.settings);
      await col.users.updateOne({ userId }, { $set: { settings: merged } });
    }
  }));

  socket.on('update-profile', auth(async (data) => {
    const userId = socket.data.userId;
    const u = await getUser(userId);
    if (!u) return;
    const set = {};
    if (typeof data.name === 'string' && data.name.trim()) {
      set.name = data.name.trim().slice(0, 30);
      set.initials = set.name.split(/\s+/).map(w => w[0]).join('').toUpperCase().slice(0, 2)
        || set.name.slice(0, 2).toUpperCase();
    }
    if (data.color) set.color = data.color;
    if (data.initials) set.initials = data.initials;
    if (data.avatar === null || typeof data.avatar === 'string') set.avatar = data.avatar;
    // смена @username
    if (typeof data.username === 'string' && data.username.trim()) {
      const nu = data.username.trim().toLowerCase().replace(/^@/, '');
      if (!/^[a-z][a-z0-9_]{2,19}$/.test(nu)) {
        return socket.emit('profile-error', { text: 'Некорректный @username' });
      }
      if (nu !== u.username) {
        if (await getUserIdByUsername(nu)) return socket.emit('profile-error', { text: '@username уже занят' });
        await col.usernames.deleteOne({ username: u.username });
        set.username = nu;
        await col.usernames.insertOne({ username: nu, userId });
      }
    }
    await col.users.updateOne({ userId }, { $set: set });
    const fresh = await getUser(userId);
    io.emit('user-updated', { userId, user: pubUser(fresh) });
    socket.emit('profile-saved', { username: fresh.username });
  }));

  socket.on('change-password', async (data = {}) => {
    try {
      const userId = socket.data.userId;
      if (!userId) return;
      const u = await getUser(userId);
      if (!u) return;
      const ok = await bcrypt.compare(String(data.oldPassword || ''), u.password);
      if (!ok) return socket.emit('toast-msg', { text: 'Неверный старый пароль' });
      const np = String(data.newPassword || '');
      if (np.length < 4) return socket.emit('toast-msg', { text: 'Минимум 4 символа' });
      const hash = await bcrypt.hash(np, 10);
      await col.users.updateOne({ userId }, { $set: { password: hash } });
      // сброс ключей E2E: старые зашифрованные чаты станут недоступны, новые — с новым ключом
      await col.encKeys.deleteMany({ by: userId });
      socket.emit('password-changed', {});
      socket.emit('toast-msg', { text: 'Пароль изменён. Ключи шифрования пересозданы.' });
    } catch (e) { console.error('[change-password]', e); }
  });

  // ---------- E2E: хранение публичных чат-ключей (зашифрованных мастер-паролем клиента) ----------
  socket.on('enc-keys-get', auth(async (data, cb) => {
    const chatId = String((data && data.chatId) || '');
    if (!chatId) { if (typeof cb === 'function') cb(null); return; }
    const rec = await getEncKey(chatId);
    if (typeof cb === 'function') cb(rec ? { ek: rec.ek, salt: rec.salt, by: rec.by } : null);
  }));

  socket.on('enc-keys-set', auth(async (data) => {
    const chatId = String((data && data.chatId) || '');
    const rec = data && data.record;
    if (!chatId || !rec || typeof rec !== 'object') return;
    // разрешаем запись только участникам личного чата или комнаты
    const userId = socket.data.userId;
    let allowed = false;
    if (chatId.indexOf('r_') === 0) allowed = inRoom(userId, await getRoom(chatId));
    else if (chatId === 'public' || chatId === 'saved') allowed = true;
    else allowed = !!(await getUser(chatId));
    if (!allowed) return;
    const exists = await getEncKey(chatId);
    if (exists) return; // ключ устанавливается один раз (первым участником)
    const doc = {
      chatId,
      ek: String(rec.ek || '').slice(0, 200),
      salt: String(rec.salt || '').slice(0, 64),
      by: userId
    };
    await col.encKeys.updateOne({ chatId }, { $set: doc }, { upsert: true });
    io.emit('enc-keys-updated', { chatId, record: { ek: doc.ek, salt: doc.salt, by: doc.by } });
  }));
});

// ---------- Старт ----------
startMongo()
  .then(() => server.listen(PORT, () => console.log('Pulse listening on ' + PORT)))
  .catch(e => { console.error('[mongo] fatal:', e); process.exit(1); });

process.on('SIGINT', async () => { try { if (client) await client.close(); } catch (e) {} process.exit(0); });
process.on('SIGTERM', async () => { try { if (client) await client.close(); } catch (e) {} process.exit(0); });
