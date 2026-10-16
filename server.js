'use strict';
const express = require('express');
const http = require('http');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
const bcrypt = require('bcryptjs');
const multer = require('multer');
const { Server } = require('socket.io');

const PORT = process.env.PORT || 3000;
const DATA_DIR = path.join(__dirname, 'data');
const DATA_FILE = path.join(DATA_DIR, 'db.json');
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

// ---------- DB ----------
let db = { users: {}, usernames: {}, messages: [], rooms: {}, invites: {}, subs: {}, hiddenChats: {} };
try {
  if (fs.existsSync(DATA_FILE)) {
    const raw = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
    db = Object.assign(db, raw);
  }
} catch (e) { console.error('[db] load failed:', e.message); }

let saveTimer = null;
function save() {
  clearTimeout(saveTimer);
  saveTimer = setTimeout(() => {
    try { fs.writeFileSync(DATA_FILE, JSON.stringify(db)); }
    catch (e) { console.error('[db] save failed:', e.message); }
  }, 200);
}

// ---------- Sessions / presence ----------
const sessions = new Map();   // token -> userId
const online = new Map();     // userId -> Set<socketId>

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

app.get('/health', (_req, res) => res.json({ ok: true, time: Date.now() }));

app.get('/stats', (_req, res) => {
  let reactions = 0, files = 0;
  db.messages.forEach(m => {
    if (m.deleted) return;
    if (m.file) files++;
    if (m.reactions) Object.values(m.reactions).forEach(a => reactions += a.length);
  });
  res.json({
    totalUsers: Object.keys(db.users).length,
    online: online.size,
    totalMessages: db.messages.filter(m => !m.deleted).length,
    totalRooms: Object.keys(db.rooms).length,
    totalFileMsgs: files,
    totalReactions: reactions
  });
});

app.get('/vapid-public-key', (_req, res) => res.json({ key: VAPID_PUBLIC || '' }));

app.post('/subscribe', (req, res) => {
  const { userId, subscription } = req.body || {};
  if (!userId || !subscription) return res.status(400).json({ error: 'bad request' });
  db.subs[userId] = subscription;
  save();
  res.json({ ok: true });
});

app.post('/unsubscribe', (req, res) => {
  const { userId } = req.body || {};
  if (userId) { delete db.subs[userId]; save(); }
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

function canSeeMessage(m, userId) {
  if (m.to === 'public') return true;
  if (m.to === 'saved') return m.from === userId;
  if (typeof m.to === 'string' && m.to.indexOf('r_') === 0) {
    const r = db.rooms[m.to];
    return !!(r && r.members && r.members.indexOf(userId) >= 0);
  }
  return m.from === userId || m.to === userId;
}

function msgsFor(userId) {
  const hidden = db.hiddenChats[userId] || {};
  return db.messages.filter(m => !hidden[m.id] && canSeeMessage(m, userId));
}

function isBlockedAny(a, b) {
  // взаимная блокировка: если хоть один заблокировал другого — контакт невозможен
  const ua = db.users[a], ub = db.users[b];
  if (!ua || !ub) return false;
  const ba = Array.isArray(ua.blocked) ? ua.blocked : [];
  const bb = Array.isArray(ub.blocked) ? ub.blocked : [];
  return ba.indexOf(b) >= 0 || bb.indexOf(a) >= 0;
}

function usersForViewer(userId) {
  const ids = new Set();
  const me = db.users[userId];
  const myBlocked = (me && Array.isArray(me.blocked)) ? me.blocked : [];
  if (me && Array.isArray(me.contacts)) me.contacts.forEach(id => ids.add(id));

  db.messages.forEach(m => {
    if (m.from === userId) {
      if (m.to !== 'public' && m.to !== 'saved' && String(m.to).indexOf('r_') !== 0) ids.add(m.to);
    } else if (canSeeMessage(m, userId)) {
      ids.add(m.from);
    }
  });
  Object.values(db.rooms).forEach(r => {
    if (r.members && r.members.indexOf(userId) >= 0) r.members.forEach(id => ids.add(id));
  });
  ids.delete(userId);
  myBlocked.forEach(id => ids.delete(id));

  const out = [];
  ids.forEach(id => { if (db.users[id]) out.push(pubUser(db.users[id])); });
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
const roomsList = () => Object.values(db.rooms).map(roomView);

function emitToUser(userId, event, data) {
  const set = online.get(userId);
  if (!set) return;
  set.forEach(sid => io.to(sid).emit(event, data));
}

function broadcastRoomsToAll() {
  const list = roomsList();
  io.sockets.sockets.forEach(s => { if (s.data.userId) s.emit('rooms', list); });
}
function broadcastUsersToAll() {
  io.sockets.sockets.forEach(s => {
    if (s.data.userId) s.emit('users', usersForViewer(s.data.userId));
  });
}

function canWriteRoom(userId, room) {
  if (!room) return false;
  const role = (room.roles && room.roles[userId]) || (room.createdBy === userId ? 'owner' : 'member');
  if (room.type === 'channel') return role === 'owner' || role === 'admin';
  return true;
}
function inRoom(userId, roomId) {
  const r = db.rooms[roomId];
  return !!(r && r.members && r.members.indexOf(userId) >= 0);
}

function sendPush(userId, title, body) {
  if (!webpush || !db.subs[userId]) return;
  webpush.sendNotification(db.subs[userId], JSON.stringify({ title, body }))
    .catch(() => { delete db.subs[userId]; save(); });
}

// ---------- Socket.IO ----------
io.on('connection', (socket) => {
  socket.data.userId = null;

  function attachUser(userId, token, isNewAccount) {
    socket.data.userId = userId;
    if (!online.has(userId)) online.set(userId, new Set());
    online.get(userId).add(socket.id);
    const u = db.users[userId];
    socket.emit('joined', {
      user: Object.assign({}, u, { password: undefined }),
      userId, token, isNewAccount: !!isNewAccount
    });
    socket.emit('history', msgsFor(userId));
    socket.emit('rooms', roomsList());
    socket.emit('users', usersForViewer(userId));
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
      if (db.usernames[username]) return socket.emit('auth-error', { text: 'Username уже занят' });

      const userId = uid('u');
      const hash = await bcrypt.hash(password, 10);
      const user = {
        userId, name, username, password: hash,
        color: data.color || '#5288c1',
        initials: data.initials || name.slice(0, 2).toUpperCase(),
        avatar: null,
        contacts: [], mutedChats: [],
        settings: { theme: 'light', accent: '#3390ec', language: 'ru' },
        createdAt: now()
      };
      db.users[userId] = user;
      db.usernames[username] = userId;
      const tok = newToken();
      sessions.set(tok, userId);
      save();
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
      const userId = db.usernames[username];
      if (!userId) return socket.emit('auth-error', { text: 'Неверный логин или пароль' });
      const u = db.users[userId];
      const ok = await bcrypt.compare(password, u.password);
      if (!ok) return socket.emit('auth-error', { text: 'Неверный логин или пароль' });
      const tok = newToken();
      sessions.set(tok, userId);
      attachUser(userId, tok, false);
    } catch (e) {
      console.error('[login]', e);
      socket.emit('auth-error', { text: 'Ошибка входа' });
    }
  });

  socket.on('auto-login', (data = {}) => {
    const { userId, token } = data;
    if (!userId || !token || !db.users[userId] || sessions.get(token) !== userId) {
      return socket.emit('auth-error', { text: 'Сессия истекла' });
    }
    attachUser(userId, token, false);
  });

  socket.on('disconnect', () => {
    const userId = socket.data.userId;
    if (!userId) return;
    const set = online.get(userId);
    if (set) {
      set.delete(socket.id);
      if (set.size === 0) {
        online.delete(userId);
        const u = db.users[userId];
        if (u) io.emit('user-updated', { userId, user: pubUser(u) });
      }
    }
  });

  const auth = (fn) => (data) => {
    if (!socket.data.userId) return;
    try { fn(data || {}); } catch (e) { console.error('[handler]', e); }
  };

  socket.on('message', auth((data) => {
    const userId = socket.data.userId;
    const u = db.users[userId];
    const to = String(data.to || '');
    if (!to) return;
    if (to.indexOf('r_') === 0) {
      if (!inRoom(userId, to)) return;
      if (!canWriteRoom(userId, db.rooms[to])) return socket.emit('error-msg', { text: 'Только админы могут писать' });
    } else if (to !== 'public' && to !== 'saved') {
      // личный чат: проверка блокировки (в любую сторону)
      if (!db.users[to]) return;
      if (isBlockedAny(userId, to)) {
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
    db.messages.push(msg);
    save();

    Object.keys(db.users).forEach(rid => {
      if (!canSeeMessage(msg, rid)) return;
      // блокировка скрывает личные сообщения от обеих сторон (в т.ч. уже отправленные)
      if (msg.to !== 'public' && msg.to !== 'saved' && String(msg.to).indexOf('r_') !== 0) {
        const other = msg.from === rid ? msg.to : msg.from;
        if (isBlockedAny(rid, other)) return;
      }
      emitToUser(rid, 'message', msg);
      if (rid !== userId) {
        const me = db.users[rid];
        const key = msg.to === 'public' ? 'public'
          : msg.to === 'saved' ? 'saved'
          : msg.to.indexOf('r_') === 0 ? msg.to
          : (msg.from === rid ? msg.to : msg.from);
        if (!me.mutedChats || me.mutedChats.indexOf(key) < 0) {
          sendPush(rid, u.name, msg.text || 'Файл');
        }
      }
    });
  }));

  socket.on('delivered', auth((data) => {
    const userId = socket.data.userId;
    (data.messageIds || []).forEach(id => {
      const m = db.messages.find(x => x.id === id);
      if (!m) return;
      if (!m.deliveredTo) m.deliveredTo = [];
      if (m.from !== userId && m.deliveredTo.indexOf(userId) < 0) m.deliveredTo.push(userId);
      emitToUser(m.from, 'message-update', m);
    });
    save();
  }));

  socket.on('read', auth((data) => {
    const userId = socket.data.userId;
    (data.messageIds || []).forEach(id => {
      const m = db.messages.find(x => x.id === id);
      if (!m) return;
      if (!m.readBy) m.readBy = [];
      if (!m.deliveredTo) m.deliveredTo = [];
      if (m.from !== userId && m.readBy.indexOf(userId) < 0) m.readBy.push(userId);
      if (m.from !== userId && m.deliveredTo.indexOf(userId) < 0) m.deliveredTo.push(userId);
      emitToUser(m.from, 'message-update', m);
    });
    save();
  }));

  socket.on('typing', auth((data) => {
    const userId = socket.data.userId;
    const u = db.users[userId];
    const to = String(data.to || '');
    if (!to || to === 'saved') return;
    const payload = { to, from: userId, fromName: u.name };
    if (to === 'public') socket.broadcast.emit('typing', payload);
    else if (to.indexOf('r_') === 0) {
      const r = db.rooms[to];
      if (!r) return;
      r.members.forEach(mid => { if (mid !== userId) emitToUser(mid, 'typing', payload); });
    } else {
      if (isBlockedAny(userId, to)) return;
      emitToUser(to, 'typing', payload);
    }
  }));

  socket.on('react-message', auth((data) => {
    const userId = socket.data.userId;
    const m = db.messages.find(x => x.id === data.messageId);
    if (!m) return;
    const em = String(data.emoji || '').slice(0, 8);
    if (!em) return;
    if (!m.reactions) m.reactions = {};
    if (!m.reactions[em]) m.reactions[em] = [];
    const i = m.reactions[em].indexOf(userId);
    if (i >= 0) m.reactions[em].splice(i, 1); else m.reactions[em].push(userId);
    if (!m.reactions[em].length) delete m.reactions[em];
    save();
    Object.keys(db.users).forEach(rid => {
      if (canSeeMessage(m, rid)) emitToUser(rid, 'message-update', m);
    });
  }));

  socket.on('pin-message', auth((data) => {
    const userId = socket.data.userId;
    const m = db.messages.find(x => x.id === data.messageId);
    if (!m) return;
    if (typeof m.to === 'string' && m.to.indexOf('r_') === 0) {
      const r = db.rooms[m.to];
      if (!r) return;
      const role = (r.roles && r.roles[userId]) || (r.createdBy === userId ? 'owner' : 'member');
      if (role !== 'owner' && role !== 'admin') return;
    }
    m.pinned = !m.pinned;
    save();
    Object.keys(db.users).forEach(rid => {
      if (canSeeMessage(m, rid)) emitToUser(rid, 'message-update', m);
    });
  }));

  socket.on('edit-message', auth((data) => {
    const userId = socket.data.userId;
    const m = db.messages.find(x => x.id === data.messageId);
    if (!m || m.from !== userId) return;
    if (now() - m.timestamp > 24 * 60 * 60 * 1000) return socket.emit('error-msg', { text: 'Слишком поздно редактировать' });
    const txt = String(data.newText || '').trim().slice(0, 4000);
    if (!txt) return;
    m.text = txt; m.edited = true;
    save();
    Object.keys(db.users).forEach(rid => {
      if (canSeeMessage(m, rid)) emitToUser(rid, 'message-update', m);
    });
  }));

  socket.on('delete-message', auth((data) => {
    const userId = socket.data.userId;
    const m = db.messages.find(x => x.id === data.messageId);
    if (!m || m.from !== userId) return;
    m.deleted = true; m.text = ''; m.file = null; m.reactions = {}; m.pinned = false;
    save();
    Object.keys(db.users).forEach(rid => {
      if (canSeeMessage(m, rid)) emitToUser(rid, 'message-update', m);
    });
  }));

  socket.on('create-room', auth((data) => {
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
      data.inviteUsernames.forEach(un => {
        const clean = String(un || '').replace(/^@/, '').trim().toLowerCase();
        const iid = db.usernames[clean];
        if (iid && iid !== userId && room.members.indexOf(iid) < 0) {
          room.members.push(iid);
          room.roles[iid] = 'member';
          emitToUser(iid, 'invited', { names: [name] });
        }
      });
    }
    db.rooms[rid] = room;
    save();
    socket.emit('room-created', roomView(room));
    broadcastRoomsToAll();
    broadcastUsersToAll();
  }));

  socket.on('invite-to-room', auth((data) => {
    const userId = socket.data.userId;
    const r = db.rooms[data.roomId];
    if (!r) return;
    const myRole = r.roles[userId] || (r.createdBy === userId ? 'owner' : 'member');
    if (myRole !== 'owner' && myRole !== 'admin') return;
    const added = [];
    (data.usernames || []).forEach(un => {
      const clean = String(un || '').replace(/^@/, '').trim().toLowerCase();
      const iid = db.usernames[clean];
      if (iid && r.members.indexOf(iid) < 0) {
        r.members.push(iid);
        r.roles[iid] = 'member';
        added.push(db.users[iid].name);
        emitToUser(iid, 'invited', { names: [r.name] });
      }
    });
    save();
    socket.emit('toast-msg', { text: added.length ? 'Приглашены: ' + added.join(', ') : 'Никого не добавили' });
    broadcastRoomsToAll();
    broadcastUsersToAll();
  }));

  socket.on('join-room', auth((data) => {
    const userId = socket.data.userId;
    const r = db.rooms[data.roomId];
    if (!r) return;
    if (r.type === 'channel') return;
    if (r.members.indexOf(userId) < 0) {
      r.members.push(userId);
      r.roles[userId] = 'member';
      save();
      broadcastRoomsToAll();
      broadcastUsersToAll();
    }
  }));

  socket.on('leave-room', auth((data) => {
    const userId = socket.data.userId;
    const r = db.rooms[data.roomId];
    if (!r) return;
    const i = r.members.indexOf(userId);
    if (i >= 0) r.members.splice(i, 1);
    delete r.roles[userId];
    if (!r.members.length) delete db.rooms[r.id];
    else if (r.createdBy === userId) {
      r.createdBy = r.members[0];
      r.roles[r.createdBy] = 'owner';
    }
    save();
    broadcastRoomsToAll();
    broadcastUsersToAll();
  }));

  socket.on('kick-user', auth((data) => {
    const userId = socket.data.userId;
    const r = db.rooms[data.roomId];
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
    save();
    broadcastRoomsToAll();
    broadcastUsersToAll();
  }));

  socket.on('set-role', auth((data) => {
    const userId = socket.data.userId;
    const r = db.rooms[data.roomId];
    if (!r) return;
    if (r.roles[userId] !== 'owner' && r.createdBy !== userId) return;
    const target = data.targetUserId;
    if (!target || target === userId) return;
    if (r.members.indexOf(target) < 0) return;
    r.roles[target] = data.role === 'admin' ? 'admin' : 'member';
    save();
    broadcastRoomsToAll();
  }));

  socket.on('add-contact', auth((data) => {
    const userId = socket.data.userId;
    const cid = data.contactId;
    if (!db.users[cid] || cid === userId) return;
    const u = db.users[userId];
    if (!u.contacts) u.contacts = [];
    if (u.contacts.indexOf(cid) < 0) u.contacts.push(cid);
    save();
    socket.emit('contact-added', { userId: cid });
    socket.emit('users', usersForViewer(userId));
  }));

  socket.on('remove-contact', auth((data) => {
    const userId = socket.data.userId;
    const cid = data.contactId;
    const u = db.users[userId];
    if (!u.contacts) u.contacts = [];
    const i = u.contacts.indexOf(cid);
    if (i >= 0) u.contacts.splice(i, 1);
    save();
    socket.emit('contact-removed', { userId: cid });
    socket.emit('users', usersForViewer(userId));
  }));

  socket.on('search-user', auth((data) => {
    const userId = socket.data.userId;
    const meU = db.users[userId];
    const myBlocked = (meU && Array.isArray(meU.blocked)) ? meU.blocked : [];
    const q = String(data.query || '').replace(/^@/, '').toLowerCase();
    if (!q) return socket.emit('search-result', { results: [] });
    const out = [];
    Object.values(db.users).forEach(u => {
      if (u.userId === userId) return;
      if (myBlocked.indexOf(u.userId) >= 0) return;               // я заблокировал — не ищу
      const other = db.users[u.userId];
      if (Array.isArray(other.blocked) && other.blocked.indexOf(userId) >= 0) return; // он заблокировал меня — невидим
      if (u.username.indexOf(q) >= 0 || u.name.toLowerCase().indexOf(q) >= 0) out.push(pubUser(u));
    });
    out.sort((a, b) => a.username.indexOf(q) - b.username.indexOf(q));
    socket.emit('search-result', { results: out.slice(0, 30) });
  }));

  socket.on('search-messages', auth((data) => {
    const userId = socket.data.userId;
    const q = String(data.query || '').toLowerCase().trim();
    if (q.length < 2) return socket.emit('search-messages-result', { query: q, results: [] });
    const results = db.messages
      .filter(m => !m.deleted && canSeeMessage(m, userId) && (m.text || '').toLowerCase().indexOf(q) >= 0)
      .slice(-100);
    socket.emit('search-messages-result', { query: q, results });
  }));

  socket.on('create-invite', auth((data) => {
    const userId = socket.data.userId;
    const r = db.rooms[data.roomId];
    if (!r) return;
    const myRole = r.roles[userId] || (r.createdBy === userId ? 'owner' : 'member');
    if (myRole !== 'owner' && myRole !== 'admin') return;
    const code = crypto.randomBytes(6).toString('hex');
    db.invites[code] = { roomId: r.id, createdBy: userId, createdAt: now() };
    save();
    socket.emit('invite-created', { link: '/join/' + code });
  }));

  socket.on('redeem-invite', auth((data) => {
    const userId = socket.data.userId;
    const inv = db.invites[String(data.code || '')];
    if (!inv) return socket.emit('invite-error', { text: 'Ссылка недействительна' });
    const r = db.rooms[inv.roomId];
    if (!r) return socket.emit('invite-error', { text: 'Комната не найдена' });
    const already = r.members.indexOf(userId) >= 0;
    if (!already) {
      r.members.push(userId);
      r.roles[userId] = 'member';
      save();
      broadcastRoomsToAll();
      broadcastUsersToAll();
    }
    socket.emit('invite-redeemed', { roomId: r.id, alreadyMember: already });
  }));

  socket.on('mute-chat', auth((data) => {
    const userId = socket.data.userId;
    const u = db.users[userId];
    if (!u.mutedChats) u.mutedChats = [];
    const cid = String(data.chatId || '');
    if (!cid) return;
    const i = u.mutedChats.indexOf(cid);
    if (data.muted && i < 0) u.mutedChats.push(cid);
    else if (!data.muted && i >= 0) u.mutedChats.splice(i, 1);
    save();
    socket.emit('mutes-updated', { mutedChats: u.mutedChats });
  }));

  // ---------- Блокировка пользователя ----------
  socket.on('block-user', auth((data) => {
    const userId = socket.data.userId;
    const target = String(data.targetUserId || '');
    if (!target || target === userId || !db.users[target]) return;
    const u = db.users[userId];
    if (!Array.isArray(u.blocked)) u.blocked = [];
    if (u.blocked.indexOf(target) < 0) u.blocked.push(target);
    // убрать из контактов в обе стороны
    if (Array.isArray(u.contacts)) {
      const ci = u.contacts.indexOf(target);
      if (ci >= 0) u.contacts.splice(ci, 1);
    }
    const t2 = db.users[target];
    if (Array.isArray(t2.contacts)) {
      const ti = t2.contacts.indexOf(userId);
      if (ti >= 0) t2.contacts.splice(ti, 1);
    }
    save();
    socket.emit('blocked-updated', { blocked: u.blocked.slice() });
    socket.emit('users', usersForViewer(userId));
    socket.emit('history', msgsFor(userId));
    broadcastUsersToAll();
    emitToUser(target, 'users', usersForViewer(target));
    socket.emit('toast-msg', { text: 'Пользователь заблокирован' });
  }));

  socket.on('unblock-user', auth((data) => {
    const userId = socket.data.userId;
    const target = String(data.targetUserId || '');
    const u = db.users[userId];
    if (!Array.isArray(u.blocked)) u.blocked = [];
    const i = u.blocked.indexOf(target);
    if (i >= 0) u.blocked.splice(i, 1);
    save();
    socket.emit('blocked-updated', { blocked: u.blocked.slice() });
    socket.emit('users', usersForViewer(userId));
    socket.emit('history', msgsFor(userId));
    broadcastUsersToAll();
    socket.emit('toast-msg', { text: 'Пользователь разблокирован' });
  }));

  // ---------- Удаление чата (только у себя) ----------
  socket.on('delete-chat', auth((data) => {
    const userId = socket.data.userId;
    const chatId = String(data.chatId || '');
    if (!chatId) return;
    if (!db.hiddenChats[userId]) db.hiddenChats[userId] = {};
    let count = 0;
    db.messages.forEach(m => {
      if (!canSeeMessage(m, userId)) return;
      let key;
      if (m.to === 'public') key = 'public';
      else if (m.to === 'saved') key = 'saved';
      else if (String(m.to).indexOf('r_') === 0) key = m.to;
      else key = (m.from === userId ? m.to : m.from);
      if (key === chatId) { db.hiddenChats[userId][m.id] = true; count++; }
    });
    // если это комната — выйти из неё (чат исчезнет из списка полностью)
    if (chatId.indexOf('r_') === 0) {
      const r = db.rooms[chatId];
      if (r) {
        const mi = r.members.indexOf(userId);
        if (mi >= 0) {
          r.members.splice(mi, 1);
          delete r.roles[userId];
          if (!r.members.length) delete db.rooms[r.id];
          else if (r.createdBy === userId) { r.createdBy = r.members[0]; r.roles[r.createdBy] = 'owner'; }
          broadcastRoomsToAll();
          broadcastUsersToAll();
        }
      }
    }
    save();
    socket.emit('chat-deleted', { chatId });
    socket.emit('history', msgsFor(userId));
    socket.emit('toast-msg', { text: 'Чат удалён' });
  }));

  socket.on('update-settings', auth((data) => {
    const userId = socket.data.userId;
    const u = db.users[userId];
    if (data.settings && typeof data.settings === 'object') {
      u.settings = Object.assign({}, u.settings, data.settings);
      save();
    }
  }));

  socket.on('update-profile', auth((data) => {
    const userId = socket.data.userId;
    const u = db.users[userId];
    if (typeof data.name === 'string' && data.name.trim()) {
      u.name = data.name.trim().slice(0, 30);
      u.initials = u.name.split(/\s+/).map(w => w[0]).join('').toUpperCase().slice(0, 2)
        || u.name.slice(0, 2).toUpperCase();
    }
    if (data.color) u.color = data.color;
    if (data.initials) u.initials = data.initials;
    if (data.avatar === null || typeof data.avatar === 'string') u.avatar = data.avatar;
    save();
    io.emit('user-updated', { userId, user: pubUser(u) });
  }));

  socket.on('change-password', async (data = {}) => {
    const userId = socket.data.userId;
    if (!userId) return;
    const u = db.users[userId];
    const ok = await bcrypt.compare(String(data.oldPassword || ''), u.password);
    if (!ok) return socket.emit('toast-msg', { text: 'Неверный старый пароль' });
    const np = String(data.newPassword || '');
    if (np.length < 4) return socket.emit('toast-msg', { text: 'Минимум 4 символа' });
    u.password = await bcrypt.hash(np, 10);
    save();
    socket.emit('toast-msg', { text: 'Пароль изменён' });
  });
});

server.listen(PORT, () => console.log('Pulse listening on ' + PORT));
