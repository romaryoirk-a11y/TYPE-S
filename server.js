const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const multer = require('multer');
const webpush = require('web-push');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { maxHttpBufferSize: 30 * 1024 * 1024 });

// --- Папки (создаются автоматически) ---
const UPLOADS_DIR = path.join(__dirname, 'public', 'uploads');
const DATA_DIR = path.join(__dirname, 'data');
const MESSAGES_FILE = path.join(DATA_DIR, 'messages.json');
const USERS_FILE = path.join(DATA_DIR, 'users.json');
const ROOMS_FILE = path.join(DATA_DIR, 'rooms.json');
const SUBS_FILE = path.join(DATA_DIR, 'subscriptions.json');
const VAPID_FILE = path.join(DATA_DIR, 'vapid.json');
[UPLOADS_DIR, DATA_DIR].forEach(d => fs.mkdirSync(d, { recursive: true }));

// --- VAPID ключи для Web Push ---
let vapidKeys;
if (fs.existsSync(VAPID_FILE)) {
  vapidKeys = JSON.parse(fs.readFileSync(VAPID_FILE, 'utf8'));
} else {
  vapidKeys = webpush.generateVAPIDKeys();
  fs.writeFileSync(VAPID_FILE, JSON.stringify(vapidKeys, null, 2));
  console.log('🔑 Сгенерированы новые VAPID-ключи');
}
webpush.setVapidDetails('mailto:admin@example.com', vapidKeys.publicKey, vapidKeys.privateKey);

app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json({ limit: '1mb' }));

app.get('/vapid-public-key', (req, res) => res.json({ key: vapidKeys.publicKey }));

// --- Загрузка файлов ---
const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, UPLOADS_DIR),
  filename: (req, file, cb) => {
    const ext = path.extname(file.originalname) || '';
    cb(null, crypto.randomBytes(10).toString('hex') + ext);
  }
});
const upload = multer({ storage, limits: { fileSize: 25 * 1024 * 1024 } });

app.post('/upload', upload.single('file'), (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'Файл не получен' });
  res.json({
    url: '/uploads/' + req.file.filename,
    name: req.file.originalname,
    type: req.file.mimetype,
    size: req.file.size
  });
});

// --- Push-подписки ---
let subscriptions = {};
try {
  if (fs.existsSync(SUBS_FILE)) subscriptions = JSON.parse(fs.readFileSync(SUBS_FILE, 'utf8'));
} catch (e) { console.error(e); }
const saveSubs = () => fs.writeFile(SUBS_FILE, JSON.stringify(subscriptions), () => {});

app.post('/subscribe', (req, res) => {
  const { userId, subscription } = req.body || {};
  if (!userId || !subscription || !subscription.endpoint) return res.status(400).json({ error: 'bad' });
  if (!subscriptions[userId]) subscriptions[userId] = [];
  if (!subscriptions[userId].some(s => s.endpoint === subscription.endpoint)) {
    subscriptions[userId].push(subscription);
    saveSubs();
  }
  res.json({ ok: true });
});

app.post('/unsubscribe', (req, res) => {
  const { userId, endpoint } = req.body || {};
  if (subscriptions[userId]) {
    subscriptions[userId] = subscriptions[userId].filter(s => s.endpoint !== endpoint);
    saveSubs();
  }
  res.json({ ok: true });
});

// --- Данные ---
let messages = [];
let knownUsers = {};
let rooms = {};
try {
  if (fs.existsSync(MESSAGES_FILE)) messages = JSON.parse(fs.readFileSync(MESSAGES_FILE, 'utf8'));
  if (fs.existsSync(USERS_FILE)) knownUsers = JSON.parse(fs.readFileSync(USERS_FILE, 'utf8'));
  if (fs.existsSync(ROOMS_FILE)) rooms = JSON.parse(fs.readFileSync(ROOMS_FILE, 'utf8'));
} catch (e) { console.error(e); }

const saveMessages = () => fs.writeFile(MESSAGES_FILE, JSON.stringify(messages), () => {});
const saveUsers = () => fs.writeFile(USERS_FILE, JSON.stringify(knownUsers), () => {});
const saveRooms = () => fs.writeFile(ROOMS_FILE, JSON.stringify(rooms), () => {});

const online = new Map();

// --- Утилиты ---
function usersList() {
  return Object.entries(knownUsers).map(([id, u]) => ({
    userId: id, name: u.name, color: u.color, initials: u.initials,
    avatar: u.avatar || null,
    online: online.has(id)
  }));
}

function messageRecipients(m) {
  if (m.to === 'public') return Object.keys(knownUsers);
  if (m.to.startsWith('r_')) return rooms[m.to]?.members || [];
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

function broadcastRooms() { io.emit('rooms', Object.values(rooms)); }

// --- Socket.IO ---
io.on('connection', socket => {
  socket.on('join', ({ userId, name, color, initials }) => {
    if (!userId || !name) return;
    socket.userId = userId;
    socket.join('user_' + userId);

    if (!online.has(userId)) online.set(userId, new Set());
    online.get(userId).add(socket.id);

    knownUsers[userId] = { ...knownUsers[userId], name, color, initials };
    saveUsers();

    socket.emit('history', messages);
    socket.emit('rooms', Object.values(rooms));
    io.emit('users', usersList());
    console.log(`[+] ${name} (${userId}) подключён`);
  });

  socket.on('update-profile', ({ userId, name, color, initials, avatar }) => {
    if (!userId || !knownUsers[userId]) return;
    knownUsers[userId] = {
      ...knownUsers[userId],
      ...(name && { name }),
      ...(color && { color }),
      ...(initials && { initials }),
      ...(avatar !== undefined && { avatar })
    };
    saveUsers();
    io.emit('users', usersList());
    io.emit('user-updated', { userId, user: knownUsers[userId] });
  });

  socket.on('message', msg => {
    if (!msg || !msg.id || !msg.from || !msg.to) return;
    if (messages.some(m => m.id === msg.id)) return;

    const isPublic = msg.to === 'public';
    const isRoom = msg.to.startsWith('r_');
    const room = isRoom ? rooms[msg.to] : null;

    if (isRoom && !room) return;

    if (room && room.type === 'channel' && !room.admins.includes(msg.from)) {
      socket.emit('error-msg', { text: 'Только администраторы могут публиковать в канал' });
      return;
    }

    let recipients;
    if (isPublic) recipients = Object.keys(knownUsers);
    else if (isRoom) {
      if (!room.members.includes(msg.from)) return;
      recipients = room.members.slice();
    } else {
      recipients = [msg.to, msg.from];
    }

    const ts = msg.timestamp || Date.now();
    const full = {
      id: msg.id, to: msg.to, from: msg.from, fromName: msg.fromName,
      text: String(msg.text || '').slice(0, 4000),
      file: msg.file || null,
      timestamp: ts,
      time: new Date(ts).toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' }),
      deliveredTo: [], readBy: []
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
  });

  socket.on('delivered', ({ userId, messageIds }) => {
    if (!userId || !Array.isArray(messageIds)) return;
    const updated = [];
    messageIds.forEach(id => {
      const m = messages.find(x => x.id === id);
      if (m && !m.deliveredTo.includes(userId)) {
        m.deliveredTo.push(userId);
        updated.push(m);
      }
    });
    if (updated.length) {
      saveMessages();
      updated.forEach(m => emitToUsers(messageRecipients(m), 'message-update', m));
    }
  });

  socket.on('read', ({ userId, messageIds }) => {
    if (!userId || !Array.isArray(messageIds)) return;
    const updated = [];
    messageIds.forEach(id => {
      const m = messages.find(x => x.id === id);
      if (m && !m.readBy.includes(userId)) {
        m.readBy.push(userId);
        if (!m.deliveredTo.includes(userId)) m.deliveredTo.push(userId);
        updated.push(m);
      }
    });
    if (updated.length) {
      saveMessages();
      updated.forEach(m => emitToUsers(messageRecipients(m), 'message-update', m));
    }
  });

  socket.on('typing', ({ to, from, fromName }) => {
    if (!to || !from) return;
    let recipients;
    if (to === 'public') recipients = Object.keys(knownUsers).filter(u => u !== from);
    else if (to.startsWith('r_')) recipients = (rooms[to]?.members || []).filter(u => u !== from);
    else recipients = [to];
    emitToUsers(recipients, 'typing', { to, from, fromName });
  });

  socket.on('create-room', ({ name, type, description, creator }) => {
    if (!name || !type || !creator) return;
    if (type !== 'group' && type !== 'channel') return;

    const id = 'r_' + crypto.randomBytes(6).toString('hex');
    const COLORS = [
      ['#ff6b6b','#ee5a6f'], ['#4facfe','#00f2fe'], ['#43e97b','#38f9d7'],
      ['#fa709a','#fee140'], ['#a18cd1','#fbc2eb'], ['#f093fb','#f5576c'],
      ['#5ee7df','#b490ca'], ['#f6d365','#fda085'], ['#667eea','#764ba2']
    ];
    const c = COLORS[Math.floor(Math.random() * COLORS.length)];
    const allUsers = Array.from(new Set([creator, ...Object.keys(knownUsers)]));

    rooms[id] = {
      id, type,
      name: name.slice(0, 60),
      description: (description || '').slice(0, 200),
      color: `linear-gradient(135deg,${c[0]},${c[1]})`,
      createdBy: creator,
      createdAt: Date.now(),
      members: allUsers,
      admins: [creator]
    };
    saveRooms();
    broadcastRooms();
    socket.emit('room-created', rooms[id]);
  });

  socket.on('join-room', ({ roomId, userId }) => {
    const r = rooms[roomId];
    if (!r || r.members.includes(userId)) return;
    r.members.push(userId);
    saveRooms();
    broadcastRooms();
  });

  socket.on('leave-room', ({ roomId, userId }) => {
    const r = rooms[roomId];
    if (!r) return;
    r.members = r.members.filter(x => x !== userId);
    r.admins = r.admins.filter(x => x !== userId);
    if (r.members.length === 0) delete rooms[roomId];
    saveRooms();
    broadcastRooms();
  });

  socket.on('disconnect', () => {
    if (socket.userId && online.has(socket.userId)) {
      online.get(socket.userId).delete(socket.id);
      if (online.get(socket.userId).size === 0) online.delete(socket.userId);
      io.emit('users', usersList());
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`\n  🚀 Мессенджер запущен: http://localhost:${PORT}\n`);
});
