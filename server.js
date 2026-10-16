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
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const { RateLimiterMemory } = require('rate-limiter-flexible');
const FileType = require('file-type');
const { Server } = require('socket.io');
const { MongoClient } = require('mongodb');

const PORT = process.env.PORT || 3000;
const DATA_DIR = path.join(__dirname, 'data');
const UPLOAD_DIR = path.join(DATA_DIR, 'uploads');
if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, { recursive: true });

// ---------- Безопасность: секреты и сессии ----------
// SESSION_SECRET используется для HMAC-подписи сессионных токенов.
// Если переменная окружения не задана — генерируем временный секрет (токены
// станут недействительны после перезапуска процесса, но сервер не упадёт).
let SESSION_SECRET = process.env.SESSION_SECRET || '';
// Защита от «пустого» значения: если переменная задана, но пуста/пробелы (частая
// ситуация на Render), считаем её не заданной и генерируем временный секрет.
// Иначе HMAC по пустому ключу искажал бы хеши токенов и приводил к мусору в БД.
if (!SESSION_SECRET.trim()) {
  SESSION_SECRET = crypto.randomBytes(32).toString('hex');
  console.warn('[warn] SESSION_SECRET не задан — сгенерирован временный. ' +
    'Все пользователи будут разлогинены при каждом рестарте сервиса. ' +
    'Задайте SESSION_SECRET в переменных окружения (на Render: Environment): ' +
    'node -e "console.log(require(\'crypto\').randomBytes(32).toString(\'hex\'))"');
}
const SESSION_TTL_MS = 30 * 24 * 60 * 60 * 1000; // 30 дней
const TOKEN_GRACE_MS = 60 * 1000;                // «продление» токена при активности (окно скользящего TTL)

const VAPID_PUBLIC = process.env.VAPID_PUBLIC_KEY || '';
const VAPID_PRIVATE = process.env.VAPID_PRIVATE_KEY || '';
let webpush = null;
if (VAPID_PUBLIC && VAPID_PRIVATE) {
  try {
    webpush = require('web-push');
    webpush.setVapidDetails('mailto:admin@example.com', VAPID_PUBLIC, VAPID_PRIVATE);
  } catch (e) { console.warn('[push] disabled:', e.message); }
}

// CORS: список разрешённых источников задаётся явно (по умолчанию — только same-origin)
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || '')
  .split(',').map(s => s.trim()).filter(Boolean);

// ---------- MongoDB ----------
// MONGODB_URI — внешняя база (MongoDB Atlas / локальный mongod).
// Если не задана — автоматически поднимается встроенная in-memory MongoDB
// (mongodb-memory-server), чтобы проект работал «из коробки» без установки Mongo.
// ВАЖНО: in-memory база подходит только для разработки — данные исчезают при рестарте!
const MONGODB_URI = process.env.MONGODB_URI || '';
let client = null;
let col = {}; // users, usernames, messages, rooms, invites, subs, hiddenChats, encKeys, sessions, files

async function startMongo() {
  let uri = MONGODB_URI;
  if (!uri) {
    if (process.env.NODE_ENV === 'production') {
      throw new Error('MONGODB_URI не задан: в продакшене in-memory БД запрещена (данные теряются при рестарте)');
    }
    const { MongoMemoryServer } = require('mongodb-memory-server');
    const mem = await MongoMemoryServer.create({ instance: { dbName: 'pulse' } });
    uri = mem.getUri();
    console.log('[mongo] запущена встроенная in-memory MongoDB (ТОЛЬКО разработка!):', uri);
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
    sessions: dbo.collection('sessions'),
    files: dbo.collection('files')
  };
  // Санация коллекций от мусора прошлых версий приложения (до создания индексов):
  // иначе уникальные/схлопнутые индексы не создаются из-за E11000 и деплой падает.
  await sanitizeLegacyData();

  await Promise.all([
    col.users.createIndex({ userId: 1 }, { unique: true }),
    col.usernames.createIndex({ username: 1 }, { unique: true }),
    col.messages.createIndex({ id: 1 }, { unique: true }),
    col.messages.createIndex({ to: 1, timestamp: -1 }),
    col.messages.createIndex({ from: 1, to: 1, timestamp: -1 }),
    col.rooms.createIndex({ id: 1 }, { unique: true }),
    col.rooms.createIndex({ members: 1 }),
    col.invites.createIndex({ code: 1 }, { unique: true }),
    col.subs.createIndex({ userId: 1 }, { unique: true }),
    col.hiddenChats.createIndex({ userId: 1 }, { unique: true }),
    col.encKeys.createIndex({ chatId: 1 }, { unique: true }),
    col.files.createIndex({ fileId: 1 }, { unique: true })
  ]);
  // Индексы сессий — отдельно и терпимо к «уже существует» (см. ensureSessionIndexes)
  await ensureSessionIndexes();
  console.log('[mongo] база готова:', dbo.databaseName);
}

// Удаляет документы с отсутствующими/пустыми ключевыми полями и дубликаты по
// уникальным полям (оставляя самый свежий). Также удаляет устаревшие индексы,
// мешающие созданию новых (например, старый TTL-индекс sessions с другим именем).
async function sanitizeLegacyData() {
  const bad = { $exists: false };
  const emptyStr = { $in: ['', null] };
  try {
    // Сессии: токены без хеша / без пользователя / без срока жизни — мусор
    // (в т.ч. исторические записи с tokenHash: null, из-за которых падал build индекса).
    await col.sessions.deleteMany({
      $or: [{ tokenHash: bad }, { tokenHash: emptyStr }, { userId: bad }, { userId: emptyStr }, { expiresAt: bad }]
    });
    // Дубликаты tokenHash (могли появиться от старых версий кода) — оставляем самый свежий.
    const sessDups = await col.sessions.aggregate([
      { $group: { _id: '$tokenHash', ids: { $push: '$_id' }, n: { $sum: 1 } } },
      { $match: { n: { $gt: 1 } } }
    ]).toArray();
    for (const d of sessDups) {
      const keep = d.ids[d.ids.length - 1];
      await col.sessions.deleteMany({ _id: { $in: d.ids.filter(x => String(x) !== String(keep)) } });
    }
    if (sessDups.length) console.warn(`[mongo] sanitize: удалены дубликаты sessions по tokenHash: ${sessDups.length}`);
    await dropStaleIndexes(col.sessions, 'token_1', 'expiresAt_1_ttl');

    // Юзернеймы: пустые строки ломают уникальный индекс
    await col.usernames.deleteMany({ username: { $in: ['', null] } });

    // Сообщения: без id или без автора
    await col.messages.deleteMany({ $or: [{ id: bad }, { id: emptyStr }, { from: bad }] });

    // Комнаты и приглашения: без идентификаторов
    await col.rooms.deleteMany({ $or: [{ id: bad }, { id: emptyStr }] });
    await col.invites.deleteMany({ $or: [{ code: bad }, { code: emptyStr }] });

    // Файлы: без id
    await col.files.deleteMany({ $or: [{ fileId: bad }, { fileId: emptyStr }] });

    // Удаление дубликатов по уникальным полям (оставляем последний по _id)
    for (const [name, key] of [['usernames', 'username'], ['rooms', 'id'], ['invites', 'code'], ['files', 'fileId']]) {
      const c = col[name];
      const dups = await c.aggregate([
        { $group: { _id: '$' + key, ids: { $push: '$_id' }, n: { $sum: 1 } } },
        { $match: { n: { $gt: 1 } } }
      ]).toArray();
      for (const d of dups) {
        const keep = d.ids[d.ids.length - 1];
        await c.deleteMany({ _id: { $in: d.ids.filter(x => String(x) !== String(keep)) } });
      }
      if (dups.length) console.warn(`[mongo] sanitize: удалены дубликаты ${name} по ${key}: ${dups.length}`);
    }
  } catch (e) {
    console.warn('[mongo] sanitize предупреждение (не критично):', e.message || e);
  }
}

async function dropStaleIndexes(c, ...names) {
  try {
    const list = await c.indexes();
    for (const idx of list) {
      if (names.includes(idx.name)) {
        await c.dropIndex(idx.name);
        console.warn('[mongo] удалён устаревший индекс:', idx.name);
      }
    }
  } catch (e) { /* коллекция может ещё не существовать */ }
}

// ---------- Sessions / presence ----------
const online = new Map();     // userId -> Set<socketId>  (в реальном времени, не хранится в БД)

function hashToken(token) {
  return crypto.createHmac('sha256', SESSION_SECRET || 'pulse-dev-only-secret').update(String(token)).digest('hex');
}
async function sessionGet(token) {
  if (!token || typeof token !== 'string' || token.length > 200) return null;
  const tokenHash = hashToken(token);
  const s = await col.sessions.findOne({ tokenHash });
  if (!s) return null;
  const nowMs = Date.now();
  if (s.expiresAt.getTime() < nowMs) {
    await col.sessions.deleteOne({ tokenHash });
    return null;
  }
  // скользящий TTL: активные сессии продлеваются, неактивные истекают
  if (s.expiresAt.getTime() - nowMs < SESSION_TTL_MS - TOKEN_GRACE_MS) {
    await col.sessions.updateOne({ tokenHash }, { $set: { expiresAt: new Date(nowMs + SESSION_TTL_MS) } }).catch(() => {});
  }
  return s.userId;
}
async function sessionSet(token, userId) {
  // Защита: не пишем в БД «пустые» сессии (tokenHash должен быть валидным хешем).
  if (!token || typeof token !== 'string' || !userId) return;
  const tokenHash = hashToken(token);
  await col.sessions.updateOne({ tokenHash }, {
    $set: { tokenHash, userId, createdAt: new Date(), expiresAt: new Date(Date.now() + SESSION_TTL_MS) }
  }, { upsert: true });
}

// Уникальный индекс на tokenHash может уже существовать в базе со старыми данными.
// createIndex вернёт IndexExistsException (85/92), если параметры совпадают, и
// настоящую ошибку при конфликте — поэтому вызываем его изолированно и терпимо.
async function ensureSessionIndexes() {
  try {
    await col.sessions.createIndex({ tokenHash: 1 }, { unique: true });
  } catch (e) {
    if (e.code !== 85 && e.codeName !== 'IndexExists') throw e;
  }
  try {
    await col.sessions.createIndex({ expiresAt: 1 }, { expireAfterSeconds: 0 });
  } catch (e) {
    if (e.code !== 85 && e.codeName !== 'IndexExists') throw e;
  }
}

// ---------- Rate limiting ----------
// HTTP: защита от брутфорса / DoS на публичные эндпоинты
const globalLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 300,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: 'too many requests' }
});
const uploadLimiter = rateLimit({ windowMs: 60 * 1000, max: 20, standardHeaders: true, legacyHeaders: false, message: { error: 'too many uploads' } });

// Socket.IO: лимиты по событиям на пользователя (in-memory, достаточно для одного инстанса)
function makeLimiter(points, durationSec) {
  const rl = new RateLimiterMemory({ points, duration: durationSec });
  return async (key) => {
    try { await rl.consume(key, 1); return true; }
    catch (e) { return false; }
  };
}
const rlMessage = makeLimiter(25, 10);   // 25 сообщений / 10 сек
const rlAuth = makeLimiter(8, 60);       // 8 попыток входа / мин
const rlTyping = makeLimiter(15, 5);     // индикатор набора — часто, но не бесконечно
const rlSearch = makeLimiter(10, 10);    // поиск пользователей / сообщений
const rlRoomOps = makeLimiter(20, 30);   // операции с комнатами/контактами/настройками
const rlEncOps = makeLimiter(20, 30);    // E2E-ключи

// ---------- Express ----------
const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  maxHttpBufferSize: 12 * 1024 * 1024,
  cors: ALLOWED_ORIGINS.length ? { origin: ALLOWED_ORIGINS, credentials: true } : false
});

app.disable('x-powered-by');
app.use(helmet({
  contentSecurityPolicy: {
    useDefaults: true,
    directives: {
      'default-src': ["'self'"],
      'script-src': ["'self'", "'unsafe-inline'", 'https://cdn.socket.io'],
      'style-src': ["'self'", "'unsafe-inline'", 'https://fonts.googleapis.com'],
      'font-src': ["'self'", 'https://fonts.gstatic.com'],
      'img-src': ["'self'", 'data:', 'blob:', '/f/'],
      'media-src': ["'self'", 'data:', 'blob:', '/f/'],
      'connect-src': ["'self'", 'wss:', 'ws:', 'https://cdn.socket.io'],
      'worker-src': ["'self'", 'blob:'],
      'object-src': ["'none'"],
      'base-uri': ["'self'"],
      'form-action': ["'self'"]
    }
  },
  crossOriginEmbedderPolicy: false,
  referrerPolicy: { policy: 'no-referrer' }
}));
app.use(globalLimiter);
app.use(express.json({ limit: '1mb' }));
app.use(express.static(path.join(__dirname, 'public')));

// ---------- Загрузка файлов (disk storage + whitelist MIME + авторизация) ----------
const ALLOWED_MIME = new Set([
  // изображения
  'image/png', 'image/jpeg', 'image/gif', 'image/webp', 'image/bmp', 'image/svg+xml', 'image/avif',
  // видео
  'video/mp4', 'video/webm', 'video/quicktime', 'video/x-matroska',
  // аудио
  'audio/mpeg', 'audio/ogg', 'audio/wav', 'audio/webm', 'audio/mp4', 'audio/aac', 'audio/x-m4a', 'audio/flac',
  // документы
  'application/pdf', 'text/plain', 'application/zip',
  'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
  'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
  'application/vnd.openxmlformats-officedocument.presentationml.presentation',
  'application/msword', 'application/vnd.ms-excel', 'application/vnd.ms-powerpoint'
]);
// блокируем потенциально исполняемые типы даже если они проходят по whitelist-эвристикам
const BLOCKED_EXT = /\.(exe|bat|cmd|com|scr|msi|jar|sh|ps1|vbs|js|jse|wsf|wsh|hta|cpl|dll|apk|iso|lnk)$/i;

const upload = multer({
  storage: multer.diskStorage({
    destination: (_req, _file, cb) => cb(null, UPLOAD_DIR),
    filename: (_req, file, cb) => {
      const ext = path.extname(file.originalname || '').toLowerCase().replace(/[^.a-z0-9]/g, '').slice(0, 10);
      cb(null, crypto.randomBytes(12).toString('hex') + ext);
    }
  }),
  limits: { fileSize: 10 * 1024 * 1024, files: 1 }
});

// загрузка требует аутентификации (Bearer: session token)
async function authHttp(req, res, next) {
  const h = req.headers.authorization || '';
  const token = h.startsWith('Bearer ') ? h.slice(7) : '';
  const userId = await sessionGet(token).catch(() => null);
  if (!userId) return res.status(401).json({ error: 'unauthorized' });
  req.userId = userId;
  next();
}

app.post('/upload', authHttp, uploadLimiter, upload.single('file'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'no file' });
    if (BLOCKED_EXT.test(req.file.originalname || '')) {
      fs.unlink(req.file.path, () => {});
      return res.status(415).json({ error: 'запрещённый тип файла' });
    }
    // проверка реального содержимого (магические байты) против MIME-подмены
    const fd = fs.openSync(req.file.path, 'r');
    let sniffed = null;
    try { sniffed = await FileType.fromFile(fd); } finally { fs.closeSync(fd); }
    const mime = (sniffed && sniffed.mime) || (/^text\//.test(req.file.mimetype) ? req.file.mimetype : '');
    if (!mime || !ALLOWED_MIME.has(mime)) {
      fs.unlink(req.file.path, () => {});
      return res.status(415).json({ error: 'неподдерживаемый тип файла' });
    }
    const fileId = uid('fl');
    // имя файла храним отдельно и экранируем при выдаче — исключаем инъекцию в Content-Disposition
    const safeName = String(req.file.originalname || 'file').replace(/[^\w.\- ]+/g, '_').slice(0, 100);
    await col.files.insertOne({
      fileId,
      ownerId: req.userId,
      storedName: req.file.filename,
      name: safeName,
      size: req.file.size,
      type: mime,
      createdAt: new Date()
    });
    res.json({ url: '/f/' + fileId, fileId, name: safeName, size: req.file.size, type: mime });
  } catch (e) {
    console.error('[upload]', e);
    if (req.file) fs.unlink(req.file.path, () => {});
    res.status(500).json({ error: 'upload failed' });
  }
});

// раздача файлов: только участникам чата, где файл был отправлен; owner всегда может
app.get('/f/:id', authHttp, async (req, res) => {
  try {
    const f = await col.files.findOne({ fileId: req.params.id });
    if (!f) return res.status(404).end();
    if (f.ownerId !== req.userId) {
      const usedIn = await col.messages.findOne({ 'file.fileId': f.fileId, deleted: { $ne: true } }, { projection: { to: 1, from: 1 } });
      if (!usedIn) return res.status(403).end();
      const to = String(usedIn.to || '');
      let allowed = false;
      if (to.indexOf('r_') === 0) {
        const r = await getRoom(to);
        allowed = inRoom(req.userId, r);
      } else if (to === 'public') {
        allowed = true;
      } else {
        allowed = (to === req.userId || usedIn.from === req.userId);
      }
      if (!allowed) return res.status(403).end();
    }
    const fp = path.join(UPLOAD_DIR, path.basename(f.storedName)); // basename исключает path traversal
    if (!fs.existsSync(fp)) return res.status(410).end();
    res.setHeader('Content-Type', f.type);
    res.setHeader('Content-Security-Policy', "default-src 'none'; img-src 'self' data:; media-src 'self'; style-src 'unsafe-inline'; sandbox");
    res.setHeader('X-Content-Type-Options', 'nosniff');
    // inline для изображений/медиа (нужно для <img>/<video>), download для остального
    const disp = (/^(image|video|audio)\//.test(f.type) || f.type === 'application/pdf') ? 'inline' : 'attachment';
    res.setHeader('Content-Disposition', disp + '; filename="' + f.name.replace(/"/g, '') + '"');
    fs.createReadStream(fp).pipe(res);
  } catch (e) {
    console.error('[file]', e);
    res.status(500).end();
  }
});

app.get('/health', (_req, res) => res.json({ ok: true, time: Date.now(), db: 'mongodb' }));

// Пока MongoDB не подключена (например, идёт переподключение в фоне),
// все обращения к БД должны возвращать 503, а не падать с TypeError.
app.use((req, res, next) => {
  if (!col.users) {
    return res.status(503).json({ error: 'Сервис временно недоступен: подключение к базе данных' });
  }
  next();
});

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

app.post('/subscribe', authHttp, async (req, res) => {
  const { subscription } = req.body || {};
  if (!subscription || typeof subscription !== 'object' || !subscription.endpoint) return res.status(400).json({ error: 'bad request' });
  await col.subs.updateOne({ userId: req.userId }, { $set: { userId: req.userId, subscription } }, { upsert: true });
  res.json({ ok: true });
});

app.post('/unsubscribe', authHttp, async (req, res) => {
  await col.subs.deleteOne({ userId: req.userId });
  res.json({ ok: true });
});

app.get('/join/:code', (req, res) => {
  const code = /^[a-f0-9]{6,24}$/i.test(req.params.code) ? req.params.code : '';
  if (!code) return res.redirect('/');
  res.redirect('/?invite=' + encodeURIComponent(code));
});

// ---------- Helpers ----------
const now = () => Date.now();
const uid = (p) => (p || 'x') + '_' + now().toString(36) + crypto.randomBytes(4).toString('hex');
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

async function getUser(userId) { return userId ? col.users.findOne({ userId }) : null; }
async function getUsersByIds(ids) {
  const uniq = [...new Set(ids.filter(Boolean))];
  if (!uniq.length) return [];
  return col.users.find({ userId: { $in: uniq } }).toArray();
}
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

// История чата для пользователя: эффективный запрос к Mongo вместо полной выборки коллекции
async function msgsFor(userId) {
  const [myRooms, hidden] = await Promise.all([
    col.rooms.find({ members: userId }, { projection: { id: 1 } }).toArray(),
    getHiddenMap(userId)
  ]);
  const roomIds = myRooms.map(r => r.id);
  const orClauses = [
    { from: userId },                          // всё, что я отправил (личные/сохранённые/комнаты)
    { to: userId },                            // личные входящие
    { to: 'public' }                           // общий чат
  ];
  if (roomIds.length) orClauses.push({ to: { $in: roomIds } });
  const messages = await col.messages
    .find({ $or: orClauses })
    .sort({ timestamp: 1 })
    .limit(5000)
    .toArray();
  const roomsById = indexById(await getAllRooms());
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
  // эффективная сборка списка контактов: агрегация по личным чатам + участники моих комнат
  // (раньше — две полные выборки коллекций messages и users на каждого зрителя)
  const [me, dmPeers, myRooms] = await Promise.all([
    getUser(userId),
    col.messages.aggregate([
      { $match: { $or: [{ from: userId }, { to: userId }] } },
      { $project: { peer: { $cond: [{ $eq: ['$from', userId] }, '$to', '$from'] } } },
      { $match: { peer: { $regex: '^u_' } } },
      { $group: { _id: '$peer' } }
    ]).toArray(),
    col.rooms.find({ members: userId }, { projection: { members: 1 } }).toArray()
  ]);
  const ids = new Set();
  const myBlocked = (me && Array.isArray(me.blocked)) ? me.blocked : [];
  if (me && Array.isArray(me.contacts)) me.contacts.forEach(id => ids.add(id));
  dmPeers.forEach(d => ids.add(d._id));
  myRooms.forEach(r => (r.members || []).forEach(id => { if (id !== userId) ids.add(id); }));

  ids.delete(userId);
  myBlocked.forEach(id => ids.delete(id));

  const candIds = [...ids];
  if (!candIds.length) return [];
  const users = await getUsersByIds(candIds);
  const usersById = indexById(users);
  // блокировка в любую сторону скрывает пользователя из списка контактов
  const out = [];
  candIds.forEach(id => {
    if (!usersById[id]) return;
    if (isBlockedAny(userId, id, usersById)) return;
    out.push(pubUser(usersById[id]));
  });
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
  // по одному запросу users на каждого онлайн-пользователя (раньше — O(N×M) полных выборок)
  const userIds = [...new Set([...io.sockets.sockets].filter(s => s.data.userId).map(s => s.data.userId))];
  for (const oneUserId of userIds) {
    try {
      const list = await usersForViewer(oneUserId);
      emitToUser(oneUserId, 'users', list);
    } catch (e) { console.error('[broadcastUsers]', e.message); }
  }
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

// Рассылка сообщения только тем, кто должен его видеть (без перебора всех пользователей)
async function deliverMessage(msg, senderName) {
  const targets = new Set();
  if (msg.to === 'public') {
    const users = await col.users.find({}, { projection: { userId: 1 } }).toArray();
    users.forEach(u => targets.add(u.userId));
  } else if (msg.to === 'saved') {
    targets.add(msg.from);
  } else if (String(msg.to).indexOf('r_') === 0) {
    const r = await getRoom(msg.to);
    if (r) (r.members || []).forEach(id => targets.add(id));
  } else {
    targets.add(msg.to);
    targets.add(msg.from);
  }
  // фильтруем по блокировкам для личных чатов
  let usersById = null;
  if (msg.to !== 'public' && msg.to !== 'saved' && String(msg.to).indexOf('r_') !== 0) {
    const users = await getUsersByIds([...targets]);
    usersById = indexById(users);
  }
  for (const rid of targets) {
    if (usersById && isBlockedAny(rid, msg.to === rid ? msg.from : msg.to, usersById)) continue;
    emitToUser(rid, 'message', msg);
    if (rid !== msg.from) {
      const meU = usersById ? usersById[rid] : await getUser(rid);
      if (!meU) continue;
      const key = msg.to === 'public' ? 'public'
        : msg.to === 'saved' ? 'saved'
        : String(msg.to).indexOf('r_') === 0 ? msg.to
        : (msg.from === rid ? msg.to : msg.from);
      if (!meU.mutedChats || meU.mutedChats.indexOf(key) < 0) {
        sendPush(rid, senderName, msg.text ? '(зашифрованное сообщение)' : 'Файл').catch(() => {});
      }
    }
  }
}

// Обновление сообщения + рассылка только участникам соответствующего чата
async function updateAndBroadcast(msgId, updateObj, extraCheck) {
  const m = await getMessage(msgId);
  if (!m) return null;
  if (extraCheck && !extraCheck(m)) return null;
  await col.messages.updateOne({ id: msgId }, updateObj);
  const fresh = await getMessage(msgId);
  if (!fresh) return null;
  const targets = new Set();
  if (fresh.to === 'public') {
    const users = await col.users.find({}, { projection: { userId: 1 } }).toArray();
    users.forEach(u => targets.add(u.userId));
  } else if (fresh.to === 'saved') {
    targets.add(fresh.from);
  } else if (String(fresh.to).indexOf('r_') === 0) {
    const r = await getRoom(fresh.to);
    if (r) (r.members || []).forEach(id => targets.add(id));
  } else {
    targets.add(fresh.from); targets.add(fresh.to);
  }
  targets.forEach(rid => emitToUser(rid, 'message-update', fresh));
  return fresh;
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
    socket.data.token = token;
    if (!online.has(userId)) online.set(userId, new Set());
    online.get(userId).add(socket.id);
    const u = await getUser(userId);
    if (!u) return;
    const safeUser = Object.assign({}, u, { password: undefined, _id: undefined });
    socket.emit('joined', { user: safeUser, userId, token, isNewAccount: !!isNewAccount });
    socket.emit('history', await msgsFor(userId));
    socket.emit('rooms', await roomsList());
    socket.emit('users', await usersForViewer(userId));
    io.emit('user-updated', { userId, user: pubUser(u) });
  }

  // auth-обёртка: асинхронная, с перехватом ошибок (исключает unhandled rejections)
  const auth = (fn) => async (data, cb) => {
    if (!socket.data.userId) return;
    try { await fn(data || {}, cb); }
    catch (e) {
      console.error('[handler]', e.message);
      try { socket.emit('error-msg', { text: 'Внутренняя ошибка сервера' }); } catch (_) {}
    }
  };
  const limited = (rl, fn) => auth(async (data, cb) => {
    const ok = await rl(socket.data.userId || socket.id);
    if (!ok) {
      if (typeof cb === 'function') { try { cb({ limited: true }); } catch (_) {} }
      return socket.emit('error-msg', { text: 'Слишком много действий — подождите немного' });
    }
    return fn(data, cb);
  });

  socket.on('register', limited(rlAuth, async (data = {}) => {
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
      color: /^#[0-9a-fA-F]{6}$/.test(String(data.color || '')) ? data.color : '#5288c1',
      initials: String(data.initials || name.slice(0, 2)).toUpperCase().slice(0, 2),
      avatar: null,
      contacts: [], mutedChats: [], blocked: [],
      settings: { theme: 'light', accent: '#3390ec', language: 'ru' },
      createdAt: now()
    };
    await col.users.insertOne(user);
    try {
      await col.usernames.insertOne({ username, userId });
    } catch (e) {
      // гонка при регистрации одинаковых username — откатываем пользователя
      await col.users.deleteOne({ userId });
      return socket.emit('auth-error', { text: 'Username уже занят' });
    }
    const tok = newToken();
    await sessionSet(tok, userId);
    attachUser(userId, tok, true);
  }));

  socket.on('login', limited(rlAuth, async (data = {}) => {
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
  }));

  socket.on('auto-login', limited(rlAuth, async (data = {}) => {
    const { token } = data;
    const owner = await sessionGet(token);
    if (!token || !owner) return socket.emit('auth-error', { text: 'Сессия истекла' });
    // userId берём ТОЛЬКО из серверной сессии, клиенту он не нужен
    attachUser(owner, token, false);
  }));

  socket.on('disconnect', () => {
    const userId = socket.data.userId;
    if (!userId) return;
    const set = online.get(userId);
    if (set) {
      set.delete(socket.id);
      if (set.size === 0) {
        online.delete(userId);
        getUser(userId).then(u => { if (u) io.emit('user-updated', { userId, user: pubUser(u) }); })
          .catch(e => console.error('[disconnect]', e.message));
      }
    }
  });

  socket.on('message', limited(rlMessage, async (data) => {
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
      const target = await getUser(to);
      if (!target) return;
      const usersById = indexById(await getUsersByIds([userId, to]));
      if (isBlockedAny(userId, to, usersById)) {
        const byMe = !!(u.blocked && u.blocked.indexOf(to) >= 0);
        return socket.emit('error-msg', { text: byMe ? 'Пользователь заблокирован. Разблокируйте его.' : 'Вы заблокированы этим пользователем' });
      }
    }
    // id и timestamp назначаются СЕРВЕРОМ (клиентским значениями управлять нельзя)
    const ts = now();
    let file = null;
    if (data.file && data.file.fileId) {
      const f = await col.files.findOne({ fileId: String(data.file.fileId) });
      // использовать можно только свои файлы
      if (f && f.ownerId === userId) {
        file = { fileId: f.fileId, name: f.name, size: f.size, type: f.type, url: '/f/' + f.fileId };
      } else if (f && f.ownerId !== userId) {
        return socket.emit('error-msg', { text: 'Недопустимое вложение' });
      }
    }
    const msg = {
      id: uid('m'),
      from: userId, fromName: u.name,
      to,
      text: String(data.text || '').slice(0, 4000),
      file,
      replyTo: String(data.replyTo || '') || null,
      timestamp: ts,
      time: fmtTime(ts),
      deliveredTo: [userId], readBy: [userId], reactions: {},
      edited: false, deleted: false, pinned: false
    };
    await col.messages.insertOne(msg);
    await deliverMessage(msg, u.name);
  }));

  socket.on('delivered', limited(rlRoomOps, async (data) => {
    const userId = socket.data.userId;
    const ids = (Array.isArray(data.messageIds) ? data.messageIds : []).slice(0, 50);
    if (!ids.length) return;
    await col.messages.updateMany(
      { id: { $in: ids }, from: { $ne: userId }, to: { $ne: 'saved' } },
      { $addToSet: { deliveredTo: userId } }
    );
    const updated = await col.messages.find({ id: { $in: ids }, deliveredTo: userId }).toArray();
    const byId = {};
    updated.forEach(m => { byId[m.id] = m; });
    // уведомляем только авторов (по одному событию на сообщение)
    const seen = new Set();
    updated.forEach(m => {
      if (m.from === userId || seen.has(m.id)) return;
      seen.add(m.id);
      emitToUser(m.from, 'message-update', byId[m.id]);
    });
  }));

  socket.on('read', limited(rlRoomOps, async (data) => {
    const userId = socket.data.userId;
    const ids = (Array.isArray(data.messageIds) ? data.messageIds : []).slice(0, 50);
    if (!ids.length) return;
    await col.messages.updateMany(
      { id: { $in: ids }, from: { $ne: userId } },
      { $addToSet: { readBy: userId, deliveredTo: userId } }
    );
    const updated = await col.messages.find({ id: { $in: ids }, readBy: userId }).toArray();
    const seen = new Set();
    updated.forEach(m => {
      if (m.from === userId || seen.has(m.id)) return;
      seen.add(m.id);
      emitToUser(m.from, 'message-update', m);
    });
  }));

  socket.on('typing', limited(rlTyping, async (data) => {
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
      const usersById = indexById(await getUsersByIds([userId, to]));
      if (isBlockedAny(userId, to, usersById)) return;
      emitToUser(to, 'typing', payload);
    }
  }));

  socket.on('react-message', limited(rlMessage, async (data) => {
    const userId = socket.data.userId;
    const m = await getMessage(data.messageId);
    if (!m || m.deleted) return;
    // реагировать могут только видящие сообщение участники
    if (String(m.to).indexOf('r_') === 0) {
      const r = await getRoom(m.to);
      if (!inRoom(userId, r)) return;
    } else if (m.to !== 'public' && m.to !== 'saved') {
      if (m.from !== userId && m.to !== userId) return;
    }
    const em = String(data.emoji || '').slice(0, 8);
    if (!em) return;
    const cur = (m.reactions && m.reactions[em]) || [];
    const next = cur.indexOf(userId) >= 0 ? cur.filter(x => x !== userId) : cur.concat(userId);
    const upd = next.length ? { $set: { ['reactions.' + em]: next } } : { $unset: { ['reactions.' + em]: '' } };
    await updateAndBroadcast(m.id, upd);
  }));

  socket.on('pin-message', limited(rlRoomOps, async (data) => {
    const userId = socket.data.userId;
    const m = await getMessage(data.messageId);
    if (!m) return;
    if (typeof m.to === 'string' && m.to.indexOf('r_') === 0) {
      const r = await getRoom(m.to);
      if (!r) return;
      const role = (r.roles && r.roles[userId]) || (r.createdBy === userId ? 'owner' : 'member');
      if (role !== 'owner' && role !== 'admin') return;
    } else if (m.to !== 'public') {
      if (m.from !== userId && m.to !== userId) return;
    }
    await updateAndBroadcast(m.id, { $set: { pinned: !m.pinned } });
  }));

  socket.on('edit-message', limited(rlMessage, async (data) => {
    const userId = socket.data.userId;
    const m = await getMessage(data.messageId);
    if (!m || m.from !== userId) return;
    if (now() - m.timestamp > 24 * 60 * 60 * 1000) return socket.emit('error-msg', { text: 'Слишком поздно редактировать' });
    const txt = String(data.newText || '').trim().slice(0, 4000);
    if (!txt) return;
    await updateAndBroadcast(m.id, { $set: { text: txt, edited: true } });
  }));

  socket.on('delete-message', limited(rlMessage, async (data) => {
    const userId = socket.data.userId;
    const m = await getMessage(data.messageId);
    if (!m || m.from !== userId) return;
    await updateAndBroadcast(m.id, { $set: { deleted: true, text: '', file: null, reactions: {}, pinned: false } });
  }));

  socket.on('create-room', limited(rlRoomOps, async (data) => {
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
      const names = data.inviteUsernames.slice(0, 100).map(un => String(un || '').replace(/^@/, '').trim().toLowerCase());
      const mappings = await col.usernames.find({ username: { $in: names } }).toArray();
      for (const mp of mappings) {
        const iid = mp.userId;
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

  socket.on('invite-to-room', limited(rlRoomOps, async (data) => {
    const userId = socket.data.userId;
    const r = await getRoom(data.roomId);
    if (!r) return;
    const myRole = r.roles[userId] || (r.createdBy === userId ? 'owner' : 'member');
    if (myRole !== 'owner' && myRole !== 'admin') return;
    const names = (Array.isArray(data.usernames) ? data.usernames : []).slice(0, 100)
      .map(un => String(un || '').replace(/^@/, '').trim().toLowerCase());
    const mappings = await col.usernames.find({ username: { $in: names } }).toArray();
    const added = [];
    for (const mp of mappings) {
      const iid = mp.userId;
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

  socket.on('join-room', limited(rlRoomOps, async (data) => {
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

  socket.on('leave-room', limited(rlRoomOps, async (data) => {
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

  socket.on('kick-user', limited(rlRoomOps, async (data) => {
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

  socket.on('set-role', limited(rlRoomOps, async (data) => {
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

  socket.on('add-contact', limited(rlRoomOps, async (data) => {
    const userId = socket.data.userId;
    const cid = data.contactId;
    if (!await getUser(cid) || cid === userId) return;
    await col.users.updateOne({ userId }, { $addToSet: { contacts: cid } });
    socket.emit('contact-added', { userId: cid });
    socket.emit('users', await usersForViewer(userId));
  }));

  socket.on('remove-contact', limited(rlRoomOps, async (data) => {
    const userId = socket.data.userId;
    const cid = data.contactId;
    await col.users.updateOne({ userId }, { $pull: { contacts: cid } });
    socket.emit('contact-removed', { userId: cid });
    socket.emit('users', await usersForViewer(userId));
  }));

  socket.on('search-user', limited(rlSearch, async (data) => {
    const userId = socket.data.userId;
    const q = String(data.query || '').replace(/^@/, '').toLowerCase().slice(0, 50);
    if (!q) return socket.emit('search-result', { results: [] });
    const meU = await getUser(userId);
    const myBlocked = (meU && Array.isArray(meU.blocked)) ? meU.blocked : [];
    const rx = new RegExp('^' + q.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'));
    const users = await col.users.find({
      userId: { $ne: userId },
      $or: [{ username: rx }, { name: { $regex: q.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), $options: 'i' } }]
    }).limit(30).toArray();
    const out = [];
    for (const u of users) {
      if (myBlocked.indexOf(u.userId) >= 0) continue;                       // я заблокировал — не ищу
      if (Array.isArray(u.blocked) && u.blocked.indexOf(userId) >= 0) continue; // он заблокировал меня — невидим
      out.push(pubUser(u));
    }
    socket.emit('search-result', { results: out });
  }));

  socket.on('search-messages', limited(rlSearch, async (data) => {
    const userId = socket.data.userId;
    const q = String(data.query || '').toLowerCase().trim().slice(0, 100);
    if (q.length < 2) return socket.emit('search-messages-result', { query: q, results: [] });
    const [myRooms, hidden] = await Promise.all([
      col.rooms.find({ members: userId }, { projection: { id: 1 } }).toArray(),
      getHiddenMap(userId)
    ]);
    const roomIds = myRooms.map(r => r.id);
    const orClauses = [{ from: userId }, { to: userId }, { to: 'public' }];
    if (roomIds.length) orClauses.push({ to: { $in: roomIds } });
    const escaped = q.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const messages = await col.messages
      .find({ deleted: { $ne: true }, text: { $regex: escaped, $options: 'i' }, $or: orClauses })
      .sort({ timestamp: -1 })
      .limit(100)
      .toArray();
    const roomsById = indexById(await getAllRooms());
    const results = messages
      .filter(m => !hidden[m.id] && canSeeMessage(m, userId, roomsById))
      .sort((a, b) => a.timestamp - b.timestamp);
    socket.emit('search-messages-result', { query: q, results });
  }));

  socket.on('create-invite', limited(rlRoomOps, async (data) => {
    const userId = socket.data.userId;
    const r = await getRoom(data.roomId);
    if (!r) return;
    const myRole = r.roles[userId] || (r.createdBy === userId ? 'owner' : 'member');
    if (myRole !== 'owner' && myRole !== 'admin') return;
    const code = crypto.randomBytes(6).toString('hex');
    await col.invites.updateOne({ code }, { $set: { code, roomId: r.id, createdBy: userId, createdAt: now() } }, { upsert: true });
    socket.emit('invite-created', { link: '/join/' + code });
  }));

  socket.on('redeem-invite', limited(rlRoomOps, async (data) => {
    const userId = socket.data.userId;
    const inv = await col.invites.findOne({ code: String(data.code || '').slice(0, 24) });
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

  socket.on('mute-chat', limited(rlRoomOps, async (data) => {
    const userId = socket.data.userId;
    const cid = String(data.chatId || '').slice(0, 64);
    if (!cid) return;
    if (data.muted) await col.users.updateOne({ userId }, { $addToSet: { mutedChats: cid } });
    else await col.users.updateOne({ userId }, { $pull: { mutedChats: cid } });
    const u = await getUser(userId);
    socket.emit('mutes-updated', { mutedChats: u.mutedChats || [] });
  }));

  // ---------- Блокировка пользователя ----------
  socket.on('block-user', limited(rlRoomOps, async (data) => {
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

  socket.on('unblock-user', limited(rlRoomOps, async (data) => {
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
  socket.on('delete-chat', limited(rlRoomOps, async (data) => {
    const userId = socket.data.userId;
    const chatId = String(data.chatId || '').slice(0, 64);
    if (!chatId) return;
    const orClauses = chatId === 'public' ? [{ to: 'public' }]
      : chatId === 'saved' ? [{ from: userId, to: 'saved' }]
      : chatId.indexOf('r_') === 0 ? [{ to: chatId }]
      : [{ from: userId, to: chatId }, { from: chatId, to: userId }];
    const messages = await col.messages.find({ $or: orClauses }).toArray();
    const hidden = await getHiddenMap(userId);
    const ids = Object.assign({}, hidden);
    messages.forEach(m => { ids[m.id] = true; });
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

  socket.on('update-settings', limited(rlRoomOps, async (data) => {
    const userId = socket.data.userId;
    if (data.settings && typeof data.settings === 'object') {
      // white-list настроек — нельзя записать произвольные поля документа
      const src = data.settings;
      const clean = {};
      if (src.theme === 'light' || src.theme === 'dark' || src.theme === 'pixel') clean.theme = src.theme;
      if (typeof src.accent === 'string' && /^#[0-9a-fA-F]{6}$/.test(src.accent)) clean.accent = src.accent;
      if (src.language === 'ru' || src.language === 'en') clean.language = src.language;
      if (Object.keys(clean).length) {
        const set = {};
        Object.entries(clean).forEach(([k, v]) => { set['settings.' + k] = v; });
        await col.users.updateOne({ userId }, { $set: set });
      }
    }
  }));

  socket.on('update-profile', limited(rlRoomOps, async (data) => {
    const userId = socket.data.userId;
    const u = await getUser(userId);
    if (!u) return;
    const set = {};
    if (typeof data.name === 'string' && data.name.trim()) {
      set.name = data.name.trim().slice(0, 30);
      set.initials = set.name.split(/\s+/).map(w => w[0]).join('').toUpperCase().slice(0, 2)
        || set.name.slice(0, 2).toUpperCase();
    }
    if (typeof data.color === 'string' && /^#[0-9a-fA-F]{6}$/.test(data.color)) set.color = data.color;
    if (typeof data.initials === 'string') set.initials = data.initials.toUpperCase().slice(0, 2);
    // avatar: fileId загруженного файла (валидация владельца) или null для удаления
    if (data.avatar === null) set.avatar = null;
    else if (typeof data.avatar === 'string' && /^fl_/.test(data.avatar)) {
      const f = await col.files.findOne({ fileId: data.avatar.slice(0, 64) });
      if (f && f.ownerId === userId && /^image\//.test(f.type)) set.avatar = '/f/' + f.fileId;
    }
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

  socket.on('change-password', limited(rlAuth, async (data = {}) => {
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
    // сброс старых сессий этого пользователя (кроме текущей) и ключей E2E
    const currentHash = hashToken(socket.data.token || '');
    await col.sessions.deleteMany({ userId, tokenHash: { $ne: currentHash } });
    await col.encKeys.deleteMany({ by: userId });
    socket.emit('password-changed', {});
    socket.emit('toast-msg', { text: 'Пароль изменён. Ключи шифрования пересозданы.' });
  }));

  // ---------- E2E: хранение публичных чат-ключей (зашифрованных мастер-паролем клиента) ----------
  socket.on('enc-keys-get', limited(rlEncOps, async (data, cb) => {
    const chatId = String((data && data.chatId) || '').slice(0, 64);
    if (!chatId) { if (typeof cb === 'function') cb(null); return; }
    const rec = await getEncKey(chatId);
    if (typeof cb === 'function') cb(rec ? { ek: rec.ek, salt: rec.salt, by: rec.by } : null);
  }));

  socket.on('enc-keys-set', limited(rlEncOps, async (data) => {
    const chatId = String((data && data.chatId) || '').slice(0, 64);
    const rec = data && data.record;
    if (!chatId || !rec || typeof rec !== 'object') return;
    const userId = socket.data.userId;
    // ИСПРАВЛЕНИЕ IDOR: право записи ключа — только у автора ключа (первого установившего).
    // Любой другой участник НЕ может перезаписать существующий ключ (иначе возможен подменный MITM).
    let allowed = false;
    if (chatId.indexOf('r_') === 0) allowed = inRoom(userId, await getRoom(chatId));
    else if (chatId === 'public' || chatId === 'saved') allowed = true;
    else {
      // личный чат: ключ может установить только один из двух реальных собеседников
      // (тот, кому адресовано хотя бы одно сообщение между нами)
      const other = chatId;
      const hasContact = await col.messages.findOne({
        $or: [{ from: userId, to: other }, { from: other, to: userId }]
      }, { projection: { _id: 1 } });
      allowed = !!(await getUser(other)) && !!hasContact;
    }
    if (!allowed) return;
    const exists = await getEncKey(chatId);
    if (exists) {
      // повторная запись разрешена только владельцу ключа (например, после смены мастер-пароля)
      if (exists.by !== userId) return;
    }
    const doc = {
      chatId,
      ek: String(rec.ek || '').slice(0, 200),
      salt: String(rec.salt || '').slice(0, 64),
      by: exists ? exists.by : userId
    };
    await col.encKeys.updateOne({ chatId }, { $set: doc }, { upsert: true });
    io.emit('enc-keys-updated', { chatId, record: { ek: doc.ek, salt: doc.salt, by: doc.by } });
  }));
});

// Очистка протухших сессий (TTL-индекс делает это сам; страховка на случай простоя процесса)
setInterval(() => {
  col.sessions && col.sessions.deleteMany({ expiresAt: { $lt: new Date() } }).catch(() => {});
}, 60 * 60 * 1000).unref();

// Глобальные обработчики — процесс не падает молча с unhandled rejection
process.on('unhandledRejection', (e) => console.error('[unhandledRejection]', e && e.stack || e));
process.on('uncaughtException', (e) => console.error('[uncaughtException]', e && e.stack || e));

// ---------- Старт ----------
// Стратегия для Render: HTTP-порт должен быть занят сразу после запуска,
// иначе деплой не пройдёт health-check. Поэтому сервер слушает немедленно,
// а подключение к MongoDB повторяется в фоне (эндпоинты без БД вернут 503).
const MONGO_RETRY_MS = 20 * 1000;

async function connectMongoWithRetry() {
  for (;;) {
    try {
      await startMongo();
      console.log('[mongo] подключения установлены, сервис полностью готов');
      return;
    } catch (e) {
      console.error('[mongo] ошибка подключения:', e.message || e);
      console.error(`[mongo] повторная попытка через ${MONGO_RETRY_MS / 1000} с...`);
      await new Promise(r => setTimeout(r, MONGO_RETRY_MS));
    }
  }
}

server.listen(PORT, () => console.log('Pulse listening on ' + PORT));
connectMongoWithRetry();

process.on('SIGINT', async () => { try { if (client) await client.close(); } catch (e) {} process.exit(0); });
process.on('SIGTERM', async () => { try { if (client) await client.close(); } catch (e) {} process.exit(0); });
