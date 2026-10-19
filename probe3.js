const http = require('http');
const Sio = require('socket.io');
const { MongoMemoryServer } = require('mongodb-memory-server');
(async () => {
  const mongod = await MongoMemoryServer.create();
  const uri = mongod.getUri();
  const { MongoClient } = require('mongodb');
  const client = new MongoClient(uri);
  await client.connect();
  const col = client.db('test').collection('users');

  const srv = http.createServer();
  const io = new Sio.Server(srv);
  io.on('connection', (socket) => {
    console.log('SRV: connection, id=', socket.id);
    const auth = (fn) => async (data, cb) => {
      if (!socket.data.userId) { console.log('SRV: userId null -> silent return'); return; }
      try { await fn(data || {}, cb); } catch (e) { console.log('SRV handler err', e.message); }
    };
    const makeLimiter = (n, win) => { const hits = []; return async (key) => { const t = Date.now(); while (hits.length && t - hits[0] > win * 1000) hits.shift(); if (hits.length >= n) return false; hits.push(t); return true; }; };
    const rlAuth = makeLimiter(8, 60);
    const limited = (rl, fn) => auth(async (data, cb) => {
      const ok = await rl(socket.data.userId || socket.id);
      if (!ok) return socket.emit('error-msg', { text: 'rate' });
      return fn(data, cb);
    });
    socket.on('register', limited(rlAuth, async (data = {}) => {
      console.log('SRV: register handler fired with', JSON.stringify(data).slice(0,60));
      const name = String(data.name || '').trim();
      await col.insertOne({ userId: 'u1', name });
      socket.data.userId = 'u1';
      socket.emit('joined', { user: { userId: 'u1', name } });
    }));
  });
  srv.listen(3444, async () => {
    const { io: cli } = require('socket.io-client');
    const s = cli('http://localhost:3444');
    s.onAny((ev) => console.log('CLI event:', ev));
    s.on('connect', () => { console.log('CLI connected'); s.emit('register', { name: 'Проба3', username: 'proba3' }); });
    setTimeout(() => { process.exit(0); }, 5000);
  });
})();
