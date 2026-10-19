const Sio = require('socket.io');
const http = require('http');
const srv = http.createServer();
const io = new Sio.Server(srv);
io.on('connection', (socket) => {
  console.log('SRV: connection');
  socket.on('register', (data) => { console.log('SRV: register received', data); socket.emit('joined', { ok: true }); });
});
srv.listen(3222, () => {
  const { io: cli } = require('socket.io-client');
  const s = cli('http://localhost:3222');
  s.onAnyEv = null;
  s.onAny((ev, ...a) => console.log('CLI event:', ev));
  s.on('connect', () => { console.log('CLI connected'); s.emit('register', { name: 'X' }); });
  setTimeout(() => process.exit(0), 3000);
});
