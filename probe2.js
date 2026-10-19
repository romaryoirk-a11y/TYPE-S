process.env.NODE_ENV = 'development';
require('./server.js');
setTimeout(() => {
  const { io } = require('socket.io-client');
  const s = io('http://localhost:' + (process.env.PORT || 3000));
  s.onAny((ev) => console.log('CLI event:', ev));
  s.on('connect', () => { console.log('CLI connected'); s.emit('register', { name: 'Проба', username: 'proba_x1', password: 'test1234' }); });
  setTimeout(() => process.exit(0), 8000);
}, 6000);
