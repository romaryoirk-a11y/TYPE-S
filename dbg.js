const { io } = require('socket.io-client');
const URL = 'http://localhost:3000';
const a = io(URL, { transports: ['websocket'] });
a.on('connect', () => console.log('connected'));
a.emit('register', { name: 'Dbg', username: 'dbg1', password: '1234' });
a.on('joined', j => {
  console.log('joined', j.user.username);
  a.emit('message', { to: 'public', text: 'hello pub', id: 'm_dbg1' });
});
a.on('message', m => console.log('got message:', m.text));
a.on('auth-error', e => console.log('auth-error:', e.text));
setTimeout(() => { console.log('TIMEOUT - no message received'); process.exit(1); }, 5000);
