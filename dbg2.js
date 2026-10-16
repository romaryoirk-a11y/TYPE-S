const { io } = require('socket.io-client');
const a = io('http://localhost:3000', { transports: ['websocket'] });
a.on('connect', () => console.log('connected'));
a.emit('register', { name: 'Dbg3', username: 'dbg3', password: '1234' });
a.on('joined', j => { console.log('joined', j.user.username); a.emit('message', { to: 'public', text: 'hello pub', id: 'm_dbg3' }); });
a.on('message', m => console.log('GOT MESSAGE:', m.text));
a.on('auth-error', e => console.log('auth-error:', e.text));
setTimeout(() => process.exit(0), 4000);
