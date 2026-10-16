const { io } = require('socket.io-client');
const a = io('http://localhost:3000', { transports: ['websocket'] });
a.on('connect_error', e => console.log('conn err', e.message));
a.emit('login', { username: 'dbg3', password: '1234' });
a.on('joined', j => {
  console.log('joined', j.user.username);
  a.emit('message', { to: 'public', text: 'second msg', id: 'm_dbg5' });
  setTimeout(() => a.emit('react-message', { messageId: 'm_dbg3', emoji: '👍' }), 800);
});
a.on('message-update', m => console.log('UPDATE:', m.id, JSON.stringify(m.reactions)));
a.on('error-msg', e => console.log('ERR-MSG:', e.text));
setTimeout(() => process.exit(0), 4000);
