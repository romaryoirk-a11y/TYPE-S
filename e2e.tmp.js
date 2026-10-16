const { io } = require('socket.io-client');
const URL = 'http://localhost:3000';
function client() { return new Promise(res => { const s = io(URL, { transports: ['websocket'] }); s.on('connect', () => res(s)); }); }
(async () => {
  const a = await client(), b = await client();
  let bGot = null;
  b.on('message', m => { if (!bGot) bGot = m; });
  const j1 = await new Promise(r => { a.once('joined', r); a.emit('register', { name:'Алиса', username:'alice', password:'test1234' }); });
  const j2 = await new Promise(r => { b.once('joined', r); b.emit('register', { name:'Боб', username:'bob', password:'test1234' }); });
  console.log('registered:', !!j1.token, !!j2.token);

  // public message
  a.emit('message', { id:'m_test1', from:j1.userId, to:'public', text:'Привет всем', timestamp:Date.now(), reactions:{} });
  await new Promise(r => setTimeout(r, 300));
  console.log('b got public msg:', bGot && bGot.text);

  // room
  const rc = await new Promise(r => { a.once('room-created', r); a.emit('create-room', { name:'Друзья', type:'group', inviteUsernames:['bob'] }); });
  console.log('room created:', rc.name, 'members:', rc.members.length);

  // react + pin
  b.emit('react-message', { messageId:'m_test1', emoji:'👍' });
  await new Promise(r => setTimeout(r, 200));
  const upd = await new Promise(r => { a.once('message-update', r); });
  console.log('reaction synced:', JSON.stringify(upd.reactions));

  // pin on personal message should NOT crash server (health check later)
  a.emit('pin-message', { messageId:'m_test1' });

  // typing + read receipts
  b.emit('delivered', { messageIds:['m_test1'] });
  b.emit('read', { messageIds:['m_test1'] });
  await new Promise(r => setTimeout(r, 200));

  // search
  const sr = await new Promise(r => { a.once('search-result', r); a.emit('search-user', { query:'bob' }); });
  console.log('search users:', sr.results.length);
  const sm = await new Promise(r => { a.once('search-messages-result', r); a.emit('search-messages', { query:'привет' }); });
  console.log('search msgs:', sm.results.length);

  // invite link
  const inv = await new Promise(r => { a.once('invite-created', r); a.emit('create-invite', { roomId: rc.id }); });
  console.log('invite link:', inv.link);

  // stats + health
  const st = await fetch(URL + '/stats').then(r => r.json());
  console.log('stats:', JSON.stringify(st));
  const h = await fetch(URL + '/health').then(r => r.json());
  console.log('server alive after pin:', h.ok);

  // upload
  const fd = new FormData();
  fd.append('file', new Blob([Buffer.from('hello')], { type:'text/plain' }), 'test.txt');
  const up = await fetch(URL + '/upload', { method:'POST', body: fd }).then(r => r.json());
  console.log('upload ok:', up.name, up.type);

  process.exit(0);
})().catch(e => { console.error('FAIL', e); process.exit(1); });
