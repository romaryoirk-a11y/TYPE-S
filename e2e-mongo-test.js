const { io } = require('socket.io-client');
const URL = 'http://localhost:3000';

function mk() { return new Promise(res => { const s = io(URL, { transports: ['websocket'] }); res(s); }); }
const once = (s, ev) => new Promise(res => s.once(ev, res));

(async () => {
  const a = await mk(), b = await mk();
  // регистрация A
  a.emit('register', { name: 'Alice T', username: 'alicet', password: '1234' });
  const ja = await once(a, 'joined');
  console.log('A joined:', ja.user.username, 'isNew:', ja.isNewAccount, 'password leaked:', 'password' in ja.user ? String(ja.user.password) : 'undefined');
  // регистрация B
  b.emit('register', { name: 'Bob T', username: 'bobt', password: '1234' });
  const jb = await once(b, 'joined');
  console.log('B joined:', jb.user.username);

  // дубликат username
  const c = await mk();
  c.emit('register', { name: 'X', username: 'alicet', password: '1234' });
  const err = await once(c, 'auth-error');
  console.log('dup username rejected:', err.text);
  c.disconnect();

  // personal message A -> B
  a.emit('message', { to: jb.userId, text: 'привет боб', id: 'm_test1' });
  const mb = await once(b, 'message');
  console.log('B got msg:', mb.text, 'from', mb.fromName);

  // read receipt
  b.emit('read', { messageIds: ['m_test1'] });
  const upd = await once(a, 'message-update');
  console.log('A read-receipt:', upd.readBy);

  // reaction
  a.emit('react-message', { messageId: 'm_test1', emoji: '👍' });
  const upd2 = await once(b, 'message-update');
  console.log('reaction on B:', JSON.stringify(upd2.reactions));

  // create room + invite B
  a.emit('create-room', { name: 'TestGroup', type: 'group', inviteUsernames: ['bobt'] });
  const rc = await once(a, 'room-created');
  console.log('room created:', rc.name, 'members:', rc.members.length);
  const roomsB = await once(b, 'rooms');
  console.log('B sees rooms:', roomsB.map(r => r.name).join(','), '| roles ok:', !!roomsB[0].roles[jb.userId]);

  // channel write restriction: B member cannot write to channel — skip; group is fine
  b.emit('message', { to: rc.id, text: 'в группе ок' });
  const mg = await once(a, 'message');
  console.log('A got group msg:', mg.text, 'to', mg.to === rc.id);

  // block test: A blocks B, then A tries to send to B
  a.emit('block-user', { targetUserId: jb.userId });
  await once(a, 'blocked-updated');
  a.emit('message', { to: jb.userId, text: 'should fail' });
  const em = await once(a, 'error-msg');
  console.log('blocked send error:', em.text);
  a.emit('unblock-user', { targetUserId: jb.userId });
  await once(a, 'blocked-updated');

  // delete-chat (hide public msgs from self)
  a.emit('delete-chat', { chatId: 'public' });
  const hist = await once(a, 'history');
  console.log('after delete-chat history visible to A:', hist.length);

  // login again (bcrypt compare via Mongo)
  const d = await mk();
  d.emit('login', { username: 'alicet', password: '1234' });
  const jd = await once(d, 'joined');
  console.log('re-login OK:', jd.user.username);
  // auto-login with token
  d.emit('auto-login', { userId: jd.userId, token: jd.token });
  const jda = await once(d, 'joined');
  console.log('auto-login OK:', jda.user.username);
  // bad token
  d.emit('auto-login', { userId: jd.userId, token: 'deadbeef' });
  const serr = await once(d, 'auth-error');
  console.log('bad token rejected:', serr.text);

  // update-profile incl. username change
  d.emit('update-profile', { username: 'alice2', name: 'Alice Two' });
  const ps = await once(d, 'profile-saved');
  console.log('username changed to:', ps.username);

  // search user by new username
  d.emit('search-user', { query: 'bob' });
  const sr = await once(d, 'search-result');
  console.log('search found:', sr.results.map(u => u.username).join(','));

  // enc keys
  d.emit('enc-keys-set', { chatId: jb.userId, record: { ek: 'AAA', salt: 'sss' } });
  await new Promise(r => setTimeout(r, 300));
  d.emit('enc-keys-get', { chatId: jb.userId }, (rec) => { console.log('enc-key stored:', rec && rec.ek); });
  await new Promise(r => setTimeout(r, 300));

  // stats reflect Mongo data
  const st = await (await fetch(URL + '/stats')).json();
  console.log('stats:', JSON.stringify(st));

  a.disconnect(); b.disconnect(); d.disconnect();
  console.log('E2E_ALL_OK');
  process.exit(0);
})().catch(e => { console.error('E2E_FAIL', e); process.exit(1); });
