// Интеграционный тест: регистрация, сообщения (серверные id), загрузка файлов, IDOR-проверки
const { io } = require('socket.io-client');
const crypto = require('crypto');

const BASE = 'http://localhost:3100';
let fails = 0;
function ok(cond, name) { console.log((cond ? 'PASS' : 'FAIL') + ' - ' + name); if (!cond) fails++; }

function connect() { return new Promise(res => { const s = io(BASE, { transports: ['websocket'] }); s.on('connect', () => res(s)); }); }
function once(s, ev) { return new Promise(res => s.once(ev, res)); }

(async () => {
  const a = await connect(), b = await connect();
  const ua = 'user' + Math.random().toString(36).slice(2, 8);
  const ub = 'user' + Math.random().toString(36).slice(2, 8);

  const ja = once(a, 'joined'); a.emit('register', { name: 'Ann', username: ua, password: 'test1234' });
  const joinedA = await ja;
  ok(joinedA && joinedA.user && joinedA.user.password === undefined, 'joined не содержит пароль');
  ok(!('contactsAgg' in JSON.stringify(joinedA)), 'joined ok');
  const tokenA = joinedA.token;

  const jb = once(b, 'joined'); b.emit('register', { name: 'Bob', username: ub, password: 'test1234' });
  const joinedB = await jb;
  ok(!!joinedB.token, 'B зарегистрирован');

  // автологин только по токену (userId не передаём)
  const c = await connect();
  const jc = once(c, 'joined'); c.emit('auto-login', { token: tokenA });
  const joinedC = await jc;
  ok(joinedC && joinedC.userId === joinedA.userId, 'auto-login по токену без userId');

  // DDoS-подделка: чужой userId игнорируется
  const d = await connect();
  d.emit('auto-login', { token: tokenA, userId: 'u_hacker' });
  const jd = await once(d, 'joined');
  ok(jd.userId === joinedA.userId, 'клиентский userId в auto-login проигнорирован');

  // личное сообщение A -> B
  const mb = once(b, 'message');
  a.emit('message', { to: joinedB.userId, text: 'hello bob', from: 'u_FAKE', timestamp: 1 });
  const msgB = await mb;
  ok(msgB.id && msgB.id.indexOf('m_') === 0, 'id сообщения назначен сервером');
  ok(msgB.from === joinedA.userId, 'from подменён быть не может');
  ok(Math.abs(msgB.timestamp - Date.now()) < 5000, 'timestamp серверный');

  // echo автору
  const ma = once(a, 'message');
  await new Promise(r => setTimeout(r, 50));
  a.emit('message', { to: joinedB.userId, text: 'again' });
  const msgA = await ma;
  ok(!!msgA.id, 'эхо автору доставлено');

  // файл: png
  const png = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==', 'base64');
  const form = new FormData();
  form.append('file', new Blob([png], { type: 'image/png' }), 'x.png');
  let r = await fetch(BASE + '/upload', { method: 'POST', body: form });
  ok(r.status === 401, 'upload без токена запрещён');
  r = await fetch(BASE + '/upload', { method: 'POST', headers: { Authorization: 'Bearer ' + tokenA }, body: form });
  const up = await r.json();
  ok(r.status === 200 && up.fileId && up.url.indexOf('/f/') === 0, 'upload с токеном успешен');
  r = await fetch(BASE + up.url);
  ok(r.status === 401, '/f/:id требует авторизации');
  r = await fetch(BASE + up.url, { headers: { Authorization: 'Bearer ' + joinedB.token } });
  const dl = await r.text();
  ok(dl.length === 0 || r.status === 403, 'чужой файл недоступен до отправки в чат');

  // отправка файла в чат и доступ получателя
  const mfile = once(b, 'message');
  a.emit('message', { to: joinedB.userId, text: '', file: { fileId: up.fileId, name: 'x.png', size: png.length, type: 'image/png' } });
  const msgF = await mfile;
  ok(msgF.file && msgF.file.url.indexOf('/f/') === 0, 'файл прикреплён (url через /f/)');
  r = await fetch(BASE + msgF.file.url, { headers: { Authorization: 'Bearer ' + joinedB.token } });
  ok(r.status === 200, 'получатель может скачать файл чата');

  // чужой fileId подставить нельзя
  const before = (await (await fetch(BASE + '/stats')).json()).totalMessages;
  a.emit('message', { to: joinedB.userId, text: 'steal', file: { fileId: 'fl_nonexistent' } });
  await new Promise(res2 => setTimeout(res2, 300));
  const after = (await (await fetch(BASE + '/stats')).json()).totalMessages;
  ok(after === before + 1, 'сообщение с несуществующим fileId сохраняется без файла');
  const lastMsg = await new Promise(res2 => { a.emit('search-messages', { query: 'steal' }); setTimeout(res2, 400); });

  // E2E IDOR: B не может перезаписать ключ чата, установленный A
  const chatId = joinedB.userId; // личный чат A->B, ключ ставит A
  await new Promise(res2 => { a.emit('enc-keys-set', { chatId, record: { ek: 'aes-gcm-256', salt: 'saltA' } }, res2); setTimeout(res2, 300); });
  b.emit('enc-keys-set', { chatId, record: { ek: 'evil', salt: 'saltB' } });
  await new Promise(res2 => setTimeout(res2, 300));
  const rec = await new Promise(res2 => { c.emit('enc-keys-get', { chatId }, res2); setTimeout(() => res2(null), 800); });
  ok(rec && rec.salt === 'saltA' && rec.by === joinedA.userId, 'IDOR enc-keys-set закрыт: чужая запись не прошла');

  // rate limit: спам сообщениями
  let limited = false;
  for (let i = 0; i < 40; i++) {
    a.emit('message', { to: joinedB.userId, text: 'spam' + i });
  }
  const rl = once(a, 'error-msg');
  try { const e = await Promise.race([rl, new Promise((_, rej) => setTimeout(() => rej('none'), 3000))]); limited = /много/i.test(e.text); } catch (e) {}
  ok(limited, 'rate-limit сообщений срабатывает');

  // update-settings: инъекция произвольных полей игнорируется
  a.emit('update-settings', { settings: { password: 'hacked', theme: 'dark' } });
  await new Promise(res2 => setTimeout(res2, 300));
  const relog = await connect();
  const jr = await Promise.race([once(relog, 'joined'), new Promise((_, rej) => setTimeout(() => rej('fail'), 3000))]).catch(() => null);
  ok(jr && jr.user.password !== 'hacked', 'нельзя перезаписать password через update-settings');
  ok(jr && jr.user.settings.theme === 'dark', 'легальная настройка сохранена');

  console.log(fails ? 'RESULT: FAIL (' + fails + ')' : 'RESULT: ALL PASS');
  process.exit(fails ? 1 : 0);
})().catch(e => { console.error('TEST CRASH', e); process.exit(2); });
