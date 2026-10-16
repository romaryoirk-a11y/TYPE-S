const { MongoClient } = require('mongodb');
(async () => {
  const c = new MongoClient('mongodb://127.0.0.1:34877/');
  await c.connect();
  const m = await c.db().collection('messages').findOne({ id: 'm_dbg3' });
  console.log('reactions type:', typeof m.reactions, JSON.stringify(m.reactions));
  console.log('has _id:', !!m._id);
  // эмуляция кода из обработчика
  const em = '👍';
  const cur = (m.reactions && m.reactions[em]) || [];
  console.log('cur:', JSON.stringify(cur), 'typeof cur:', typeof cur);
  await c.close();
})();
