const { MongoClient } = require('mongodb');
(async () => {
  const uri = process.argv[2];
  const c = new MongoClient(uri);
  await c.connect();
  const dbo = c.db();
  console.log('collections:', (await dbo.listCollections().toArray()).map(x=>x.name).join(','));
  console.log('users:', await dbo.collection('users').countDocuments());
  console.log('messages:', await dbo.collection('messages').countDocuments());
  const msgs = await dbo.collection('messages').find({}).toArray();
  console.log(JSON.stringify(msgs.map(m=>({id:m.id,to:m.to,text:m.text})), null, 1));
  await c.close();
})().catch(e=>{console.error('ERR',e.message);process.exit(1)});
