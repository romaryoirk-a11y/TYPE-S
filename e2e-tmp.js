const { io } = require('socket.io-client');
const URL='http://localhost:3111';
function mkUser(name,un,pass){return new Promise((res,rej)=>{const s=io(URL);s.on('joined',d=>res({s,me:d.user,token:d.token}));s.on('auth-error',e=>rej(e.text));setTimeout(()=>rej('timeout '+un),9000);s.on('connect',()=>s.emit('register',{name,username:un,password:pass}));});}
(async()=>{try{
 const A=await mkUser('Анна Тестова','anna_e2e','test1234');console.log('[1] register OK:',A.me.name);
 const upd=new Promise((res,rej)=>{A.s.on('user-updated',d=>{if(d.userId===A.me.userId)res(d.user)});setTimeout(()=>rej('no user-updated'),6000);A.s.emit('update-profile',{name:'Анна Обновлённая',bio:'Я люблю Aster'});});
 const u2=await upd;console.log('[2] profile update OK:',u2.name,'| bio:',JSON.stringify(u2.bio));
 const B=await mkUser('Борис Тестов','boris_e2e','test1234');console.log('[3] second user OK:',B.me.name);
 const gotMsg=new Promise((res,rej)=>{B.s.on('message',m=>res(m));setTimeout(()=>rej('B не получил msg'),6000);setTimeout(()=>{A.s.emit('message',{to:'public',text:'Привет общий чат e2e'})},300);});
 const m=await gotMsg;console.log('[4] GENERAL CHAT OK:',m.to==='public','|',m.text);
 const sg=await new Promise((res,rej)=>{A.s.emit('stories-get',{},l=>res(l));setTimeout(()=>rej('stories-get timeout'),5000);});
 console.log('[5] stories-get OK, isArray:',Array.isArray(sg));
 const hist=await new Promise((res,rej)=>{B.s.on('history',h=>res(h));setTimeout(()=>rej('history timeout'),6000);});
 const inHist=Array.isArray(hist)&&hist.some(x=>x.to==='public'&&/общий чат e2e/.test(x.text||''));
 console.log('[6] history contains public msg:',inHist);
 console.log('ALL E2E CHECKS PASSED');process.exit(0);
}catch(e){console.error('E2E FAIL:',e&&e.message||e);process.exit(1);}})();
