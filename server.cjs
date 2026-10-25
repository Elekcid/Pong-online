'use strict';
// Small relay for an experimental two-player game. No external packages.
const http=require('node:http'),fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const rooms=new Map(),players=new Map();
function json(res,status,value){res.writeHead(status,{'Content-Type':'application/json','Cache-Control':'no-store'});res.end(JSON.stringify(value))}
function emit(p,data){if(p?.stream&&!p.stream.destroyed){if(p.stream.writableLength>128000){p.stream.destroy();return}p.stream.write('data: '+JSON.stringify(data)+'\n\n')}}
function closeRoom(room){if(!room||!rooms.has(room.code))return;rooms.delete(room.code);for(const p of room.players){players.delete(p.token);emit(p,{type:'left'});p.stream?.end()}}
const server=http.createServer(async(req,res)=>{
 const u=new URL(req.url,'http://localhost');
 if(req.method==='GET'&&u.pathname==='/events'){
  const p=players.get(u.searchParams.get('token'));if(!p)return json(res,403,{error:'Session expirée. Créez ou rejoignez un salon.'});
  if(p.stream)return json(res,409,{error:'Session déjà connectée.'});
  res.writeHead(200,{'Content-Type':'text/event-stream','Cache-Control':'no-cache, no-transform','Connection':'keep-alive','X-Accel-Buffering':'no'});res.flushHeaders();p.stream=res;p.seen=Date.now();emit(p,{type:'connected'});
  if(p.room.players.length===2&&p.room.players.every(x=>x.stream)){p.room.ready=true;for(const x of p.room.players)emit(x,{type:'ready',names:p.room.players.map(x=>x.name)})}
  res.on('close',()=>{p.stream=null;closeRoom(p.room)});return;
 }
 if(req.method==='POST'){
  // Cross-origin websites cannot drive a running game server through a browser.
  if(req.headers.origin&&req.headers.origin!==`${req.headers['x-forwarded-proto']||'http'}://${req.headers.host}`)return json(res,403,{error:'Origine refusée.'});
  let body='';try{for await(const chunk of req){body+=chunk;if(body.length>16384)return json(res,413,{error:'Message trop long.'})}body=JSON.parse(body||'{}')}catch{return json(res,400,{error:'Message invalide.'})}
  if(u.pathname==='/join'){
   const name=String(body.name||'Joueur').trim().slice(0,24)||'Joueur';let room;
   if(body.code){room=rooms.get(String(body.code).toUpperCase());if(!room)return json(res,404,{error:'Salon introuvable.'});if(room.players.length>=2)return json(res,409,{error:'Ce salon est complet.'})}
   else{if(rooms.size>=200)return json(res,503,{error:'Serveur occupé.'});let code;do{code=crypto.randomBytes(3).toString('hex').toUpperCase()}while(rooms.has(code));room={code,players:[],ready:false};rooms.set(code,room)}
   const p={token:crypto.randomBytes(24).toString('hex'),name,room,seat:room.players.length,stream:null,seen:Date.now(),window:Date.now(),messages:0};room.players.push(p);players.set(p.token,p);return json(res,200,{token:p.token,code:room.code,seat:p.seat});
  }
  const p=players.get(body.token);if(!p)return json(res,403,{error:'Session terminée.'});p.seen=Date.now();
  if(u.pathname==='/leave'){closeRoom(p.room);return json(res,200,{ok:true})}
  if(u.pathname!=='/send')return json(res,404,{error:'Route inconnue.'});
  if(Date.now()-p.window>1000){p.window=Date.now();p.messages=0}if(++p.messages>90)return json(res,429,{error:'Trop de messages.'});
  const m=body.message;if(!m||typeof m!=='object')return json(res,400,{error:'Message invalide.'});
  let message;
  if(p.seat===0&&m.type==='snapshot')message=m;
  else if(p.seat===1&&m.type==='input'&&Number.isFinite(m.value))message={type:'input',value:Math.max(-1,Math.min(1,m.value))};
  else if(m.type==='action'&&['pause','confirm'].includes(m.action))message={type:'action',action:m.action};
  else return json(res,400,{error:'Action refusée.'});
  if(p.room.ready)emit(p.room.players[1-p.seat],message);return json(res,200,{ok:true});
 }
 if(req.method==='GET'&&(u.pathname==='/'||u.pathname==='/index.html')){res.writeHead(200,{'Content-Type':'text/html; charset=utf-8','Cache-Control':'no-store'});fs.createReadStream(path.join(__dirname,'index.html')).pipe(res);return}
 json(res,404,{error:'Introuvable.'});
});
const timer=setInterval(()=>{for(const room of rooms.values()){if(room.players.some(p=>Date.now()-p.seen>15000)){closeRoom(room);continue}for(const p of room.players)p.stream?.write(': keepalive\n\n')}},5000);timer.unref();
server.listen(Number(process.env.PORT)||3000,process.env.HOST||'0.0.0.0',()=>console.log(`Pong prêt : http://localhost:${server.address().port} — Ctrl+C pour arrêter`));
module.exports={server,rooms,players};
