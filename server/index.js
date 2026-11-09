const express=require("express");
const http=require("http");
const {Server}=require("socket.io");
const {Pool}=require("pg");
const bcrypt=require("bcryptjs");
const jwt=require("jsonwebtoken");
const crypto=require("crypto");
const app=express();
app.use(express.json({limit:"32kb"}));
const pool=process.env.DATABASE_URL?new Pool({connectionString:process.env.DATABASE_URL,ssl:process.env.DATABASE_URL.includes("localhost")?false:{rejectUnauthorized:false}}):null;
const JWT_SECRET=process.env.JWT_SECRET||crypto.randomBytes(32).toString("hex");
async function initDb(){if(!pool)return;await pool.query(`CREATE TABLE IF NOT EXISTS users(id BIGSERIAL PRIMARY KEY,email TEXT UNIQUE NOT NULL,password_hash TEXT NOT NULL,nickname TEXT NOT NULL DEFAULT 'Гость',age SMALLINT NOT NULL DEFAULT 18,gender TEXT NOT NULL DEFAULT 'any',country TEXT NOT NULL DEFAULT '',language TEXT NOT NULL DEFAULT 'ru',created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())`)}
function auth(req,res,next){try{req.user=jwt.verify((req.headers.authorization||"").replace(/^Bearer /i,""),JWT_SECRET);next()}catch(e){res.status(401).json({error:"Нужно войти в аккаунт."})}}
function publicUser(u){return {id:u.id,email:u.email,nickname:u.nickname,age:u.age,gender:u.gender,country:u.country,language:u.language}}
app.post("/api/register",async(req,res)=>{try{if(!pool)return res.status(503).json({error:"База данных ещё не подключена на сервере."});const email=String(req.body.email||"").trim().toLowerCase(),password=String(req.body.password||""),nickname=String(req.body.nickname||"Гость").trim().slice(0,24)||"Гость";if(!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email))return res.status(400).json({error:"Укажи корректный email."});if(password.length<8)return res.status(400).json({error:"Пароль должен содержать минимум 8 символов."});const hash=await bcrypt.hash(password,12);const q=await pool.query("INSERT INTO users(email,password_hash,nickname) VALUES($1,$2,$3) RETURNING *",[email,hash,nickname]);const u=q.rows[0],token=jwt.sign({id:u.id,email:u.email},JWT_SECRET,{expiresIn:"30d"});res.json({token,user:publicUser(u)})}catch(e){if(e.code==="23505")return res.status(409).json({error:"Этот email уже зарегистрирован."});console.error(e);res.status(500).json({error:"Не удалось создать аккаунт."})}});
app.post("/api/login",async(req,res)=>{try{if(!pool)return res.status(503).json({error:"База данных ещё не подключена на сервере."});const email=String(req.body.email||"").trim().toLowerCase(),password=String(req.body.password||"");const q=await pool.query("SELECT * FROM users WHERE email=$1",[email]);const u=q.rows[0];if(!u||!(await bcrypt.compare(password,u.password_hash)))return res.status(401).json({error:"Неверный email или пароль."});const token=jwt.sign({id:u.id,email:u.email},JWT_SECRET,{expiresIn:"30d"});res.json({token,user:publicUser(u)})}catch(e){console.error(e);res.status(500).json({error:"Не удалось выполнить вход."})}});
app.get("/api/me",auth,async(req,res)=>{try{const q=await pool.query("SELECT * FROM users WHERE id=$1",[req.user.id]);if(!q.rows[0])return res.status(404).json({error:"Аккаунт не найден."});res.json({user:publicUser(q.rows[0])})}catch(e){res.status(500).json({error:"Ошибка профиля."})}});
app.put("/api/me",auth,async(req,res)=>{try{const b=req.body,age=Math.max(18,Math.min(99,Number(b.age)||18)),gender=["male","female","any"].includes(b.gender)?b.gender:"any";const q=await pool.query("UPDATE users SET nickname=$1,age=$2,gender=$3,country=$4,language=$5 WHERE id=$6 RETURNING *",[String(b.nickname||"Гость").trim().slice(0,24)||"Гость",age,gender,String(b.country||"").slice(0,50),["ru","uk","en"].includes(b.language)?b.language:"ru",req.user.id]);res.json({user:publicUser(q.rows[0])})}catch(e){res.status(500).json({error:"Не удалось сохранить профиль."})}});
app.get("/",(_req,res)=>res.send("AnonChat server is running"));
app.get("/health",(_req,res)=>res.json({ok:true}));
const server=http.createServer(app);
const io=new Server(server,{cors:{origin:["https://potaaa89.github.io"],methods:["GET","POST"]},transports:["websocket","polling"]});
const waiting=[],partnerOf=new Map(),lastMessages=new Map(),callState=new Map(),searchPrefs=new Map();
function removeFromQueue(id){const i=waiting.indexOf(id);if(i!==-1)waiting.splice(i,1)}
function relay(socket,event,payload){const p=partnerOf.get(socket.id);if(p&&io.sockets.sockets.has(p))io.to(p).emit(event,payload)}
function endCall(id,notify=true){const p=partnerOf.get(id);callState.delete(id);if(p){callState.delete(p);if(notify&&io.sockets.sockets.has(p))io.to(p).emit("call_ended")}}
function leaveChat(id,notify=true){removeFromQueue(id);endCall(id,notify);const p=partnerOf.get(id);partnerOf.delete(id);if(p){partnerOf.delete(p);if(notify&&io.sockets.sockets.has(p))io.to(p).emit("partner_left")}}
function compatible(a,b){const ap=a?.profile||{},bp=b?.profile||{},af=a?.filters||{},bf=b?.filters||{};const ok=(f,v)=>!f||f==="any"||f===v;return ok(af.gender,bp.gender)&&(!af.country||af.country===bp.country)&&ok(af.language,bp.language)&&ok(bf.gender,ap.gender)&&(!bf.country||bf.country===ap.country)&&ok(bf.language,ap.language)}
function findPartner(socket,prefs){if(partnerOf.has(socket.id))return;if(prefs)searchPrefs.set(socket.id,{profile:prefs.profile||{},filters:prefs.filters||{}});const mine=searchPrefs.get(socket.id)||{profile:{},filters:{}};removeFromQueue(socket.id);for(let i=0;i<waiting.length;i++){const cid=waiting[i],c=io.sockets.sockets.get(cid),other=searchPrefs.get(cid);if(c&&c.connected&&cid!==socket.id&&!partnerOf.has(cid)&&compatible(mine,other)){waiting.splice(i,1);partnerOf.set(socket.id,cid);partnerOf.set(cid,socket.id);socket.emit("matched");c.emit("matched");return}}if(!waiting.includes(socket.id))waiting.push(socket.id);socket.emit("searching")}
io.on("connection",socket=>{
 socket.on("find",prefs=>findPartner(socket,prefs));
 socket.on("next",prefs=>{leaveChat(socket.id,true);findPartner(socket,prefs)});
 socket.on("chat_message",raw=>{const t=typeof raw==="string"?raw.trim():"";if(!t||t.length>1000)return;const now=Date.now(),recent=(lastMessages.get(socket.id)||[]).filter(x=>now-x<5000);if(recent.length>=8)return socket.emit("rate_limited");recent.push(now);lastMessages.set(socket.id,recent);relay(socket,"chat_message",{text:t,time:now})});
 socket.on("call_request",mode=>{const p=partnerOf.get(socket.id);if(!p||!io.sockets.sockets.has(p)||callState.has(socket.id)||callState.has(p))return;const m=mode==="audio"?"audio":"video";callState.set(socket.id,"calling");callState.set(p,"incoming");io.to(p).emit("call_incoming",{mode:m})});
 socket.on("call_accept",()=>{const p=partnerOf.get(socket.id);if(!p||callState.get(p)!=="calling"||callState.get(socket.id)!=="incoming")return;callState.set(socket.id,"active");callState.set(p,"active");io.to(p).emit("call_accepted")});
 socket.on("call_reject",()=>{const p=partnerOf.get(socket.id);if(p){callState.delete(socket.id);callState.delete(p);io.to(p).emit("call_rejected")}});
 socket.on("call_cancel",()=>{const p=partnerOf.get(socket.id);endCall(socket.id,true);if(p)io.to(p).emit("call_cancelled")});
 socket.on("call_end",()=>endCall(socket.id,true));
 socket.on("webrtc_offer",d=>relay(socket,"webrtc_offer",d));
 socket.on("webrtc_answer",d=>relay(socket,"webrtc_answer",d));
 socket.on("webrtc_ice",d=>relay(socket,"webrtc_ice",d));
 socket.on("report",reason=>{const p=partnerOf.get(socket.id);if(p)console.log("Report",{reporter:socket.id,reported:p,reason:String(reason||"other").slice(0,80),at:new Date().toISOString()});socket.emit("report_received");leaveChat(socket.id,true);findPartner(socket)});
 socket.on("disconnect",()=>{leaveChat(socket.id,true);lastMessages.delete(socket.id);searchPrefs.delete(socket.id)});
});
const PORT=process.env.PORT||3000;initDb().then(()=>server.listen(PORT,"0.0.0.0",()=>console.log("AnonChat listening on "+PORT))).catch(e=>{console.error("Database initialization failed",e);process.exit(1)});