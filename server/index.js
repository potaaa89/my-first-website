const express=require("express");
const http=require("http");
const {Server}=require("socket.io");
const app=express();
app.get("/",(_req,res)=>res.send("AnonChat server is running"));
app.get("/health",(_req,res)=>res.json({ok:true}));
const server=http.createServer(app);
const io=new Server(server,{cors:{origin:["https://potaaa89.github.io"],methods:["GET","POST"]},transports:["websocket","polling"]});
const waiting=[],partnerOf=new Map(),lastMessages=new Map(),callState=new Map();
function removeFromQueue(id){const i=waiting.indexOf(id);if(i!==-1)waiting.splice(i,1)}
function relay(socket,event,payload){const p=partnerOf.get(socket.id);if(p&&io.sockets.sockets.has(p))io.to(p).emit(event,payload)}
function endCall(id,notify=true){const p=partnerOf.get(id);callState.delete(id);if(p){callState.delete(p);if(notify&&io.sockets.sockets.has(p))io.to(p).emit("call_ended")}}
function leaveChat(id,notify=true){removeFromQueue(id);endCall(id,notify);const p=partnerOf.get(id);partnerOf.delete(id);if(p){partnerOf.delete(p);if(notify&&io.sockets.sockets.has(p))io.to(p).emit("partner_left")}}
function findPartner(socket){if(partnerOf.has(socket.id))return;removeFromQueue(socket.id);while(waiting.length){const cid=waiting.shift(),c=io.sockets.sockets.get(cid);if(c&&c.connected&&cid!==socket.id&&!partnerOf.has(cid)){partnerOf.set(socket.id,cid);partnerOf.set(cid,socket.id);socket.emit("matched");c.emit("matched");return}}waiting.push(socket.id);socket.emit("searching")}
io.on("connection",socket=>{
 socket.on("find",()=>findPartner(socket));
 socket.on("next",()=>{leaveChat(socket.id,true);findPartner(socket)});
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
 socket.on("disconnect",()=>{leaveChat(socket.id,true);lastMessages.delete(socket.id)});
});
const PORT=process.env.PORT||3000;server.listen(PORT,"0.0.0.0",()=>console.log("AnonChat listening on "+PORT));