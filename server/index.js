const express = require("express");
const http = require("http");
const { Server } = require("socket.io");

const app = express();
app.get("/", (_req, res) => res.send("AnonChat server is running"));
app.get("/health", (_req, res) => res.json({ ok: true }));

const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: ["https://potaaa89.github.io"],
    methods: ["GET", "POST"]
  },
  transports: ["websocket", "polling"]
});

const waiting = [];
const partnerOf = new Map();
const lastMessages = new Map();

function removeFromQueue(id) {
  const index = waiting.indexOf(id);
  if (index !== -1) waiting.splice(index, 1);
}
function leaveChat(id, notify = true) {
  removeFromQueue(id);
  const partner = partnerOf.get(id);
  partnerOf.delete(id);
  if (partner) {
    partnerOf.delete(partner);
    if (notify && io.sockets.sockets.has(partner)) {
      io.to(partner).emit("partner_left");
    }
  }
}
function findPartner(socket) {
  if (partnerOf.has(socket.id)) return;
  removeFromQueue(socket.id);
  while (waiting.length) {
    const candidateId = waiting.shift();
    const candidate = io.sockets.sockets.get(candidateId);
    if (candidate && candidate.connected && candidateId !== socket.id && !partnerOf.has(candidateId)) {
      partnerOf.set(socket.id, candidateId);
      partnerOf.set(candidateId, socket.id);
      socket.emit("matched");
      candidate.emit("matched");
      return;
    }
  }
  waiting.push(socket.id);
  socket.emit("searching");
}

io.on("connection", socket => {
  socket.on("find", () => findPartner(socket));
  socket.on("next", () => {
    leaveChat(socket.id, true);
    findPartner(socket);
  });
  socket.on("chat_message", raw => {
    const text = typeof raw === "string" ? raw.trim() : "";
    if (!text || text.length > 1000) return;
    const now = Date.now();
    const previous = lastMessages.get(socket.id) || [];
    const recent = previous.filter(t => now - t < 5000);
    if (recent.length >= 8) {
      socket.emit("rate_limited");
      return;
    }
    recent.push(now);
    lastMessages.set(socket.id, recent);
    const partner = partnerOf.get(socket.id);
    if (partner && io.sockets.sockets.has(partner)) {
      io.to(partner).emit("chat_message", { text, time: now });
    }
  });
  socket.on("report", reason => {
    const partner = partnerOf.get(socket.id);
    if (partner) console.log("Report received", { reporter: socket.id, reported: partner, reason: String(reason || "other").slice(0, 80), at: new Date().toISOString() });
    socket.emit("report_received");
    leaveChat(socket.id, true);
    findPartner(socket);
  });
  socket.on("disconnect", () => {
    leaveChat(socket.id, true);
    lastMessages.delete(socket.id);
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, "0.0.0.0", () => console.log("AnonChat listening on " + PORT));