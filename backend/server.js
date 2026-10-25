const http = require("http");
const path = require("path");
const express = require("express");
const { WebSocketServer } = require("ws");

const app = express();
app.use(express.static(path.join(__dirname, "public")));

const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// rooms: roomId -> Set of sockets (max 2 for a 1-to-1 call)
const rooms = new Map();

wss.on("connection", (ws) => {
  ws.room = null;

  ws.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw);
    } catch {
      return;
    }

    if (msg.type === "join") {
      const room = String(msg.room || "").slice(0, 40);
      if (!room) return;
      const peers = rooms.get(room) || new Set();
      if (peers.size >= 2) {
        ws.send(JSON.stringify({ type: "full" }));
        return;
      }
      peers.add(ws);
      rooms.set(room, peers);
      ws.room = room;
      // First person waits; second person triggers the offer from the first.
      ws.send(JSON.stringify({ type: "joined", count: peers.size }));
      if (peers.size === 2) {
        for (const p of peers) {
          if (p !== ws) p.send(JSON.stringify({ type: "ready" }));
        }
      }
      return;
    }

    // offer / answer / candidate: relay to the other person in the room
    if (ws.room && ["offer", "answer", "candidate"].includes(msg.type)) {
      for (const p of rooms.get(ws.room) || []) {
        if (p !== ws && p.readyState === 1) p.send(JSON.stringify(msg));
      }
    }
  });

  ws.on("close", () => {
    if (!ws.room) return;
    const peers = rooms.get(ws.room);
    if (!peers) return;
    peers.delete(ws);
    for (const p of peers) p.send(JSON.stringify({ type: "left" }));
    if (peers.size === 0) rooms.delete(ws.room);
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Faceapp running on port ${PORT}`));
