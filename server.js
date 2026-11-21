const http = require("http");
const fs = require("fs");
const path = require("path");
const WebSocket = require("ws");

const PORT = process.env.PORT || 10000;
const LEADERBOARD_FILE = path.join(__dirname, "leaderboard.json");

let top5Leaderboard = [];

function loadLeaderboard() {
  try {
    if (fs.existsSync(LEADERBOARD_FILE)) {
      const raw = fs.readFileSync(LEADERBOARD_FILE, "utf8");
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed)) {
        top5Leaderboard = parsed
          .slice()
          .sort((a, b) => Number(b.score || 0) - Number(a.score || 0))
          .slice(0, 5);
      }
    }
  } catch (_) {
    top5Leaderboard = [];
  }
}

function saveLeaderboard() {
  try {
    fs.writeFileSync(LEADERBOARD_FILE, JSON.stringify(top5Leaderboard, null, 2), "utf8");
  } catch (_) {}
}

function mergeLeaderboardEntries(incomingEntries) {
  if (!Array.isArray(incomingEntries)) return top5Leaderboard;
  const combined = [...top5Leaderboard, ...incomingEntries]
    .filter((item) => item && typeof item.score === "number" && item.score > 0)
    .map((item) => ({
      name: String(item.name || "드라이버").slice(0, 16),
      score: Number(item.score || 0),
      carName: String(item.carName || "스포츠카").slice(0, 20),
      room: String(item.room || "7777").slice(0, 6),
      timeSec: Number(item.timeSec || 0)
    }));

  const seen = new Set();
  const deduped = [];
  combined.sort((a, b) => b.score - a.score);
  for (const entry of combined) {
    const key = `${entry.name}__${entry.score}__${entry.room}__${entry.timeSec}`;
    if (seen.has(key)) continue;
    seen.add(key);
    deduped.push(entry);
    if (deduped.length >= 5) break;
  }

  top5Leaderboard = deduped;
  saveLeaderboard();
  return top5Leaderboard;
}

loadLeaderboard();

// rooms: Map<roomCode, Map<sessionId, { ws, player }>>
const rooms = new Map();

function getRoom(roomCode) {
  if (!rooms.has(roomCode)) {
    rooms.set(roomCode, new Map());
  }
  return rooms.get(roomCode);
}

function broadcastToRoom(roomCode, payload, excludeSessionId = null) {
  const room = rooms.get(roomCode);
  if (!room) return;
  const message = JSON.stringify(payload);
  for (const [sid, client] of room.entries()) {
    if (excludeSessionId && sid === excludeSessionId) continue;
    if (client.ws && client.ws.readyState === WebSocket.OPEN) {
      client.ws.send(message);
    }
  }
}

function broadcastAll(payload) {
  const message = JSON.stringify(payload);
  for (const [, room] of rooms.entries()) {
    for (const [, client] of room.entries()) {
      if (client.ws && client.ws.readyState === WebSocket.OPEN) {
        client.ws.send(message);
      }
    }
  }
}

function sendRoomRoster(roomCode) {
  const room = rooms.get(roomCode);
  if (!room) return;
  const players = [];
  for (const [, client] of room.entries()) {
    if (client.player) {
      players.push(client.player);
    }
  }
  broadcastToRoom(roomCode, {
    type: "ROOM_ROSTER",
    roomCode,
    players
  });
}

const server = http.createServer((req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");

  if (req.method === "OPTIONS") {
    res.writeHead(204);
    res.end();
    return;
  }

  const reqUrl = new URL(req.url || "/", `http://${req.headers.host || "localhost"}`);

  if (reqUrl.pathname === "/leaderboard" && req.method === "GET") {
    res.writeHead(200, { "Content-Type": "application/json; charset=utf-8" });
    res.end(JSON.stringify({ entries: top5Leaderboard }));
    return;
  }

  if (reqUrl.pathname === "/leaderboard" && req.method === "POST") {
    let body = "";
    req.on("data", (chunk) => {
      body += chunk.toString();
      if (body.length > 1e6) req.destroy();
    });
    req.on("end", () => {
      try {
        const parsed = JSON.parse(body || "{}");
        const updated = mergeLeaderboardEntries(parsed.entries || []);
        broadcastAll({ type: "LEADERBOARD_UPDATE", entries: updated });
        res.writeHead(200, { "Content-Type": "application/json; charset=utf-8" });
        res.end(JSON.stringify({ ok: true, entries: updated }));
      } catch (_) {
        res.writeHead(400, { "Content-Type": "application/json; charset=utf-8" });
        res.end(JSON.stringify({ ok: false, error: "Invalid JSON" }));
      }
    });
    return;
  }

  res.writeHead(200, { "Content-Type": "text/plain; charset=utf-8" });
  res.end("Coin Coast Racing WebSocket & Leaderboard Server Running");
});

const wss = new WebSocket.Server({ server });

wss.on("connection", (ws, req) => {
  const url = new URL(req.url || "/", `http://${req.headers.host || "localhost"}`);
  const initialRoom = (url.searchParams.get("room") || "7777").trim();
  const sessionId = url.searchParams.get("sessionId") || `drv_${Date.now()}_${Math.random().toString(36).slice(2, 6)}`;

  let activeRoomCode = initialRoom;
  getRoom(activeRoomCode).set(sessionId, { ws, player: null });

  ws.send(JSON.stringify({ type: "LEADERBOARD_UPDATE", entries: top5Leaderboard }));

  ws.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch (_) {
      return;
    }
    if (!msg || !msg.type) return;

    if (msg.type === "PLAYER_JOIN" && msg.player) {
      const targetRoom = String(msg.player.roomCode || activeRoomCode || "7777").trim();
      if (targetRoom !== activeRoomCode) {
        const prevRoom = rooms.get(activeRoomCode);
        if (prevRoom) {
          prevRoom.delete(sessionId);
          broadcastToRoom(activeRoomCode, { type: "PLAYER_LEAVE", sessionId });
          sendRoomRoster(activeRoomCode);
        }
        activeRoomCode = targetRoom;
      }

      const room = getRoom(activeRoomCode);
      room.set(sessionId, {
        ws,
        player: {
          sessionId,
          name: String(msg.player.name || "드라이버").slice(0, 16),
          carId: String(msg.player.carId || "apex_gt"),
          roomCode: activeRoomCode,
          x: Number(msg.player.x ?? 0),
          z: Number(msg.player.z ?? -4),
          score: Number(msg.player.score ?? 0),
          hp: Number(msg.player.hp ?? 100),
          lastSeen: Date.now()
        }
      });
      sendRoomRoster(activeRoomCode);
      return;
    }

    if (msg.type === "PLAYER_STATE") {
      const room = getRoom(activeRoomCode);
      const entry = room.get(sessionId);
      if (entry && entry.player) {
        if (typeof msg.x === "number") entry.player.x = msg.x;
        if (typeof msg.z === "number") entry.player.z = msg.z;
        if (typeof msg.score === "number") entry.player.score = msg.score;
        if (typeof msg.hp === "number") entry.player.hp = msg.hp;
        entry.player.lastSeen = Date.now();
      }

      broadcastToRoom(
        activeRoomCode,
        {
          type: "PLAYER_STATE",
          sessionId,
          x: msg.x,
          z: msg.z,
          score: msg.score,
          hp: msg.hp
        },
        sessionId
      );
      return;
    }

    if (msg.type === "ROOM_START") {
      const targetRoom = String(msg.roomCode || activeRoomCode || "7777").trim();
      broadcastToRoom(targetRoom, {
        type: "GRID_START",
        senderId: sessionId,
        room: targetRoom,
        carId: msg.carId || "apex_gt",
        courseSeed: msg.courseSeed
      });
      return;
    }

    if (msg.type === "PLAYER_LEAVE") {
      const room = rooms.get(activeRoomCode);
      if (room && room.has(sessionId)) {
        room.delete(sessionId);
        broadcastToRoom(activeRoomCode, { type: "PLAYER_LEAVE", sessionId });
        sendRoomRoster(activeRoomCode);
      }
    }
  });

  ws.on("close", () => {
    const room = rooms.get(activeRoomCode);
    if (!room) return;
    room.delete(sessionId);
    broadcastToRoom(activeRoomCode, { type: "PLAYER_LEAVE", sessionId });
    sendRoomRoster(activeRoomCode);
    if (room.size === 0) {
      rooms.delete(activeRoomCode);
    }
  });
});

server.listen(PORT, () => {
  console.log(`WebSocket & Leaderboard server listening on port ${PORT}`);
});