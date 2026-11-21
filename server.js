const http = require("http");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8080;

// HTTP 서버 생성 (무료 클라우드 헬스체크 및 기본 응답용)
const server = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain; charset=utf-8" });
  res.end("Coin Coast Racing WebSocket Relay Server is running.");
});

const wss = new WebSocketServer({ server });

// roomCode -> Set of { ws, clientId, room }
const rooms = new Map();

function joinRoom(client, nextRoom) {
  const roomKey = String(nextRoom || "7777").trim() || "7777";
  if (client.room && rooms.has(client.room)) {
    rooms.get(client.room).delete(client);
    if (rooms.get(client.room).size === 0) {
      rooms.delete(client.room);
    }
  }
  client.room = roomKey;
  if (!rooms.has(roomKey)) {
    rooms.set(roomKey, new Set());
  }
  rooms.get(roomKey).add(client);
}

function broadcastToRoom(senderClient, rawMessage, roomKey) {
  const targetRoom = rooms.get(roomKey);
  if (!targetRoom) return;

  for (const peer of targetRoom) {
    if (peer !== senderClient && peer.ws.readyState === 1) {
      peer.ws.send(rawMessage);
    }
  }
}

wss.on("connection", (ws) => {
  const client = {
    ws,
    clientId: null,
    room: "7777"
  };
  joinRoom(client, client.room);

  ws.on("message", (data) => {
    const text = data.toString();
    let parsed = null;
    try {
      parsed = JSON.parse(text);
    } catch (_) {
      return;
    }
    if (!parsed || typeof parsed !== "object") return;

    if (parsed.clientId) {
      client.clientId = parsed.clientId;
    }

    const targetRoomKey = String(parsed.room || client.room || "7777").trim() || "7777";
    if (targetRoomKey !== client.room) {
      joinRoom(client, targetRoomKey);
    }

    // 같은 방의 다른 기기들에게 브로드캐스트
    broadcastToRoom(client, text, targetRoomKey);
  });

  ws.on("close", () => {
    const roomKey = client.room;
    if (roomKey && rooms.has(roomKey)) {
      rooms.get(roomKey).delete(client);
      if (client.clientId) {
        const leavePayload = JSON.stringify({
          type: "LEAVE",
          clientId: client.clientId,
          room: roomKey,
          ts: Date.now()
        });
        broadcastToRoom(client, leavePayload, roomKey);
      }
      if (rooms.get(roomKey).size === 0) {
        rooms.delete(roomKey);
      }
    }
  });
});

server.listen(PORT, () => {
  console.log(`WebSocket server listening on port ${PORT}`);
});