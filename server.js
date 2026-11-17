import http from "node:http";
import { WebSocketServer, WebSocket } from "ws";
import { TikTokLiveConnection, WebcastEvent, ControlEvent } from "tiktok-live-connector";

const PORT = Number(process.env.PORT || 10000);
const username = String(process.env.TIKTOK_USERNAME || "").trim().replace(/^@/, "");
const clients = new Set();
let connection = null;
let reconnectTimer = null;
let connecting = false;
let shuttingDown = false;

const server = http.createServer((req, res) => {
  if (req.url === "/health") {
    res.writeHead(200, { "content-type": "application/json; charset=utf-8" });
    res.end(JSON.stringify({
      ok: true,
      service: "quiz-culture-tiktok-relay",
      tiktokUsernameConfigured: Boolean(username),
      tiktokConnected: Boolean(connection)
    }));
    return;
  }
  res.writeHead(200, { "content-type": "text/plain; charset=utf-8" });
  res.end("QUIZ CULTURE G — TikTok LIVE Relay");
});

const wss = new WebSocketServer({ server, path: "/ws" });

wss.on("connection", socket => {
  clients.add(socket);
  socket.send(JSON.stringify({ type: "relay_status", status: connection ? "connected" : "waiting" }));
  socket.on("close", () => clients.delete(socket));
  socket.on("error", () => clients.delete(socket));
});

function broadcast(payload) {
  const message = JSON.stringify(payload);
  for (const client of clients) {
    if (client.readyState === WebSocket.OPEN) client.send(message);
  }
}

function displayName(user) {
  return String(user?.uniqueId || user?.displayId || user?.nickname || "").trim();
}

function scheduleReconnect() {
  if (shuttingDown || reconnectTimer || !username) return;
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    connectTikTok();
  }, 30000);
}

async function connectTikTok() {
  if (shuttingDown || connecting || connection || !username) return;
  connecting = true;
  try {
    const live = new TikTokLiveConnection(username, { enableExtendedGiftInfo: true });
    connection = live;

    live.on(ControlEvent.CONNECTED, state => {
      console.log("[TikTok] Connecté, roomId:", state?.roomId || "inconnu");
      broadcast({ type: "relay_status", status: "connected" });
    });

    live.on(ControlEvent.DISCONNECTED, info => {
      console.log("[TikTok] Déconnecté:", info?.code || "", info?.reason || "");
      connection = null;
      broadcast({ type: "relay_status", status: "disconnected" });
      scheduleReconnect();
    });

    live.on(ControlEvent.ERROR, error => {
      console.error("[TikTok] Erreur:", error?.info || error?.message || error);
    });

    live.on(WebcastEvent.CHAT, data => {
      const name = displayName(data?.user);
      if (name && data?.comment) {
        broadcast({ type: "chat", username: name, comment: String(data.comment) });
      }
    });

    live.on(WebcastEvent.LIKE, data => {
      const count = Number(data?.likeCount || 0);
      if (count > 0) broadcast({ type: "like", username: displayName(data?.user), count });
    });

    live.on(WebcastEvent.SUB_NOTIFY, data => {
      const name = displayName(data?.user);
      if (name) broadcast({ type: "subscribe", username: name });
    });

    live.on(WebcastEvent.GIFT, data => {
      const giftType = Number(data?.gift?.type ?? data?.giftDetails?.giftType ?? 0);
      if (giftType === 1 && Number(data?.repeatEnd) !== 1) return;
      const name = displayName(data?.user);
      const giftName = String(data?.gift?.name || data?.giftDetails?.giftName || data?.extendedGiftInfo?.name || "");
      if (!name || !giftName) return;
      broadcast({
        type: "gift",
        username: name,
        giftName,
        count: Number(data?.repeatCount || 1),
        giftId: Number(data?.giftId || 0)
      });
    });

    await live.connect();
  } catch (error) {
    console.error("[TikTok] Connexion impossible:", error?.message || error);
    connection = null;
    scheduleReconnect();
  } finally {
    connecting = false;
  }
}

server.listen(PORT, "0.0.0.0", () => {
  console.log(`[Relay] Serveur prêt sur le port ${PORT}`);
  if (!username) {
    console.log("[Relay] Configure TIKTOK_USERNAME dans les variables d’environnement Render.");
  } else {
    console.log("[Relay] Compte TikTok public configuré :", "@" + username);
    connectTikTok();
  }
});

async function shutdown() {
  shuttingDown = true;
  if (reconnectTimer) clearTimeout(reconnectTimer);
  try { await connection?.disconnect(); } catch {}
  for (const client of clients) client.close();
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 5000).unref();
}
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
