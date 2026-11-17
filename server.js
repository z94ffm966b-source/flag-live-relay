import http from "node:http";
import { WebSocketServer, WebSocket } from "ws";
import { TikTokLiveConnection, WebcastEvent, ControlEvent } from "tiktok-live-connector";

const PORT = Number(process.env.PORT || 10000);
const username = String(process.env.TIKTOK_USERNAME || "quizzzz123_").trim().replace(/^@/, "");
const clients = new Set();
let connection = null;
let tiktokConnected = false;
let reconnectTimer = null;
let connecting = false;
let shuttingDown = false;

const server = http.createServer(async (req, res) => {
  const requestUrl = new URL(req.url, "http://localhost");
  const pathname = requestUrl.pathname.replace(/\\/+$/, "");
  if (pathname === "/test-event") {
    if (process.env.ENABLE_TEST_EVENTS !== "true") {
      res.writeHead(404, { "content-type": "application/json; charset=utf-8" });
      res.end(JSON.stringify({ ok: false, error: "Test events disabled" }));
      return;
    }
    if (req.method !== "POST") {
      res.writeHead(405, { allow: "POST", "content-type": "application/json; charset=utf-8" });
      res.end(JSON.stringify({ ok: false, error: "Use POST" }));
      return;
    }
    if (process.env.TEST_EVENT_KEY && req.headers["x-test-key"] !== process.env.TEST_EVENT_KEY) {
      res.writeHead(401, { "content-type": "application/json; charset=utf-8" });
      res.end(JSON.stringify({ ok: false, error: "Invalid test key" }));
      return;
    }
    try {
      let raw = "";
      for await (const chunk of req) {
        raw += chunk;
        if (raw.length > 4096) throw new Error("Payload too large");
      }
      const event = JSON.parse(raw || "{}");
      const type = String(event.type || "").toLowerCase();
      const allowed = ["chat", "like", "subscribe", "gift"];
      if (!allowed.includes(type)) throw new Error("Unsupported test event type");
      const username = String(event.username || "JoueurTest").trim().slice(0, 30);
      let payload;
      if (type === "chat") {
        payload = { type, username, comment: String(event.comment || "A").slice(0, 200) };
      } else if (type === "like") {
        payload = { type, username, count: Math.max(1, Math.min(10000, Number(event.count) || 1)) };
      } else if (type === "subscribe") {
        payload = { type, username };
      } else {
        payload = { type, username, giftName: String(event.giftName || "Rose").slice(0, 60), count: Math.max(1, Math.min(99, Number(event.count) || 1)) };
      }
      broadcast(payload);
      res.writeHead(200, { "content-type": "application/json; charset=utf-8" });
      res.end(JSON.stringify({ ok: true, deliveredTo: clients.size, event: payload }));
    } catch (error) {
      res.writeHead(400, { "content-type": "application/json; charset=utf-8" });
      res.end(JSON.stringify({ ok: false, error: error.message || "Invalid request" }));
    }
    return;
  }
  if (pathname === "/health") {
    res.writeHead(200, { "content-type": "application/json; charset=utf-8" });
    res.end(JSON.stringify({
      ok: true,
      service: "quiz-culture-tiktok-relay",
      tiktokUsernameConfigured: Boolean(username),
      tiktokConnected
    }));
    return;
  }
  res.writeHead(200, { "content-type": "text/plain; charset=utf-8" });
  res.end("QUIZ CULTURE G — TikTok LIVE Relay");
});

const wss = new WebSocketServer({ server, path: "/ws" });

wss.on("connection", socket => {
  clients.add(socket);
  socket.send(JSON.stringify({ type: "relay_status", status: tiktokConnected ? "connected" : "waiting" }));
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
  }, 15000);
}

async function connectTikTok() {
  if (shuttingDown || connecting || connection || !username) return;
  connecting = true;
  try {
    const live = new TikTokLiveConnection(username, { enableExtendedGiftInfo: true });
    connection = live;

    live.on(ControlEvent.CONNECTED, state => {
      tiktokConnected = true;
      console.log("[TikTok] Connecté à @" + username + ", roomId:", state?.roomId || "inconnu");
      broadcast({ type: "relay_status", status: "connected" });
    });

    live.on(ControlEvent.DISCONNECTED, info => {
      console.log("[TikTok] Déconnecté:", info?.code || "", info?.reason || "");
      connection = null;
      tiktokConnected = false;
      broadcast({ type: "relay_status", status: "waiting" });
      scheduleReconnect();
    });

    live.on(ControlEvent.ERROR, error => {
      console.error("[TikTok] Erreur détaillée:", {
        name: error?.name,
        message: error?.message,
        info: error?.info,
        exception: error?.exception?.message,
        stack: error?.stack?.split("\n").slice(0, 5).join("\n")
      });
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
    console.error("[TikTok] Connexion impossible — diagnostic:", {
      name: error?.name,
      message: error?.message,
      info: error?.info,
      exception: error?.exception?.message,
      stack: error?.stack?.split("\n").slice(0, 7).join("\n")
    });
    connection = null;
    tiktokConnected = false;
    broadcast({ type: "relay_status", status: "waiting" });
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
  tiktokConnected = false;
  try { await connection?.disconnect(); } catch {}
  for (const client of clients) client.close();
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 5000).unref();
}
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
