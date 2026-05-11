/**
 * Farid Villa – Built-in MQTT Broker
 *
 * Single port architecture — works on Railway's restricted environment.
 *
 * The HTTP server's underlying net.Server receives ALL TCP connections.
 * We intercept BEFORE Node's HTTP parser by listening on the net.Server
 * 'connection' event and peeking at the first byte:
 *
 *   0x10 = MQTT CONNECT → handle as raw MQTT (ESP32)
 *   other = HTTP/WS     → let Node's HTTP server handle it normally
 *
 * WebSocket MQTT (browser) is handled via ws library on the HTTP server.
 */

const WebSocket = require('ws');

// ── State ─────────────────────────────────────────────────────
const subscriptions   = new Map();
const retained        = new Map();
const backendHandlers = new Map();

// ── MQTT Encoding ─────────────────────────────────────────────
function encodeVarLen(n) {
  const out = [];
  do {
    let b = n % 128;
    n = Math.floor(n / 128);
    if (n > 0) b |= 0x80;
    out.push(b);
  } while (n > 0);
  return Buffer.from(out);
}

function decodeVarLen(buf, offset) {
  let mult = 1, val = 0, i = 0, b;
  do {
    if (offset + i >= buf.length) return { len: 0, bytes: 1 };
    b = buf[offset + i];
    val += (b & 0x7f) * mult;
    mult *= 128;
    i++;
  } while ((b & 0x80) && i < 4);
  return { len: val, bytes: i };
}

// ── Packet builders ───────────────────────────────────────────
const CONNACK  = Buffer.from([0x20, 0x02, 0x00, 0x00]);
const PINGRESP = Buffer.from([0xd0, 0x00]);

function suback(pid) {
  return Buffer.concat([Buffer.from([0x90, 0x03]), pid, Buffer.from([0x00])]);
}

function pubPacket(topic, payload, retain) {
  const t = Buffer.from(topic);
  const p = Buffer.isBuffer(payload) ? payload : Buffer.from(String(payload));
  const rl = 2 + t.length + p.length;
  return Buffer.concat([
    Buffer.from([retain ? 0x31 : 0x30]),
    encodeVarLen(rl),
    Buffer.from([t.length >> 8, t.length & 0xff]),
    t, p,
  ]);
}

// ── Send ──────────────────────────────────────────────────────
function send(client, pkt) {
  try {
    if (client && typeof client.write === 'function' && !client.destroyed)
      client.write(pkt);
    else if (client && typeof client.send === 'function' && client.readyState === WebSocket.OPEN)
      client.send(pkt);
  } catch (_) {}
}

// ── Pub/Sub ───────────────────────────────────────────────────
function addSub(client, topic) {
  if (!subscriptions.has(client)) subscriptions.set(client, []);
  const list = subscriptions.get(client);
  if (!list.includes(topic)) list.push(topic);
}

function drop(client) { subscriptions.delete(client); }

function publish(topic, payload, retain = false) {
  if (retain) {
    payload.length === 0 ? retained.delete(topic) : retained.set(topic, payload);
  }
  const pkt = pubPacket(topic, payload);
  for (const [c, topics] of subscriptions)
    if (topics.includes(topic)) send(c, pkt);

  console.log(`[Broker] >> ${topic}: ${payload.toString().slice(0, 80)}`);

  const h = backendHandlers.get(topic);
  if (h) try { h(topic, payload.toString()); } catch (e) { console.error('[Broker]', e.message); }
}

// ── MQTT packet processing ────────────────────────────────────
function processBuffer(client, buf) {
  let pos = 0;
  while (pos < buf.length) {
    if (pos + 1 >= buf.length) break;
    const b0  = buf[pos];
    const typ = b0 >> 4;
    const flg = b0 & 0x0f;
    const { len: remLen, bytes: lb } = decodeVarLen(buf, pos + 1);
    const hLen  = 1 + lb;
    const total = hLen + remLen;
    if (pos + total > buf.length) break;
    const off = pos + hLen;

    switch (typ) {
      case 1: { // CONNECT
        try {
          const pnl = (buf[off] << 8) | buf[off + 1];
          const cidOff = off + 2 + pnl + 4;
          const cidLen = (buf[cidOff] << 8) | buf[cidOff + 1];
          console.log('[Broker] CONNECT', buf.slice(cidOff + 2, cidOff + 2 + cidLen).toString());
        } catch (_) { console.log('[Broker] CONNECT'); }
        send(client, CONNACK);
        break;
      }
      case 3: { // PUBLISH
        const tl    = (buf[off] << 8) | buf[off + 1];
        const topic = buf.slice(off + 2, off + 2 + tl).toString();
        const data  = buf.slice(off + 2 + tl, off + remLen);
        publish(topic, data, (flg & 0x01) !== 0);
        break;
      }
      case 8: { // SUBSCRIBE
        const end = off + remLen;
        const pid = buf.slice(off, off + 2);
        let i = off + 2;
        while (i < end) {
          const tl    = (buf[i] << 8) | buf[i + 1];
          const topic = buf.slice(i + 2, i + 2 + tl).toString();
          i += 2 + tl + 1;
          addSub(client, topic);
          console.log('[Broker] SUBSCRIBE', topic);
          if (retained.has(topic)) send(client, pubPacket(topic, retained.get(topic), true));
        }
        send(client, suback(pid));
        break;
      }
      case 12: send(client, PINGRESP); break; // PINGREQ
      case 14: drop(client); break;           // DISCONNECT
    }
    pos += total;
  }
}

// ── Handle a raw TCP MQTT socket (ESP32) ──────────────────────
// Called by index.js mux with the socket and the already-read first chunk
function handleTcpClient(socket, firstChunk) {
  console.log('[Broker] ESP32 TCP connected:', socket.remoteAddress);
  subscriptions.set(socket, []);
  let buf = firstChunk ? Buffer.from(firstChunk) : Buffer.alloc(0);

  // Process the first chunk immediately
  if (buf.length > 0) {
    processBuffer(socket, buf);
    buf = Buffer.alloc(0);
  }

  socket.on('data', (chunk) => {
    buf = Buffer.concat([buf, chunk]);
    processBuffer(socket, buf);
    buf = Buffer.alloc(0);
  });
  socket.on('close',   () => { drop(socket); console.log('[Broker] ESP32 disconnected'); });
  socket.on('error',   () => drop(socket));
  socket.setTimeout(0);
}

// ── Public API ────────────────────────────────────────────────
function backendPublish(topic, payload, retain = false) {
  const str = typeof payload === 'object' ? JSON.stringify(payload) : String(payload);
  publish(topic, Buffer.from(str), retain);
}

function backendSubscribe(topic, handler) {
  backendHandlers.set(topic, handler);
}

/**
 * attachBroker(httpServer)
 * Only handles WebSocket MQTT (browser).
 * TCP MQTT (ESP32) is handled by handleTcpClient called from index.js mux.
 */
function attachBroker(httpServer) {
  const wss = new WebSocket.Server({ server: httpServer });
  wss.on('connection', (ws) => {
    console.log('[Broker] Browser WS connected');
    subscriptions.set(ws, []);
    ws.on('message', (data) => processBuffer(ws, Buffer.isBuffer(data) ? data : Buffer.from(data)));
    ws.on('close',   () => { drop(ws); console.log('[Broker] Browser WS disconnected'); });
    ws.on('error',   () => drop(ws));
  });
  console.log('[Broker] WebSocket MQTT ready');
}

module.exports = { attachBroker, backendPublish, backendSubscribe, handleTcpClient };
