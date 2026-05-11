/**
 * Farid Villa – Built-in MQTT Broker
 *
 * Two transports:
 *   1. WebSocket  — attached to the Express HTTP server (browser frontend)
 *   2. TCP        — dedicated server on TCP_INTERNAL_PORT (ESP32 via Railway proxy)
 *
 * Railway TCP proxy points to TCP_INTERNAL_PORT (1883).
 * Railway HTTP port (PORT) is separate — no conflict.
 */

const net = require('net');
const WebSocket = require('ws');

// Internal TCP port for ESP32 MQTT connections
// Railway TCP proxy must point to this port
const TCP_INTERNAL_PORT = parseInt(process.env.TCP_INTERNAL_PORT) || 1885;

// ── State ─────────────────────────────────────────────────────
const subscriptions = new Map(); // Map<client, string[]>
const retained = new Map(); // Map<topic, Buffer>
const backendHandlers = new Map(); // Map<topic, fn>

// ── MQTT Encoding ─────────────────────────────────────────────
function encodeRemainingLength(length) {
  const bytes = [];
  do {
    let digit = length % 128;
    length = Math.floor(length / 128);
    if (length > 0) digit |= 0x80;
    bytes.push(digit);
  } while (length > 0);
  return Buffer.from(bytes);
}

function parseRemainingLength(buffer, offset) {
  let multiplier = 1, value = 0, bytes = 0, encodedByte;
  do {
    if (offset + bytes >= buffer.length) return { length: 0, bytes: 1 };
    encodedByte = buffer[offset + bytes];
    value += (encodedByte & 0x7f) * multiplier;
    multiplier *= 128;
    bytes++;
  } while ((encodedByte & 0x80) !== 0 && bytes < 4);
  return { length: value, bytes };
}

// ── Packet Builders ───────────────────────────────────────────
const CONNACK = Buffer.from([0x20, 0x02, 0x00, 0x00]);
const PINGRESP = Buffer.from([0xd0, 0x00]);

function buildSuback(packetId) {
  return Buffer.concat([Buffer.from([0x90, 0x03]), packetId, Buffer.from([0x00])]);
}

function buildPublish(topic, payload, retain = false) {
  const t = Buffer.from(topic, 'utf8');
  const p = Buffer.isBuffer(payload) ? payload : Buffer.from(String(payload), 'utf8');
  const rl = 2 + t.length + p.length;
  return Buffer.concat([
    Buffer.from([retain ? 0x31 : 0x30]),
    encodeRemainingLength(rl),
    Buffer.from([t.length >> 8, t.length & 0xff]),
    t, p,
  ]);
}

// ── Send ──────────────────────────────────────────────────────
function send(client, packet) {
  if (!client) return;
  try {
    if (typeof client.write === 'function' && !client.destroyed) {
      client.write(packet);
    } else if (typeof client.send === 'function' && client.readyState === WebSocket.OPEN) {
      client.send(packet);
    }
  } catch (_) { }
}

// ── Subscriptions ─────────────────────────────────────────────
function subscribe(client, topic) {
  if (!subscriptions.has(client)) subscriptions.set(client, []);
  const list = subscriptions.get(client);
  if (!list.includes(topic)) list.push(topic);
}

function remove(client) {
  subscriptions.delete(client);
}

// ── Publish ───────────────────────────────────────────────────
function publish(topic, payload, retain = false) {
  if (retain) {
    payload.length === 0 ? retained.delete(topic) : retained.set(topic, payload);
  }
  const pkt = buildPublish(topic, payload);
  for (const [client, topics] of subscriptions) {
    if (topics.includes(topic)) send(client, pkt);
  }
  console.log(`[Broker] >> ${topic}: ${payload.toString().slice(0, 80)}`);

  const h = backendHandlers.get(topic);
  if (h) { try { h(topic, payload.toString()); } catch (e) { console.error('[Broker] handler:', e.message); } }
}

// ── MQTT Packet Parser ────────────────────────────────────────
function onConnect(client, buf, offset) {
  try {
    const pnLen = (buf[offset] << 8) | buf[offset + 1];
    offset += 2 + pnLen + 4;
    const cidLen = (buf[offset] << 8) | buf[offset + 1];
    const cid = buf.slice(offset + 2, offset + 2 + cidLen).toString();
    console.log(`[Broker] CONNECT ${cid}`);
  } catch (_) { console.log('[Broker] CONNECT'); }
  send(client, CONNACK);
}

function onSubscribe(client, buf, offset, remLen) {
  const end = offset + remLen;
  const pid = buf.slice(offset, offset + 2);
  offset += 2;
  while (offset < end) {
    const tl = (buf[offset] << 8) | buf[offset + 1];
    offset += 2;
    const topic = buf.slice(offset, offset + tl).toString();
    offset += tl + 1;
    subscribe(client, topic);
    console.log(`[Broker] SUBSCRIBE ${topic}`);
    if (retained.has(topic)) send(client, buildPublish(topic, retained.get(topic), true));
  }
  send(client, buildSuback(pid));
}

function onPublish(client, buf, offset, remLen, flags) {
  const tl = (buf[offset] << 8) | buf[offset + 1];
  const topic = buf.slice(offset + 2, offset + 2 + tl).toString();
  const data = buf.slice(offset + 2 + tl, offset + remLen);
  publish(topic, data, (flags & 0x01) !== 0);
}

function processBuffer(client, buffer) {
  let pos = 0;
  while (pos < buffer.length) {
    if (pos + 1 >= buffer.length) break;
    const b0 = buffer[pos];
    const typ = b0 >> 4;
    const flg = b0 & 0x0f;
    const { length: remLen, bytes: lb } = parseRemainingLength(buffer, pos + 1);
    const hLen = 1 + lb;
    const total = hLen + remLen;
    if (pos + total > buffer.length) break;
    const off = pos + hLen;
    switch (typ) {
      case 1: onConnect(client, buffer, off); break;
      case 3: onPublish(client, buffer, off, remLen, flg); break;
      case 8: onSubscribe(client, buffer, off, remLen); break;
      case 12: send(client, PINGRESP); break;
      case 14: remove(client); break;
    }
    pos += total;
  }
}

// ── TCP client handler ────────────────────────────────────────
function handleTcpClient(socket) {
  console.log('[Broker] TCP connected:', socket.remoteAddress);
  subscriptions.set(socket, []);
  let buf = Buffer.alloc(0);

  socket.on('data', (data) => {
    // Railway TCP proxy might prepend a PROXY protocol header (v1)
    if (buf.length === 0 && data.length >= 5 && data.slice(0, 5).toString() === 'PROXY') {
      const headerEnd = data.indexOf('\r\n');
      if (headerEnd !== -1) {
        console.log(`[Broker] Stripped PROXY header`);
        data = data.slice(headerEnd + 2);
      }
    }

    if (data.length === 0) return;

    buf = Buffer.concat([buf, data]);
    processBuffer(socket, buf);
    // Keep unprocessed tail
    // (simple approach: reset — works for well-formed packets)
    buf = Buffer.alloc(0);
  });

  socket.on('close', () => { remove(socket); console.log('[Broker] TCP disconnected'); });
  socket.on('error', (e) => { remove(socket); console.error('[Broker] TCP error:', e.message); });
  socket.setTimeout(120000); // 2 min keepalive timeout
  socket.on('timeout', () => socket.destroy());
}

// ── WebSocket client handler ──────────────────────────────────
function handleWsClient(ws) {
  console.log('[Broker] WS connected');
  subscriptions.set(ws, []);

  ws.on('message', (data) => {
    processBuffer(ws, Buffer.isBuffer(data) ? data : Buffer.from(data));
  });
  ws.on('close', () => { remove(ws); console.log('[Broker] WS disconnected'); });
  ws.on('error', () => remove(ws));
}

// ── Public API ────────────────────────────────────────────────
function backendPublish(topic, payload, retain = false) {
  const str = typeof payload === 'object' ? JSON.stringify(payload) : String(payload);
  publish(topic, Buffer.from(str, 'utf8'), retain);
}

function backendSubscribe(topic, handler) {
  backendHandlers.set(topic, handler);
}

/**
 * Attach broker to HTTP server (WebSocket) and start dedicated TCP server
 * @param {http.Server} httpServer
 */
function attachBroker(httpServer) {
  // WebSocket MQTT — for browser
  const wss = new WebSocket.Server({ server: httpServer });
  wss.on('connection', handleWsClient);
  console.log('[Broker] WebSocket MQTT ready');

  // TCP MQTT — for ESP32 (Railway TCP proxy → this port)
  // We use TCP_INTERNAL_PORT but if it's the same as PORT, we don't bind to avoid EADDRINUSE.
  // Instead, index.js will multiplex raw MQTT connections directly to handleTcpClient.
  let tcpPort = parseInt(process.env.TCP_INTERNAL_PORT) || 1885;

  if (tcpPort === parseInt(process.env.PORT)) {
    console.log(`[Broker] TCP_INTERNAL_PORT is same as PORT (${tcpPort}). Relying on multiplexer in index.js.`);
    return;
  }

  const tcpServer = net.createServer(handleTcpClient);

  tcpServer.on('error', (err) => {
    if (err.code === 'EADDRINUSE') {
      console.error(`[Broker] TCP port ${tcpPort} in use — retrying in 3s`);
      setTimeout(() => tcpServer.listen(tcpPort, '0.0.0.0'), 3000);
    } else {
      console.error('[Broker] TCP server error:', err.message);
    }
  });

  tcpServer.listen(tcpPort, '0.0.0.0', () => {
    console.log(`[Broker] TCP MQTT ready on port ${tcpPort}`);
  });
}

module.exports = { attachBroker, backendPublish, backendSubscribe, handleTcpClient };
