/**
 * Farid Villa – Built-in MQTT Broker
 *
 * Runs on the SAME port as the HTTP server by detecting the protocol
 * from the first byte of each incoming TCP connection:
 *   - MQTT CONNECT starts with 0x10  → handle as MQTT
 *   - HTTP starts with GET/POST/etc  → pass to HTTP server
 *
 * This means Railway only needs ONE port for everything:
 *   HTTP  → browser frontend
 *   WS    → browser MQTT (mqtt.js)
 *   TCP   → ESP32 MQTT (via Railway TCP proxy → same port)
 */

const net       = require('net');
const WebSocket = require('ws');

// ── Subscription store ────────────────────────────────────────
const subscriptions = new Map(); // Map<client, string[]>
const retained      = new Map(); // Map<topic, Buffer>
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

// ── MQTT Packet Builders ──────────────────────────────────────
function buildConnack()          { return Buffer.from([0x20, 0x02, 0x00, 0x00]); }
function buildPingresp()         { return Buffer.from([0xd0, 0x00]); }
function buildSuback(packetId)   { return Buffer.concat([Buffer.from([0x90, 0x03]), packetId, Buffer.from([0x00])]); }

function buildPublishPacket(topic, payload, retain = false) {
  const topicBuf   = Buffer.from(topic, 'utf8');
  const payloadBuf = Buffer.isBuffer(payload) ? payload : Buffer.from(String(payload), 'utf8');
  const remainLen  = 2 + topicBuf.length + payloadBuf.length;
  return Buffer.concat([
    Buffer.from([retain ? 0x31 : 0x30]),
    encodeRemainingLength(remainLen),
    Buffer.from([topicBuf.length >> 8, topicBuf.length & 0xff]),
    topicBuf,
    payloadBuf,
  ]);
}

// ── Send helpers ──────────────────────────────────────────────
function sendPacket(client, packet) {
  if (!client) return;
  try {
    if (typeof client.write === 'function' && !client.destroyed) {
      client.write(packet);
    } else if (typeof client.send === 'function' && client.readyState === WebSocket.OPEN) {
      client.send(packet);
    }
  } catch (e) { /* ignore */ }
}

// ── Subscription helpers ──────────────────────────────────────
function addSubscription(client, topic) {
  if (!subscriptions.has(client)) subscriptions.set(client, []);
  const topics = subscriptions.get(client);
  if (!topics.includes(topic)) topics.push(topic);
}

function cleanupClient(client) {
  subscriptions.delete(client);
}

// ── Publish ───────────────────────────────────────────────────
function publishToSubscribers(topic, payload, retainFlag) {
  if (retainFlag) {
    payload.length === 0 ? retained.delete(topic) : retained.set(topic, payload);
  }
  const packet = buildPublishPacket(topic, payload);
  for (const [client, topics] of subscriptions.entries()) {
    if (topics.includes(topic)) sendPacket(client, packet);
  }
  console.log(`[Broker] PUBLISH ${topic} → ${payload.toString().substring(0, 80)}`);

  // Dispatch to backend handlers
  const handler = backendHandlers.get(topic);
  if (handler) {
    try { handler(topic, payload.toString()); } catch (e) {
      console.error('[Broker] handler error:', e.message);
    }
  }
}

// ── MQTT Packet Parser ────────────────────────────────────────
function handleConnect(client, buffer, offset) {
  try {
    const pnLen = (buffer[offset] << 8) | buffer[offset + 1];
    offset += 2 + pnLen + 4;
    const cidLen = (buffer[offset] << 8) | buffer[offset + 1];
    offset += 2;
    const clientId = buffer.slice(offset, offset + cidLen).toString('utf8');
    console.log(`[Broker] CONNECT clientId=${clientId}`);
  } catch (e) {
    console.log('[Broker] CONNECT');
  }
  sendPacket(client, buildConnack());
}

function handleSubscribe(client, buffer, offset, remainingLength) {
  const end = offset + remainingLength;
  const packetId = buffer.slice(offset, offset + 2);
  offset += 2;
  while (offset < end) {
    const topicLen = (buffer[offset] << 8) | buffer[offset + 1];
    offset += 2;
    const topic = buffer.slice(offset, offset + topicLen).toString('utf8');
    offset += topicLen + 1; // +1 for QoS byte
    addSubscription(client, topic);
    console.log(`[Broker] SUBSCRIBE ${topic}`);
    if (retained.has(topic)) sendPacket(client, buildPublishPacket(topic, retained.get(topic), true));
  }
  sendPacket(client, buildSuback(packetId));
}

function handlePublish(client, buffer, offset, remainingLength, flags) {
  const topicLen = (buffer[offset] << 8) | buffer[offset + 1];
  offset += 2;
  const topic   = buffer.slice(offset, offset + topicLen).toString('utf8');
  offset += topicLen;
  const payload = buffer.slice(offset, offset + remainingLength - 2 - topicLen);
  publishToSubscribers(topic, payload, (flags & 0x01) !== 0);
}

function processMQTTBuffer(client, buffer) {
  let pos = 0;
  while (pos < buffer.length) {
    if (pos + 1 >= buffer.length) break;
    const firstByte  = buffer[pos];
    const packetType = firstByte >> 4;
    const flags      = firstByte & 0x0f;
    const { length: remainLen, bytes: lenBytes } = parseRemainingLength(buffer, pos + 1);
    const headerLen = 1 + lenBytes;
    const totalLen  = headerLen + remainLen;
    if (pos + totalLen > buffer.length) break;
    const offset = pos + headerLen;
    switch (packetType) {
      case 1:  handleConnect(client, buffer, offset); break;
      case 3:  handlePublish(client, buffer, offset, remainLen, flags); break;
      case 8:  handleSubscribe(client, buffer, offset, remainLen); break;
      case 12: sendPacket(client, buildPingresp()); break;
      case 14: cleanupClient(client); break;
    }
    pos += totalLen;
  }
}

// ── Public API ────────────────────────────────────────────────
function backendPublish(topic, payload, retain = false) {
  const str = typeof payload === 'object' ? JSON.stringify(payload) : String(payload);
  publishToSubscribers(topic, Buffer.from(str, 'utf8'), retain);
}

function backendSubscribe(topic, handler) {
  backendHandlers.set(topic, handler);
}

/**
 * attachBroker — attaches WebSocket MQTT to the HTTP server
 * AND intercepts raw TCP MQTT connections on the SAME port
 * by replacing the server's connection handler.
 *
 * @param {http.Server} httpServer
 */
function attachBroker(httpServer) {
  // ── WebSocket MQTT (browser) ────────────────────────────────
  const wss = new WebSocket.Server({ server: httpServer });
  wss.on('connection', (ws) => {
    console.log('[Broker] WS client connected');
    subscriptions.set(ws, []);
    ws.on('message', (data) => {
      processMQTTBuffer(ws, Buffer.isBuffer(data) ? data : Buffer.from(data));
    });
    ws.on('close', () => { cleanupClient(ws); console.log('[Broker] WS client disconnected'); });
    ws.on('error', () => cleanupClient(ws));
  });
  console.log('[Broker] WebSocket MQTT broker attached');

  // ── Raw TCP MQTT (ESP32) on the SAME port ───────────────────
  // We intercept the raw 'connection' event BEFORE Node's HTTP parser
  // by peeking at the first byte. MQTT CONNECT = 0x10, HTTP = ASCII letter.
  const originalEmit = httpServer.emit.bind(httpServer);

  httpServer.on('connection', (socket) => {
    socket.once('data', (firstChunk) => {
      const firstByte = firstChunk[0];

      // MQTT CONNECT packet type = 1 → first byte = 0x10
      if (firstByte === 0x10) {
        // Raw MQTT TCP connection (ESP32)
        console.log('[Broker] TCP MQTT client connected:', socket.remoteAddress);
        subscriptions.set(socket, []);

        let buf = firstChunk; // process the first chunk immediately
        processMQTTBuffer(socket, buf);
        buf = Buffer.alloc(0);

        socket.on('data', (data) => {
          buf = Buffer.concat([buf, data]);
          processMQTTBuffer(socket, buf);
          buf = Buffer.alloc(0);
        });
        socket.on('close', () => { cleanupClient(socket); console.log('[Broker] TCP MQTT client disconnected'); });
        socket.on('error', () => cleanupClient(socket));
      } else {
        // HTTP / WebSocket upgrade — put the data back and let Node handle it
        socket.unshift(firstChunk);
        // Nothing else needed — the HTTP server already has this socket
      }
    });
  });

  console.log('[Broker] TCP MQTT detection active on HTTP port');
}

module.exports = { attachBroker, backendPublish, backendSubscribe };
