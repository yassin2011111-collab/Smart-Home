/**
 * Farid Villa – Built-in MQTT Broker
 * Supports: TCP (for ESP32) + WebSocket (for browser frontend)
 * No external broker needed — runs inside the same Node.js process
 */

const net = require('net');
const WebSocket = require('ws');

// All connected clients and their subscriptions
// Map<client, string[]>  (client = net.Socket or WebSocket)
const subscriptions = new Map();

// Retained messages: Map<topic, payload>
const retained = new Map();

// ── MQTT Encoding Helpers ─────────────────────────────────────

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
  let multiplier = 1;
  let value = 0;
  let bytes = 0;
  let encodedByte;
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

function buildConnack() {
  return Buffer.from([0x20, 0x02, 0x00, 0x00]);
}

function buildPingresp() {
  return Buffer.from([0xd0, 0x00]);
}

function buildSuback(packetId) {
  return Buffer.concat([Buffer.from([0x90, 0x03]), packetId, Buffer.from([0x00])]);
}

function buildPublishPacket(topic, payload, retain = false) {
  const topicBuf   = Buffer.from(topic, 'utf8');
  const payloadBuf = Buffer.isBuffer(payload) ? payload : Buffer.from(payload, 'utf8');
  const remainLen  = 2 + topicBuf.length + payloadBuf.length;
  const flags      = retain ? 0x31 : 0x30; // bit 0 = retain flag
  return Buffer.concat([
    Buffer.from([flags]),
    encodeRemainingLength(remainLen),
    Buffer.from([topicBuf.length >> 8, topicBuf.length & 0xff]),
    topicBuf,
    payloadBuf,
  ]);
}

// ── Send to a client (TCP socket or WebSocket) ────────────────

function sendPacket(client, packet) {
  if (!client) return;
  try {
    if (typeof client.write === 'function') {
      // TCP socket
      if (!client.destroyed) client.write(packet);
    } else if (typeof client.send === 'function') {
      // WebSocket
      if (client.readyState === WebSocket.OPEN) client.send(packet);
    }
  } catch (e) {
    console.error('[Broker] sendPacket error:', e.message);
  }
}

// ── Subscription management ───────────────────────────────────

function addSubscription(client, topic) {
  if (!subscriptions.has(client)) subscriptions.set(client, []);
  const topics = subscriptions.get(client);
  if (!topics.includes(topic)) topics.push(topic);
}

function publishToSubscribers(topic, payload, retainFlag, senderClient) {
  // Store retained message
  if (retainFlag) {
    if (payload.length === 0) {
      retained.delete(topic);
    } else {
      retained.set(topic, payload);
    }
  }

  const packet = buildPublishPacket(topic, payload);
  for (const [client, topics] of subscriptions.entries()) {
    if (topics.includes(topic)) {
      sendPacket(client, packet);
    }
  }

  console.log(`[Broker] PUBLISH ${topic} → ${payload.toString().substring(0, 80)}`);
}

// ── Packet Handlers ───────────────────────────────────────────

function handleConnect(client, buffer, offset) {
  // Extract client ID for logging
  try {
    const protocolNameLen = (buffer[offset] << 8) | buffer[offset + 1];
    offset += 2 + protocolNameLen + 4; // skip protocol name + level + flags + keepalive
    const clientIdLen = (buffer[offset] << 8) | buffer[offset + 1];
    offset += 2;
    const clientId = buffer.slice(offset, offset + clientIdLen).toString('utf8');
    console.log(`[Broker] CONNECT clientId=${clientId}`);
  } catch (e) {
    console.log('[Broker] CONNECT (could not parse clientId)');
  }
  sendPacket(client, buildConnack());
}

function handleSubscribe(client, buffer, offset, remainingLength) {
  const end      = offset + remainingLength;
  const packetId = buffer.slice(offset, offset + 2);
  offset += 2;

  while (offset < end) {
    const topicLen = (buffer[offset] << 8) | buffer[offset + 1];
    offset += 2;
    const topic = buffer.slice(offset, offset + topicLen).toString('utf8');
    offset += topicLen;
    offset++; // QoS byte

    addSubscription(client, topic);
    console.log(`[Broker] SUBSCRIBE ${topic}`);

    // Send retained message if exists
    if (retained.has(topic)) {
      const retainedPayload = retained.get(topic);
      sendPacket(client, buildPublishPacket(topic, retainedPayload, true));
    }
  }

  sendPacket(client, buildSuback(packetId));
}

function handlePublish(client, buffer, offset, remainingLength, flags) {
  const topicLen   = (buffer[offset] << 8) | buffer[offset + 1];
  offset += 2;
  const topic      = buffer.slice(offset, offset + topicLen).toString('utf8');
  offset += topicLen;
  const payload    = buffer.slice(offset, offset + remainingLength - 2 - topicLen);
  const retainFlag = (flags & 0x01) !== 0;

  publishToSubscribers(topic, payload, retainFlag, client);
}

function handleMQTTPacket(client, buffer) {
  let pos = 0;
  while (pos < buffer.length) {
    if (pos + 1 >= buffer.length) break;

    const firstByte = buffer[pos];
    const packetType = firstByte >> 4;
    const flags      = firstByte & 0x0f;

    const { length: remainLen, bytes: lenBytes } = parseRemainingLength(buffer, pos + 1);
    const headerLen = 1 + lenBytes;
    const totalLen  = headerLen + remainLen;

    if (pos + totalLen > buffer.length) break; // partial packet

    const offset = pos + headerLen;

    switch (packetType) {
      case 1:  handleConnect(client, buffer, offset); break;
      case 3:  handlePublish(client, buffer, offset, remainLen, flags); break;
      case 8:  handleSubscribe(client, buffer, offset, remainLen); break;
      case 12: sendPacket(client, buildPingresp()); break;
      case 14: cleanupClient(client); break; // DISCONNECT
      default: break;
    }

    pos += totalLen;
  }
}

// ── Client cleanup ────────────────────────────────────────────

function cleanupClient(client) {
  subscriptions.delete(client);
}

// ── Public API ────────────────────────────────────────────────

/**
 * Publish a message from the backend (not from a client)
 * @param {string} topic
 * @param {string|object} payload
 * @param {boolean} retain
 */
function backendPublish(topic, payload, retain = false) {
  const payloadStr = typeof payload === 'object' ? JSON.stringify(payload) : String(payload);
  publishToSubscribers(topic, Buffer.from(payloadStr, 'utf8'), retain, null);
}

/**
 * Register a handler for messages on a topic (backend subscription)
 * @param {string} topic
 * @param {function} handler  (topic, payloadString) => void
 */
const backendHandlers = new Map();
function backendSubscribe(topic, handler) {
  backendHandlers.set(topic, handler);
}

// Hook into publishToSubscribers to also call backend handlers
const _origPublish = publishToSubscribers;
// We override by wrapping the internal call in handlePublish
// Instead, we call handlers directly after publish:
function dispatchToBackend(topic, payload) {
  const handler = backendHandlers.get(topic);
  if (handler) {
    try {
      handler(topic, payload.toString());
    } catch (e) {
      console.error('[Broker] Backend handler error:', e.message);
    }
  }
}

// Patch handlePublish to also dispatch to backend
const _origHandlePublish = handlePublish;
function handlePublishPatched(client, buffer, offset, remainingLength, flags) {
  const topicLen = (buffer[offset] << 8) | buffer[offset + 1];
  const topic    = buffer.slice(offset + 2, offset + 2 + topicLen).toString('utf8');
  const payload  = buffer.slice(offset + 2 + topicLen, offset + remainingLength);
  const retain   = (flags & 0x01) !== 0;

  publishToSubscribers(topic, payload, retain, client);
  dispatchToBackend(topic, payload);
}

// Replace in packet handler
function handleMQTTPacketFinal(client, buffer) {
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
      case 3:  handlePublishPatched(client, buffer, offset, remainLen, flags); break;
      case 8:  handleSubscribe(client, buffer, offset, remainLen); break;
      case 12: sendPacket(client, buildPingresp()); break;
      case 14: cleanupClient(client); break;
      default: break;
    }

    pos += totalLen;
  }
}

/**
 * Attach the broker to an existing HTTP server (WebSocket) and start TCP server
 * @param {http.Server} httpServer
 * @param {number} tcpPort
 */
function attachBroker(httpServer, tcpPort) {
  // WebSocket broker (for browser frontend)
  const wss = new WebSocket.Server({ server: httpServer });
  wss.on('connection', (ws) => {
    console.log('[Broker] WS client connected');
    subscriptions.set(ws, []);
    ws.on('message', (data) => {
      const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
      handleMQTTPacketFinal(ws, buf);
    });
    ws.on('close', () => {
      cleanupClient(ws);
      console.log('[Broker] WS client disconnected');
    });
    ws.on('error', (e) => {
      cleanupClient(ws);
    });
  });
  console.log('[Broker] WebSocket MQTT broker attached to HTTP server');

  // TCP broker (for ESP32)
  if (tcpPort) {
    const tcpServer = net.createServer((socket) => {
      console.log('[Broker] TCP client connected:', socket.remoteAddress);
      subscriptions.set(socket, []);
      let buffer = Buffer.alloc(0);
      socket.on('data', (data) => {
        buffer = Buffer.concat([buffer, data]);
        handleMQTTPacketFinal(socket, buffer);
        buffer = Buffer.alloc(0); // simple: reset after processing
      });
      socket.on('close', () => {
        cleanupClient(socket);
        console.log('[Broker] TCP client disconnected');
      });
      socket.on('error', (e) => {
        cleanupClient(socket);
      });
    });

    tcpServer.listen(tcpPort, '0.0.0.0', () => {
      console.log(`[Broker] TCP MQTT broker listening on port ${tcpPort}`);
    });
  }
}

module.exports = { attachBroker, backendPublish, backendSubscribe };
