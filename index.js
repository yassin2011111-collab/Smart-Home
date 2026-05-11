require('dotenv').config();
const http = require('http');
const net = require('net');
const express = require('express');
const cors = require('cors');
const path = require('path');
const { google } = require('googleapis');
const { attachBroker, backendPublish, backendSubscribe, handleTcpClient } = require('./broker');

// ── Config ────────────────────────────────────────────────────
// Railway exposes exactly one port. The net.Server mux below listens on it
// and routes connections to either the HTTP server or the MQTT TCP handler.
const PORT          = parseInt(process.env.PORT) || 3000;
const HTTP_INTERNAL = 3001; // Express listens here; never exposed externally
const SHEET_ID      = process.env.SHEET_ID;

// ── Express App ───────────────────────────────────────────────
const app = express();

process.on('uncaughtException', (err) => {
  console.error('[System] Uncaught Exception:', err);
});
process.on('unhandledRejection', (reason) => {
  console.error('[System] Unhandled Rejection:', reason);
});

app.use(cors({ origin: '*', credentials: true }));
app.use(express.json());

// Serve frontend (index.html) as static
app.use(express.static(path.join(__dirname, 'public')));

// ── In-memory device state ────────────────────────────────────
const deviceState = {
  room1: { light: 'OFF', motion: 'CLEAR' },
  room2: { light: 'OFF', motion: 'CLEAR' },
};

// ── Google Sheets Helper ──────────────────────────────────────
function getGoogleAuth() {
  const credentials = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT || '{}');
  if (credentials.private_key) {
    credentials.private_key = credentials.private_key.replace(/\\n/g, '\n');
  }
  return new google.auth.GoogleAuth({
    credentials,
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
}

async function logToSheet(room, event, state) {
  if (!SHEET_ID || !process.env.GOOGLE_SERVICE_ACCOUNT) return;
  try {
    const auth = getGoogleAuth();
    const sheets = google.sheets({ version: 'v4', auth });
    const now = new Date();
    const date = now.toLocaleDateString('en-GB', { timeZone: 'Africa/Cairo' });
    const time = now.toLocaleTimeString('en-GB', { timeZone: 'Africa/Cairo' });
    await sheets.spreadsheets.values.append({
      spreadsheetId: SHEET_ID,
      range: 'Sheet1!A:E',
      valueInputOption: 'USER_ENTERED',
      requestBody: { values: [[room, event, state, date, time]] },
    });
    console.log(`[Sheets] Logged: ${room} | ${event} | ${state}`);
  } catch (err) {
    console.error('[Sheets] Log error:', err.message);
  }
}

async function getLogsFromSheet() {
  if (!SHEET_ID || !process.env.GOOGLE_SERVICE_ACCOUNT) return [];
  try {
    const auth = getGoogleAuth();
    const sheets = google.sheets({ version: 'v4', auth });
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: 'Sheet1!A:E',
    });
    return res.data.values || [];
  } catch (err) {
    console.error('[Sheets] Read error:', err.message);
    return [];
  }
}

// ── MQTT Message Handlers (ESP32 → Broker → Backend) ─────────

backendSubscribe('home/room1/motion', async (topic, payload) => {
  const state = payload.trim();
  deviceState.room1.motion = state;
  console.log(`[Backend] Room1 motion: ${state}`);
  backendPublish('home/ui', JSON.stringify({
    room: 'room1', event: 'motion', state, devices: deviceState,
    time: new Date().toLocaleTimeString('en-GB', { timeZone: 'Africa/Cairo' }),
  }));
  await logToSheet('Room 1', 'Motion', state);
});

backendSubscribe('home/room2/motion', async (topic, payload) => {
  const state = payload.trim();
  deviceState.room2.motion = state;
  console.log(`[Backend] Room2 motion: ${state}`);
  backendPublish('home/ui', JSON.stringify({
    room: 'room2', event: 'motion', state, devices: deviceState,
    time: new Date().toLocaleTimeString('en-GB', { timeZone: 'Africa/Cairo' }),
  }));
  await logToSheet('Room 2', 'Motion', state);
});

backendSubscribe('home/room1/light/state', (topic, payload) => {
  deviceState.room1.light = payload.trim();
  backendPublish('home/ui', JSON.stringify({ devices: deviceState }));
});

backendSubscribe('home/room2/light/state', (topic, payload) => {
  deviceState.room2.light = payload.trim();
  backendPublish('home/ui', JSON.stringify({ devices: deviceState }));
});

backendSubscribe('home/request_status', () => {
  backendPublish('home/status', JSON.stringify({ devices: deviceState }), true);
});

// ── REST API ──────────────────────────────────────────────────

app.get('/api/status', (req, res) => {
  res.json({ success: true, devices: deviceState });
});

app.post('/api/control', async (req, res) => {
  const { room, state } = req.body;
  if (!['room1', 'room2'].includes(room)) {
    return res.status(400).json({ success: false, message: 'Invalid room' });
  }
  const s = (state || '').toUpperCase();
  if (!['ON', 'OFF'].includes(s)) {
    return res.status(400).json({ success: false, message: 'State must be ON or OFF' });
  }
  backendPublish(`home/${room}/light`, s, true);
  deviceState[room].light = s;
  backendPublish('home/ui', JSON.stringify({
    room, event: 'light', state: s, devices: deviceState,
    time: new Date().toLocaleTimeString('en-GB', { timeZone: 'Africa/Cairo' }),
  }));
  await logToSheet(room === 'room1' ? 'Room 1' : 'Room 2', 'Light', s);
  res.json({ success: true, room, state: s });
});

app.get('/api/logs', async (req, res) => {
  try {
    const logs = await getLogsFromSheet();
    res.json({ success: true, logs });
  } catch (err) {
    res.status(500).json({ success: false, message: err.message });
  }
});

app.get('/api/health', (req, res) => {
  res.json({ status: 'ok', project: 'Farid Villa Smart Home', devices: deviceState });
});

app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// ── HTTP Server (internal) ────────────────────────────────────
// Listens on HTTP_INTERNAL (3001) — only reachable from the mux below.
const httpServer = http.createServer(app);

httpServer.listen(HTTP_INTERNAL, '127.0.0.1', () => {
  console.log(`[HTTP] Internal server ready on 127.0.0.1:${HTTP_INTERNAL}`);
  attachBroker(httpServer); // WebSocket MQTT on the same HTTP server
});

httpServer.on('error', (err) => {
  console.error(`[HTTP] Server error: ${err.message}`);
  process.exit(1);
});

// ── Protocol-Detection Multiplexer ───────────────────────────
//
// Listens on the single Railway PORT. Peeks at the first byte of every
// incoming TCP connection to decide whether it is HTTP or raw MQTT:
//
//   HTTP  — first byte is an ASCII letter (method: GET, POST, PUT, …)
//   MQTT  — first byte is an MQTT control-packet type byte (0x10–0x3F)
//           0x10 = CONNECT, 0x20 = CONNACK, 0x30 = PUBLISH, etc.
//
// The buffered byte is prepended back to the stream so neither handler
// sees a truncated payload.

// HTTP method first bytes (ASCII codes of G, P, D, H, O, C, T)
const HTTP_FIRST_BYTES = new Set([
  0x47, // G  — GET
  0x50, // P  — POST, PUT, PATCH
  0x44, // D  — DELETE
  0x48, // H  — HEAD
  0x4f, // O  — OPTIONS
  0x43, // C  — CONNECT
  0x54, // T  — TRACE
]);

const muxServer = net.createServer((socket) => {
  socket.once('error', (err) => {
    console.error('[Mux] Socket error before routing:', err.message);
    socket.destroy();
  });

  // Wait for the first chunk — we only need the very first byte.
  socket.once('data', (firstChunk) => {
    const firstByte = firstChunk[0];
    const isHttp = HTTP_FIRST_BYTES.has(firstByte);

    if (isHttp) {
      // ── Route to Express HTTP server ──────────────────────
      const target = net.createConnection({ host: '127.0.0.1', port: HTTP_INTERNAL }, () => {
        // Replay the buffered first chunk so Express sees the full request.
        target.write(firstChunk);
        socket.pipe(target);
        target.pipe(socket);
      });

      target.on('error', (err) => {
        console.error('[Mux] HTTP target error:', err.message);
        socket.destroy();
      });

      socket.on('error', () => target.destroy());
      socket.on('close', () => target.destroy());
      target.on('close', () => socket.destroy());

    } else {
      // ── Route to MQTT broker (TCP handler) ────────────────
      // Pause the socket so handleTcpClient can attach its own 'data'
      // listener before we replay the buffered chunk.
      socket.pause();
      handleTcpClient(socket);

      // Replay the first chunk into the socket's read buffer so the
      // broker's 'data' handler receives it intact.
      socket.emit('data', firstChunk);
      socket.resume();
    }
  });
});

muxServer.on('error', (err) => {
  console.error(`[Mux] Server error: ${err.message}`);
  process.exit(1);
});

muxServer.listen(PORT, '0.0.0.0', () => {
  console.log(`\n🏠  Farid Villa Smart Home`);
  console.log(`🌐  Mux server listening on port ${PORT} (HTTP + MQTT)`);
  console.log(`📡  HTTP → 127.0.0.1:${HTTP_INTERNAL} | MQTT TCP → broker`);
});
