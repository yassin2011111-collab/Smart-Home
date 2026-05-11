require('dotenv').config();
const net     = require('net');
const http    = require('http');
const express = require('express');
const cors    = require('cors');
const path    = require('path');
const { google } = require('googleapis');
const { attachBroker, backendPublish, backendSubscribe, handleTcpClient } = require('./broker');

// ── Config ────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
// Express binds internally on this port — never exposed externally
const INTERNAL_HTTP_PORT = 3001;
const SHEET_ID = process.env.SHEET_ID;

// ── Express App ───────────────────────────────────────────────
const app = express();

process.on('uncaughtException',  (err) => console.error('[System] Uncaught:', err.message));
process.on('unhandledRejection', (r)   => console.error('[System] Rejection:', r));

app.use(cors({ origin: '*', credentials: true }));
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ── In-memory device state ────────────────────────────────────
const deviceState = {
  room1: { light: 'OFF', motion: 'CLEAR' },
  room2: { light: 'OFF', motion: 'CLEAR' },
};

// ── Google Sheets ─────────────────────────────────────────────
function getGoogleAuth() {
  const creds = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT || '{}');
  if (creds.private_key) creds.private_key = creds.private_key.replace(/\\n/g, '\n');
  return new google.auth.GoogleAuth({
    credentials: creds,
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
}

async function logToSheet(room, event, state) {
  if (!SHEET_ID || !process.env.GOOGLE_SERVICE_ACCOUNT) return;
  try {
    const sheets = google.sheets({ version: 'v4', auth: getGoogleAuth() });
    const now  = new Date();
    await sheets.spreadsheets.values.append({
      spreadsheetId: SHEET_ID,
      range: 'Sheet1!A:E',
      valueInputOption: 'USER_ENTERED',
      requestBody: { values: [[room, event, state,
        now.toLocaleDateString('en-GB', { timeZone: 'Africa/Cairo' }),
        now.toLocaleTimeString('en-GB', { timeZone: 'Africa/Cairo' }),
      ]] },
    });
    console.log(`[Sheets] ${room} | ${event} | ${state}`);
  } catch (e) { console.error('[Sheets]', e.message); }
}

async function getLogsFromSheet() {
  if (!SHEET_ID || !process.env.GOOGLE_SERVICE_ACCOUNT) return [];
  try {
    const sheets = google.sheets({ version: 'v4', auth: getGoogleAuth() });
    const res = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: 'Sheet1!A:E' });
    return res.data.values || [];
  } catch (e) { console.error('[Sheets]', e.message); return []; }
}

// ── MQTT backend subscriptions ────────────────────────────────
backendSubscribe('home/room1/motion', async (_, payload) => {
  deviceState.room1.motion = payload.trim();
  backendPublish('home/ui', JSON.stringify({ room: 'room1', event: 'motion', state: payload.trim(), devices: deviceState, time: new Date().toLocaleTimeString('en-GB', { timeZone: 'Africa/Cairo' }) }));
  await logToSheet('Room 1', 'Motion', payload.trim());
});

backendSubscribe('home/room2/motion', async (_, payload) => {
  deviceState.room2.motion = payload.trim();
  backendPublish('home/ui', JSON.stringify({ room: 'room2', event: 'motion', state: payload.trim(), devices: deviceState, time: new Date().toLocaleTimeString('en-GB', { timeZone: 'Africa/Cairo' }) }));
  await logToSheet('Room 2', 'Motion', payload.trim());
});

backendSubscribe('home/request_status', () => {
  backendPublish('home/status', JSON.stringify({ devices: deviceState }), true);
});

// ── REST API ──────────────────────────────────────────────────
app.get('/api/status',  (req, res) => res.json({ success: true, devices: deviceState }));
app.get('/api/health',  (req, res) => res.json({ status: 'ok', project: 'Farid Villa Smart Home' }));

app.post('/api/control', async (req, res) => {
  const { room, state } = req.body;
  if (!['room1', 'room2'].includes(room))
    return res.status(400).json({ success: false, message: 'Invalid room' });
  const s = (state || '').toUpperCase();
  if (!['ON', 'OFF'].includes(s))
    return res.status(400).json({ success: false, message: 'State must be ON or OFF' });

  backendPublish(`home/${room}/light`, s, true);
  deviceState[room].light = s;
  backendPublish('home/ui', JSON.stringify({ room, event: 'light', state: s, devices: deviceState, time: new Date().toLocaleTimeString('en-GB', { timeZone: 'Africa/Cairo' }) }));
  await logToSheet(room === 'room1' ? 'Room 1' : 'Room 2', 'Light', s);
  res.json({ success: true, room, state: s });
});

app.get('/api/logs', async (req, res) => {
  try { res.json({ success: true, logs: await getLogsFromSheet() }); }
  catch (e) { res.status(500).json({ success: false, message: e.message }); }
});

app.get('*', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

// ── Start ─────────────────────────────────────────────────────
// Step 1: Express listens on internal port (127.0.0.1 only — not exposed)
const httpServer = http.createServer(app);

// Attach WebSocket MQTT broker to the HTTP server
attachBroker(httpServer);

httpServer.listen(INTERNAL_HTTP_PORT, '127.0.0.1', () => {
  console.log(`[HTTP] Express listening internally on ${INTERNAL_HTTP_PORT}`);

  // Step 2: net.Server mux listens on Railway's PORT
  // Detects protocol from first byte and routes:
  //   MQTT (0x10–0x3F) → handleTcpClient (ESP32)
  //   HTTP/WS          → proxy to 127.0.0.1:3001 (Express)
  const mux = net.createServer((socket) => {
    socket.once('data', (chunk) => {
      const b = chunk[0];
      const isMQTT = b >= 0x10 && b <= 0x3f;

      if (isMQTT) {
        // ESP32 raw MQTT connection
        handleTcpClient(socket, chunk);
      } else {
        // HTTP / WebSocket — proxy to internal Express server
        const proxy = net.connect(INTERNAL_HTTP_PORT, '127.0.0.1', () => {
          proxy.write(chunk);           // replay first chunk
          socket.pipe(proxy);
          proxy.pipe(socket);
        });
        proxy.on('error', () => socket.destroy());
        socket.on('error', () => proxy.destroy());
      }
    });

    socket.on('error', () => {});
  });

  mux.listen(PORT, '0.0.0.0', () => {
    console.log(`\n🏠  Farid Villa Smart Home`);
    console.log(`🌐  Mux server on port ${PORT} (HTTP + WS + TCP MQTT)`);
    console.log(`📡  TCP MQTT proxy active — Railway TCP proxy → port ${PORT}`);
  });

  mux.on('error', (err) => {
    console.error('[Mux] Error:', err.message);
    process.exit(1);
  });
});
