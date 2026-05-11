require('dotenv').config();
const http = require('http');
const net = require('net');
const express = require('express');
const cors = require('cors');
const path = require('path');
const { google } = require('googleapis');
const { attachBroker, backendPublish, backendSubscribe, handleTcpClient } = require('./broker');
// ── Config ────────────────────────────────────────────────────
// Railway sets PORT automatically. Everything (HTTP + WS + TCP MQTT)
// runs on this single port. No separate TCP_PORT needed.
const PORT = process.env.PORT || 3000;
const SHEET_ID = process.env.SHEET_ID;

// ── Express App ───────────────────────────────────────────────
const app = express();

process.on('uncaughtException', (err) => {
  console.error('[System] Uncaught Exception:', err);
});
process.on('unhandledRejection', (reason, promise) => {
  console.error('[System] Unhandled Rejection:', reason);
});

app.use(cors({
  origin: '*',
  credentials: true,
}));
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

// ESP32 publishes motion events
backendSubscribe('home/room1/motion', async (topic, payload) => {
  const state = payload.trim(); // 'DETECTED' or 'CLEAR'
  deviceState.room1.motion = state;
  console.log(`[Backend] Room1 motion: ${state}`);

  // Forward to frontend
  backendPublish('home/ui', JSON.stringify({
    room: 'room1',
    event: 'motion',
    state,
    devices: deviceState,
    time: new Date().toLocaleTimeString('en-GB', { timeZone: 'Africa/Cairo' }),
  }));

  // Log to Google Sheets
  await logToSheet('Room 1', 'Motion', state);
});

backendSubscribe('home/room2/motion', async (topic, payload) => {
  const state = payload.trim();
  deviceState.room2.motion = state;
  console.log(`[Backend] Room2 motion: ${state}`);

  backendPublish('home/ui', JSON.stringify({
    room: 'room2',
    event: 'motion',
    state,
    devices: deviceState,
    time: new Date().toLocaleTimeString('en-GB', { timeZone: 'Africa/Cairo' }),
  }));

  await logToSheet('Room 2', 'Motion', state);
});

// ESP32 publishes light state changes (when PIR auto-controls LED)
backendSubscribe('home/room1/light/state', (topic, payload) => {
  deviceState.room1.light = payload.trim();
  backendPublish('home/ui', JSON.stringify({ devices: deviceState }));
});

backendSubscribe('home/room2/light/state', (topic, payload) => {
  deviceState.room2.light = payload.trim();
  backendPublish('home/ui', JSON.stringify({ devices: deviceState }));
});

// Frontend requests current status
backendSubscribe('home/request_status', (topic, payload) => {
  backendPublish('home/status', JSON.stringify({ devices: deviceState }), true);
});

// ── REST API ──────────────────────────────────────────────────

// GET /api/status — current device state
app.get('/api/status', (req, res) => {
  res.json({ success: true, devices: deviceState });
});

// POST /api/control — control a light from frontend
// Body: { room: 'room1', state: 'ON' }
app.post('/api/control', async (req, res) => {
  const { room, state } = req.body;
  if (!['room1', 'room2'].includes(room)) {
    return res.status(400).json({ success: false, message: 'Invalid room' });
  }
  const s = (state || '').toUpperCase();
  if (!['ON', 'OFF'].includes(s)) {
    return res.status(400).json({ success: false, message: 'State must be ON or OFF' });
  }

  // Publish to broker → ESP32 receives it
  const topic = `home/${room}/light`;
  backendPublish(topic, s, true);

  // Update in-memory state
  deviceState[room].light = s;

  // Notify frontend
  backendPublish('home/ui', JSON.stringify({
    room,
    event: 'light',
    state: s,
    devices: deviceState,
    time: new Date().toLocaleTimeString('en-GB', { timeZone: 'Africa/Cairo' }),
  }));

  // Log to Google Sheets
  await logToSheet(room === 'room1' ? 'Room 1' : 'Room 2', 'Light', s);

  res.json({ success: true, room, state: s });
});

// GET /api/logs — activity history from Google Sheets
app.get('/api/logs', async (req, res) => {
  try {
    const logs = await getLogsFromSheet();
    res.json({ success: true, logs });
  } catch (err) {
    res.status(500).json({ success: false, message: err.message });
  }
});

// Health check
app.get('/api/health', (req, res) => {
  res.json({ status: 'ok', project: 'Farid Villa Smart Home', devices: deviceState });
});

// Serve frontend for all other routes
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// ── Start Server ──────────────────────────────────────────────
const httpServer = http.createServer(app);

httpServer.listen(PORT, () => {
  console.log(`\n🏠  Farid Villa Smart Home`);
  console.log(`🌐  HTTP + WebSocket server running on port ${PORT}`);
  attachBroker(httpServer);
});

httpServer.on('error', (err) => {
  console.error(`[Server] Error: ${err.message}`);
  process.exit(1);
});
