require('dotenv').config();
const http = require('http');
const express = require('express');
const cors = require('cors');
const path = require('path');
const { google } = require('googleapis');
const mqtt = require('mqtt');

// ── Config ────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
const SHEET_ID = process.env.SHEET_ID;

// HiveMQ Cloud Credentials
const MQTT_HOST = process.env.MQTT_HOST || '9d3c2efbd1594219aee390c520d4d949.s1.eu.hivemq.cloud';
const MQTT_PORT = process.env.MQTT_PORT || 8883;
const MQTT_WS_PORT = 8884;
const MQTT_USERNAME = process.env.MQTT_USERNAME || 'yassin_ahmed';
const MQTT_PASSWORD = process.env.MQTT_PASSWORD || 'Yassin2005';

// ── Express App ───────────────────────────────────────────────
const app = express();

process.on('uncaughtException', (err) => console.error('[System] Uncaught:', err.message));
process.on('unhandledRejection', (r) => console.error('[System] Rejection:', r));

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
    const now = new Date();
    await sheets.spreadsheets.values.append({
      spreadsheetId: SHEET_ID,
      range: 'Sheet1!A:E',
      valueInputOption: 'USER_ENTERED',
      requestBody: {
        values: [[room, event, state,
          now.toLocaleDateString('en-GB', { timeZone: 'Africa/Cairo' }),
          now.toLocaleTimeString('en-GB', { timeZone: 'Africa/Cairo' }),
        ]]
      },
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

// ── HiveMQ Connection ─────────────────────────────────────────
console.log(`[MQTT] Connecting to HiveMQ Cloud at mqtts://${MQTT_HOST}:${MQTT_PORT}`);
const mqttClient = mqtt.connect(`mqtts://${MQTT_HOST}:${MQTT_PORT}`, {
  username: MQTT_USERNAME,
  password: MQTT_PASSWORD,
  clientId: 'farid_backend_' + Math.random().toString(16).substring(2, 8),
  rejectUnauthorized: true, // required for HiveMQ Cloud TLS
});

mqttClient.on('connect', () => {
  console.log('[MQTT] Connected to HiveMQ Cloud successfully!');
  mqttClient.subscribe('home/room1/motion');
  mqttClient.subscribe('home/room2/motion');
  mqttClient.subscribe('home/request_status');
});

mqttClient.on('error', (err) => {
  console.error('[MQTT] Connection Error:', err.message);
});

mqttClient.on('message', async (topic, payloadRaw) => {
  const payload = payloadRaw.toString().trim();
  console.log(`[MQTT Received] ${topic} -> ${payload}`);

  if (topic === 'home/room1/motion') {
    deviceState.room1.motion = payload;
    mqttClient.publish('home/ui', JSON.stringify({ room: 'room1', event: 'motion', state: payload, devices: deviceState, time: new Date().toLocaleTimeString('en-GB', { timeZone: 'Africa/Cairo' }) }));
    await logToSheet('Room 1', 'Motion', payload);
  }

  else if (topic === 'home/room2/motion') {
    deviceState.room2.motion = payload;
    mqttClient.publish('home/ui', JSON.stringify({ room: 'room2', event: 'motion', state: payload, devices: deviceState, time: new Date().toLocaleTimeString('en-GB', { timeZone: 'Africa/Cairo' }) }));
    await logToSheet('Room 2', 'Motion', payload);
  }

  else if (topic === 'home/request_status') {
    mqttClient.publish('home/status', JSON.stringify({ devices: deviceState }), { retain: true });
  }
});

// ── REST API ──────────────────────────────────────────────────
app.get('/api/mqtt-config', (req, res) => {
  res.json({
    host: MQTT_HOST,
    port: MQTT_WS_PORT,
    username: MQTT_USERNAME,
    password: MQTT_PASSWORD
  });
});

app.get('/api/status', (req, res) => res.json({ success: true, devices: deviceState }));
app.get('/api/health', (req, res) => res.json({ status: 'ok', project: 'Farid Villa Smart Home' }));

app.post('/api/control', async (req, res) => {
  const { room, state } = req.body;
  if (!['room1', 'room2'].includes(room))
    return res.status(400).json({ success: false, message: 'Invalid room' });
  const s = (state || '').toUpperCase();
  if (!['ON', 'OFF'].includes(s))
    return res.status(400).json({ success: false, message: 'State must be ON or OFF' });

  mqttClient.publish(`home/${room}/light`, s, { retain: true });
  deviceState[room].light = s;
  mqttClient.publish('home/ui', JSON.stringify({ room, event: 'light', state: s, devices: deviceState, time: new Date().toLocaleTimeString('en-GB', { timeZone: 'Africa/Cairo' }) }));
  await logToSheet(room === 'room1' ? 'Room 1' : 'Room 2', 'Light', s);
  res.json({ success: true, room, state: s });
});

app.get('/api/logs', async (req, res) => {
  try { res.json({ success: true, logs: await getLogsFromSheet() }); }
  catch (e) { res.status(500).json({ success: false, message: e.message }); }
});

app.get('*', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

// ── Start ─────────────────────────────────────────────────────
const httpServer = http.createServer(app);

httpServer.listen(PORT, '0.0.0.0', () => {
  console.log(`\n🏠  Farid Villa Smart Home`);
  console.log(`🌐  HTTP server successfully running on port ${PORT}`);
});
