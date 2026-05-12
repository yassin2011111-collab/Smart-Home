require('dotenv').config();
const http = require('http');
const express = require('express');
const cors = require('cors');
const path = require('path');
const { google } = require('googleapis');
const mqtt = require('mqtt');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const Database = require('better-sqlite3');

// ── Config ────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
// Flexible Sheet ID (check both names)
const SHEET_ID = process.env.GOOGLE_SHEET_ID || process.env.SHEET_ID;
const JWT_SECRET = process.env.JWT_SECRET || 'farid-villa-secret-key-2024';

// HiveMQ Cloud Credentials
const MQTT_HOST = process.env.MQTT_HOST || '9d3c2efbd1594219aee390c520d4d949.s1.eu.hivemq.cloud';
const MQTT_PORT = process.env.MQTT_PORT || 8883;
const MQTT_WS_PORT = 8884;
const MQTT_USERNAME = process.env.MQTT_USERNAME || 'yassin_ahmed';
const MQTT_PASSWORD = process.env.MQTT_PASSWORD || 'Yassin2005';

// ── Database Setup ───────────────────────────────────────────
// On Railway, /tmp is sometimes safer for ephemeral SQLite if not using a volume
const db = new Database('database.sqlite');
db.exec(`
  CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT UNIQUE,
    password TEXT
  )
`);

// Create default admin if not exists
const adminEmail = process.env.ADMIN_EMAIL || 'admin@farid.villa';
const adminPass = process.env.ADMIN_PASSWORD || 'admin123';
const existingUser = db.prepare('SELECT * FROM users WHERE email = ?').get(adminEmail);
if (!existingUser) {
    const hashed = bcrypt.hashSync(adminPass, 10);
    db.prepare('INSERT INTO users (email, password) VALUES (?, ?)').run(adminEmail, hashed);
    console.log(`[DB] Created default admin user: ${adminEmail}`);
}

// ── Express App ───────────────────────────────────────────────
const app = express();

process.on('uncaughtException', (err) => console.error('[System] Uncaught:', err.message));
process.on('unhandledRejection', (r) => console.error('[System] Rejection:', r));

app.use(cors({ origin: '*', credentials: true }));
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ── Auth Middleware ───────────────────────────────────────────
const auth = (req, res, next) => {
    const token = req.header('Authorization')?.replace('Bearer ', '');
    if (!token) return res.status(401).json({ success: false, message: 'Access denied' });
    try {
        const decoded = jwt.verify(token, JWT_SECRET);
        req.user = decoded;
        next();
    } catch (e) {
        res.status(401).json({ success: false, message: 'Invalid token' });
    }
};

// ── In-memory device state ────────────────────────────────────
const deviceState = {
    room1: { light: 'OFF', motion: 'CLEAR' },
    room2: { light: 'OFF', motion: 'CLEAR' },
};

// ── Google Sheets ─────────────────────────────────────────────
function getGoogleAuth() {
    // Check for Railway specific variable names from user screenshot
    const credsRaw = process.env['GOOGLE-PRIVATE-KEY'] || process.env.GOOGLE_SERVICE_ACCOUNT || '{}';
    let creds = {};
    try {
        creds = JSON.parse(credsRaw);
    } catch (e) {
        console.error('[Sheets] Failed to parse GOOGLE-PRIVATE-KEY JSON');
    }

    if (creds.private_key) creds.private_key = creds.private_key.replace(/\\n/g, '\n');
    
    return new google.auth.GoogleAuth({
        credentials: creds,
        scopes: ['https://www.googleapis.com/auth/spreadsheets'],
    });
}

async function logToSheet(room, event, state) {
    if (!SHEET_ID) return;
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
    if (!SHEET_ID) return [];
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

// Auth Routes
app.post('/api/auth/login', (req, res) => {
    const { email, password } = req.body;
    const user = db.prepare('SELECT * FROM users WHERE email = ?').get(email);
    if (!user || !bcrypt.compareSync(password, user.password)) {
        return res.status(401).json({ success: false, message: 'Invalid credentials' });
    }
    const token = jwt.sign({ id: user.id, email: user.email }, JWT_SECRET, { expiresIn: '24h' });
    res.json({ success: true, token, user: { email: user.email } });
});

// Protected Config (to give frontend MQTT creds)
app.get('/api/mqtt-config', auth, (req, res) => {
    res.json({
        host: MQTT_HOST,
        port: MQTT_WS_PORT,
        username: MQTT_USERNAME,
        password: MQTT_PASSWORD
    });
});

app.get('/api/status', auth, (req, res) => res.json({ success: true, devices: deviceState }));
app.get('/api/health', (req, res) => res.json({ status: 'ok', project: 'Farid Villa Smart Home' }));

app.post('/api/control', auth, async (req, res) => {
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

app.get('/api/logs', auth, async (req, res) => {
    try { res.json({ success: true, logs: await getLogsFromSheet() }); }
    catch (e) { res.status(500).json({ success: false, message: e.message }); }
});

app.get('*', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

// ── Start ─────────────────────────────────────────────────────
const httpServer = http.createServer(app);

httpServer.listen(PORT, '0.0.0.0', () => {
    console.log(`\n🏠  Farid Villa Smart Home`);
    console.log(`🌐  HTTP server successfully running on port ${PORT}`);
    console.log(`🔑  Default Admin: ${adminEmail} / ${adminPass}`);
});
