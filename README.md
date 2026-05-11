# 🏠 Farid Villa – Smart Home System

## Architecture

```
ESP32 (PIR + LEDs)
    │  TCP (plain MQTT)
    ▼
Railway TCP Proxy
    │
    ▼
Built-in MQTT Broker (inside Node.js)
    │  WebSocket MQTT
    ▼
Browser Frontend (index.html)
    │  REST API
    ▼
Node.js Backend (index.js)
    │
    ▼
Google Sheets (activity log)
```

## Project Structure

```
smart-home/
├── index.js          ← Main server (Express + MQTT backend logic)
├── broker.js         ← Built-in MQTT broker (TCP + WebSocket)
├── package.json
├── .env              ← Local environment variables
├── Procfile          ← Railway start command
├── public/
│   └── index.html    ← Frontend dashboard (served by Express)
└── arduino/
    └── sketch_farid_villa/
        └── sketch_farid_villa.ino  ← ESP32 firmware
```

## Run Locally

```bash
cd smart-home
npm install
node index.js
```

Open: http://localhost:3000

## Deploy to Railway

1. Push this folder to GitHub
2. Create new Railway project → Deploy from GitHub
3. Add environment variables in Railway dashboard:

| Variable | Value |
|---|---|
| `PORT` | (Railway sets this automatically) |
| `TCP_PORT` | `1883` |
| `SHEET_ID` | Your Google Sheet ID |
| `GOOGLE_SERVICE_ACCOUNT` | Full service account JSON (one line) |

4. Add a **TCP Proxy** in Railway: ✅ Already configured
   - Host: `yamabiko.proxy.rlwy.net`
   - Port: `25231`
   - Target: `:1883`

5. ESP32 sketch already has the correct host and port — just upload it

## MQTT Topics

| Topic | Direction | Payload |
|---|---|---|
| `home/room1/light` | Backend → ESP32 | `ON` or `OFF` |
| `home/room2/light` | Backend → ESP32 | `ON` or `OFF` |
| `home/room1/motion` | ESP32 → Backend | `DETECTED` or `CLEAR` |
| `home/room2/motion` | ESP32 → Backend | `DETECTED` or `CLEAR` |
| `home/ui` | Backend → Frontend | JSON with full device state |
| `home/status` | Backend → Frontend | JSON with device state (retained) |
| `home/request_status` | Frontend → Backend | `{}` |

## REST API

| Method | Endpoint | Description |
|---|---|---|
| GET | `/api/health` | Server health check |
| GET | `/api/status` | Current device states |
| POST | `/api/control` | Control a light `{room, state}` |
| GET | `/api/logs` | Activity history from Google Sheets |

## Google Sheets Setup

1. Create a Google Sheet with columns: Room, Event, State, Date, Time
2. Create a Service Account in Google Cloud Console
3. Share the sheet with the service account email
4. Copy the Sheet ID from the URL
5. Download the service account JSON key
6. Set `SHEET_ID` and `GOOGLE_SERVICE_ACCOUNT` in Railway env vars

## ESP32 Wiring

| Component | GPIO |
|---|---|
| PIR Sensor 1 | 14 |
| PIR Sensor 2 | 33 |
| LED 1 | 27 |
| LED 2 | 25 |
