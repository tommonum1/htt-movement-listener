const express = require('express');
const cors = require('cors');
const stompit = require('stompit');

const app = express();
app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 3000;
const NR_HOST = process.env.NR_HOST || 'publicdatafeeds.networkrail.co.uk';
const NR_PORT = Number(process.env.NR_PORT || 61618);
const NR_USERNAME = process.env.NR_USERNAME;
const NR_PASSWORD = process.env.NR_PASSWORD;
const NR_TOPIC = process.env.NR_TOPIC || '/topic/TRAIN_MVT_ALL_TOC';
const MAX_AGE_MS = Number(process.env.MAX_AGE_MS || 6 * 60 * 60 * 1000); // keep 6 hours

if (!NR_USERNAME || !NR_PASSWORD) {
  console.warn('WARNING: NR_USERNAME and NR_PASSWORD environment variables are not set yet.');
}

const latestByTrainId = new Map();
const latestByHeadcode = new Map();
const recentEvents = [];
let status = {
  connected: false,
  connecting: false,
  lastConnectAt: null,
  lastMessageAt: null,
  messagesReceived: 0,
  movementsReceived: 0,
  lastError: null,
  topic: NR_TOPIC,
};

function cleanTrainId(value) {
  return String(value || '').trim().toUpperCase();
}

function getBodyField(body, names) {
  for (const n of names) {
    if (body[n] !== undefined && body[n] !== null && body[n] !== '') return body[n];
  }
  return null;
}

function normaliseMovement(message) {
  const header = message.header || {};
  const body = message.body || {};
  const messageType = header.msg_type || body.msg_type || '';

  const trainId = cleanTrainId(getBodyField(body, ['train_id', 'trainId']));
  const headcode = cleanTrainId(getBodyField(body, ['train_service_code', 'train_service_code']));
  const locStanox = cleanTrainId(getBodyField(body, ['loc_stanox', 'locStanox']));
  const plannedTimestamp = Number(getBodyField(body, ['planned_timestamp', 'plannedTimestamp']) || 0);
  const actualTimestamp = Number(getBodyField(body, ['actual_timestamp', 'actualTimestamp']) || 0);
  const gbttTimestamp = Number(getBodyField(body, ['gbtt_timestamp', 'gbttTimestamp']) || 0);
  const eventType = getBodyField(body, ['event_type', 'eventType']);
  const variationStatus = getBodyField(body, ['variation_status', 'variationStatus']);
  const timetableVariation = getBodyField(body, ['timetable_variation', 'timetableVariation']);

  const eventTime = actualTimestamp || plannedTimestamp || gbttTimestamp || Date.now();

  return {
    receivedAt: Date.now(),
    messageType,
    trainId,
    headcode,
    locStanox,
    eventType,
    variationStatus,
    timetableVariation,
    plannedTimestamp: plannedTimestamp || null,
    actualTimestamp: actualTimestamp || null,
    gbttTimestamp: gbttTimestamp || null,
    eventTime,
    rawBody: body,
  };
}

function remember(event) {
  if (!event.trainId && !event.headcode) return;

  if (event.trainId) latestByTrainId.set(event.trainId, event);
  if (event.headcode) latestByHeadcode.set(event.headcode, event);

  recentEvents.push(event);
  if (recentEvents.length > 1000) recentEvents.splice(0, recentEvents.length - 1000);

  const cutoff = Date.now() - MAX_AGE_MS;
  for (const [k, v] of latestByTrainId) if (v.receivedAt < cutoff) latestByTrainId.delete(k);
  for (const [k, v] of latestByHeadcode) if (v.receivedAt < cutoff) latestByHeadcode.delete(k);
}

function connect() {
  if (status.connecting) return;
  status.connecting = true;
  status.lastError = null;

  const connectOptions = {
    host: NR_HOST,
    port: NR_PORT,
    connectHeaders: {
      host: '/',
      login: NR_USERNAME,
      passcode: NR_PASSWORD,
      'heart-beat': '15000,15000'
    }
  };

  console.log(`Connecting to Network Rail STOMP ${NR_HOST}:${NR_PORT} ${NR_TOPIC}`);

  stompit.connect(connectOptions, (error, client) => {
    status.connecting = false;

    if (error) {
      status.connected = false;
      status.lastError = String(error.message || error);
      console.error('STOMP connect error:', status.lastError);
      setTimeout(connect, 10000);
      return;
    }

    status.connected = true;
    status.lastConnectAt = Date.now();
    console.log('Connected to Network Rail STOMP');

    const subscribeHeaders = {
      destination: NR_TOPIC,
      ack: 'auto',
      id: 'htt-movement-listener'
    };

    client.subscribe(subscribeHeaders, (subError, message) => {
      if (subError) {
        status.lastError = String(subError.message || subError);
        console.error('STOMP subscribe error:', status.lastError);
        return;
      }

      message.readString('utf-8', (readError, bodyText) => {
        if (readError) {
          status.lastError = String(readError.message || readError);
          console.error('STOMP read error:', status.lastError);
          return;
        }

        status.messagesReceived += 1;
        status.lastMessageAt = Date.now();

        try {
          const messages = JSON.parse(bodyText);
          const arr = Array.isArray(messages) ? messages : [messages];
          for (const msg of arr) {
            const type = (msg.header && msg.header.msg_type) || (msg.body && msg.body.msg_type);
            if (type === '0003') {
              const event = normaliseMovement(msg);
              remember(event);
              status.movementsReceived += 1;
            }
          }
        } catch (e) {
          status.lastError = 'JSON parse failed: ' + String(e.message || e);
          console.error(status.lastError, bodyText.slice(0, 300));
        }
      });
    });

    client.on('error', (e) => {
      status.connected = false;
      status.lastError = String(e.message || e);
      console.error('STOMP client error:', status.lastError);
      try { client.disconnect(); } catch (_) {}
      setTimeout(connect, 10000);
    });

    client.on('end', () => {
      status.connected = false;
      console.warn('STOMP connection ended; reconnecting...');
      setTimeout(connect, 10000);
    });
  });
}

app.get('/', (req, res) => {
  res.json({ ok: true, service: 'HTT Movement Listener', status });
});

app.get('/health', (req, res) => {
  res.json({ ok: true, status, counts: { byTrainId: latestByTrainId.size, byHeadcode: latestByHeadcode.size, recent: recentEvents.length } });
});

app.get('/latest/:id', (req, res) => {
  const id = cleanTrainId(req.params.id);
  const event = latestByTrainId.get(id) || latestByHeadcode.get(id) || null;
  res.json({ ok: true, id, event });
});

app.get('/latest', (req, res) => {
  const ids = String(req.query.ids || '').split(',').map(cleanTrainId).filter(Boolean).slice(0, 200);
  const out = {};
  for (const id of ids) out[id] = latestByTrainId.get(id) || latestByHeadcode.get(id) || null;
  res.json({ ok: true, results: out });
});

app.get('/recent', (req, res) => {
  const limit = Math.min(Number(req.query.limit || 50), 200);
  res.json({ ok: true, events: recentEvents.slice(-limit).reverse() });
});

app.listen(PORT, () => {
  console.log(`HTT Movement Listener HTTP server running on port ${PORT}`);
  connect();
});
