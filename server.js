const express = require('express');
const cors = require('cors');
const stompit = require('stompit');

const app = express();
app.use(cors());
app.use(express.json({ limit: '1mb' }));

const PORT = process.env.PORT || 3000;
const NR_HOST = process.env.NR_HOST || 'publicdatafeeds.networkrail.co.uk';
const NR_PORT = Number(process.env.NR_PORT || 61618);
const NR_USERNAME = process.env.NR_USERNAME;
const NR_PASSWORD = process.env.NR_PASSWORD;
const NR_TOPIC = process.env.NR_TOPIC || '/topic/TRAIN_MVT_ALL_TOC';

const MAX_AGE_MS = Number(process.env.MAX_AGE_MS || 12 * 60 * 60 * 1000);
const ACTIVE_AGE_MS = Number(process.env.ACTIVE_AGE_MS || 45 * 60 * 1000);
const RECENT_LIMIT = Number(process.env.RECENT_LIMIT || 3000);

const latestByTrainId = new Map();
const latestByUid = new Map();
const latestByScheduleUid = new Map();
const recentEvents = [];

let status = {
  version: 'movement-listener-v2',
  connected: false,
  connecting: false,
  lastConnectAt: null,
  lastMessageAt: null,
  messagesReceived: 0,
  movementsReceived: 0,
  activationsReceived: 0,
  cancellationsReceived: 0,
  lastError: null,
  topic: NR_TOPIC,
};

function clean(value) {
  return String(value || '').trim().toUpperCase();
}

function bodyField(body, names) {
  for (const n of names) {
    if (body[n] !== undefined && body[n] !== null && body[n] !== '') return body[n];
  }
  return null;
}

function toNumber(value) {
  const n = Number(value || 0);
  return Number.isFinite(n) ? n : 0;
}

function normaliseTrustMessage(message) {
  const header = message.header || {};
  const body = message.body || {};
  const type = clean(header.msg_type || body.msg_type);

  const trainId = clean(bodyField(body, ['train_id', 'trainId']));
  const trainUid = clean(bodyField(body, ['train_uid', 'trainUid']));
  const scheduleUid = clean(bodyField(body, ['schedule_uid', 'scheduleUid', 'train_uid']));
  const locStanox = clean(bodyField(body, ['loc_stanox', 'locStanox']));
  const originalLocStanox = clean(bodyField(body, ['original_loc_stanox', 'originalLocStanox']));
  const nextReportStanox = clean(bodyField(body, ['next_report_stanox', 'nextReportStanox']));
  const reportingStanox = locStanox || originalLocStanox;

  const plannedTimestamp = toNumber(bodyField(body, ['planned_timestamp', 'plannedTimestamp']));
  const actualTimestamp = toNumber(bodyField(body, ['actual_timestamp', 'actualTimestamp']));
  const gbttTimestamp = toNumber(bodyField(body, ['gbtt_timestamp', 'gbttTimestamp']));
  const depTimestamp = toNumber(bodyField(body, ['dep_timestamp', 'depTimestamp']));
  const originDepTimestamp = toNumber(bodyField(body, ['orig_dep_timestamp', 'origin_dep_timestamp', 'origDepTimestamp']));
  const canxTimestamp = toNumber(bodyField(body, ['canx_timestamp', 'canxTimestamp']));

  const eventTime = actualTimestamp || depTimestamp || canxTimestamp || plannedTimestamp || gbttTimestamp || originDepTimestamp || Date.now();

  return {
    receivedAt: Date.now(),
    messageType: type,
    trainId,
    trainUid,
    scheduleUid,
    locStanox,
    originalLocStanox,
    reportingStanox,
    nextReportStanox,
    eventType: bodyField(body, ['event_type', 'eventType']),
    variationStatus: bodyField(body, ['variation_status', 'variationStatus']),
    timetableVariation: bodyField(body, ['timetable_variation', 'timetableVariation']),
    direction: bodyField(body, ['direction_ind', 'directionInd']),
    line: bodyField(body, ['line_ind', 'lineInd']),
    platform: bodyField(body, ['platform']),
    plannedTimestamp: plannedTimestamp || null,
    actualTimestamp: actualTimestamp || null,
    gbttTimestamp: gbttTimestamp || null,
    depTimestamp: depTimestamp || null,
    originDepTimestamp: originDepTimestamp || null,
    canxTimestamp: canxTimestamp || null,
    eventTime,
    rawBody: body
  };
}

function isMovementLike(event) {
  return event.messageType === '0003' || event.messageType === '0001' || event.messageType === '0002' || event.messageType === '0005';
}

function remember(event) {
  if (!isMovementLike(event)) return;

  if (event.messageType === '0001') status.activationsReceived += 1;
  if (event.messageType === '0002') status.cancellationsReceived += 1;
  if (event.messageType === '0003') status.movementsReceived += 1;

  if (event.trainId) latestByTrainId.set(event.trainId, event);
  if (event.trainUid) latestByUid.set(event.trainUid, event);
  if (event.scheduleUid) latestByScheduleUid.set(event.scheduleUid, event);

  recentEvents.push(event);
  if (recentEvents.length > RECENT_LIMIT) recentEvents.splice(0, recentEvents.length - RECENT_LIMIT);
  purgeOld();
}

function purgeOld() {
  const cutoff = Date.now() - MAX_AGE_MS;
  for (const [k, v] of latestByTrainId) if (v.receivedAt < cutoff) latestByTrainId.delete(k);
  for (const [k, v] of latestByUid) if (v.receivedAt < cutoff) latestByUid.delete(k);
  for (const [k, v] of latestByScheduleUid) if (v.receivedAt < cutoff) latestByScheduleUid.delete(k);
}

function publicEvent(event) {
  if (!event) return null;
  const ageMs = Date.now() - event.receivedAt;
  return {
    ...event,
    isActive: ageMs <= ACTIVE_AGE_MS,
    isStale: ageMs > ACTIVE_AGE_MS,
    trustAgeSeconds: Math.round(ageMs / 1000),
    confidence: ageMs <= ACTIVE_AGE_MS ? 'TRUST LIVE' : 'TRUST STALE'
  };
}

function findLatestForIds(ids) {
  for (const raw of ids) {
    const id = clean(raw);
    if (!id) continue;
    const found = latestByTrainId.get(id) || latestByUid.get(id) || latestByScheduleUid.get(id);
    if (found) return found;
  }
  return null;
}

function connect() {
  if (status.connecting || status.connected) return;
  status.connecting = true;
  status.lastError = null;

  if (!NR_USERNAME || !NR_PASSWORD) {
    status.lastError = 'Missing NR_USERNAME or NR_PASSWORD environment variable';
    console.error(status.lastError);
    status.connecting = false;
    setTimeout(connect, 30000);
    return;
  }

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

    client.subscribe({ destination: NR_TOPIC, ack: 'auto', id: 'htt-movement-listener-v2' }, (subError, message) => {
      if (subError) {
        status.lastError = String(subError.message || subError);
        console.error('STOMP subscribe error:', status.lastError);
        return;
      }

      message.readString('utf8', (readError, bodyText) => {
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
          for (const msg of arr) remember(normaliseTrustMessage(msg));
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
  res.json({
    ok: true,
    status,
    counts: {
      byTrainId: latestByTrainId.size,
      byUid: latestByUid.size,
      byScheduleUid: latestByScheduleUid.size,
      recent: recentEvents.length
    }
  });
});

app.get('/latest/:id', (req, res) => {
  const ids = String(req.params.id || '').split(',');
  const event = findLatestForIds(ids);
  res.json({ ok: true, id: req.params.id, event: publicEvent(event) });
});

app.get('/latest', (req, res) => {
  const ids = String(req.query.ids || '').split(',').map(clean).filter(Boolean).slice(0, 500);
  const results = {};
  for (const id of ids) results[id] = publicEvent(findLatestForIds([id]));
  res.json({ ok: true, results });
});

app.get('/match', (req, res) => {
  const ids = String(req.query.ids || '').split(',').map(clean).filter(Boolean).slice(0, 30);
  const includeStale = String(req.query.includeStale || '').toLowerCase() === 'true';
  const event = publicEvent(findLatestForIds(ids));
  if (!event || (!includeStale && event.isStale)) return res.json({ ok: true, matched: false, event: null });
  res.json({ ok: true, matched: true, event });
});

app.get('/recent', (req, res) => {
  const limit = Math.min(Number(req.query.limit || 50), 300);
  const type = clean(req.query.type || '');
  let events = recentEvents;
  if (type) events = events.filter(e => e.messageType === type);
  res.json({ ok: true, events: events.slice(-limit).reverse().map(publicEvent) });
});

app.get('/active', (req, res) => {
  const limit = Math.min(Number(req.query.limit || 100), 500);
  const active = [];
  for (const event of latestByTrainId.values()) {
    const pub = publicEvent(event);
    if (pub && pub.isActive && pub.messageType !== '0002') active.push(pub);
  }
  active.sort((a, b) => b.receivedAt - a.receivedAt);
  res.json({ ok: true, count: active.length, events: active.slice(0, limit) });
});

app.listen(PORT, () => {
  console.log(`HTT Movement Listener v2 running on port ${PORT}`);
  connect();
});
