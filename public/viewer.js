'use strict';

const params = new URLSearchParams(location.search);
const id = params.get('id');
const nameEl = document.getElementById('name');
const topicEl = document.getElementById('topic');
const valueEl = document.getElementById('value');
const metaEl = document.getElementById('meta');
const payloadEl = document.getElementById('payload');
const liveEl = document.getElementById('live');
const rowsEl = document.getElementById('rows');
const canvas = document.getElementById('chart');

let stream = null;
let points = [];
let revision = null;
let events = null;

const GEN_LABEL = {
  constant: 'постоянное',
  uniform: 'равномерное',
  gaussian: 'нормальное',
  random_walk: 'блуждание',
  sine: 'синус',
  square: 'меандр',
  sawtooth: 'пила',
  ramp: 'рост',
  counter: 'счётчик',
};

function formatNumber(value, decimals) {
  if (value == null || Number.isNaN(Number(value))) return '—';
  return Number(value).toFixed(decimals);
}

function formatPeriod(ms) {
  if (ms < 1000) return `${ms} мс`;
  const sec = ms / 1000;
  const text = Number.isInteger(sec) ? String(sec) : String(Math.round(sec * 1000) / 1000).replace('.', ',');
  return `${text} с`;
}

function formatTime(ts) {
  const date = new Date(ts);
  const pad = (value, length = 2) => String(value).padStart(length, '0');
  return `${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}.${pad(date.getMilliseconds(), 3)}`;
}

function showMissing() {
  nameEl.textContent = 'Поток удалён';
  topicEl.textContent = '';
  valueEl.textContent = '—';
  metaEl.textContent = 'Публикация остановлена.';
  payloadEl.textContent = '';
  liveEl.dataset.state = 'error';
  liveEl.querySelector('span').textContent = 'удалён';
}

function paintMeta() {
  if (!stream) return;
  nameEl.textContent = stream.name;
  document.title = stream.name;
  topicEl.textContent = stream.topic;
  const range = `${formatNumber(stream.min, stream.decimals)}…${formatNumber(stream.max, stream.decimals)}`;
  metaEl.textContent = [
    GEN_LABEL[stream.generationType] || stream.generationType,
    stream.unit ? `${range} ${stream.unit}` : range,
    formatPeriod(stream.periodMs),
    `отправлено ${stream.published}`,
  ].join(' · ');
  const connected = stream._connected !== false;
  if (!stream.enabled) {
    liveEl.dataset.state = 'paused';
    liveEl.querySelector('span').textContent = 'пауза';
  } else if (!connected) {
    liveEl.dataset.state = 'wait';
    liveEl.querySelector('span').textContent = 'ждёт брокер';
  } else {
    liveEl.dataset.state = 'live';
    liveEl.querySelector('span').textContent = 'в эфире';
  }
}

function showValue(sample) {
  if (!stream) return;
  valueEl.textContent = `${formatNumber(sample.value, stream.decimals)}${stream.unit ? ` ${stream.unit}` : ''}`;
  payloadEl.textContent = sample.payload;
}

function fillRow(row, sample) {
  row.replaceChildren();
  const time = document.createElement('td');
  time.className = 'time';
  time.textContent = formatTime(sample.ts);
  const value = document.createElement('td');
  value.className = 'num';
  value.textContent = formatNumber(sample.value, stream ? stream.decimals : 2);
  const sent = document.createElement('td');
  sent.textContent = sample.sent ? 'да' : 'нет';
  if (!sample.sent) sent.className = 'miss';
  const payload = document.createElement('td');
  payload.className = 'payload';
  payload.textContent = sample.payload;
  row.append(time, value, sent, payload);
}

function addRow(sample) {
  const row = document.createElement('tr');
  row.dataset.ts = String(sample.ts);
  fillRow(row, sample);
  rowsEl.prepend(row);
  while (rowsEl.children.length > 80) rowsEl.lastElementChild.remove();
}

function rebuildTable() {
  rowsEl.replaceChildren();
  points.slice(-80).forEach(addRow);
}

function yScale() {
  if (!stream) return { min: 0, max: 1 };
  const counterLike = stream.generationType === 'counter' || stream.max === stream.min;
  if (!counterLike) return { min: stream.min, max: stream.max };
  if (!points.length) return { min: stream.min, max: stream.max === stream.min ? stream.min + 1 : stream.max };
  let min = Math.min(...points.map((point) => point.value));
  let max = Math.max(...points.map((point) => point.value));
  if (min === max) return { min: min - 1, max: max + 1 };
  const pad = (max - min) * 0.08;
  return { min: min - pad, max: max + pad };
}

function draw() {
  const rect = canvas.getBoundingClientRect();
  const dpr = window.devicePixelRatio || 1;
  canvas.width = Math.max(1, Math.floor(rect.width * dpr));
  canvas.height = Math.max(1, Math.floor(rect.height * dpr));
  const ctx = canvas.getContext('2d');
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  const width = rect.width;
  const height = rect.height;
  ctx.clearRect(0, 0, width, height);
  const left = 64;
  const right = 12;
  const top = 12;
  const bottom = 20;
  const plotW = Math.max(1, width - left - right);
  const plotH = Math.max(1, height - top - bottom);
  const scale = yScale();
  ctx.strokeStyle = '#362d59';
  ctx.fillStyle = 'rgba(255,255,255,0.72)';
  ctx.font = '12px Rubik, sans-serif';
  ctx.lineWidth = 1;
  for (let i = 0; i < 4; i += 1) {
    const y = top + (plotH * i) / 3;
    ctx.beginPath();
    ctx.moveTo(left, y);
    ctx.lineTo(left + plotW, y);
    ctx.stroke();
    const value = scale.max - ((scale.max - scale.min) * i) / 3;
    const decimals = stream ? stream.decimals : 2;
    ctx.fillText(formatNumber(value, decimals), 0, y + 4);
  }
  if (!points.length) {
    ctx.fillText('Ждём первое значение', left, top + plotH / 2);
    return;
  }
  ctx.beginPath();
  ctx.strokeStyle = '#fa7faa';
  ctx.lineWidth = 2;
  ctx.lineJoin = 'round';
  ctx.lineCap = 'round';
  points.forEach((point, index) => {
    const x = points.length === 1 ? left + plotW / 2 : left + (index / (points.length - 1)) * plotW;
    const y = top + ((scale.max - point.value) / (scale.max - scale.min || 1)) * plotH;
    if (index === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  });
  ctx.stroke();
}

function upsert(sample) {
  const index = points.findIndex((point) => point.ts === sample.ts);
  if (index >= 0) {
    points[index] = sample;
    const row = rowsEl.querySelector(`[data-ts="${sample.ts}"]`);
    if (row) fillRow(row, sample);
  } else {
    points.push(sample);
    if (points.length > 500) points.shift();
    addRow(sample);
  }
  showValue(sample);
  draw();
}

async function load() {
  if (!id) {
    showMissing();
    return;
  }
  const res = await fetch(`/api/streams/${encodeURIComponent(id)}`);
  if (res.status === 404) {
    showMissing();
    stream = null;
    return;
  }
  const data = await res.json();
  stream = data.stream;
  points = (stream.history || []).slice();
  paintMeta();
  rebuildTable();
  if (points.length) showValue(points[points.length - 1]);
  else payloadEl.textContent = 'ожидание значений';
  draw();
}

function connect() {
  events = new EventSource('/api/events');
  events.addEventListener('hello', (event) => {
    const state = JSON.parse(event.data);
    revision = state.revision;
    if (stream) {
      stream._connected = Boolean(state.brokerStatus && state.brokerStatus.connected);
      const fresh = state.streams.find((item) => item.id === id);
      if (!fresh) {
        stream = null;
        showMissing();
        return;
      }
      Object.assign(stream, fresh);
      paintMeta();
    }
  });
  events.addEventListener('sample', (event) => {
    const sample = JSON.parse(event.data);
    if (!stream || sample.id !== id) return;
    stream.published = sample.published;
    stream.lastError = sample.lastError || '';
    upsert(sample);
    paintMeta();
  });
  events.addEventListener('status', (event) => {
    if (!stream) return;
    const status = JSON.parse(event.data);
    stream._connected = Boolean(status.connected);
    paintMeta();
  });
  events.addEventListener('heartbeat', async (event) => {
    const beat = JSON.parse(event.data);
    if (!stream) return;
    stream._connected = Boolean(beat.broker && beat.broker.connected);
    if (beat.revision !== revision) {
      revision = beat.revision;
      await load();
    } else {
      paintMeta();
    }
  });
}

if (!id) showMissing();
else {
  load().then(connect).catch(() => {
    nameEl.textContent = 'Нет связи со службой';
  });
  const observer = new ResizeObserver(() => draw());
  observer.observe(canvas);
}
