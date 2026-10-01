'use strict';

const SIGNAL_LABEL = {
  temperature: 'температура',
  pressure: 'давление',
  level: 'уровень',
  flow: 'расход',
  humidity: 'влажность',
  voltage: 'напряжение',
  current: 'ток',
  discrete: 'дискретный',
  counter: 'счётчик',
  custom: 'произвольный',
};

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

const GEN_HELP = {
  constant: 'В каждом сообщении одно и то же значение внутри диапазона.',
  uniform: 'Каждое значение равновероятно между минимумом и максимумом.',
  gaussian: 'Нормальный шум вокруг середины диапазона. Края почти не достигаются.',
  random_walk: 'Плавный дрейф: шаг случайный и не больше заданной доли диапазона.',
  sine: 'Плавная волна от минимума к максимуму и обратно.',
  square: 'Два уровня — минимум и максимум — с заданной скважностью.',
  sawtooth: 'Линейный рост от минимума к максимуму и резкий сброс.',
  ramp: 'Рост от минимума к максимуму за указанное время.',
  counter: 'К предыдущему значению прибавляется шаг. Первое сообщение равно старту.',
};

const PRESETS = {
  temperature: { unit: '°C', min: 15, max: 35, decimals: 1, generationType: 'sine', wavePeriodSec: 600, periodSec: 1 },
  pressure: { unit: 'бар', min: 0, max: 10, decimals: 2, generationType: 'gaussian', periodSec: 1 },
  level: { unit: '%', min: 0, max: 100, decimals: 1, generationType: 'random_walk', stepPercent: 1.5, periodSec: 1 },
  flow: { unit: 'м³/ч', min: 0, max: 120, decimals: 2, generationType: 'gaussian', periodSec: 1 },
  humidity: { unit: '%', min: 30, max: 70, decimals: 1, generationType: 'sine', wavePeriodSec: 1800, periodSec: 2 },
  voltage: { unit: 'В', min: 210, max: 240, decimals: 1, generationType: 'gaussian', periodSec: 1 },
  current: { unit: 'А', min: 0, max: 40, decimals: 2, generationType: 'random_walk', stepPercent: 3, periodSec: 1 },
  discrete: { unit: '', min: 0, max: 1, decimals: 0, generationType: 'square', wavePeriodSec: 20, dutyPercent: 50, periodSec: 1 },
  counter: { unit: 'имп', min: 0, max: 1000000000, decimals: 0, generationType: 'counter', counterStep: 1, counterStart: 0, periodSec: 1 },
  custom: { unit: '', min: 0, max: 100, decimals: 2, generationType: 'uniform', periodSec: 1 },
};

const PORT_BY_PROTOCOL = { mqtt: 1883, mqtts: 8883, ws: 9001, wss: 9001 };

const brokerForm = document.getElementById('broker-form');
const addForm = document.getElementById('add-form');
const streamList = document.getElementById('stream-list');
const streamEmpty = document.getElementById('stream-empty');
const streamCount = document.getElementById('stream-count');
const editDialog = document.getElementById('edit-dialog');
const deleteDialog = document.getElementById('delete-dialog');

let model = null;
let brokerDirty = false;
let fillingBroker = false;
let events = null;
let sseLive = false;
let clockSkew = 0;
let uptimeAnchor = null;
let pendingDelete = null;

function api(url, { method = 'GET', body } = {}) {
  return fetch(url, {
    method,
    headers: body ? { 'Content-Type': 'application/json' } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  }).then(async (res) => {
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data.error || 'Ошибка запроса');
    return data;
  });
}

function noteServerNow(serverNow) {
  if (typeof serverNow === 'number') clockSkew = Date.now() - serverNow;
}

function noteUptime(sec) {
  if (typeof sec === 'number') uptimeAnchor = { sec, at: Date.now() };
}

function currentUptime() {
  if (!uptimeAnchor) return 0;
  return uptimeAnchor.sec + (Date.now() - uptimeAnchor.at) / 1000;
}

function formatDuration(totalSec) {
  const sec = Math.max(0, Math.floor(totalSec));
  const days = Math.floor(sec / 86400);
  const hours = Math.floor((sec % 86400) / 3600);
  const minutes = Math.floor((sec % 3600) / 60);
  if (days > 0) return `${days} д ${hours} ч`;
  if (hours > 0) return `${hours} ч ${minutes} мин`;
  if (minutes > 0) return `${minutes} мин`;
  return `${sec} с`;
}

function formatPeriod(ms) {
  if (ms < 1000) return `${ms} мс`;
  const sec = ms / 1000;
  const text = Number.isInteger(sec) ? String(sec) : String(Math.round(sec * 1000) / 1000).replace('.', ',');
  return `${text} с`;
}

function formatNumber(value, decimals) {
  if (value == null || Number.isNaN(Number(value))) return '—';
  return Number(value).toFixed(decimals);
}

function humanBrokerMessage(message) {
  if (!message) return '';
  if (/ECONNREFUSED/i.test(message)) return 'порт закрыт, брокер не отвечает';
  if (/ENOTFOUND/i.test(message)) return 'хост не найден';
  if (/ETIMEDOUT|timed out/i.test(message)) return 'таймаут соединения';
  if (/not authorized|bad user|bad user name|not authorised/i.test(message)) return 'отказано в доступе';
  if (/certificate|self signed|UNABLE_TO_VERIFY/i.test(message)) return 'ошибка сертификата TLS';
  return message;
}

function brokerAddress(broker) {
  if (!broker || !broker.host) return '';
  const host = broker.host.includes(':') && !broker.host.startsWith('[') ? `[${broker.host}]` : broker.host;
  if (broker.protocol === 'ws' || broker.protocol === 'wss') {
    return `${broker.protocol}://${host}:${broker.port}${broker.path || '/mqtt'}`;
  }
  return `${broker.protocol}://${host}:${broker.port}`;
}

function setInput(form, name, value) {
  const el = form.elements[name];
  if (!el || value == null) return;
  el.value = value;
}

function applyVisibility(form) {
  const generation = form.elements.generationType.value;
  form.querySelectorAll('[data-show]').forEach((el) => {
    const show = el.dataset.show.split(/\s+/).includes(generation);
    el.hidden = !show;
    el.querySelectorAll('input, select, textarea').forEach((input) => {
      input.disabled = !show;
    });
  });
}

function updateHelp(form) {
  const help = form.querySelector('[data-gen-help]');
  if (help) help.textContent = GEN_HELP[form.elements.generationType.value] || '';
}

function applyPreset(form, signalType) {
  const preset = PRESETS[signalType];
  if (!preset) return;
  Object.entries(preset).forEach(([key, value]) => setInput(form, key, value));
}

function bindStreamForm(form, { preset }) {
  const sync = () => {
    applyVisibility(form);
    updateHelp(form);
  };
  form.elements.generationType.addEventListener('change', sync);
  if (preset) {
    form.elements.signalType.addEventListener('change', () => {
      applyPreset(form, form.elements.signalType.value);
      sync();
    });
  }
  sync();
}

function num(form, name) {
  const el = form.elements[name];
  if (!el || el.disabled || el.value === '') return undefined;
  const value = Number(el.value);
  return Number.isFinite(value) ? value : undefined;
}

function collectStream(form) {
  return {
    name: form.elements.name.value,
    signalType: form.elements.signalType.value,
    generationType: form.elements.generationType.value,
    min: num(form, 'min'),
    max: num(form, 'max'),
    decimals: num(form, 'decimals'),
    periodSec: num(form, 'periodSec'),
    unit: form.elements.unit.value,
    topic: form.elements.topic.value,
    qos: num(form, 'qos'),
    payloadFormat: form.elements.payloadFormat.value,
    retain: form.elements.retain.value === 'true',
    constantValue: num(form, 'constantValue'),
    wavePeriodSec: num(form, 'wavePeriodSec'),
    dutyPercent: num(form, 'dutyPercent'),
    stepPercent: num(form, 'stepPercent'),
    rampDurationSec: num(form, 'rampDurationSec'),
    rampMode: form.elements.rampMode.disabled ? undefined : form.elements.rampMode.value,
    counterStart: num(form, 'counterStart'),
    counterStep: num(form, 'counterStep'),
    counterWrap: !form.elements.counterWrap.disabled && form.elements.counterWrap.value === 'true',
  };
}

function fillStreamForm(form, stream) {
  setInput(form, 'name', stream.name);
  setInput(form, 'signalType', stream.signalType);
  setInput(form, 'generationType', stream.generationType);
  setInput(form, 'min', stream.min);
  setInput(form, 'max', stream.max);
  setInput(form, 'decimals', stream.decimals);
  setInput(form, 'periodSec', Math.round(stream.periodMs) / 1000);
  setInput(form, 'unit', stream.unit || '');
  setInput(form, 'topic', stream.topic);
  setInput(form, 'qos', stream.qos);
  setInput(form, 'payloadFormat', stream.payloadFormat);
  setInput(form, 'retain', stream.retain ? 'true' : 'false');
  setInput(form, 'constantValue', stream.constantValue == null ? '' : stream.constantValue);
  setInput(form, 'wavePeriodSec', stream.wavePeriodSec);
  setInput(form, 'dutyPercent', stream.dutyPercent);
  setInput(form, 'stepPercent', stream.stepPercent);
  setInput(form, 'rampDurationSec', stream.rampDurationSec);
  setInput(form, 'rampMode', stream.rampMode || 'repeat');
  setInput(form, 'counterStart', stream.counterStart);
  setInput(form, 'counterStep', stream.counterStep);
  setInput(form, 'counterWrap', stream.counterWrap ? 'true' : 'false');
  applyVisibility(form);
  updateHelp(form);
}

function syncBrokerFields() {
  const protocol = brokerForm.elements.protocol.value;
  const ws = protocol === 'ws' || protocol === 'wss';
  const tls = protocol === 'mqtts' || protocol === 'wss';
  document.getElementById('path-field').hidden = !ws;
  document.getElementById('tls-field').hidden = !tls;
  brokerForm.elements.path.disabled = !ws;
}

function fillBroker(broker) {
  const data = broker || {};
  fillingBroker = true;
  setInput(brokerForm, 'protocol', data.protocol || 'mqtt');
  setInput(brokerForm, 'host', data.host || '127.0.0.1');
  setInput(brokerForm, 'port', data.port || PORT_BY_PROTOCOL[data.protocol || 'mqtt']);
  setInput(brokerForm, 'path', data.path || '/mqtt');
  setInput(brokerForm, 'username', data.username || '');
  setInput(brokerForm, 'password', data.password || '');
  setInput(brokerForm, 'clientId', data.clientId || '');
  setInput(brokerForm, 'rejectUnauthorized', data.rejectUnauthorized === false ? 'false' : 'true');
  syncBrokerFields();
  fillingBroker = false;
}

function collectBroker() {
  const protocol = brokerForm.elements.protocol.value;
  return {
    protocol,
    host: brokerForm.elements.host.value,
    port: Number(brokerForm.elements.port.value),
    path: protocol === 'ws' || protocol === 'wss' ? brokerForm.elements.path.value : '',
    username: brokerForm.elements.username.value,
    password: brokerForm.elements.password.value,
    clientId: brokerForm.elements.clientId.value,
    rejectUnauthorized: brokerForm.elements.rejectUnauthorized.value !== 'false',
  };
}

function connectedSeconds() {
  const status = model && model.brokerStatus;
  if (!status || status.state !== 'connected' || !status.connectedAt) return null;
  return Math.max(0, (Date.now() - clockSkew - status.connectedAt) / 1000);
}

function renderStatus() {
  if (!model) return;
  const status = model.brokerStatus || { state: 'disconnected', message: '', connected: false };
  const pill = document.getElementById('broker-pill');
  const text = document.getElementById('broker-pill-text');
  const detail = document.getElementById('broker-detail');
  const labels = {
    disconnected: 'Нет соединения',
    connecting: 'Подключение…',
    connected: 'Подключен',
    reconnecting: 'Переподключение…',
    error: 'Ошибка',
  };
  pill.dataset.state = status.state || 'disconnected';
  text.textContent = labels[status.state] || labels.disconnected;
  const parts = [];
  if (model && model.broker) parts.push(brokerAddress(model.broker));
  const linkSec = connectedSeconds();
  if (linkSec != null) parts.push(formatDuration(linkSec));
  const message = humanBrokerMessage(status.message);
  if (message && status.state !== 'connected') parts.push(message);
  if (sseLive === false && events) parts.push('нет связи со службой, переподключение…');
  detail.textContent = parts.filter(Boolean).join(' · ');
  const submit = document.getElementById('broker-submit');
  if (!status.connected) submit.textContent = 'Подключить';
  else if (brokerDirty) submit.textContent = 'Переподключить';
  else submit.textContent = 'Отключить';
  const uptime = document.getElementById('uptime');
  uptime.textContent = model ? `процесс работает ${formatDuration(currentUptime())}` : 'нет связи со службой';
}

function metaLine(stream) {
  const range = `${formatNumber(stream.min, stream.decimals)}…${formatNumber(stream.max, stream.decimals)}`;
  const parts = [
    SIGNAL_LABEL[stream.signalType] || stream.signalType,
    GEN_LABEL[stream.generationType] || stream.generationType,
    stream.unit ? `${range} ${stream.unit}` : range,
    formatPeriod(stream.periodMs),
    `QoS ${stream.qos}`,
    stream.payloadFormat === 'json' ? 'JSON' : 'число',
    `отправлено ${stream.published}`,
  ];
  if (stream.dropped) parts.push(`не доставлено ${stream.dropped}`);
  if (stream.skipped) parts.push(`пропущено тактов ${stream.skipped}`);
  if (stream.coalesced) parts.push(`схлопнуто ${stream.coalesced}`);
  return parts.join(' · ');
}

function badgeOf(stream) {
  if (!stream.enabled) return { text: 'Пауза', state: 'paused' };
  const connected = Boolean(model && model.brokerStatus && model.brokerStatus.connected);
  if (!connected) return { text: 'Ждёт брокер', state: 'wait' };
  if (stream.lastError) return { text: 'Ошибка', state: 'error' };
  return { text: 'В эфире', state: 'live' };
}

function ageText(ts) {
  if (!ts) return 'ещё не было значений';
  const sec = Math.max(0, Math.round((Date.now() - ts) / 1000));
  if (sec < 1) return 'только что';
  if (sec < 60) return `${sec} с назад`;
  const min = Math.floor(sec / 60);
  if (min < 60) return `${min} мин назад`;
  return `${Math.floor(min / 60)} ч назад`;
}

function drawSpark(svg, values) {
  while (svg.firstChild) svg.removeChild(svg.firstChild);
  if (!values || values.length < 2) return;
  const width = 160;
  const height = 36;
  const min = Math.min(...values);
  const max = Math.max(...values);
  const span = max - min || 1;
  const points = values.map((value, index) => {
    const x = (index / (values.length - 1)) * width;
    const y = height - 2 - ((value - min) / span) * (height - 4);
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(' ');
  const line = document.createElementNS('http://www.w3.org/2000/svg', 'polyline');
  line.setAttribute('points', points);
  line.setAttribute('fill', 'none');
  line.setAttribute('stroke', '#fa7faa');
  line.setAttribute('stroke-width', '1.75');
  line.setAttribute('stroke-linejoin', 'round');
  line.setAttribute('stroke-linecap', 'round');
  svg.appendChild(line);
}

function fillCard(card, stream) {
  card.querySelector('[data-field="value"]').textContent = stream.lastValue == null
    ? '—'
    : `${formatNumber(stream.lastValue, stream.decimals)}${stream.unit ? ` ${stream.unit}` : ''}`;
  card.querySelector('[data-field="age"]').textContent = ageText(stream.lastTs);
  card.querySelector('[data-field="meta"]').textContent = metaLine(stream);
  const badge = badgeOf(stream);
  const pill = card.querySelector('[data-field="badge"]');
  pill.dataset.state = badge.state;
  pill.querySelector('span').textContent = badge.text;
  const error = card.querySelector('[data-field="error"]');
  error.textContent = stream.lastError || '';
  error.hidden = !stream.lastError;
  const pause = card.querySelector('[data-action="pause"]');
  pause.textContent = stream.enabled ? 'Пауза' : 'Пуск';
  drawSpark(card.querySelector('[data-field="spark"]'), stream.spark || []);
}

function streamById(id) {
  return model && model.streams.find((stream) => stream.id === id);
}

function buildCard(stream) {
  const article = document.createElement('article');
  article.className = 'card stream';
  article.dataset.id = stream.id;

  const valueBox = document.createElement('div');
  const value = document.createElement('p');
  value.className = 'value';
  value.dataset.field = 'value';
  const age = document.createElement('p');
  age.className = 'muted';
  age.dataset.field = 'age';
  valueBox.append(value, age);

  const mid = document.createElement('div');
  const titleRow = document.createElement('div');
  titleRow.className = 'title-row';
  const heading = document.createElement('h3');
  heading.textContent = stream.name;
  const pill = document.createElement('span');
  pill.className = 'pill';
  pill.dataset.field = 'badge';
  const dot = document.createElement('i');
  const pillText = document.createElement('span');
  pill.append(dot, pillText);
  titleRow.append(heading, pill);
  const topic = document.createElement('p');
  topic.className = 'topic';
  topic.textContent = stream.topic;
  const meta = document.createElement('p');
  meta.className = 'muted';
  meta.dataset.field = 'meta';
  const error = document.createElement('p');
  error.className = 'error-text';
  error.dataset.field = 'error';
  const spark = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
  spark.classList.add('spark');
  spark.dataset.field = 'spark';
  spark.setAttribute('viewBox', '0 0 160 36');
  spark.setAttribute('aria-hidden', 'true');
  mid.append(titleRow, topic, meta, error, spark);

  const actions = document.createElement('div');
  actions.className = 'actions';
  const pause = document.createElement('button');
  pause.type = 'button';
  pause.className = 'btn btn-ghost';
  pause.dataset.action = 'pause';
  pause.addEventListener('click', () => toggleStream(stream.id));
  const link = document.createElement('a');
  link.className = 'btn btn-ghost';
  link.href = `/viewer.html?id=${encodeURIComponent(stream.id)}`;
  link.target = '_blank';
  link.rel = 'noopener';
  link.textContent = 'Открыть поток';
  link.addEventListener('click', (event) => {
    const win = window.open(link.href, `yav-stream-${stream.id}`, 'popup=yes,width=1080,height=800,resizable=yes,scrollbars=yes');
    if (win) {
      event.preventDefault();
      win.focus();
    }
  });
  const edit = document.createElement('button');
  edit.type = 'button';
  edit.className = 'btn btn-ghost';
  edit.textContent = 'Изменить';
  edit.addEventListener('click', () => openEdit(stream.id));
  const remove = document.createElement('button');
  remove.type = 'button';
  remove.className = 'btn btn-ghost';
  remove.textContent = 'Удалить';
  remove.addEventListener('click', () => askDelete(stream.id));
  actions.append(pause, link, edit, remove);

  article.append(valueBox, mid, actions);
  fillCard(article, stream);
  return article;
}

function renderStreams() {
  const streams = model ? model.streams : [];
  streamCount.textContent = String(streams.length);
  streamEmpty.hidden = streams.length > 0;
  streamList.replaceChildren(...streams.map(buildCard));
}

function patchSample(sample) {
  const stream = streamById(sample.id);
  if (!stream) return;
  stream.lastValue = sample.value;
  stream.lastTs = sample.ts;
  stream.published = sample.published;
  stream.dropped = sample.dropped;
  stream.errors = sample.errors;
  stream.skipped = sample.skipped;
  stream.coalesced = sample.coalesced;
  stream.lastError = sample.lastError || '';
  stream.spark = stream.spark || [];
  if (stream.sparkTs === sample.ts && stream.spark.length) {
    stream.spark[stream.spark.length - 1] = sample.value;
  } else {
    stream.spark.push(sample.value);
    if (stream.spark.length > 40) stream.spark.shift();
    stream.sparkTs = sample.ts;
  }
  const card = streamList.querySelector(`[data-id="${CSS.escape(sample.id)}"]`);
  if (card) fillCard(card, stream);
}

function adopt(data) {
  noteServerNow(data.now);
  noteUptime(data.uptimeSec);
  const signature = (data.streams || []).map((stream) => [
    stream.id, stream.enabled, stream.name, stream.topic, stream.generationType, stream.periodMs, stream.unit,
  ].join(':')).join('|');
  const previous = model && model._signature;
  model = data;
  model._signature = signature;
  if (!brokerDirty) fillBroker(data.broker);
  renderStatus();
  if (signature !== previous) renderStreams();
}

async function refresh() {
  const data = await api('/api/state');
  adopt(data);
  renderStreams();
}

function connectSse() {
  if (events) events.close();
  sseLive = true;
  events = new EventSource('/api/events');
  events.addEventListener('hello', (event) => {
    sseLive = true;
    adopt(JSON.parse(event.data));
    renderStreams();
  });
  events.addEventListener('sample', (event) => {
    sseLive = true;
    patchSample(JSON.parse(event.data));
  });
  events.addEventListener('status', (event) => {
    sseLive = true;
    if (!model) return;
    model.brokerStatus = JSON.parse(event.data);
    renderStatus();
    updateBadges();
  });
  events.addEventListener('heartbeat', (event) => {
    sseLive = true;
    const beat = JSON.parse(event.data);
    noteServerNow(beat.now);
    noteUptime(beat.uptimeSec);
    if (model) model.brokerStatus = beat.broker;
    renderStatus();
    if (model && beat.revision !== model.revision) refresh().catch(showLoadError);
  });
  events.onerror = () => {
    sseLive = events.readyState === EventSource.OPEN;
    renderStatus();
  };
}

function updateBadges() {
  if (!model) return;
  model.streams.forEach((stream) => {
    const card = streamList.querySelector(`[data-id="${CSS.escape(stream.id)}"]`);
    if (card) fillCard(card, stream);
  });
}

function showLoadError(error) {
  document.getElementById('uptime').textContent = 'нет связи со службой';
  document.getElementById('broker-error').textContent = error.message;
}

async function toggleStream(id) {
  const stream = streamById(id);
  if (!stream) return;
  const action = stream.enabled ? 'pause' : 'resume';
  try {
    await api(`/api/streams/${id}/${action}`, { method: 'POST' });
    await refresh();
  } catch (error) {
    const card = streamList.querySelector(`[data-id="${CSS.escape(id)}"]`);
    if (card) card.querySelector('[data-field="error"]').textContent = error.message;
  }
}

function openEdit(id) {
  const stream = streamById(id);
  if (!stream) return;
  const host = editDialog.querySelector('.dialog-body');
  const form = addForm.cloneNode(true);
  form.id = 'edit-form';
  form.querySelectorAll('[data-add-only]').forEach((el) => el.remove());
  form.querySelector('[data-submit]').textContent = 'Сохранить';
  host.replaceChildren(form);
  fillStreamForm(form, stream);
  bindStreamForm(form, { preset: false });
  const error = form.querySelector('[data-error]');
  form.addEventListener('submit', async (event) => {
    event.preventDefault();
    error.textContent = '';
    try {
      await api(`/api/streams/${id}`, { method: 'PUT', body: collectStream(form) });
      editDialog.close();
      await refresh();
    } catch (err) {
      error.textContent = err.message;
    }
  });
  editDialog.showModal();
  form.elements.name.focus();
}

function askDelete(id) {
  const stream = streamById(id);
  if (!stream) return;
  pendingDelete = id;
  document.getElementById('delete-text').textContent = `Поток «${stream.name}» остановится. Сообщения, которые уже ушли в брокер, останутся там.`;
  deleteDialog.showModal();
}

brokerForm.addEventListener('input', () => {
  if (fillingBroker) return;
  brokerDirty = true;
  renderStatus();
});
brokerForm.addEventListener('change', () => {
  if (fillingBroker) return;
  brokerDirty = true;
  renderStatus();
});
brokerForm.elements.protocol.addEventListener('change', () => {
  const next = PORT_BY_PROTOCOL[brokerForm.elements.protocol.value];
  const current = Number(brokerForm.elements.port.value);
  const known = new Set(Object.values(PORT_BY_PROTOCOL));
  if (!brokerForm.elements.port.value || known.has(current)) {
    brokerForm.elements.port.value = String(next);
  }
  syncBrokerFields();
});
document.getElementById('toggle-password').addEventListener('click', () => {
  const input = brokerForm.elements.password;
  const show = input.type === 'password';
  input.type = show ? 'text' : 'password';
  document.getElementById('toggle-password').textContent = show ? 'Скрыть' : 'Показать';
});
brokerForm.addEventListener('submit', async (event) => {
  event.preventDefault();
  const error = document.getElementById('broker-error');
  const submit = document.getElementById('broker-submit');
  error.textContent = '';
  submit.disabled = true;
  try {
    const connected = Boolean(model && model.brokerStatus && model.brokerStatus.connected);
    const data = !connected || brokerDirty
      ? await api('/api/broker/connect', { method: 'POST', body: collectBroker() })
      : await api('/api/broker/disconnect', { method: 'POST' });
    brokerDirty = false;
    adopt(data);
    renderStreams();
  } catch (err) {
    error.textContent = err.message;
  } finally {
    submit.disabled = false;
  }
});

bindStreamForm(addForm, { preset: true });
addForm.addEventListener('submit', async (event) => {
  event.preventDefault();
  const error = addForm.querySelector('[data-error]');
  const submit = addForm.querySelector('[data-submit]');
  error.textContent = '';
  submit.disabled = true;
  try {
    await api('/api/streams', { method: 'POST', body: collectStream(addForm) });
    addForm.elements.name.value = '';
    addForm.elements.topic.value = '';
    await refresh();
    addForm.elements.name.focus();
  } catch (err) {
    error.textContent = err.message;
  } finally {
    submit.disabled = false;
  }
});

document.getElementById('edit-cancel').addEventListener('click', () => editDialog.close());
document.getElementById('delete-cancel').addEventListener('click', () => {
  pendingDelete = null;
  deleteDialog.close();
});
document.getElementById('delete-ok').addEventListener('click', async () => {
  const id = pendingDelete;
  pendingDelete = null;
  deleteDialog.close();
  if (!id) return;
  try {
    await api(`/api/streams/${id}`, { method: 'DELETE' });
    await refresh();
  } catch (error) {
    document.getElementById('broker-error').textContent = error.message;
  }
});

setInterval(() => {
  renderStatus();
  document.querySelectorAll('[data-field="age"]').forEach((node) => {
    const card = node.closest('[data-id]');
    const stream = card && streamById(card.dataset.id);
    if (stream) node.textContent = ageText(stream.lastTs);
  });
}, 1000);

refresh()
  .then(() => {
    renderStreams();
    connectSse();
  })
  .catch((error) => {
    showLoadError(error);
    connectSse();
  });
