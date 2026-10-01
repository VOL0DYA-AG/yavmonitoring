'use strict';

const crypto = require('crypto');

const SIGNAL_TYPES = [
  'temperature',
  'pressure',
  'level',
  'flow',
  'humidity',
  'voltage',
  'current',
  'discrete',
  'counter',
  'custom',
];

const GENERATION_TYPES = [
  'constant',
  'uniform',
  'gaussian',
  'random_walk',
  'sine',
  'square',
  'sawtooth',
  'ramp',
  'counter',
];

const PROTOCOLS = ['mqtt', 'mqtts', 'ws', 'wss'];
const PAYLOADS = ['json', 'value'];

class ValidationError extends Error {
  constructor(message) {
    super(message);
    this.status = 400;
    this.name = 'ValidationError';
  }
}

function fail(message) {
  throw new ValidationError(message);
}

function asString(value, label, { max = 200, allowEmpty = false, trim = true } = {}) {
  if (typeof value !== 'string') fail(`${label}: ожидается строка`);
  const out = trim ? value.trim() : value;
  if (!allowEmpty && out.length === 0) fail(`${label}: пустое значение`);
  if (out.length > max) fail(`${label}: длиннее ${max} символов`);
  return out;
}

function asNumber(value, label, { min, max, integer = false } = {}) {
  const n = typeof value === 'number' ? value : Number(value);
  if (!Number.isFinite(n)) fail(`${label}: ожидается число`);
  if (integer && !Number.isInteger(n)) fail(`${label}: ожидается целое`);
  if (min != null && n < min) fail(`${label}: меньше ${min}`);
  if (max != null && n > max) fail(`${label}: больше ${max}`);
  return n;
}

function asBool(value, fallback = false) {
  if (value == null) return fallback;
  return value === true || value === 'true' || value === 1 || value === '1';
}

function validateTopic(topic) {
  const value = asString(topic, 'MQTT-топик', { max: 512 });
  if (value.includes('#') || value.includes('+')) {
    fail('MQTT-топик не может содержать + или #: публиковать в шаблон нельзя');
  }
  if (value.includes('\0')) fail('MQTT-топик содержит недопустимый символ');
  return value;
}

function validateBroker(input = {}, previous = {}) {
  const protocol = asString(input.protocol || previous.protocol || 'mqtt', 'Протокол');
  if (!PROTOCOLS.includes(protocol)) fail('Неизвестный протокол');
  const host = asString(input.host ?? previous.host ?? '', 'Хост', { max: 255 });
  if (host.includes('://') || /[\s/@]/.test(host)) fail('Хост указан неверно');
  const port = asNumber(input.port ?? previous.port, 'Порт', { min: 1, max: 65535, integer: true });
  let path = '';
  if (protocol === 'ws' || protocol === 'wss') {
    path = asString(input.path || previous.path || '/mqtt', 'Путь', { max: 200 });
    if (!path.startsWith('/')) fail('Путь WebSocket должен начинаться с /');
  }
  const username = input.username == null || input.username === ''
    ? ''
    : asString(String(input.username), 'Пользователь', { max: 200 });
  if (input.password != null && typeof input.password !== 'string') fail('Пароль: ожидается строка');
  const password = input.password == null ? '' : input.password;
  if (password.length > 500) fail('Пароль: длиннее 500 символов');
  let clientId = input.clientId == null ? '' : String(input.clientId).trim();
  if (!clientId) clientId = previous.clientId || `yavmon-${crypto.randomBytes(4).toString('hex')}`;
  if (!/^[\x21-\x7E]+$/.test(clientId) || clientId.length > 128) {
    fail('Client ID: до 128 печатных латинских символов без пробелов');
  }
  const rejectUnauthorized = input.rejectUnauthorized == null
    ? previous.rejectUnauthorized !== false
    : asBool(input.rejectUnauthorized, true);
  return {
    protocol,
    host,
    port,
    path,
    username,
    password,
    clientId,
    rejectUnauthorized: Boolean(rejectUnauthorized),
  };
}

function validateStream(input) {
  const name = asString(input.name, 'Имя', { max: 80 });
  const signalType = asString(input.signalType, 'Тип сигнала');
  if (!SIGNAL_TYPES.includes(signalType)) fail('Неизвестный тип сигнала');
  const generationType = asString(input.generationType, 'Тип генерации');
  if (!GENERATION_TYPES.includes(generationType)) fail('Неизвестный тип генерации');
  const min = asNumber(input.min, 'Минимум');
  const max = asNumber(input.max, 'Максимум');
  if (min > max) fail('Минимум больше максимума');
  if (min === max && generationType !== 'constant') {
    fail('Для этого типа генерации минимум и максимум должны различаться');
  }
  const decimals = asNumber(input.decimals, 'Знаки после запятой', { min: 0, max: 6, integer: true });
  const periodSec = asNumber(
    input.periodSec ?? (input.periodMs != null ? input.periodMs / 1000 : undefined),
    'Период',
    { min: 0.1, max: 86400 },
  );
  const periodMs = Math.round(periodSec * 1000);
  const topic = validateTopic(input.topic);
  const qos = asNumber(input.qos ?? 0, 'QoS', { min: 0, max: 2, integer: true });
  const retain = asBool(input.retain, false);
  const payloadFormat = input.payloadFormat || 'json';
  if (!PAYLOADS.includes(payloadFormat)) fail('Неизвестный формат сообщения');
  const unit = input.unit == null || input.unit === ''
    ? ''
    : asString(String(input.unit), 'Единица', { max: 16 });

  const stream = {
    name,
    signalType,
    generationType,
    min,
    max,
    decimals,
    periodMs,
    topic,
    qos,
    retain,
    payloadFormat,
    unit,
    constantValue: null,
    wavePeriodSec: 60,
    dutyPercent: 50,
    stepPercent: 2,
    rampDurationSec: 300,
    rampMode: 'repeat',
    counterStep: 1,
    counterStart: min,
    counterWrap: false,
  };

  if (generationType === 'constant') {
    const fallback = min + (max - min) / 2;
    const value = input.constantValue == null || input.constantValue === ''
      ? fallback
      : asNumber(input.constantValue, 'Значение');
    if (value < min || value > max) fail('Постоянное значение вне диапазона');
    stream.constantValue = value;
  }
  if (generationType === 'sine' || generationType === 'square' || generationType === 'sawtooth') {
    stream.wavePeriodSec = asNumber(input.wavePeriodSec ?? 60, 'Период волны', { min: 0.2, max: 86400 });
  }
  if (generationType === 'square') {
    stream.dutyPercent = asNumber(input.dutyPercent ?? 50, 'Скважность', { min: 1, max: 99 });
  }
  if (generationType === 'random_walk') {
    stream.stepPercent = asNumber(input.stepPercent ?? 2, 'Шаг блуждания', { min: 0.1, max: 100 });
  }
  if (generationType === 'ramp') {
    stream.rampDurationSec = asNumber(input.rampDurationSec ?? 300, 'Длительность роста', {
      min: 0.2,
      max: 864000,
    });
    const mode = input.rampMode || 'repeat';
    if (mode !== 'repeat' && mode !== 'hold') fail('Неизвестный режим роста');
    stream.rampMode = mode;
  }
  if (generationType === 'counter') {
    stream.counterStep = asNumber(input.counterStep ?? 1, 'Шаг счётчика', {
      min: 1e-9,
      max: Number.MAX_SAFE_INTEGER,
    });
    const start = input.counterStart == null || input.counterStart === ''
      ? min
      : asNumber(input.counterStart, 'Старт счётчика');
    if (start < min || start > max) fail('Старт счётчика вне диапазона');
    if (max > Number.MAX_SAFE_INTEGER || start > Number.MAX_SAFE_INTEGER) {
      fail('Счётчик не может превышать 9007199254740991');
    }
    stream.counterStart = start;
    stream.counterWrap = asBool(input.counterWrap, false);
  }
  return stream;
}

module.exports = {
  ValidationError,
  validateBroker,
  validateStream,
  validateTopic,
  SIGNAL_TYPES,
  GENERATION_TYPES,
};
