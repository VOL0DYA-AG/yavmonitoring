'use strict';

const crypto = require('crypto');
const { EventEmitter } = require('events');
const { nextValue, planTick } = require('./generator');
const { buildPayload } = require('./payload');
const { validateBroker, validateStream } = require('./validate');
const { log } = require('./log');

const HISTORY_LIMIT = 500;
const PUBLISH_TIMEOUT_MS = 20000;
const MAX_STREAMS = 500;

class Engine extends EventEmitter {
  constructor({ store, broker, now = () => Date.now() } = {}) {
    super();
    this.store = store;
    this.broker = broker;
    this.now = now;
    this.streams = new Map();
    this.brokerConfig = null;
    this.desiredConnected = false;
    this.revision = 1;
    this.dirty = false;
    this.timer = null;
    this.persistTimer = null;
    this.heartTimer = null;
    this.statsTimer = null;
    this.setMaxListeners(200);
    this.broker.on('status', (status) => this.emit('status', status));
  }

  load() {
    const data = this.store.load();
    this.desiredConnected = Boolean(data.desiredConnected);
    this.brokerConfig = data.broker || null;
    this.streams.clear();
    for (const saved of data.streams || []) {
      const stream = hydrate(saved);
      if (!stream) continue;
      this.streams.set(stream.id, stream);
    }
    if (this.desiredConnected && this.brokerConfig) {
      this.broker.connect(this.brokerConfig);
    }
  }

  start() {
    if (this.timer) return;
    this.timer = setInterval(() => {
      try {
        this.tick(this.now());
      } catch (err) {
        log('error', 'tick failed', { error: err.stack || err.message });
      }
    }, 50);
    this.persistTimer = setInterval(() => {
      if (!this.dirty) return;
      try {
        this.flush();
      } catch (err) {
        log('error', 'persist failed', { error: err.message });
      }
    }, 30000);
    this.heartTimer = setInterval(() => {
      this.emit('heartbeat', this.heartbeat());
    }, 15000);
    this.statsTimer = setInterval(() => this.logStats(), 10 * 60 * 1000);
  }

  stop() {
    clearInterval(this.timer);
    clearInterval(this.persistTimer);
    clearInterval(this.heartTimer);
    clearInterval(this.statsTimer);
    this.timer = null;
    this.persistTimer = null;
    this.heartTimer = null;
    this.statsTimer = null;
  }

  heartbeat() {
    return {
      revision: this.revision,
      uptimeSec: Math.round(process.uptime()),
      now: this.now(),
      broker: this.broker.status(),
    };
  }

  flush() {
    this.store.save({
      version: 1,
      desiredConnected: this.desiredConnected,
      broker: this.brokerConfig,
      streams: [...this.streams.values()].map(serialize),
    });
    this.dirty = false;
  }

  connect(input) {
    const config = validateBroker(input || {}, this.brokerConfig || {});
    this.brokerConfig = config;
    this.desiredConnected = true;
    this.revision += 1;
    this.flush();
    this.broker.connect(config);
    return this.publicState();
  }

  disconnect() {
    this.desiredConnected = false;
    this.revision += 1;
    this.flush();
    this.broker.disconnect();
    return this.publicState();
  }

  addStream(input) {
    if (this.streams.size >= MAX_STREAMS) {
      const err = new Error(`Достигнут предел: ${MAX_STREAMS} потоков`);
      err.status = 400;
      throw err;
    }
    const config = validateStream(input);
    const stream = {
      id: crypto.randomUUID(),
      createdAt: this.now(),
      enabled: true,
      ...config,
      startedAt: null,
      walk: null,
      counter: null,
      nextAt: this.now(),
      published: 0,
      dropped: 0,
      errors: 0,
      skipped: 0,
      coalesced: 0,
      inflight: false,
      pending: null,
      lastValue: null,
      lastTs: null,
      lastError: '',
      history: [],
    };
    this.streams.set(stream.id, stream);
    this.revision += 1;
    this.flush();
    log('info', 'stream added', { id: stream.id, topic: stream.topic });
    return this.summary(stream);
  }

  updateStream(id, input) {
    const stream = this.must(id);
    const config = validateStream(input);
    const generationChanged = config.generationType !== stream.generationType;
    Object.assign(stream, config);
    if (generationChanged) {
      stream.startedAt = null;
      stream.walk = null;
      stream.counter = null;
      stream.pending = null;
    }
    if (stream.nextAt != null && stream.nextAt - this.now() > stream.periodMs) {
      stream.nextAt = this.now() + stream.periodMs;
    }
    this.revision += 1;
    this.flush();
    return this.summary(stream);
  }

  setEnabled(id, enabled) {
    const stream = this.must(id);
    stream.enabled = Boolean(enabled);
    if (stream.enabled) stream.nextAt = this.now();
    this.revision += 1;
    this.flush();
    return this.summary(stream);
  }

  removeStream(id) {
    if (!this.streams.has(id)) notFound();
    this.streams.delete(id);
    this.revision += 1;
    this.flush();
    log('info', 'stream removed', { id });
  }

  tick(now = this.now()) {
    for (const stream of this.streams.values()) {
      if (!stream.enabled) continue;
      const plan = planTick(stream.nextAt, now, stream.periodMs);
      if (plan.skipped) stream.skipped += plan.skipped;
      stream.nextAt = plan.nextAt;
      if (!plan.fire) continue;
      this.generate(stream, now);
    }
  }

  generate(stream, now) {
    const value = nextValue(stream, now);
    const payload = buildPayload(stream, value, now);
    stream.lastValue = value;
    stream.lastTs = now;
    const entry = { ts: now, value, payload, sent: false };
    pushHistory(stream, entry);
    this.dirty = true;

    if (!this.broker.connected) {
      stream.dropped += 1;
      this.emit('sample', sampleEvent(stream, entry));
      return;
    }
    if (stream.inflight) {
      if (stream.pending) stream.coalesced += 1;
      stream.pending = entry;
      this.emit('sample', sampleEvent(stream, entry));
      return;
    }
    this.send(stream, entry);
  }

  send(stream, entry) {
    stream.inflight = true;
    let settled = false;
    let timer = null;
    const finish = (result) => {
      if (settled) return;
      settled = true;
      if (timer) clearTimeout(timer);
      stream.inflight = false;
      if (!result || result.ok !== true) {
        const reason = result && result.reason ? result.reason : 'ошибка публикации';
        if (reason === 'offline') stream.dropped += 1;
        else {
          stream.errors += 1;
          stream.lastError = reason;
        }
        entry.sent = false;
      } else {
        stream.published += 1;
        stream.lastError = '';
        entry.sent = true;
      }
      this.dirty = true;
      this.emit('sample', sampleEvent(stream, entry));
      if (!stream.pending) return;
      const next = stream.pending;
      stream.pending = null;
      if (stream.enabled && this.broker.connected) this.send(stream, next);
      else {
        stream.dropped += 1;
        next.sent = false;
        this.emit('sample', sampleEvent(stream, next));
      }
    };
    timer = setTimeout(() => finish({ ok: false, reason: 'таймаут публикации' }), PUBLISH_TIMEOUT_MS);
    if (typeof timer.unref === 'function') timer.unref();
    try {
      Promise.resolve(this.broker.publish(stream.topic, entry.payload, {
        qos: stream.qos,
        retain: stream.retain,
      })).then(
        (result) => finish(result),
        (err) => finish({ ok: false, reason: err.message || 'ошибка публикации' }),
      );
    } catch (err) {
      finish({ ok: false, reason: err.message || 'ошибка публикации' });
    }
  }

  getStream(id) {
    const stream = this.streams.get(id);
    if (!stream) return null;
    return {
      ...this.summary(stream),
      history: stream.history.slice(-HISTORY_LIMIT),
    };
  }

  summary(stream) {
    return {
      id: stream.id,
      createdAt: stream.createdAt,
      enabled: stream.enabled,
      name: stream.name,
      signalType: stream.signalType,
      generationType: stream.generationType,
      min: stream.min,
      max: stream.max,
      decimals: stream.decimals,
      periodMs: stream.periodMs,
      topic: stream.topic,
      qos: stream.qos,
      retain: stream.retain,
      payloadFormat: stream.payloadFormat,
      unit: stream.unit,
      constantValue: stream.constantValue,
      wavePeriodSec: stream.wavePeriodSec,
      dutyPercent: stream.dutyPercent,
      stepPercent: stream.stepPercent,
      rampDurationSec: stream.rampDurationSec,
      rampMode: stream.rampMode,
      counterStep: stream.counterStep,
      counterStart: stream.counterStart,
      counterWrap: stream.counterWrap,
      lastValue: stream.lastValue,
      lastTs: stream.lastTs,
      published: stream.published,
      dropped: stream.dropped,
      errors: stream.errors,
      skipped: stream.skipped,
      coalesced: stream.coalesced,
      lastError: stream.lastError,
      spark: stream.history.slice(-40).map((point) => point.value),
    };
  }

  publicState() {
    const streams = [...this.streams.values()]
      .sort((a, b) => a.createdAt - b.createdAt)
      .map((stream) => this.summary(stream));
    return {
      revision: this.revision,
      uptimeSec: Math.round(process.uptime()),
      now: this.now(),
      desiredConnected: this.desiredConnected,
      broker: this.brokerConfig,
      brokerStatus: this.broker.status(),
      streams,
    };
  }

  must(id) {
    const stream = this.streams.get(id);
    if (!stream) notFound();
    return stream;
  }

  logStats() {
    let published = 0;
    let dropped = 0;
    let errors = 0;
    let running = 0;
    for (const stream of this.streams.values()) {
      published += stream.published;
      dropped += stream.dropped;
      errors += stream.errors;
      if (stream.enabled) running += 1;
    }
    log('info', 'stats', {
      streams: this.streams.size,
      running,
      published,
      dropped,
      errors,
      broker: this.broker.state,
    });
  }
}

function notFound() {
  const err = new Error('Поток не найден');
  err.status = 404;
  throw err;
}

function pushHistory(stream, entry) {
  stream.history.push(entry);
  if (stream.history.length > HISTORY_LIMIT) {
    stream.history.splice(0, stream.history.length - HISTORY_LIMIT);
  }
}

function sampleEvent(stream, entry) {
  return {
    id: stream.id,
    ts: entry.ts,
    value: entry.value,
    payload: entry.payload,
    sent: entry.sent,
    published: stream.published,
    dropped: stream.dropped,
    errors: stream.errors,
    skipped: stream.skipped,
    coalesced: stream.coalesced,
    lastError: stream.lastError,
  };
}

function serialize(stream) {
  return {
    id: stream.id,
    createdAt: stream.createdAt,
    enabled: stream.enabled,
    name: stream.name,
    signalType: stream.signalType,
    generationType: stream.generationType,
    min: stream.min,
    max: stream.max,
    decimals: stream.decimals,
    periodMs: stream.periodMs,
    topic: stream.topic,
    qos: stream.qos,
    retain: stream.retain,
    payloadFormat: stream.payloadFormat,
    unit: stream.unit,
    constantValue: stream.constantValue,
    wavePeriodSec: stream.wavePeriodSec,
    dutyPercent: stream.dutyPercent,
    stepPercent: stream.stepPercent,
    rampDurationSec: stream.rampDurationSec,
    rampMode: stream.rampMode,
    counterStep: stream.counterStep,
    counterStart: stream.counterStart,
    counterWrap: stream.counterWrap,
    startedAt: stream.startedAt,
    walk: stream.walk,
    counter: stream.counter,
    published: stream.published,
    dropped: stream.dropped,
    errors: stream.errors,
    skipped: stream.skipped,
    coalesced: stream.coalesced,
    lastValue: stream.lastValue,
    lastTs: stream.lastTs,
    lastError: stream.lastError,
  };
}

function hydrate(saved) {
  if (!saved || typeof saved.id !== 'string' || typeof saved.topic !== 'string') {
    log('error', 'skip broken stream');
    return null;
  }
  return {
    id: saved.id,
    createdAt: saved.createdAt || Date.now(),
    enabled: saved.enabled !== false,
    name: saved.name || 'поток',
    signalType: saved.signalType || 'custom',
    generationType: saved.generationType || 'constant',
    min: numberOr(saved.min, 0),
    max: numberOr(saved.max, 1),
    decimals: numberOr(saved.decimals, 0),
    periodMs: Math.max(100, numberOr(saved.periodMs, 1000)),
    topic: saved.topic,
    qos: saved.qos === 1 || saved.qos === 2 ? saved.qos : 0,
    retain: Boolean(saved.retain),
    payloadFormat: saved.payloadFormat === 'value' ? 'value' : 'json',
    unit: saved.unit || '',
    constantValue: saved.constantValue == null ? null : saved.constantValue,
    wavePeriodSec: numberOr(saved.wavePeriodSec, 60),
    dutyPercent: numberOr(saved.dutyPercent, 50),
    stepPercent: numberOr(saved.stepPercent, 2),
    rampDurationSec: numberOr(saved.rampDurationSec, 300),
    rampMode: saved.rampMode === 'hold' ? 'hold' : 'repeat',
    counterStep: numberOr(saved.counterStep, 1),
    counterStart: numberOr(saved.counterStart, numberOr(saved.min, 0)),
    counterWrap: Boolean(saved.counterWrap),
    startedAt: saved.startedAt ?? null,
    walk: saved.walk ?? null,
    counter: saved.counter ?? null,
    nextAt: null,
    published: numberOr(saved.published, 0),
    dropped: numberOr(saved.dropped, 0),
    errors: numberOr(saved.errors, 0),
    skipped: numberOr(saved.skipped, 0),
    coalesced: numberOr(saved.coalesced, 0),
    inflight: false,
    pending: null,
    lastValue: saved.lastValue ?? null,
    lastTs: saved.lastTs ?? null,
    lastError: saved.lastError || '',
    history: [],
  };
}

function numberOr(value, fallback) {
  return typeof value === 'number' && Number.isFinite(value) ? value : fallback;
}

module.exports = { Engine, HISTORY_LIMIT, serialize, hydrate };
