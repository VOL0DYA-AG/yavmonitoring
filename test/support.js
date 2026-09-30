'use strict';

const { EventEmitter } = require('events');

class MemoryStore {
  constructor(initial) {
    this.data = initial || null;
  }

  load() {
    if (!this.data) return { version: 1, desiredConnected: false, broker: null, streams: [] };
    return JSON.parse(JSON.stringify(this.data));
  }

  save(state) {
    this.data = JSON.parse(JSON.stringify(state));
  }
}

class FakeBroker extends EventEmitter {
  constructor() {
    super();
    this.connected = false;
    this.published = [];
    this.connects = 0;
    this.state = 'disconnected';
    this.message = '';
    this.connectedAt = null;
  }

  status() {
    return {
      state: this.connected ? 'connected' : this.state,
      message: this.message,
      connected: this.connected,
      connectedAt: this.connectedAt,
    };
  }

  connect() {
    this.connects += 1;
    this.connected = true;
    this.state = 'connected';
    this.connectedAt = Date.now();
    this.emit('status', this.status());
  }

  disconnect() {
    this.connected = false;
    this.state = 'disconnected';
    this.connectedAt = null;
    this.emit('status', this.status());
  }

  publish(topic, payload, opts) {
    if (!this.connected) return Promise.resolve({ ok: false, reason: 'offline' });
    this.published.push({ topic, payload, opts });
    return Promise.resolve({ ok: true });
  }
}

function temperature(overrides = {}) {
  return {
    name: 'Температура',
    signalType: 'temperature',
    generationType: 'sine',
    min: 15,
    max: 35,
    decimals: 1,
    periodSec: 1,
    topic: 'plant/line1/temperature',
    qos: 0,
    retain: false,
    payloadFormat: 'json',
    unit: '°C',
    wavePeriodSec: 60,
    ...overrides,
  };
}

function flush() {
  return new Promise((resolve) => setImmediate(resolve));
}

module.exports = { MemoryStore, FakeBroker, temperature, flush };
