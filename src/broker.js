'use strict';

const { EventEmitter } = require('events');
const mqtt = require('mqtt');
const { log } = require('./log');

function formatHost(host) {
  if (host.includes(':') && !host.startsWith('[')) return `[${host}]`;
  return host;
}

function buildUrl(config) {
  const host = formatHost(config.host);
  if (config.protocol === 'ws' || config.protocol === 'wss') {
    const wsPath = config.path || '/mqtt';
    return `${config.protocol}://${host}:${config.port}${wsPath}`;
  }
  return `${config.protocol}://${host}:${config.port}`;
}

class MqttBroker extends EventEmitter {
  constructor() {
    super();
    this.client = null;
    this.config = null;
    this.connected = false;
    this.state = 'disconnected';
    this.message = '';
    this.connectedAt = null;
    this._generation = 0;
    this._lastWarn = 0;
  }

  status() {
    return {
      state: this.state,
      message: this.message,
      connected: this.connected,
      connectedAt: this.connectedAt,
    };
  }

  connect(config) {
    this._generation += 1;
    const generation = this._generation;
    this._closeClient();
    this.config = config;
    this.connected = false;
    this.connectedAt = null;
    this._set('connecting', '');
    const url = buildUrl(config);
    log('info', 'mqtt connecting', {
      url,
      clientId: config.clientId,
    });
    const client = mqtt.connect(url, {
      clientId: config.clientId,
      username: config.username || undefined,
      password: config.password || undefined,
      keepalive: 30,
      reconnectPeriod: 5000,
      connectTimeout: 10000,
      protocolVersion: 4,
      clean: true,
      resubscribe: false,
      queueQoSZero: false,
      rejectUnauthorized: config.rejectUnauthorized !== false,
    });
    this.client = client;
    const alive = () => generation === this._generation && this.client === client;

    client.on('connect', () => {
      if (!alive()) return;
      this.connected = true;
      this.connectedAt = Date.now();
      this._lastWarn = 0;
      this._set('connected', '');
      log('info', 'mqtt connected', { url });
    });
    client.on('reconnect', () => {
      if (!alive()) return;
      this.connected = false;
      this._set('reconnecting', this.message || 'повторное подключение');
      this._warn('mqtt reconnecting', { url });
    });
    client.on('close', () => {
      if (!alive()) return;
      this.connected = false;
      if (this.state !== 'disconnected') this._set('reconnecting', 'соединение закрыто');
    });
    client.on('offline', () => {
      if (!alive()) return;
      this.connected = false;
      if (this.state !== 'disconnected') this._set('reconnecting', 'брокер недоступен');
    });
    client.on('error', (err) => {
      if (!alive()) return;
      const message = err && err.message ? err.message : 'ошибка MQTT';
      if (client.connected) {
        this.connected = true;
        this._set('connected', message);
      } else {
        this.connected = false;
        this._set('error', message);
      }
      this._warn('mqtt error', { url, error: message });
    });
  }

  disconnect() {
    this._generation += 1;
    this.connected = false;
    this.connectedAt = null;
    this._set('disconnected', '');
    this._closeClient();
    log('info', 'mqtt disconnected');
  }

  publish(topic, payload, { qos = 0, retain = false } = {}) {
    if (!this.client || !this.client.connected) {
      this.connected = false;
      return Promise.resolve({ ok: false, reason: 'offline' });
    }
    return new Promise((resolve) => {
      this.client.publish(topic, payload, { qos, retain }, (err) => {
        if (err) resolve({ ok: false, reason: err.message || 'ошибка публикации' });
        else resolve({ ok: true });
      });
    });
  }

  _closeClient() {
    const client = this.client;
    this.client = null;
    if (!client) return;
    client.removeAllListeners();
    client.end(true);
  }

  _set(state, message) {
    const text = message || '';
    if (this.state === state && this.message === text) return;
    this.state = state;
    this.message = text;
    this.emit('status', this.status());
  }

  _warn(msg, extra) {
    const now = Date.now();
    if (now - this._lastWarn < 60000) return;
    this._lastWarn = now;
    log('warn', msg, extra);
  }
}

module.exports = { MqttBroker, buildUrl, formatHost };
