'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { buildPayload } = require('../src/payload');
const { validateStream, validateBroker, ValidationError } = require('../src/validate');
const { buildUrl } = require('../src/broker');

test('payload keeps the requested decimals', () => {
  const json = buildPayload({
    payloadFormat: 'json',
    decimals: 1,
    name: 'Температура',
    unit: '°C',
  }, 1.24, Date.parse('2026-09-30T00:00:00.000Z'));
  assert.deepEqual(JSON.parse(json), {
    ts: '2026-09-30T00:00:00.000Z',
    value: 1.2,
    name: 'Температура',
    unit: '°C',
    quality: 'good',
  });
  assert.equal(buildPayload({ payloadFormat: 'value', decimals: 2, name: 'x' }, 1.2, 0), '1.20');
});

test('stream validation rejects wildcards and inverted range', () => {
  assert.throws(() => validateStream({
    name: 'x',
    signalType: 'custom',
    generationType: 'uniform',
    min: 0,
    max: 1,
    decimals: 0,
    periodSec: 1,
    topic: 'plant/#',
  }), ValidationError);
  assert.throws(() => validateStream({
    name: 'x',
    signalType: 'custom',
    generationType: 'uniform',
    min: 5,
    max: 1,
    decimals: 0,
    periodSec: 1,
    topic: 'plant/x',
  }), /Минимум больше максимума/);
  assert.throws(() => validateStream({
    name: 'x',
    signalType: 'custom',
    generationType: 'uniform',
    min: 0,
    max: 1,
    decimals: 0,
    periodSec: 0.01,
    topic: 'plant/x',
  }), /Период/);
});

test('broker url and client id', () => {
  const broker = validateBroker({
    protocol: 'ws',
    host: '::1',
    port: 9001,
    path: '/mqtt',
    username: '',
    password: 'secret',
    clientId: '',
    rejectUnauthorized: true,
  });
  assert.match(broker.clientId, /^yavmon-[0-9a-f]{8}$/);
  assert.equal(buildUrl({ ...broker, host: '::1' }), 'ws://[::1]:9001/mqtt');
  assert.equal(buildUrl({ protocol: 'mqtt', host: '10.0.0.8', port: 1883 }), 'mqtt://10.0.0.8:1883');
  assert.equal(broker.password, 'secret');
});
