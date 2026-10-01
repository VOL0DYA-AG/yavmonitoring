'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { Engine, HISTORY_LIMIT } = require('../src/engine');
const { MemoryStore, FakeBroker, temperature, flush } = require('./support');

function setup(now = () => Date.now()) {
  const store = new MemoryStore();
  const broker = new FakeBroker();
  const engine = new Engine({ store, broker, now });
  engine.load();
  return { store, broker, engine };
}

test('publishes on schedule and does not burst after a stall', async () => {
  let clock = 0;
  const { broker, engine } = setup(() => clock);
  broker.connected = true;
  const created = engine.addStream(temperature({ generationType: 'constant', constantValue: 21, min: 0, max: 40 }));
  const stream = engine.streams.get(created.id);
  stream.nextAt = 0;
  engine.tick(0);
  await flush();
  assert.equal(broker.published.length, 1);
  assert.equal(JSON.parse(broker.published[0].payload).value, 21);
  assert.equal(broker.published[0].topic, 'plant/line1/temperature');

  stream.nextAt = 0;
  engine.tick(100000);
  await flush();
  assert.equal(broker.published.length, 2);
  assert.equal(stream.skipped, 99);
  engine.tick(100010);
  await flush();
  assert.equal(broker.published.length, 2);
});

test('offline samples are counted and not queued', async () => {
  const { broker, engine } = setup(() => 0);
  broker.connected = false;
  const created = engine.addStream(temperature());
  const stream = engine.streams.get(created.id);
  for (let i = 0; i < HISTORY_LIMIT + 50; i += 1) engine.tick(i * 1000);
  assert.equal(broker.published.length, 0);
  assert.equal(stream.dropped, HISTORY_LIMIT + 50);
  assert.equal(stream.history.length, HISTORY_LIMIT);
  assert.equal(stream.history.at(-1).sent, false);
});

test('slow broker keeps only the latest unpublished sample', async () => {
  const { broker, engine } = setup(() => 0);
  broker.connected = true;
  let pending = null;
  let calls = 0;
  broker.publish = (topic, payload, opts) => {
    calls += 1;
    return new Promise((resolve) => {
      pending = { topic, payload, opts, resolve };
    });
  };
  const created = engine.addStream(temperature({ generationType: 'constant', constantValue: 1, min: 0, max: 10, decimals: 0 }));
  const stream = engine.streams.get(created.id);
  stream.nextAt = 0;
  engine.tick(0);
  engine.tick(1000);
  engine.tick(2000);
  assert.equal(calls, 1);
  assert.equal(stream.coalesced, 1);
  pending.resolve({ ok: true });
  await flush();
  assert.equal(calls, 2);
  pending.resolve({ ok: true });
  await flush();
  assert.equal(stream.published, 2);
});

test('counter survives a restart', () => {
  const store = new MemoryStore();
  const first = new Engine({ store, broker: new FakeBroker(), now: () => 0 });
  first.load();
  const created = first.addStream(temperature({
    generationType: 'counter',
    signalType: 'counter',
    min: 0,
    max: 1000,
    decimals: 0,
    counterStart: 0,
    counterStep: 1,
    unit: 'имп',
    topic: 'plant/counter',
  }));
  const stream = first.streams.get(created.id);
  stream.nextAt = 0;
  first.tick(0);
  first.tick(1000);
  first.tick(2000);
  assert.equal(stream.counter, 2);
  first.flush();

  const second = new Engine({ store, broker: new FakeBroker(), now: () => 10000 });
  second.load();
  const restored = [...second.streams.values()][0];
  assert.equal(restored.counter, 2);
  restored.nextAt = 10000;
  second.tick(10000);
  assert.equal(restored.counter, 3);
  assert.equal(restored.enabled, true);
});

test('paused stream does not publish and desired connection is restored', () => {
  const store = new MemoryStore();
  const broker = new FakeBroker();
  const engine = new Engine({ store, broker, now: () => 0 });
  engine.load();
  engine.connect({
    protocol: 'mqtt',
    host: '127.0.0.1',
    port: 1883,
    password: 'secret',
    clientId: 'bench-1',
  });
  const created = engine.addStream(temperature());
  engine.setEnabled(created.id, false);
  broker.published.length = 0;
  engine.tick(0);
  engine.tick(5000);
  assert.equal(broker.published.length, 0);

  const restarted = new Engine({ store, broker: new FakeBroker(), now: () => 0 });
  restarted.load();
  assert.equal(restarted.broker.connects, 1);
  assert.equal(restarted.desiredConnected, true);
  assert.equal([...restarted.streams.values()][0].enabled, false);
  assert.equal(restarted.brokerConfig.password, 'secret');
});
