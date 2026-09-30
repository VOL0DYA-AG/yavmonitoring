'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { Engine } = require('../src/engine');
const { createServer } = require('../src/server');
const { MemoryStore, FakeBroker, temperature, flush } = require('./support');

async function listen(server) {
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  return `http://127.0.0.1:${server.address().port}`;
}

test('http api creates, pauses and deletes a stream', async (t) => {
  const broker = new FakeBroker();
  broker.connected = true;
  const engine = new Engine({ store: new MemoryStore(), broker, now: () => Date.now() });
  engine.load();
  const server = createServer(engine);
  const base = await listen(server);
  t.after(() => new Promise((resolve) => server.close(resolve)));

  const health = await fetch(`${base}/api/health`);
  assert.equal(health.status, 200);
  assert.equal((await health.json()).ok, true);

  const page = await fetch(`${base}/`);
  assert.equal(page.status, 200);
  assert.match(await page.text(), /Подключение/);

  const bad = await fetch(`${base}/api/streams`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(temperature({ topic: 'bad/#' })),
  });
  assert.equal(bad.status, 400);

  const created = await fetch(`${base}/api/streams`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(temperature()),
  });
  assert.equal(created.status, 201);
  const { stream } = await created.json();
  engine.tick(Date.now());
  await flush();

  const detail = await fetch(`${base}/api/streams/${stream.id}`);
  const body = await detail.json();
  assert.equal(body.stream.history.length, 1);
  assert.equal(body.stream.history[0].sent, true);
  assert.match(body.stream.history[0].payload, /"value":/);

  const paused = await fetch(`${base}/api/streams/${stream.id}/pause`, { method: 'POST' });
  assert.equal((await paused.json()).stream.enabled, false);

  const state = await fetch(`${base}/api/state`);
  const snapshot = await state.json();
  assert.equal(snapshot.streams.length, 1);
  assert.equal(snapshot.brokerStatus.connected, true);

  const removed = await fetch(`${base}/api/streams/${stream.id}`, { method: 'DELETE' });
  assert.equal(removed.status, 200);
  assert.equal((await fetch(`${base}/api/streams/${stream.id}`)).status, 404);

  const connected = await fetch(`${base}/api/broker/connect`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      protocol: 'mqtt',
      host: '127.0.0.1',
      port: 1883,
      password: '',
      clientId: 'api-test',
    }),
  });
  assert.equal(connected.status, 200);
  assert.equal(broker.connects, 1);
  const disconnected = await fetch(`${base}/api/broker/disconnect`, { method: 'POST' });
  assert.equal((await disconnected.json()).desiredConnected, false);
});
