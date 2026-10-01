'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const net = require('net');
const mqtt = require('mqtt');
const { MqttBroker } = require('../src/broker');

function once(emitter, event, timeoutMs = 5000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`timeout waiting for ${event}`)), timeoutMs);
    emitter.once(event, (...args) => {
      clearTimeout(timer);
      resolve(args);
    });
  });
}

test('mqtt client publishes a retained-free qos 0 payload', async (t) => {
  const aedes = require('aedes')();
  const server = net.createServer(aedes.handle);
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const port = server.address().port;
  const broker = new MqttBroker();
  const subscriber = mqtt.connect(`mqtt://127.0.0.1:${port}`, {
    clientId: 'yav-test-sub',
    protocolVersion: 4,
    reconnectPeriod: 0,
  });
  t.after(async () => {
    subscriber.end(true);
    broker.disconnect();
    await new Promise((resolve) => aedes.close(resolve));
    await new Promise((resolve) => server.close(resolve));
  });

  await once(subscriber, 'connect');
  await new Promise((resolve, reject) => {
    subscriber.subscribe('plant/line1/temperature', (err) => (err ? reject(err) : resolve()));
  });
  const incoming = once(subscriber, 'message');
  const connected = new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('mqtt connect timeout')), 5000);
    broker.on('status', (status) => {
      if (!status.connected) return;
      clearTimeout(timer);
      resolve();
    });
  });
  broker.connect({
    protocol: 'mqtt',
    host: '127.0.0.1',
    port,
    username: '',
    password: '',
    clientId: 'yav-test-pub',
    rejectUnauthorized: true,
  });
  await connected;
  const result = await broker.publish(
    'plant/line1/temperature',
    JSON.stringify({ value: 21.5 }),
    { qos: 0, retain: false },
  );
  assert.equal(result.ok, true);
  const [topic, payload] = await incoming;
  assert.equal(topic, 'plant/line1/temperature');
  assert.equal(JSON.parse(payload.toString()).value, 21.5);
});
