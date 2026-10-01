'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { Store } = require('../src/store');

test('state file is readable only by the owner', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'yav-'));
  const file = path.join(dir, 'state.json');
  const store = new Store(file);
  store.save({ version: 1, desiredConnected: false, broker: null, streams: [{ id: 'a' }] });
  assert.equal(fs.statSync(file).mode & 0o777, 0o600);
  assert.equal(store.load().streams.length, 1);
  fs.writeFileSync(file, '{');
  assert.deepEqual(store.load().streams, []);
  assert.equal(fs.existsSync(file), false);
  assert.ok(fs.readdirSync(dir).some((name) => name.startsWith('state.json.bak-')));
});
