'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { nextValue, planTick } = require('../src/generator');

test('sine hits midpoint, max and min', () => {
  const stream = {
    generationType: 'sine',
    min: 15,
    max: 35,
    decimals: 1,
    wavePeriodSec: 60,
    startedAt: 0,
  };
  assert.equal(nextValue(stream, 0), 25);
  assert.equal(nextValue(stream, 15000), 35);
  assert.equal(nextValue(stream, 30000), 25);
  assert.equal(nextValue(stream, 45000), 15);
});

test('square follows duty cycle', () => {
  const stream = {
    generationType: 'square',
    min: 0,
    max: 1,
    decimals: 0,
    wavePeriodSec: 10,
    dutyPercent: 50,
    startedAt: 0,
  };
  assert.equal(nextValue(stream, 0), 1);
  assert.equal(nextValue(stream, 4999), 1);
  assert.equal(nextValue(stream, 5000), 0);
});

test('ramp holds at max and repeat wraps', () => {
  const hold = {
    generationType: 'ramp',
    min: 0,
    max: 10,
    decimals: 0,
    rampDurationSec: 10,
    rampMode: 'hold',
    startedAt: 0,
  };
  assert.equal(nextValue(hold, 0), 0);
  assert.equal(nextValue(hold, 5000), 5);
  assert.equal(nextValue(hold, 20000), 10);

  const repeat = {
    generationType: 'ramp',
    min: 0,
    max: 10,
    decimals: 0,
    rampDurationSec: 10,
    rampMode: 'repeat',
    startedAt: 0,
  };
  assert.equal(nextValue(repeat, 10000), 0);
});

test('counter starts at the given value and wraps', () => {
  const stream = {
    generationType: 'counter',
    min: 0,
    max: 2,
    decimals: 0,
    counterStep: 1,
    counterStart: 0,
    counterWrap: true,
    counter: null,
  };
  assert.deepEqual([0, 1, 2, 3, 4].map((step) => nextValue(stream, step * 1000)), [0, 1, 2, 0, 1]);
});

test('counter stays on max without wrap and keeps fractional steps', () => {
  const stuck = {
    generationType: 'counter',
    min: 0,
    max: 2,
    decimals: 0,
    counterStep: 1,
    counterStart: 0,
    counterWrap: false,
    counter: null,
  };
  assert.deepEqual([0, 1, 2, 3].map((step) => nextValue(stuck, step)), [0, 1, 2, 2]);

  const fine = {
    generationType: 'counter',
    min: 0,
    max: 10,
    decimals: 1,
    counterStep: 0.1,
    counterStart: 0,
    counterWrap: false,
    counter: null,
  };
  assert.equal(nextValue(fine, 0), 0);
  assert.equal(nextValue(fine, 1), 0.1);
  assert.equal(nextValue(fine, 2), 0.2);
});

test('constant, uniform, gaussian and walk stay inside the range', () => {
  const constant = { generationType: 'constant', min: 0, max: 10, decimals: 2, constantValue: 3.456 };
  assert.equal(nextValue(constant, 0), 3.46);

  for (let i = 0; i < 200; i += 1) {
    const uniform = { generationType: 'uniform', min: -5, max: 5, decimals: 3 };
    const value = nextValue(uniform, i);
    assert.ok(value >= -5 && value <= 5);
  }

  let sum = 0;
  for (let i = 0; i < 2000; i += 1) {
    const gaussian = { generationType: 'gaussian', min: 0, max: 100, decimals: 4 };
    const value = nextValue(gaussian, i);
    assert.ok(value >= 0 && value <= 100);
    sum += value;
  }
  assert.ok(Math.abs(sum / 2000 - 50) < 5);

  const walk = { generationType: 'random_walk', min: 0, max: 10, decimals: 3, stepPercent: 20, walk: null };
  for (let i = 0; i < 1000; i += 1) {
    const value = nextValue(walk, i);
    assert.ok(value >= 0 && value <= 10);
  }
});

test('planTick publishes once after a long stall', () => {
  assert.deepEqual(planTick(null, 1000, 1000), { fire: true, nextAt: 2000, skipped: 0 });
  assert.deepEqual(planTick(5000, 1000, 1000), { fire: true, nextAt: 2000, skipped: 0 });
  const stalled = planTick(0, 100000, 1000);
  assert.equal(stalled.fire, true);
  assert.equal(stalled.nextAt, 101000);
  assert.equal(stalled.skipped, 99);
  assert.deepEqual(planTick(2000, 1500, 1000), { fire: false, nextAt: 2000, skipped: 0 });
  assert.deepEqual(planTick(1000, 1000, 1000), { fire: true, nextAt: 2000, skipped: 0 });
});
