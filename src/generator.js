'use strict';

function roundTo(value, decimals) {
  if (!Number.isFinite(value)) return 0;
  const digits = Math.max(0, Math.min(6, decimals | 0));
  return Number(value.toFixed(digits));
}

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function gaussian() {
  let u = 0;
  let v = 0;
  while (u === 0) u = Math.random();
  while (v === 0) v = Math.random();
  return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v);
}

/**
 * Next simulated sample. Mutates stream.startedAt / walk / counter.
 * The stored counter and walk stay unrounded so small steps are not lost.
 */
function nextValue(stream, nowMs) {
  const min = stream.min;
  const max = stream.max;
  const span = max - min;
  if (stream.startedAt == null) stream.startedAt = nowMs;

  if (stream.generationType === 'counter') {
    const step = stream.counterStep ?? 1;
    const start = stream.counterStart ?? min;
    if (stream.counter == null) {
      stream.counter = start;
    } else {
      let next = stream.counter + step;
      if (next > max) next = stream.counterWrap ? start : max;
      if (next < min) next = min;
      stream.counter = next;
    }
    return roundTo(stream.counter, stream.decimals);
  }

  let raw;
  switch (stream.generationType) {
    case 'constant': {
      const fallback = min + span / 2;
      raw = stream.constantValue == null ? fallback : stream.constantValue;
      break;
    }
    case 'uniform':
      raw = min + Math.random() * span;
      break;
    case 'gaussian': {
      const mean = min + span / 2;
      const sigma = span / 6 || 1e-9;
      raw = mean + gaussian() * sigma;
      break;
    }
    case 'random_walk': {
      const step = span * ((stream.stepPercent ?? 2) / 100);
      const prev = stream.walk == null ? min + span / 2 : stream.walk;
      const delta = (Math.random() * 2 - 1) * step;
      raw = clamp(prev + delta, min, max);
      stream.walk = raw;
      break;
    }
    case 'sine': {
      const periodMs = Math.max(1, (stream.wavePeriodSec ?? 60) * 1000);
      const t = (nowMs - stream.startedAt) / periodMs;
      raw = min + span * (0.5 + 0.5 * Math.sin(2 * Math.PI * t));
      break;
    }
    case 'square': {
      const periodMs = Math.max(1, (stream.wavePeriodSec ?? 60) * 1000);
      const duty = clamp((stream.dutyPercent ?? 50) / 100, 0.01, 0.99);
      const phase = ((nowMs - stream.startedAt) % periodMs) / periodMs;
      raw = phase < duty ? max : min;
      break;
    }
    case 'sawtooth': {
      const periodMs = Math.max(1, (stream.wavePeriodSec ?? 60) * 1000);
      const phase = ((nowMs - stream.startedAt) % periodMs) / periodMs;
      raw = min + span * phase;
      break;
    }
    case 'ramp': {
      const durationMs = Math.max(1, (stream.rampDurationSec ?? 300) * 1000);
      const elapsed = Math.max(0, nowMs - stream.startedAt);
      let phase = elapsed / durationMs;
      if (stream.rampMode === 'hold') phase = Math.min(1, phase);
      else phase = phase % 1;
      raw = min + span * phase;
      break;
    }
    default:
      raw = min;
  }

  return roundTo(clamp(raw, min, max), stream.decimals);
}

/**
 * One sample per due stream. A stall longer than two periods does not
 * replay the missed backlog: that burst would flood the broker after a pause.
 */
function planTick(nextAt, now, periodMs) {
  const period = Math.max(1, periodMs);
  if (nextAt == null) return { fire: true, nextAt: now + period, skipped: 0 };
  if (nextAt - now > period * 2) return { fire: true, nextAt: now + period, skipped: 0 };
  if (now < nextAt) return { fire: false, nextAt, skipped: 0 };
  const missed = Math.floor((now - nextAt) / period);
  if (missed >= 2) {
    return { fire: true, nextAt: now + period, skipped: missed - 1 };
  }
  let upcoming = nextAt + period;
  if (upcoming <= now) upcoming = now + period;
  return { fire: true, nextAt: upcoming, skipped: 0 };
}

module.exports = { nextValue, planTick, roundTo, clamp };
