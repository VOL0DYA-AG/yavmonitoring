'use strict';

function buildPayload(stream, value, ts) {
  const fixed = Number(value).toFixed(stream.decimals);
  if (stream.payloadFormat === 'value') return fixed;
  const body = {
    ts: new Date(ts).toISOString(),
    value: Number(fixed),
    name: stream.name,
  };
  if (stream.unit) body.unit = stream.unit;
  body.quality = 'good';
  return JSON.stringify(body);
}

module.exports = { buildPayload };
