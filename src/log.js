'use strict';

function log(level, msg, extra) {
  const line = { ts: new Date().toISOString(), level, msg };
  if (extra) Object.assign(line, extra);
  const text = JSON.stringify(line);
  if (level === 'error') console.error(text);
  else console.log(text);
}

module.exports = { log };
