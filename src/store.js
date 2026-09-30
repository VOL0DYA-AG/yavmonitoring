'use strict';

const fs = require('fs');
const path = require('path');
const { log } = require('./log');

function emptyState() {
  return { version: 1, desiredConnected: false, broker: null, streams: [] };
}

class Store {
  constructor(file) {
    this.file = file;
  }

  load() {
    try {
      if (!fs.existsSync(this.file)) return emptyState();
      const data = JSON.parse(fs.readFileSync(this.file, 'utf8'));
      if (!data || data.version !== 1 || !Array.isArray(data.streams)) {
        throw new Error('неподдерживаемая схема');
      }
      return data;
    } catch (err) {
      log('error', 'state file unreadable, starting empty', { error: err.message, file: this.file });
      try {
        if (fs.existsSync(this.file)) {
          fs.renameSync(this.file, `${this.file}.bak-${Date.now()}`);
        }
      } catch (renameErr) {
        log('error', 'failed to quarantine state file', { error: renameErr.message });
      }
      return emptyState();
    }
  }

  save(state) {
    const dir = path.dirname(this.file);
    fs.mkdirSync(dir, { recursive: true });
    const tmp = `${this.file}.${process.pid}.tmp`;
    fs.writeFileSync(tmp, JSON.stringify(state), { mode: 0o600 });
    fs.renameSync(tmp, this.file);
    fs.chmodSync(this.file, 0o600);
  }
}

module.exports = { Store, emptyState };
