'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { Store } = require('./store');
const { MqttBroker } = require('./broker');
const { Engine } = require('./engine');
const { log } = require('./log');

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.woff2': 'font/woff2',
  '.txt': 'text/plain; charset=utf-8',
};

const SECURITY_HEADERS = {
  'X-Content-Type-Options': 'nosniff',
  'Referrer-Policy': 'no-referrer',
  'Content-Security-Policy': "default-src 'self'; style-src 'self'; font-src 'self'; img-src 'self'; connect-src 'self'; base-uri 'none'; form-action 'self'",
};

function createServer(engine, options = {}) {
  const publicDir = options.publicDir || path.join(__dirname, '..', 'public');
  return http.createServer((req, res) => {
    Promise.resolve()
      .then(() => route(req, res, engine, publicDir))
      .catch((err) => {
        if (res.headersSent) return;
        const status = err.status || 500;
        if (status >= 500) log('error', 'request failed', { error: err.stack || err.message });
        sendJson(res, status, { error: status >= 500 ? 'Внутренняя ошибка' : err.message });
      });
  });
}

async function route(req, res, engine, publicDir) {
  const url = new URL(req.url, 'http://127.0.0.1');
  const pathname = decodeURIComponent(url.pathname);
  if (pathname.startsWith('/api/')) {
    await routeApi(req, res, engine, pathname);
    return;
  }
  serveStatic(res, publicDir, pathname);
}

async function routeApi(req, res, engine, pathname) {
  if (req.method === 'GET' && pathname === '/api/health') {
    const state = engine.publicState();
    const running = state.streams.filter((stream) => stream.enabled).length;
    sendJson(res, 200, {
      ok: true,
      uptimeSec: state.uptimeSec,
      broker: state.brokerStatus.state,
      streams: state.streams.length,
      running,
    });
    return;
  }
  if (req.method === 'GET' && pathname === '/api/state') {
    sendJson(res, 200, engine.publicState());
    return;
  }
  if (req.method === 'GET' && pathname === '/api/events') {
    serveEvents(req, res, engine);
    return;
  }
  if (req.method === 'POST' && pathname === '/api/broker/connect') {
    sendJson(res, 200, engine.connect(await readJson(req)));
    return;
  }
  if (req.method === 'POST' && pathname === '/api/broker/disconnect') {
    sendJson(res, 200, engine.disconnect());
    return;
  }
  if (req.method === 'POST' && pathname === '/api/streams') {
    sendJson(res, 201, { stream: engine.addStream(await readJson(req)) });
    return;
  }

  const streamMatch = pathname.match(/^\/api\/streams\/([0-9a-f-]{36})(\/pause|\/resume)?$/i);
  if (streamMatch) {
    const id = streamMatch[1];
    const action = streamMatch[2] || '';
    if (req.method === 'GET' && !action) {
      const stream = engine.getStream(id);
      if (!stream) {
        sendJson(res, 404, { error: 'Поток не найден' });
        return;
      }
      sendJson(res, 200, { stream });
      return;
    }
    if (req.method === 'PUT' && !action) {
      sendJson(res, 200, { stream: engine.updateStream(id, await readJson(req)) });
      return;
    }
    if (req.method === 'DELETE' && !action) {
      engine.removeStream(id);
      sendJson(res, 200, { ok: true });
      return;
    }
    if (req.method === 'POST' && action === '/pause') {
      sendJson(res, 200, { stream: engine.setEnabled(id, false) });
      return;
    }
    if (req.method === 'POST' && action === '/resume') {
      sendJson(res, 200, { stream: engine.setEnabled(id, true) });
      return;
    }
  }

  sendJson(res, 404, { error: 'Не найдено' });
}

function serveEvents(req, res, engine) {
  res.writeHead(200, {
    ...SECURITY_HEADERS,
    'Content-Type': 'text/event-stream; charset=utf-8',
    'Cache-Control': 'no-cache, no-transform',
    Connection: 'keep-alive',
  });
  const send = (event, data) => {
    res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
  };
  send('hello', engine.publicState());
  const onSample = (sample) => send('sample', sample);
  const onStatus = (status) => send('status', status);
  const onHeartbeat = (beat) => send('heartbeat', beat);
  engine.on('sample', onSample);
  engine.on('status', onStatus);
  engine.on('heartbeat', onHeartbeat);
  const ping = setInterval(() => res.write(': ping\n\n'), 20000);
  const close = () => {
    clearInterval(ping);
    engine.off('sample', onSample);
    engine.off('status', onStatus);
    engine.off('heartbeat', onHeartbeat);
  };
  req.on('close', close);
  res.on('error', close);
}

function serveStatic(res, publicDir, pathname) {
  const file = safeFile(publicDir, pathname);
  if (!file) {
    sendJson(res, 400, { error: 'Некорректный путь' });
    return;
  }
  fs.stat(file, (err, stat) => {
    if (err || !stat.isFile()) {
      sendJson(res, 404, { error: 'Не найдено' });
      return;
    }
    const ext = path.extname(file);
    res.writeHead(200, {
      ...SECURITY_HEADERS,
      'Content-Type': MIME[ext] || 'application/octet-stream',
      'Cache-Control': ext === '.woff2' ? 'public, max-age=86400' : 'no-cache',
    });
    const stream = fs.createReadStream(file);
    stream.on('error', () => {
      if (!res.headersSent) sendJson(res, 404, { error: 'Не найдено' });
      else res.destroy();
    });
    stream.pipe(res);
  });
}

function safeFile(publicDir, pathname) {
  const root = path.resolve(publicDir);
  const rel = pathname === '/' ? 'index.html' : pathname.replace(/^\/+/, '');
  if (!rel || rel.includes('\0')) return null;
  const file = path.resolve(root, rel);
  if (file !== root && !file.startsWith(root + path.sep)) return null;
  return file;
}

function readJson(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    req.on('data', (chunk) => {
      size += chunk.length;
      if (size > 1_000_000) {
        reject(Object.assign(new Error('Слишком большое тело запроса'), { status: 413 }));
        req.destroy();
        return;
      }
      chunks.push(chunk);
    });
    req.on('end', () => {
      if (!chunks.length) {
        resolve({});
        return;
      }
      try {
        resolve(JSON.parse(Buffer.concat(chunks).toString('utf8')));
      } catch {
        reject(Object.assign(new Error('Некорректный JSON'), { status: 400 }));
      }
    });
    req.on('error', reject);
  });
}

function sendJson(res, status, body) {
  const payload = JSON.stringify(body);
  res.writeHead(status, {
    ...SECURITY_HEADERS,
    'Content-Type': 'application/json; charset=utf-8',
    'Cache-Control': 'no-store',
  });
  res.end(payload);
}

function main() {
  const major = Number(process.versions.node.split('.')[0]);
  if (major < 18) {
    console.error('Нужен Node.js 18 или новее');
    process.exit(1);
  }
  const dataDir = path.resolve(process.env.DATA_DIR || path.join(__dirname, '..', 'data'));
  const host = process.env.HOST || '0.0.0.0';
  const port = Number(process.env.PORT || 8080);
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    console.error('PORT должен быть целым числом от 1 до 65535');
    process.exit(1);
  }
  const store = new Store(path.join(dataDir, 'state.json'));
  const broker = new MqttBroker();
  const engine = new Engine({ store, broker });
  engine.load();
  const server = createServer(engine);
  server.keepAliveTimeout = 65000;
  server.headersTimeout = 70000;
  server.listen(port, host, () => {
    log('info', 'listening', { host, port, dataDir });
    engine.start();
  });
  server.on('error', (err) => {
    log('error', 'http server failed', { error: err.message });
    process.exit(1);
  });

  let stopping = false;
  const shutdown = (signal) => {
    if (stopping) return;
    stopping = true;
    log('info', 'shutdown', { signal });
    engine.stop();
    try {
      engine.flush();
    } catch (err) {
      log('error', 'flush on shutdown failed', { error: err.message });
    }
    broker.disconnect();
    server.close(() => process.exit(0));
    setTimeout(() => process.exit(0), 5000).unref();
  };
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('uncaughtException', (err) => {
    log('error', 'uncaughtException', { error: err.stack || err.message });
    try {
      engine.flush();
    } catch { /* already failing */ }
    process.exit(1);
  });
  process.on('unhandledRejection', (err) => {
    const error = err && err.stack ? err.stack : String(err);
    log('error', 'unhandledRejection', { error });
  });
}

if (require.main === module) main();

module.exports = { createServer, main };
