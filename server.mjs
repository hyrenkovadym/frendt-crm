// FRENDT CRM — сервер з базою даних SQLite.
// Запуск:  node --experimental-sqlite server.mjs      (Node.js 22.5+)
// Налаштування через змінні середовища:
//   PORT      — порт (за замовчуванням 4173)
//   HOST      — адреса (за замовчуванням 127.0.0.1; на сервері — 0.0.0.0)
//   DATA_DIR  — папка з базою та резервними копіями (за замовчуванням ~/FRENDT-CRM-data)
import { createServer } from 'node:http';
import { readFile, mkdir, readdir, rm, copyFile } from 'node:fs/promises';
import { existsSync, readFileSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { homedir } from 'node:os';
import { fileURLToPath } from 'node:url';
import { DatabaseSync } from 'node:sqlite';
import { timingSafeEqual } from 'node:crypto';

const here = dirname(fileURLToPath(import.meta.url));
const PORT = Number(process.env.PORT) || 4173;
const HOST = process.env.HOST || '127.0.0.1';
const DATA_DIR = process.env.DATA_DIR || join(homedir(), 'FRENDT-CRM-data');
const CRM_USERNAME = process.env.CRM_USERNAME || '';
const CRM_PASSWORD = process.env.CRM_PASSWORD || '';
const BACKUP_DIR = join(DATA_DIR, 'backups');
await mkdir(BACKUP_DIR, { recursive: true });
const DB_FILE = join(DATA_DIR, 'frendt-crm.db');

/* __EMBEDDED_HTML__ */
const HTML_FILE = join(here, 'public', 'index.html');
const getHtml = () => (typeof EMBEDDED_HTML === 'string' ? EMBEDDED_HTML : readFileSync(HTML_FILE, 'utf8'));

// ---------- База даних ----------
const db = new DatabaseSync(DB_FILE);
db.exec(`
  PRAGMA journal_mode = WAL;
  PRAGMA busy_timeout = 5000;
  CREATE TABLE IF NOT EXISTS requests (id INTEGER PRIMARY KEY, data TEXT NOT NULL, updated_at TEXT NOT NULL, rev INTEGER NOT NULL);
  CREATE TABLE IF NOT EXISTS clients  (edrpou TEXT PRIMARY KEY, data TEXT NOT NULL, updated_at TEXT NOT NULL, rev INTEGER NOT NULL);
  CREATE TABLE IF NOT EXISTS dicts    (k TEXT PRIMARY KEY, data TEXT NOT NULL, updated_at TEXT NOT NULL);
  CREATE TABLE IF NOT EXISTS meta     (k TEXT PRIMARY KEY, v TEXT NOT NULL);
  CREATE TABLE IF NOT EXISTS history  (id INTEGER PRIMARY KEY AUTOINCREMENT, at TEXT NOT NULL, kind TEXT NOT NULL, key TEXT NOT NULL, data TEXT NOT NULL);
  INSERT OR IGNORE INTO meta (k, v) VALUES ('rev', '0');
`);
const q = {
  rev: db.prepare(`SELECT CAST(v AS INTEGER) AS v FROM meta WHERE k='rev'`),
  bump: db.prepare(`UPDATE meta SET v = CAST(v AS INTEGER) + 1 WHERE k='rev'`),
  allReq: db.prepare(`SELECT data FROM requests ORDER BY id DESC`),
  allCli: db.prepare(`SELECT data FROM clients ORDER BY rowid`),
  getDicts: db.prepare(`SELECT data FROM dicts WHERE k='custom'`),
  maxId: db.prepare(`SELECT COALESCE(MAX(id), 0) AS m FROM requests`),
  upReq: db.prepare(`INSERT INTO requests (id, data, updated_at, rev) VALUES (?, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET data=excluded.data, updated_at=excluded.updated_at, rev=excluded.rev`),
  upCli: db.prepare(`INSERT INTO clients (edrpou, data, updated_at, rev) VALUES (?, ?, ?, ?) ON CONFLICT(edrpou) DO UPDATE SET data=excluded.data, updated_at=excluded.updated_at, rev=excluded.rev`),
  upDicts: db.prepare(`INSERT INTO dicts (k, data, updated_at) VALUES ('custom', ?, ?) ON CONFLICT(k) DO UPDATE SET data=excluded.data, updated_at=excluded.updated_at`),
  hist: db.prepare(`INSERT INTO history (at, kind, key, data) VALUES (?, ?, ?, ?)`),
  count: db.prepare(`SELECT (SELECT COUNT(*) FROM requests) AS r, (SELECT COUNT(*) FROM clients) AS c`)
};
const rev = () => q.rev.get().v;
const now = () => new Date().toISOString();
function tx(fn) { db.exec('BEGIN IMMEDIATE'); try { const r = fn(); db.exec('COMMIT'); return r; } catch (e) { db.exec('ROLLBACK'); throw e; } }

function state() {
  const d = q.getDicts.get();
  const { r, c } = q.count.get();
  return {
    rev: rev(),
    requests: r ? q.allReq.all().map(x => JSON.parse(x.data)) : null,
    clients: c ? q.allCli.all().map(x => JSON.parse(x.data)) : null,
    dicts: d ? JSON.parse(d.data) : null,
    updated: now()
  };
}

// Зберегти лише змінені записи (заявки — за номером, клієнти — за ЄДРПОУ)
function sync(body) {
  return tx(() => {
    q.bump.run(); const r = rev(); const t = now();
    for (const it of Array.isArray(body.requests) ? body.requests : []) {
      if (!it || it.id == null) continue;
      const s = JSON.stringify(it); q.upReq.run(Number(it.id), s, t, r); q.hist.run(t, 'request', String(it.id), s);
    }
    for (const it of Array.isArray(body.clients) ? body.clients : []) {
      const k = String(it?.edrpou ?? '').trim(); if (!k) continue;
      const s = JSON.stringify(it); q.upCli.run(k, s, t, r); q.hist.run(t, 'client', k, s);
    }
    if (body.dicts && typeof body.dicts === 'object') { const s = JSON.stringify(body.dicts); q.upDicts.run(s, t); q.hist.run(t, 'dicts', 'custom', s); }
    return r;
  });
}

// Нова заявка: номер видає сервер, щоб у двох людей не вийшов однаковий
function createRequest(item) {
  return tx(() => {
    q.bump.run(); const r = rev(); const t = now();
    const id = q.maxId.get().m + 1;
    const rec = { ...item, id };
    const s = JSON.stringify(rec); q.upReq.run(id, s, t, r); q.hist.run(t, 'request', String(id), s);
    return rec;
  });
}

// Перенесення даних зі старого data.json (якщо база ще порожня)
for (const old of [join(DATA_DIR, 'data.json'), join(here, 'data.json')]) {
  const { r, c } = q.count.get();
  if (r || c || !existsSync(old)) continue;
  try { const d = JSON.parse(await readFile(old, 'utf8')); sync({ requests: d.requests || [], clients: d.clients || [], dicts: d.dicts || undefined }); console.log('Імпортовано дані з', old); } catch (e) { console.error('Не вдалося імпортувати', old, e.message); }
}

// ---------- Резервні копії: при запуску та щодня, зберігаються 30 останніх ----------
async function backup() {
  try {
    db.exec('PRAGMA wal_checkpoint(TRUNCATE)');
    const name = `frendt-crm-${new Date().toISOString().slice(0, 16).replace(/[:T]/g, '-')}.db`;
    await copyFile(DB_FILE, join(BACKUP_DIR, name));
    const files = (await readdir(BACKUP_DIR)).filter(f => f.endsWith('.db')).sort();
    for (const f of files.slice(0, Math.max(0, files.length - 30))) await rm(join(BACKUP_DIR, f));
  } catch (e) { console.error('Резервна копія:', e.message); }
}
await backup();
setInterval(backup, 24 * 3600 * 1000);

// ---------- HTTP ----------
const send = (res, code, body, type = 'application/json; charset=utf-8') => { res.writeHead(code, { 'Content-Type': type, 'Cache-Control': 'no-store' }); res.end(typeof body === 'string' ? body : JSON.stringify(body)); };
async function readJson(req) { let raw = ''; for await (const ch of req) { raw += ch; if (raw.length > 25e6) throw new Error('too large'); } return JSON.parse(raw || '{}'); }
function sameSecret(a, b) {
  const aa = Buffer.from(String(a));
  const bb = Buffer.from(String(b));
  return aa.length === bb.length && timingSafeEqual(aa, bb);
}
function authorized(req) {
  if (!CRM_USERNAME || !CRM_PASSWORD) return true;
  const header = String(req.headers.authorization || '');
  if (!header.startsWith('Basic ')) return false;
  try {
    const decoded = Buffer.from(header.slice(6), 'base64').toString('utf8');
    const split = decoded.indexOf(':');
    return split >= 0 && sameSecret(decoded.slice(0, split), CRM_USERNAME) && sameSecret(decoded.slice(split + 1), CRM_PASSWORD);
  } catch { return false; }
}

createServer(async (req, res) => {
  try {
    const url = new URL(req.url, 'http://localhost');
    const p = url.pathname;
    if (p === '/api/health') return send(res, 200, { ok: true });
    if (!authorized(req)) {
      res.writeHead(401, { 'WWW-Authenticate': 'Basic realm="FRENDT CRM", charset="UTF-8"', 'Cache-Control': 'no-store' });
      return res.end('Authentication required');
    }
    if (p === '/api/data' && req.method === 'GET') return send(res, 200, state());
    if ((p === '/api/data' || p === '/api/sync') && (req.method === 'PUT' || req.method === 'POST')) { sync(await readJson(req)); return send(res, 200, state()); }
    if (p === '/api/requests' && req.method === 'POST') { const rec = createRequest(await readJson(req)); return send(res, 200, { request: rec, rev: rev() }); }
    if (p === '/' || p === '/index.html') return send(res, 200, getHtml(), 'text/html; charset=utf-8');
    return send(res, 404, 'Not found', 'text/plain');
  } catch (e) { console.error(e); return send(res, 400, { error: e.message }); }
}).listen(PORT, HOST, () => {
  console.log(`FRENDT CRM OK: http://${HOST === '0.0.0.0' ? '127.0.0.1' : HOST}:${PORT}`);
  console.log(`База даних: ${DB_FILE}`);
  console.log(`Резервні копії: ${BACKUP_DIR}`);
});
