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
import { scryptSync, randomBytes, timingSafeEqual } from 'node:crypto';

const here = dirname(fileURLToPath(import.meta.url));
const PORT = Number(process.env.PORT) || 4173;
const HOST = process.env.HOST || '127.0.0.1';
const DATA_DIR = process.env.DATA_DIR || join(homedir(), 'FRENDT-CRM-data');
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
  CREATE TABLE IF NOT EXISTS users    (login TEXT PRIMARY KEY, name TEXT NOT NULL, role TEXT NOT NULL, pass TEXT NOT NULL, created_at TEXT NOT NULL);
  CREATE TABLE IF NOT EXISTS sessions (token TEXT PRIMARY KEY, login TEXT NOT NULL, expires INTEGER NOT NULL);
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

// ---------- Перше заповнення: усі заявки й клієнти з Google-таблиці «Облік заявок FRENDT» ----------
/* __EMBEDDED_SEED__ */
const SEED_FILE = join(here, 'seed.json');
(function seedOnce() {
  if (db.prepare(`SELECT v FROM meta WHERE k='sheet_seed_v1'`).get()) return;
  const seed = typeof EMBEDDED_SEED === 'object' ? EMBEDDED_SEED : (existsSync(SEED_FILE) ? JSON.parse(readFileSync(SEED_FILE, 'utf8')) : null);
  if (!seed) return;
  tx(() => {
    q.bump.run(); const r = rev(); const t = now();
    db.exec('DELETE FROM requests; DELETE FROM clients;');
    for (const it of seed.requests) q.upReq.run(Number(it.id), JSON.stringify(it), t, r);
    for (const it of seed.clients) { const k = String(it.edrpou ?? '').trim(); if (k) q.upCli.run(k, JSON.stringify(it), t, r); }
    q.hist.run(t, 'seed', 'sheet_seed_v1', JSON.stringify({ requests: seed.requests.length, clients: seed.clients.length }));
    db.prepare(`INSERT INTO meta (k, v) VALUES ('sheet_seed_v1', ?)`).run(t);
  });
  console.log(`Завантажено з таблиці: ${seed.requests.length} заявок, ${seed.clients.length} клієнтів`);
})();

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
function sync(body, who = '') {
  return tx(() => {
    q.bump.run(); const r = rev(); const t = now();
    for (const it of Array.isArray(body.requests) ? body.requests : []) {
      if (!it || it.id == null) continue;
      const s = JSON.stringify(it); q.upReq.run(Number(it.id), s, t, r); q.hist.run(t, 'request' + (who ? ':' + who : ''), String(it.id), s);
    }
    for (const it of Array.isArray(body.clients) ? body.clients : []) {
      const k = String(it?.edrpou ?? '').trim(); if (!k) continue;
      const s = JSON.stringify(it); q.upCli.run(k, s, t, r); q.hist.run(t, 'client' + (who ? ':' + who : ''), k, s);
    }
    if (body.dicts && typeof body.dicts === 'object') { const s = JSON.stringify(body.dicts); q.upDicts.run(s, t); q.hist.run(t, 'dicts', 'custom', s); }
    // Видалення (лише з режиму адміністратора в CRM); старий вміст лишається в історії
    const del = body.deleted || {};
    for (const id of Array.isArray(del.requests) ? del.requests : []) { db.prepare('DELETE FROM requests WHERE id = ?').run(Number(id)); q.hist.run(t, 'request-deleted' + (who ? ':' + who : ''), String(id), '{}'); }
    for (const k of Array.isArray(del.clients) ? del.clients : []) { db.prepare('DELETE FROM clients WHERE edrpou = ?').run(String(k)); q.hist.run(t, 'client-deleted' + (who ? ':' + who : ''), String(k), '{}'); }
    return r;
  });
}

// Нова заявка: номер видає сервер, щоб у двох людей не вийшов однаковий
function createRequest(item, who = '') {
  return tx(() => {
    q.bump.run(); const r = rev(); const t = now();
    const id = q.maxId.get().m + 1;
    const rec = { ...item, id };
    const s = JSON.stringify(rec); q.upReq.run(id, s, t, r); q.hist.run(t, 'request-new' + (who ? ':' + who : ''), String(id), s);
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

// ---------- Користувачі та вхід ----------
// Паролі зберігаються лише як scrypt-хеш; сесія — випадковий токен у cookie HttpOnly на 30 днів.
const hashPass = (p) => { const salt = randomBytes(16).toString('hex'); return salt + ':' + scryptSync(String(p), salt, 64).toString('hex'); };
const checkPass = (p, h) => { const [salt, hex] = String(h).split(':'); if (!salt || !hex) return false; const a = Buffer.from(hex, 'hex'), b = scryptSync(String(p), salt, 64); return a.length === b.length && timingSafeEqual(a, b); };
const U = {
  get: db.prepare('SELECT login, name, role, pass FROM users WHERE login = ?'),
  all: db.prepare('SELECT login, name, role, created_at FROM users ORDER BY role, name'),
  count: db.prepare('SELECT COUNT(*) AS n FROM users'),
  admins: db.prepare(`SELECT COUNT(*) AS n FROM users WHERE role = 'admin'`),
  add: db.prepare('INSERT INTO users (login, name, role, pass, created_at) VALUES (?, ?, ?, ?, ?)'),
  upd: db.prepare('UPDATE users SET name = ?, role = ? WHERE login = ?'),
  pass: db.prepare('UPDATE users SET pass = ? WHERE login = ?'),
  del: db.prepare('DELETE FROM users WHERE login = ?'),
  sAdd: db.prepare('INSERT INTO sessions (token, login, expires) VALUES (?, ?, ?)'),
  sGet: db.prepare('SELECT s.login, u.name, u.role FROM sessions s JOIN users u ON u.login = s.login WHERE s.token = ? AND s.expires > ?'),
  sDel: db.prepare('DELETE FROM sessions WHERE token = ?'),
  sDelUser: db.prepare('DELETE FROM sessions WHERE login = ?'),
  sClean: db.prepare('DELETE FROM sessions WHERE expires < ?')
};
const cleanLogin = (l) => String(l || '').trim().toLowerCase();
// Перший адміністратор: нові ADMIN_* або сумісні зі старим розгортанням CRM_*.
const initialAdminLogin = cleanLogin(process.env.ADMIN_LOGIN || process.env.CRM_USERNAME || 'admin');
const initialAdminPassword = process.env.ADMIN_PASSWORD || process.env.CRM_PASSWORD || '';
const initialAdminName = process.env.ADMIN_NAME || 'Володимир Гиренко';
if (!U.count.get().n && initialAdminPassword) {
  U.add.run(initialAdminLogin, initialAdminName, 'admin', hashPass(initialAdminPassword), now());
  console.log('Створено адміністратора:', initialAdminLogin);
}
U.sClean.run(Date.now()); setInterval(() => U.sClean.run(Date.now()), 3600 * 1000);
const cookies = (req) => Object.fromEntries(String(req.headers.cookie || '').split(';').map(x => x.trim().split('=')).filter(x => x[0]).map(([k, ...v]) => [k, decodeURIComponent(v.join('='))]));
const whoami = (req) => { const t = cookies(req).frendt_sid; return t ? U.sGet.get(t, Date.now()) || null : null; };
const isLocal = (req) => { const a = req.socket.remoteAddress || ''; return !req.headers['x-forwarded-for'] && (a === '127.0.0.1' || a === '::1' || a === '::ffff:127.0.0.1'); };
const secure = (req) => req.headers['x-forwarded-proto'] === 'https';
function startSession(req, res, login) {
  const token = randomBytes(32).toString('hex'); const days = 30;
  U.sAdd.run(token, login, Date.now() + days * 864e5);
  res.setHeader('Set-Cookie', `frendt_sid=${token}; Path=/; HttpOnly; SameSite=Lax; Max-Age=${days * 86400}${secure(req) ? '; Secure' : ''}`);
}
const tries = new Map(); // захист від підбору пароля: 10 спроб за 15 хв з однієї адреси
const ipOf = (req) => String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
function limited(req) { const k = ipOf(req), t = Date.now(), a = (tries.get(k) || []).filter(x => t - x < 15 * 60e3); tries.set(k, a); return a.length >= 10; }
const fail = (req) => { const k = ipOf(req); tries.set(k, [...(tries.get(k) || []), Date.now()]); };
const publicUser = (u) => u && ({ login: u.login, name: u.name, role: u.role });

// ---------- HTTP ----------
const send = (res, code, body, type = 'application/json; charset=utf-8') => { res.writeHead(code, { 'Content-Type': type, 'Cache-Control': 'no-store', 'X-Frame-Options': 'SAMEORIGIN', 'X-Content-Type-Options': 'nosniff', 'Referrer-Policy': 'same-origin' }); res.end(typeof body === 'string' ? body : JSON.stringify(body)); };
async function readJson(req) { let raw = ''; for await (const ch of req) { raw += ch; if (raw.length > 25e6) throw new Error('too large'); } return JSON.parse(raw || '{}'); }

createServer(async (req, res) => {
  try {
    const url = new URL(req.url, 'http://localhost');
    const p = url.pathname;
    if (p === '/api/health') return send(res, 200, { ok: true });
    // --- вхід ---
    if (p === '/api/me' && req.method === 'GET') { const me = whoami(req); return send(res, me ? 200 : 401, me ? { user: publicUser(me) } : { error: 'auth', setup: !U.count.get().n, setupAllowed: !U.count.get().n && isLocal(req) }); }
    if (p === '/api/setup' && req.method === 'POST') {
      if (U.count.get().n) return send(res, 403, { error: 'Адміністратор уже є' });
      if (!isLocal(req)) return send(res, 403, { error: 'Першого адміністратора можна створити лише на комп’ютері, де запущено сервер, або через ADMIN_PASSWORD' });
      const b = await readJson(req); const login = cleanLogin(b.login); if (!login || String(b.password || '').length < 6) return send(res, 400, { error: 'Логін і пароль від 6 символів' });
      U.add.run(login, String(b.name || 'Адміністратор').trim(), 'admin', hashPass(b.password), now()); startSession(req, res, login); return send(res, 200, { user: publicUser(U.get.get(login)) });
    }
    if (p === '/api/login' && req.method === 'POST') {
      if (limited(req)) return send(res, 429, { error: 'Забагато спроб. Спробуйте за 15 хвилин' });
      const b = await readJson(req); const u = U.get.get(cleanLogin(b.login));
      if (!u || !checkPass(b.password, u.pass)) { fail(req); return send(res, 401, { error: 'Неправильний логін або пароль' }); }
      startSession(req, res, u.login); return send(res, 200, { user: publicUser(u) });
    }
    if (p === '/api/logout' && req.method === 'POST') { const t = cookies(req).frendt_sid; if (t) U.sDel.run(t); res.setHeader('Set-Cookie', 'frendt_sid=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0'); return send(res, 200, { ok: true }); }
    if (p.startsWith('/api/')) {
      const me = whoami(req); if (!me) return send(res, 401, { error: 'auth' });
      const admin = me.role === 'admin';
      if (p === '/api/password' && req.method === 'POST') { const b = await readJson(req); const u = U.get.get(me.login); if (!checkPass(b.old, u.pass)) return send(res, 400, { error: 'Поточний пароль неправильний' }); if (String(b.password || '').length < 6) return send(res, 400, { error: 'Новий пароль — від 6 символів' }); U.pass.run(hashPass(b.password), me.login); return send(res, 200, { ok: true }); }
      if (p === '/api/users') {
        if (!admin) return send(res, 403, { error: 'Лише для адміністратора' });
        if (req.method === 'GET') return send(res, 200, { users: U.all.all() });
        const b = await readJson(req); const login = cleanLogin(b.login); const u = U.get.get(login);
        if (req.method === 'POST') {
          if (!login || !/^[a-z0-9._-]{2,40}$/.test(login)) return send(res, 400, { error: 'Логін: латинські літери, цифри, крапка, дефіс' });
          const role = b.role === 'admin' ? 'admin' : 'manager'; const name = String(b.name || login).trim();
          if (!u) { if (String(b.password || '').length < 6) return send(res, 400, { error: 'Пароль — від 6 символів' }); U.add.run(login, name, role, hashPass(b.password), now()); }
          else { if (u.role === 'admin' && role !== 'admin' && U.admins.get().n < 2) return send(res, 400, { error: 'Має залишитися хоча б один адміністратор' }); U.upd.run(name, role, login); if (b.password) { if (String(b.password).length < 6) return send(res, 400, { error: 'Пароль — від 6 символів' }); U.pass.run(hashPass(b.password), login); U.sDelUser.run(login); } }
          return send(res, 200, { users: U.all.all() });
        }
        if (req.method === 'DELETE') { if (!u) return send(res, 404, { error: 'Немає такого користувача' }); if (login === me.login) return send(res, 400, { error: 'Не можна видалити себе' }); if (u.role === 'admin' && U.admins.get().n < 2) return send(res, 400, { error: 'Має залишитися хоча б один адміністратор' }); U.sDelUser.run(login); U.del.run(login); return send(res, 200, { users: U.all.all() }); }
      }
      if (p === '/api/data' && req.method === 'GET') return send(res, 200, { ...state(), me: publicUser(me) });
      if ((p === '/api/data' || p === '/api/sync') && (req.method === 'PUT' || req.method === 'POST')) { const b = await readJson(req); if (!admin) { delete b.deleted; } sync(b, me.login); return send(res, 200, state()); }
      if (p === '/api/requests' && req.method === 'POST') { const rec = createRequest({ ...(await readJson(req)), author: me.name }, me.login); return send(res, 200, { request: rec, rev: rev() }); }
      return send(res, 404, { error: 'Not found' });
    }
    if (p === '/' || p === '/index.html') return send(res, 200, getHtml(), 'text/html; charset=utf-8');
    return send(res, 404, 'Not found', 'text/plain');
  } catch (e) { console.error(e); return send(res, 400, { error: e.message }); }
}).listen(PORT, HOST, () => {
  console.log(`FRENDT CRM OK: http://${HOST === '0.0.0.0' ? '127.0.0.1' : HOST}:${PORT}`);
  console.log(`База даних: ${DB_FILE}`);
  console.log(`Резервні копії: ${BACKUP_DIR}`);
});
