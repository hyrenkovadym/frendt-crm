// FRENDT CRM — сервер з базою даних SQLite.
// Запуск:  node --experimental-sqlite server.mjs      (Node.js 22.5+)
// Налаштування через змінні середовища:
//   PORT      — порт (за замовчуванням 4173)
//   HOST      — адреса (за замовчуванням 127.0.0.1; на сервері — 0.0.0.0)
//   DATA_DIR  — папка з базою та резервними копіями (за замовчуванням ~/FRENDT-CRM-data)
import { createServer } from 'node:http';
import { readFile, writeFile, mkdir, readdir, rm, copyFile } from 'node:fs/promises';
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
const CHAT_FILES_DIR = join(DATA_DIR, 'chat-files');
await mkdir(CHAT_FILES_DIR, { recursive: true });
const DB_FILE = join(DATA_DIR, 'frendt-crm.db');

/* __EMBEDDED_HTML__ */
const HTML_FILE = join(here, 'public', 'index.html');
const LEADS_FILE = join(here, 'leads.json');
let LEADS_JSON = null;
const getLeadsJson = () => {
  if (LEADS_JSON === null) {
    const leads = existsSync(LEADS_FILE) ? JSON.parse(readFileSync(LEADS_FILE, 'utf8')) : [];
    LEADS_JSON = JSON.stringify({ leads });
  }
  return LEADS_JSON;
};
const getHtml = (nonce = '') => {
  const html = typeof EMBEDDED_HTML === 'string' ? EMBEDDED_HTML : readFileSync(HTML_FILE, 'utf8');
  return nonce ? html.replace('<script>', `<script nonce="${nonce}">`) : html;
};

// ---------- База даних ----------
const db = new DatabaseSync(DB_FILE);
db.exec(`
  PRAGMA journal_mode = WAL;
  PRAGMA busy_timeout = 5000;
  CREATE TABLE IF NOT EXISTS requests (id INTEGER PRIMARY KEY, data TEXT NOT NULL, updated_at TEXT NOT NULL, rev INTEGER NOT NULL);
  CREATE TABLE IF NOT EXISTS clients  (edrpou TEXT PRIMARY KEY, data TEXT NOT NULL, updated_at TEXT NOT NULL, rev INTEGER NOT NULL);
  CREATE TABLE IF NOT EXISTS dicts    (k TEXT PRIMARY KEY, data TEXT NOT NULL, updated_at TEXT NOT NULL);
  CREATE TABLE IF NOT EXISTS meta     (k TEXT PRIMARY KEY, v TEXT NOT NULL);
  CREATE TABLE IF NOT EXISTS tasks    (id INTEGER PRIMARY KEY, manager TEXT NOT NULL DEFAULT '', data TEXT NOT NULL, updated_at TEXT NOT NULL);
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
  getReq: db.prepare(`SELECT data FROM requests WHERE id = ?`),
  getCli: db.prepare(`SELECT data FROM clients WHERE edrpou = ?`),
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

// ---------- Задачі менеджерів (окрема таблиця; заявки й клієнти не змінюються) ----------
const TASKS_FILE = join(here, 'tasks.json');
const TASK_KEYS = ['edrpou','company','region','district','address','lpr','phone','phones','email','bitrix','task','c1','c2','ordered','manager','callDate'];
const cleanTask = (body) => { const out = {}; for (const key of TASK_KEYS) { const value = String(body?.[key] ?? '').trim(); if (value) out[key] = value; } return out; };
const taskQ = {
  all: db.prepare('SELECT id, data FROM tasks ORDER BY id'),
  mine: db.prepare('SELECT id, data FROM tasks WHERE manager = ? ORDER BY id'),
  get: db.prepare('SELECT id, manager, data FROM tasks WHERE id = ?'),
  put: db.prepare('INSERT OR REPLACE INTO tasks (id, manager, data, updated_at) VALUES (?, ?, ?, ?)'),
  max: db.prepare('SELECT COALESCE(MAX(id), 0) AS m FROM tasks'),
  count: db.prepare('SELECT COUNT(*) AS n FROM tasks'),
  duplicate: db.prepare("SELECT id, manager, data FROM tasks WHERE json_extract(data, '$.edrpou') = ? AND id <> ? LIMIT 1")
};
(function seedTasksOnce() {
  if (db.prepare("SELECT v FROM meta WHERE k='tasks_seed_v1'").get()) return;
  const rows = existsSync(TASKS_FILE) ? JSON.parse(readFileSync(TASKS_FILE, 'utf8')) : [];
  if (!Array.isArray(rows) || !rows.length) return;
  const timestamp = now();
  tx(() => {
    if (!taskQ.count.get().n) for (const row of rows) {
      const task = {}; TASK_KEYS.forEach((key, index) => { if (row[index + 1]) task[key] = row[index + 1]; });
      taskQ.put.run(Number(row[0]), task.manager || '', JSON.stringify(task), timestamp);
    }
    db.prepare("INSERT OR REPLACE INTO meta (k, v) VALUES ('tasks_seed_v1', ?)").run(timestamp);
  });
  console.log(`Задачі менеджерів: ${taskQ.count.get().n}`);
})();
const taskRows = (rows) => rows.map(row => ({ id: row.id, ...JSON.parse(row.data) }));

// ---------- Перше заповнення: усі заявки й клієнти з Google-таблиці «Облік заявок FRENDT» ----------
/* __EMBEDDED_SEED__ */
const SEED_FILE = join(here, 'seed.json');
(function seedOnce() {
  if (db.prepare(`SELECT v FROM meta WHERE k='sheet_seed_v1'`).get()) return;
  // база вже з даними — НІКОЛИ не стираємо й не перезаливаємо (навіть якщо позначка загубилась)
  { const c0 = q.count.get(); if (c0.r || c0.c) { db.prepare(`INSERT OR REPLACE INTO meta (k, v) VALUES ('sheet_seed_v1', ?)`).run(now()); return; } }
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
    const bases = body.bases && typeof body.bases === 'object' ? body.bases : null;
    if (bases) {
      const conflicts = { requests: [], clients: [], dicts: false };
      const requestBases = bases.requests && typeof bases.requests === 'object' ? bases.requests : {};
      const clientBases = bases.clients && typeof bases.clients === 'object' ? bases.clients : {};
      const checkBase = (hasBase, base, current) => hasBase && ((base === null && current !== null) || (typeof base === 'string' && current !== base));
      for (const it of Array.isArray(body.requests) ? body.requests : []) {
        if (!it || it.id == null) continue;
        const key = String(Number(it.id));
        const current = q.getReq.get(Number(it.id))?.data ?? null;
        if (checkBase(Object.hasOwn(requestBases, key), requestBases[key], current)) conflicts.requests.push(Number(it.id));
      }
      for (const it of Array.isArray(body.clients) ? body.clients : []) {
        const key = String(it?.edrpou ?? '').trim();
        if (!key) continue;
        const current = q.getCli.get(key)?.data ?? null;
        if (checkBase(Object.hasOwn(clientBases, key), clientBases[key], current)) conflicts.clients.push(key);
      }
      const del = body.deleted || {};
      for (const id of Array.isArray(del.requests) ? del.requests : []) {
        const key = String(Number(id));
        const current = q.getReq.get(Number(id))?.data ?? null;
        if (checkBase(Object.hasOwn(requestBases, key), requestBases[key], current)) conflicts.requests.push(Number(id));
      }
      for (const raw of Array.isArray(del.clients) ? del.clients : []) {
        const key = String(raw).trim();
        const current = q.getCli.get(key)?.data ?? null;
        if (checkBase(Object.hasOwn(clientBases, key), clientBases[key], current)) conflicts.clients.push(key);
      }
      if (body.dicts && Object.hasOwn(bases, 'dicts')) {
        const current = q.getDicts.get()?.data ?? null;
        conflicts.dicts = checkBase(true, bases.dicts, current);
      }
      conflicts.requests = [...new Set(conflicts.requests)];
      conflicts.clients = [...new Set(conflicts.clients)];
      if (conflicts.requests.length || conflicts.clients.length || conflicts.dicts) {
        const error = new Error('Запис уже змінив інший користувач');
        error.status = 409;
        error.conflicts = conflicts;
        throw error;
      }
    }
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
const send = (res, code, body, type = 'application/json; charset=utf-8', extra = {}) => { res.writeHead(code, { 'Content-Type': type, 'Cache-Control': 'no-store', 'X-Frame-Options': 'SAMEORIGIN', 'X-Content-Type-Options': 'nosniff', 'Referrer-Policy': 'same-origin', 'Strict-Transport-Security': 'max-age=31536000; includeSubDomains', 'Cross-Origin-Opener-Policy': 'same-origin', 'Permissions-Policy': 'camera=(), microphone=(), geolocation=()', ...extra }); res.end(typeof body === 'string' ? body : JSON.stringify(body)); };
async function readJson(req) { let raw = ''; for await (const ch of req) { raw += ch; if (raw.length > 5e6) throw new Error('too large'); } return JSON.parse(raw || '{}'); }
function trustedBrowserRequest(req) {
  if (req.headers['sec-fetch-site'] === 'cross-site') return false;
  const origin = req.headers.origin;
  if (!origin) return true;
  try { return new URL(origin).host === req.headers.host; } catch { return false; }
}

// ---------- Ringostat: дзвінки, записи, коментарі в картці клієнта ----------
// RINGOSTAT_AUTH_KEY, RINGOSTAT_PROJECT_ID — з кабінету Ringostat («Інтеграції» → «Ringostat API»).
// Без ключа CRM працює як раніше, просто дзвінків немає.
db.exec(`
  CREATE TABLE IF NOT EXISTS calls (id TEXT PRIMARY KEY, at TEXT NOT NULL, dir TEXT, caller TEXT, dst TEXT, pkey TEXT, employee TEXT,
    disposition TEXT, billsec INTEGER NOT NULL DEFAULT 0, recording TEXT, missed INTEGER NOT NULL DEFAULT 0, handled INTEGER NOT NULL DEFAULT 0, raw TEXT);
  CREATE INDEX IF NOT EXISTS calls_pkey ON calls(pkey);
  CREATE INDEX IF NOT EXISTS calls_at ON calls(at);
  CREATE TABLE IF NOT EXISTS notes (id INTEGER PRIMARY KEY AUTOINCREMENT, edrpou TEXT, call_id TEXT, author TEXT, text TEXT, rating INTEGER, at TEXT NOT NULL);
  CREATE INDEX IF NOT EXISTS notes_edrpou ON notes(edrpou);
`);
const RS_KEY = String(process.env.RINGOSTAT_AUTH_KEY || '').trim();
const RS_PROJECT = String(process.env.RINGOSTAT_PROJECT_ID || '').trim();
const metaGet = (k) => db.prepare('SELECT v FROM meta WHERE k = ?').get(k)?.v ?? null;
const metaSet = (k, v) => db.prepare('INSERT OR REPLACE INTO meta (k, v) VALUES (?, ?)').run(k, String(v));
const RS_TOKEN = String(process.env.RINGOSTAT_WEBHOOK_TOKEN || '').trim() || metaGet('rs_token') || (() => { const t = randomBytes(18).toString('base64url'); metaSet('rs_token', t); return t; })();
const pkey = (v) => { const d = String(v ?? '').replace(/\D/g, ''); return d.length >= 9 ? d.slice(-9) : ''; };
const phoneKeys = (...vals) => [...new Set(vals.flatMap(v => String(v ?? '').split(/[,;\n/]+/)).map(pkey).filter(Boolean))];
const KYIV = new Intl.DateTimeFormat('sv-SE', { timeZone: 'Europe/Kyiv', year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit', hourCycle: 'h23' });
const kyiv = (d) => KYIV.format(d).replace('T', ' ');
const pick = (o, names) => { for (const n of names) { const v = o?.[n]; if (v !== undefined && v !== null && String(v).trim() !== '') return String(v).trim(); } return ''; };
function normCall(o) {
  const at = pick(o, ['calldate', 'call_date', 'date', 'start_time', 'datetime', 'time']).replace('T', ' ').slice(0, 19);
  const type = pick(o, ['call_type', 'type', 'direction']).toLowerCase();
  const caller = pick(o, ['caller', 'src', 'from', 'caller_number', 'number_from', 'callerid']);
  const dst = pick(o, ['dst', 'destination', 'to', 'called', 'number_to', 'dst_number', 'callee']);
  const dir = /out|callback/.test(type) ? 'out' : 'in';
  const disposition = pick(o, ['disposition', 'status', 'call_status']).toUpperCase();
  const billsec = Number.parseInt(pick(o, ['billsec', 'talk_time', 'bill_sec', 'duration_talk']) || '0', 10) || 0;
  const answered = billsec > 0 || /^ANSWER/.test(disposition);
  // один і той самий дзвінок з вебхука і з журналу має збігтися — ключ з часу і номерів
  const id = `${at}|${caller.replace(/\D/g, '').slice(-9)}|${dst.replace(/\D/g, '').slice(-9)}`;
  if (!at) return null;
  return { id, at, dir, caller, dst, pkey: pkey(dir === 'out' ? dst : caller), employee: crmName(pick(o, ['employee_fio', 'employee', 'employee_name', 'manager', 'operator'])),
    disposition, billsec, recording: pick(o, ['recording', 'recording_url', 'record', 'record_url', 'recording_wav']), missed: dir === 'in' && !answered ? 1 : 0 };
}
const CQ = {
  up: db.prepare(`INSERT INTO calls (id, at, dir, caller, dst, pkey, employee, disposition, billsec, recording, missed, raw) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
    ON CONFLICT(id) DO UPDATE SET at=excluded.at, dir=excluded.dir, caller=excluded.caller, dst=excluded.dst, pkey=excluded.pkey,
    employee=COALESCE(NULLIF(excluded.employee,''), calls.employee), disposition=excluded.disposition, billsec=excluded.billsec,
    recording=COALESCE(NULLIF(excluded.recording,''), calls.recording), missed=excluded.missed, raw=excluded.raw`),
  get: db.prepare('SELECT * FROM calls WHERE id = ?'),
  done: db.prepare('UPDATE calls SET handled = 1 WHERE id = ?'),
  count: db.prepare('SELECT COUNT(*) AS n, MAX(at) AS last FROM calls'),
  taskByEdrpou: db.prepare(`SELECT id, manager, data FROM tasks WHERE json_extract(data, '$.edrpou') = ? ORDER BY id LIMIT 1`),
  allTasks: db.prepare('SELECT id, manager, data FROM tasks'),
  allClients: db.prepare('SELECT edrpou, data FROM clients'),
  notes: db.prepare('SELECT id, edrpou, call_id, author, text, rating, at FROM notes WHERE edrpou = ? ORDER BY at DESC'),
  addNote: db.prepare('INSERT INTO notes (edrpou, call_id, author, text, rating, at) VALUES (?,?,?,?,?,?)'),
};
function clientByKey() { const m = new Map(); for (const r of CQ.allClients.all()) { const c = JSON.parse(r.data); for (const k of phoneKeys(c.phone1, c.phone2)) if (!m.has(k)) m.set(k, c); } return m; }
const uaDay = (d) => { const s = kyiv(d); return `${s.slice(8, 10)}.${s.slice(5, 7)}.${s.slice(0, 4)}`; };
// Пропущений вхідний (лише свіжий, не з історії) → задача «передзвонити» в «Задачах менеджерів»
function handleMissed(c, cMap) {
  if (!c.missed || c.handled) return;
  if (Date.now() - Date.parse(c.at.replace(' ', 'T') + '+02:00') > 3 * 864e5) { CQ.done.run(c.id); return; }
  const when = `${c.at.slice(8, 10)}.${c.at.slice(5, 7)} ${c.at.slice(11, 16)}`; const line = `Пропущений дзвінок ${when} з ${c.caller}`;
  const client = cMap.get(c.pkey); let row = client?.edrpou ? CQ.taskByEdrpou.get(String(client.edrpou)) : null;
  if (!row && c.pkey) row = CQ.allTasks.all().find(t => { const x = JSON.parse(t.data); return phoneKeys(x.phone, x.phones).includes(c.pkey); }) || null;
  tx(() => {
    if (row) {
      const t = JSON.parse(row.data); t.callDate = uaDay(new Date()); t.c1 = t.c1 ? `${line}\n${t.c1}` : line;
      taskQ.put.run(row.id, t.manager || '', JSON.stringify(t), now());
    } else {
      const t = client ? { edrpou: String(client.edrpou || ''), company: client.client || '', region: client.region || '', district: client.district || '', lpr: client.contact || '',
        phone: client.phone1 || c.caller, manager: client.manager || c.employee || '', task: 'Передзвонити — пропущений дзвінок', c1: line, callDate: uaDay(new Date()) }
        : { phone: c.caller, manager: c.employee || '', task: 'Новий номер — пропущений дзвінок', c1: line, callDate: uaDay(new Date()) };
      for (const k of Object.keys(t)) if (!t[k]) delete t[k];
      taskQ.put.run(taskQ.max.get().m + 1, t.manager || '', JSON.stringify(t), now());
    }
    CQ.done.run(c.id);
  });
}
function storeCalls(list) {
  let n = 0; const cMap = clientByKey(); const fresh = [];
  for (const raw of list) { const c = normCall(raw); if (!c) continue; CQ.up.run(c.id, c.at, c.dir, c.caller, c.dst, c.pkey, c.employee, c.disposition, c.billsec, c.recording, c.missed, JSON.stringify(raw).slice(0, 4000)); n++; fresh.push(c.id); }
  for (const id of fresh) { const c = CQ.get.get(id); if (c) try { handleMissed(c, cMap); } catch (e) { console.error('Ringostat задача:', e.message); } }
  return n;
}
const RS_FIELDS = 'calldate,caller,dst,disposition,billsec,call_type,employee_fio,recording';
async function rsList(from, to) {
  const headers = { 'Auth-key': RS_KEY, 'Content-Type': 'application/json' }; if (RS_PROJECT) headers['x-project-id'] = RS_PROJECT;
  for (const fields of [`uniqueid,${RS_FIELDS}`, RS_FIELDS]) {
    const u = new URL(process.env.RINGOSTAT_API_URL || 'https://api.ringostat.net/calls/list'); u.search = new URLSearchParams({ export_type: 'json', from, to, fields }).toString();
    const r = await fetch(u, { headers, signal: AbortSignal.timeout(60000) });
    const text = await r.text(); if (!r.ok) { if (fields.startsWith('uniqueid')) continue; throw new Error(`Ringostat ${r.status}: ${text.slice(0, 200)}`); }
    const j = JSON.parse(text || '[]'); return Array.isArray(j) ? j : (j.data || j.calls || j.items || []);
  }
  return [];
}
let rsBusy = false;
async function rsPoll() {
  if (!RS_KEY || rsBusy) return; rsBusy = true;
  try {
    const last = metaGet('rs_last'); const started = new Date();
    const from = kyiv(new Date((last ? Date.parse(last) : Date.now() - 864e5) - 2 * 3600e3));
    const n = storeCalls(await rsList(from, kyiv(started)));
    metaSet('rs_last', started.toISOString()); metaSet('rs_err', ''); if (n) console.log('Ringostat: оновлено дзвінків', n);
  } catch (e) { metaSet('rs_err', `${new Date().toISOString()} ${e.message}`); console.error('Ringostat:', e.message); } finally { rsBusy = false; }
}
// Історія за рік — один раз, тижнями, у фоні (задачі з неї не створюються)
async function rsBackfill() {
  if (!RS_KEY) return; const end = Date.now() - 365 * 864e5;
  let cursor = Date.parse(metaGet('rs_back') || new Date().toISOString());
  while (cursor > end) {
    const from = Math.max(end, cursor - 7 * 864e5);
    try { storeCalls(await rsList(kyiv(new Date(from)), kyiv(new Date(cursor)))); cursor = from; metaSet('rs_back', new Date(cursor).toISOString()); }
    catch (e) { metaSet('rs_err', `${new Date().toISOString()} історія: ${e.message}`); await new Promise(r => setTimeout(r, 5 * 60e3)); }
    await new Promise(r => setTimeout(r, 15e3));
  }
}
if (RS_KEY) { setTimeout(rsPoll, 10e3); setInterval(rsPoll, 10 * 60e3); setTimeout(rsBackfill, 60e3); }
// Хто що чує: адміністратор — усе; менеджер — свої дзвінки і пропущені своїх клієнтів
// У Ringostat ім'я буває «Прізвище Ім'я», у CRM — «Ім'я Прізвище»: порівнюємо без урахування порядку слів
const nameKey = (n) => String(n || '').toLowerCase().replace(/['’`ʼ]/g, '').split(/\s+/).filter(Boolean).sort().join(' ');
const crmName = (n) => { if (!n) return ''; const k = nameKey(n); const u = db.prepare('SELECT name FROM users').all().find(x => nameKey(x.name) === k); return u ? u.name : n; };
function canHear(me, admin, c, cMap) { if (admin) return true; if (c.employee) return nameKey(c.employee) === nameKey(me.name); return nameKey(cMap.get(c.pkey)?.manager) === nameKey(me.name); }
const callOut = (c, cMap) => ({ id: c.id, at: c.at, dir: c.dir, number: c.dir === 'out' ? c.dst : c.caller, line: c.dir === 'out' ? c.caller : c.dst, employee: c.employee || '',
  disposition: c.disposition || '', billsec: c.billsec || 0, missed: !!c.missed, rec: !!c.recording, client: cMap.get(c.pkey)?.edrpou || '' });
async function readBody(req) { let raw = ''; for await (const ch of req) { raw += ch; if (raw.length > 1e6) throw new Error('too large'); }
  const t = String(req.headers['content-type'] || ''); if (t.includes('json')) return JSON.parse(raw || '{}'); return Object.fromEntries(new URLSearchParams(raw)); }
async function ringostatRoutes(req, res, p, url, me, admin) {
  if (p === '/api/ringostat/status' && req.method === 'GET') {
    if (!admin) return send(res, 403, { error: 'Лише для адміністратора' }); const s = CQ.count.get();
    return send(res, 200, { configured: !!RS_KEY, calls: s.n, lastCall: s.last, lastSync: metaGet('rs_last'), history: metaGet('rs_back'), error: metaGet('rs_err') || '', hook: `/api/ringostat/hook?token=${RS_TOKEN}` });
  }
  if (p === '/api/calls' && req.method === 'GET') {
    const cMap = clientByKey(); const code = String(url.searchParams.get('edrpou') || '').trim(); const keys = new Set(phoneKeys(url.searchParams.get('phones')));
    if (code) { for (const r of CQ.allClients.all()) { if (String(r.edrpou).trim() !== code) continue; const c = JSON.parse(r.data); phoneKeys(c.phone1, c.phone2).forEach(k => keys.add(k)); }
      for (const t of CQ.allTasks.all()) { const x = JSON.parse(t.data); if (String(x.edrpou || '').trim() === code) phoneKeys(x.phone, x.phones).forEach(k => keys.add(k)); } }
    if (!keys.size) return send(res, 200, { calls: [], configured: !!RS_KEY });
    const ks = [...keys].slice(0, 40); const rows = db.prepare(`SELECT * FROM calls WHERE pkey IN (${ks.map(() => '?').join(',')}) ORDER BY at DESC LIMIT 1000`).all(...ks);
    return send(res, 200, { calls: rows.filter(c => canHear(me, admin, c, cMap)).map(c => callOut(c, cMap)), configured: !!RS_KEY });
  }
  const m = /^\/api\/calls\/([^/]+)\/audio$/.exec(p);
  if (m && req.method === 'GET') {
    const c = CQ.get.get(decodeURIComponent(m[1])); if (!c || !c.recording) return send(res, 404, { error: 'Запису немає' });
    if (!canHear(me, admin, c, clientByKey())) return send(res, 403, { error: 'Це дзвінок іншого менеджера' });
    const up = await fetch(c.recording, { headers: req.headers.range ? { Range: req.headers.range } : {}, redirect: 'follow', signal: AbortSignal.timeout(120000) });
    if (!up.ok && up.status !== 206) return send(res, 502, { error: 'Ringostat не віддав запис (посилання зберігаються близько року)' });
    const h = { 'Content-Type': up.headers.get('content-type') || 'audio/mpeg', 'Cache-Control': 'private, max-age=3600', 'Accept-Ranges': 'bytes', 'X-Content-Type-Options': 'nosniff' };
    for (const k of ['content-length', 'content-range']) { const v = up.headers.get(k); if (v) h[k] = v; }
    if (url.searchParams.get('download')) h['Content-Disposition'] = `attachment; filename="call-${c.at.slice(0, 10)}.mp3"`;
    res.writeHead(up.status, h); for await (const chunk of up.body) res.write(chunk); return res.end();
  }
  if (p === '/api/notes') {
    if (req.method === 'GET') { const code = String(url.searchParams.get('edrpou') || '').trim(); return send(res, 200, { notes: code ? CQ.notes.all(code) : [] }); }
    if (req.method === 'POST') { const b = await readJson(req); const code = String(b.edrpou || '').trim(); const text = String(b.text || '').trim().slice(0, 4000);
      const rating = Number.isInteger(b.rating) && b.rating >= 1 && b.rating <= 5 ? b.rating : null;
      if (!code || (!text && !rating)) return send(res, 400, { error: 'Порожній коментар' });
      CQ.addNote.run(code, String(b.call_id || '').slice(0, 200) || null, me.name, text, rating, now()); return send(res, 200, { notes: CQ.notes.all(code) }); }
  }
  return null;
}

// ---------- Обговорення заявок (чат біля кожної заявки) ----------
db.exec(`CREATE TABLE IF NOT EXISTS chats (req_id INTEGER PRIMARY KEY, data TEXT NOT NULL, updated_at TEXT NOT NULL)`);
const CHQ = {
  get: db.prepare('SELECT data FROM chats WHERE req_id = ?'),
  put: db.prepare('INSERT OR REPLACE INTO chats (req_id, data, updated_at) VALUES (?, ?, ?)'),
  all: db.prepare('SELECT req_id, data FROM chats'),
};
const chId = () => Date.now().toString(36) + randomBytes(3).toString('hex');
const clip = (v, n) => String(v ?? '').slice(0, n);
function chatOf(id) {
  const row = CHQ.get.get(id); if (row) return JSON.parse(row.data);
  const r = db.prepare('SELECT data FROM requests WHERE id = ?').get(id); const req = r ? JSON.parse(r.data) : null;
  let mgr = req?.manager || ''; if (!mgr && req?.edrpou) { const cr = db.prepare('SELECT data FROM clients WHERE edrpou = ?').get(String(req.edrpou).trim()); if (cr) mgr = JSON.parse(cr.data).manager || ''; } return { members: mgr ? [mgr] : [], msgs: [], read: {}, pinned: null };
}
function chatAct(id, b, who) {
  const c = chatOf(id); const t = Date.now(); c.read = c.read || {}; c.members = c.members || []; c.msgs = c.msgs || [];
  const sys = (text) => c.msgs.push({ id: chId(), t, sys: text });
  if (b.type === 'msg') {
    const text = clip(b.text, 4000).trim(); const files = (Array.isArray(b.files) ? b.files : []).slice(0, 10).map(f => {
      if (typeof f === 'string') return clip(f, 200);
      const file = { name: clip(f?.name, 160), url: clip(f?.url, 240), type: clip(f?.type, 80), size: Math.max(0, Number(f?.size) || 0) };
      return file.name && /^\/api\/chat\/files\/[a-z0-9.-]+$/i.test(file.url) && /^image\/(png|jpeg|webp|gif)$/i.test(file.type) ? file : null;
    }).filter(Boolean);
    if (!text && !files.length) return { error: 'Порожнє повідомлення' };
    if (!c.members.includes(who)) c.members.push(who);
    c.msgs.push({ id: chId(), t, who, text, files, reply: b.reply ? clip(b.reply, 40) : null });
    for (const n of (Array.isArray(b.mentions) ? b.mentions : []).slice(0, 10).map(x => clip(x, 80))) if (n && text.includes('@' + n) && !c.members.includes(n)) { c.members.push(n); sys(`${n} доданий(а) до обговорення через згадку`); }
    c.read[who] = t;
  } else if (b.type === 'join') { if (!c.members.includes(who)) { c.members.push(who); sys(`${who} приєднався(лась) до обговорення`); } }
  else if (b.type === 'add') { const n = clip(b.name, 80).trim(); if (n && !c.members.includes(n)) { c.members.push(n); sys(`${who} додав(ла) ${n}`); } }
  else if (b.type === 'remove') { const n = clip(b.name, 80); if (c.members.includes(n)) { c.members = c.members.filter(x => x !== n); sys(`${who} прибрав(ла) ${n}`); } }
  else if (b.type === 'react') { const m = c.msgs.find(x => x.id === b.msg); const e = clip(b.emoji, 8); if (m && e) { m.reacts = m.reacts || {}; const a = m.reacts[e] = m.reacts[e] || []; const i = a.indexOf(who); i >= 0 ? a.splice(i, 1) : a.push(who); } }
  else if (b.type === 'pin') { const was = c.pinned === b.msg; if (was || c.msgs.some(x => x.id === b.msg)) { c.pinned = was ? null : clip(b.msg, 40); sys(`${who} ${was ? 'відкріпив(ла)' : 'закріпив(ла)'} повідомлення`); } }
  else if (b.type === 'read') c.read[who] = t;
  else return { error: 'Невідома дія' };
  if (c.msgs.length > 3000) c.msgs = c.msgs.slice(-3000);
  CHQ.put.run(id, JSON.stringify(c), now()); return { chat: c };
}
async function chatRoutes(req, res, p, url, me) {
  if (p === '/api/chat/upload' && req.method === 'POST') {
    const b = await readJson(req), id = Number(b.req); if (!Number.isInteger(id) || !db.prepare('SELECT 1 FROM requests WHERE id = ?').get(id)) return send(res, 404, { error: 'Немає такої заявки' });
    const m = /^data:(image\/(png|jpeg|webp|gif));base64,([a-z0-9+/=]+)$/i.exec(String(b.data || '')); if (!m) return send(res, 400, { error: 'Дозволені PNG, JPG, WebP або GIF' });
    const data = Buffer.from(m[3], 'base64'); if (!data.length || data.length > 3 * 1024 * 1024) return send(res, 413, { error: 'Зображення має бути до 3 МБ' });
    const ext = m[2].toLowerCase() === 'jpeg' ? 'jpg' : m[2].toLowerCase(), name = `${id}-${Date.now().toString(36)}-${randomBytes(4).toString('hex')}.${ext}`;
    await writeFile(join(CHAT_FILES_DIR, name), data); return send(res, 200, { file: { name: clip(b.name || `screenshot.${ext}`, 160), url: `/api/chat/files/${name}`, type: m[1].toLowerCase(), size: data.length } });
  }
  const fm = /^\/api\/chat\/files\/([a-z0-9.-]+)$/i.exec(p);
  if (fm && req.method === 'GET') { const ext = fm[1].split('.').pop().toLowerCase(), types = { png: 'image/png', jpg: 'image/jpeg', webp: 'image/webp', gif: 'image/gif' }; if (!types[ext]) return send(res, 404, { error: 'Файл не знайдено' });
    try { const data = await readFile(join(CHAT_FILES_DIR, fm[1])); res.writeHead(200, { 'Content-Type': types[ext], 'Content-Length': data.length, 'Cache-Control': 'private, max-age=31536000, immutable', 'X-Content-Type-Options': 'nosniff' }); return res.end(data); } catch { return send(res, 404, { error: 'Файл не знайдено' }); } }
  if (p === '/api/chat/all' && req.method === 'GET') { const out = {}; for (const r of CHQ.all.all()) { const c = JSON.parse(r.data); if ((c.members || []).includes(me.name)) out[r.req_id] = c; } return send(res, 200, { chats: out }); }
  if (p === '/api/chat') {
    if (req.method === 'GET') { const id = Number(url.searchParams.get('req')); if (!Number.isInteger(id)) return send(res, 400, { error: 'req' }); return send(res, 200, { chat: chatOf(id) }); }
    if (req.method === 'POST') { const b = await readJson(req); const id = Number(b.req); if (!Number.isInteger(id) || !db.prepare('SELECT 1 FROM requests WHERE id = ?').get(id)) return send(res, 404, { error: 'Немає такої заявки' });
      const r = chatAct(id, b, me.name); return r.error ? send(res, 400, r) : send(res, 200, r); }
  }
  return null;
}

createServer(async (req, res) => {
  try {
    const url = new URL(req.url, 'http://localhost');
    const p = url.pathname;
    if (p.startsWith('/api/') && ['POST', 'PUT', 'PATCH', 'DELETE'].includes(req.method) && !trustedBrowserRequest(req)) return send(res, 403, { error: 'Blocked cross-site request' });
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
      tries.delete(ipOf(req)); startSession(req, res, u.login); return send(res, 200, { user: publicUser(u) });
    }
    if (p === '/api/logout' && req.method === 'POST') { const t = cookies(req).frendt_sid; if (t) U.sDel.run(t); res.setHeader('Set-Cookie', 'frendt_sid=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0'); return send(res, 200, { ok: true }); }
    // Ringostat сам надсилає сюди кожен дзвінок (вебхук); захист — секретний token в адресі
    if (p === '/api/ringostat/hook' && req.method === 'POST') {
      if (url.searchParams.get('token') !== RS_TOKEN) return send(res, 403, { error: 'token' });
      const b = await readBody(req); const list = Array.isArray(b) ? b : (Array.isArray(b.calls) ? b.calls : [b]);
      return send(res, 200, { ok: true, stored: storeCalls(list) });
    }
    if (p.startsWith('/api/')) {
      const me = whoami(req); if (!me) return send(res, 401, { error: 'auth' });
      const admin = me.role === 'admin';
      { const handled = await ringostatRoutes(req, res, p, url, me, admin); if (handled !== null) return; }
      { const handled = await chatRoutes(req, res, p, url, me); if (handled !== null) return; }
      if (p === '/api/leads' && req.method === 'GET') {
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'private, max-age=300', 'X-Content-Type-Options': 'nosniff' });
        return res.end(getLeadsJson());
      }
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
      if (p === '/api/tasks') {
        if (req.method === 'GET') return send(res, 200, { tasks: taskRows(admin ? taskQ.all.all() : taskQ.mine.all(me.name)) });
        const body = await readJson(req); const task = cleanTask(body); if (!admin) task.manager = me.name;
        const duplicateError = (id, oldCode = '') => {
          if (!task.edrpou || task.edrpou === oldCode) return null;
          const duplicate = taskQ.duplicate.get(task.edrpou, id); if (!duplicate) return null;
          const saved = JSON.parse(duplicate.data);
          return `Задача з ЄДРПОУ ${task.edrpou} вже є — «${saved.company || ''}» (${duplicate.manager || 'без менеджера'}). Однаковий ЄДРПОУ зберегти не можна.`;
        };
        if (req.method === 'POST') {
          const error = duplicateError(0); if (error) return send(res, 409, { error });
          const id = taskQ.max.get().m + 1; taskQ.put.run(id, task.manager || '', JSON.stringify(task), now());
          return send(res, 200, { task: { id, ...task } });
        }
        if (req.method === 'PUT') {
          const id = Number(body.id); const old = taskQ.get.get(id); if (!old) return send(res, 404, { error: 'Немає такої задачі' });
          if (!admin && old.manager !== me.name) return send(res, 403, { error: 'Це задача іншого менеджера' });
          const error = duplicateError(id, JSON.parse(old.data).edrpou || ''); if (error) return send(res, 409, { error });
          taskQ.put.run(id, task.manager || '', JSON.stringify(task), now());
          return send(res, 200, { task: { id, ...task } });
        }
      }
      if (p === '/api/data' && req.method === 'GET') {
        const knownRev = Number(url.searchParams.get('rev'));
        const currentRev = rev();
        if (Number.isFinite(knownRev) && knownRev === currentRev) return send(res, 200, { rev: currentRev, unchanged: true });
        return send(res, 200, { ...state(), me: publicUser(me) });
      }
      if ((p === '/api/data' || p === '/api/sync') && (req.method === 'PUT' || req.method === 'POST')) { const b = await readJson(req); if (!admin) { delete b.deleted; delete b.dicts; } sync(b, me.login); return send(res, 200, state()); }
      if (p === '/api/requests' && req.method === 'POST') { const rec = createRequest({ ...(await readJson(req)), author: me.name }, me.login); return send(res, 200, { request: rec, rev: rev() }); }
      return send(res, 404, { error: 'Not found' });
    }
    if (p === '/' || p === '/index.html') {
      const nonce = randomBytes(18).toString('base64url');
      const csp = `default-src 'self'; base-uri 'none'; object-src 'none'; frame-ancestors 'self'; form-action 'self'; script-src 'self' 'nonce-${nonce}' https://docs.google.com; style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; font-src 'self' https://fonts.gstatic.com data:; img-src 'self' data:; connect-src 'self' https://docs.google.com; upgrade-insecure-requests`;
      return send(res, 200, getHtml(nonce), 'text/html; charset=utf-8', { 'Content-Security-Policy': csp });
    }
    return send(res, 404, 'Not found', 'text/plain');
  } catch (e) {
    if (e.status === 409) return send(res, 409, { error: e.message, conflicts: e.conflicts, ...state() });
    console.error(e); return send(res, 400, { error: e.message });
  }
}).listen(PORT, HOST, () => {
  console.log(`FRENDT CRM OK: http://${HOST === '0.0.0.0' ? '127.0.0.1' : HOST}:${PORT}`);
  console.log(`База даних: ${DB_FILE}`);
  console.log(`Резервні копії: ${BACKUP_DIR}`);
});
