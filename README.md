# FRENDT CRM — сервер із базою даних

Без зовнішніх залежностей: потрібен лише **Node.js 22.5 або новіший** (база SQLite вбудована в Node.js).

## Запуск
```
npm start
```
або
```
node --experimental-sqlite --no-warnings server.mjs
```
Відкрити: http://127.0.0.1:4173

## Налаштування (змінні середовища)
| Змінна | Що означає | За замовчуванням |
|---|---|---|
| `PORT` | порт | `4173` |
| `HOST` | адреса; на сервері в інтернеті — `0.0.0.0` | `127.0.0.1` |
| `DATA_DIR` | папка з базою `frendt-crm.db` і резервними копіями | `~/FRENDT-CRM-data` |
| `CRM_USERNAME` | логін для входу до CRM | порожньо (захист вимкнено локально); Render: `VolodymyrHirenko` |
| `CRM_PASSWORD` | пароль для входу до CRM | порожньо (захист вимкнено локально) |

## Що де зберігається
- `DATA_DIR/frendt-crm.db` — база: заявки, клієнти, доповнення довідників, журнал змін (`history`).
- `DATA_DIR/backups/` — резервна копія при кожному запуску та щодня, зберігаються 30 останніх.
- Якщо в `DATA_DIR` лежить старий `data.json`, а база порожня — дані імпортуються автоматично.

## VPS (Ubuntu)
```
# Node.js 22
curl -fsSL https://deb.nodesource.com/setup_22.x | sudo -E bash - && sudo apt install -y nodejs
# запуск як служба
sudo npm i -g pm2
HOST=0.0.0.0 PORT=4173 pm2 start "npm start" --name frendt-crm && pm2 save && pm2 startup
```
Далі — домен і HTTPS через nginx/Caddy (проксі на порт 4173).

## Render
- Build command: *(порожньо)*, Start command: `npm start`
- Environment: `HOST=0.0.0.0`, `DATA_DIR=/var/data`
- Обов'язково задайте `CRM_PASSWORD`; Blueprint вже запитує його під час створення сервісу.
- Обов'язково підключити **Persistent Disk** з mount path `/var/data` — без нього база зникатиме при кожному перезапуску.

## Важливо перед роботою з реальними даними
Зараз вхід без пароля: хто має посилання — бачить усі дані. Перед відкриттям у інтернет потрібно додати вхід для працівників.
