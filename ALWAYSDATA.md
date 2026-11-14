# Alwaysdata service (main)

Server papkasi: `/home/small/savebotnew`.

```sh
python3 -m venv venv
venv/bin/python -m pip install -r requirements.txt
cp .env.example .env
chmod 600 .env
```

Mavjud `.env` faylini almashtirmang. `.env` ichida `API_ID`, `API_HASH`,
`BOT_TOKEN`, `OWNER_ID` qiymatlarini kiriting. `DB_URI` MongoDB backup uchun.

Alwaysdata panel → **Advanced → Services → Add a service**:

- Name: `savebotnew-main`
- Command: `/bin/sh /home/small/savebotnew/start-service.sh`
- Working directory (maydon mavjud bo'lsa): `/home/small/savebotnew`
- Monitoring command: bo'sh

Port kerak emas: bot Telegramga o'zi ulanadi. Service foregroundda ishlaydi;
alwaysdata to'xtagan processni avtomatik qayta ishga tushiradi. Logs panelda
yoki `/home/small/admin/logs/services/` ichida. O'zgarishdan keyin
service uchun paneldagi Restart tugmasidan foydalaning.

Tekshiruv:

```sh
venv/bin/python -m pip check
venv/bin/python verify_migration.py
```

Manba: https://help.alwaysdata.com/en/docs/web-hosting/services/
