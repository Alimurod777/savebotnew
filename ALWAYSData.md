# Alwaysdata service

Loyiha Python 3.11 va `venv/` bilan ishlaydi. SSH orqali loyiha papkasida:

```sh
PYTHON_VERSION=3.11 python -m venv venv
venv/bin/python -m pip install --upgrade pip
venv/bin/python -m pip install -r requirements.txt
```

`.env` mavjud bo'lmasa, `cp .env.example .env` bajaring. Faylga o'zingizning
`API_ID`, `API_HASH`, `BOT_TOKEN`, `OWNER_ID` qiymatlarini kiriting va
`chmod 600 .env` bajaring. MongoDB kerak bo'lsa `DB_URI` ni ham kiriting.
Service environment qiymatlari `.env` dan ustun turadi.

## Panelda servis yaratish

Alwaysdata **Advanced > Services > Add a service**:

| Maydon | Qiymat (joriy akkaunt uchun) |
| --- | --- |
| Name | `savebotnew-wzgram` |
| Command | `/home/telegramdata4/savebotnew-wzgram/start_alwaysdata.sh` |
| Working directory (mavjud bo'lsa) | `/home/telegramdata4/savebotnew-wzgram` |
| Environment | `PYTHON_VERSION=3.11` |

Boshqa akkaunt/papkada Command yo'lini moslang. Launcher o'zi loyiha papkasiga
o'tadi va `venv/bin/python` ishlatadi; `source activate` talab qilinmaydi.
`nohup`, `screen`, `&` yoki alohida gunicorn kerak emas. Telegram bot uchun
kiruvchi HTTP port ochish talab qilinmaydi. Oldingi bot nusxasini to'xtatib,
keyin servisni yoqing.

Alwaysdata servislarni foreground rejimida boshqaradi, to'xtagan processni
qayta ishga tushiradi. Restartdagi `SIGHUP` botning mavjud shutdown handleriga
ulanadi. Service loglari: `/home/telegramdata4/admin/logs/services/` yoki
paneldagi servis **Logs** tugmasi. Ketma-ket startup xatolari servisning
avtomatik o'chirilishiga olib kelishi mumkin. Services pullik rejalarda mavjud.

## Tekshirish va yangilash

```sh
venv/bin/python -m pip check
venv/bin/python verify_migration.py
```

Media uchun `mediainfo`, `7z`, `ffmpeg`, `ffprobe` kerak. Dastlabki ikkitasi
joriy hostda bor. `static-ffmpeg` requirements ichida bor; birinchi video
ishlovida FFmpeg binarylarini yuklaydi. Oldindan tayyorlash uchun:

```sh
venv/bin/python -c 'import static_ffmpeg; static_ffmpeg.add_paths()'
```

Kod yangilangach dependencylar o'zgargan bo'lsa `pip install -r requirements.txt`
ni qaytaring va paneldan servisni restart qiling. `venv/` boshqa serverdan
ko'chirilmaydi: yangi serverda qayta yaratiladi. Runtime holat `data/`,
`local_db/`, `sessions/` ichida saqlanadi; `.env` bilan birga private backup qiling.

Manbalar: [Alwaysdata Services](https://help.alwaysdata.com/en/docs/web-hosting/services/),
[Python configuration](https://help.alwaysdata.com/en/docs/web-hosting/languages/python/configuration/).
