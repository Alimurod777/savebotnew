# Alwaysdata'da ishga tushirish

Root huquqi kerak emas: hammasi loyiha ichidagi `venv` papkasiga o'rnatiladi.
Quyida `ACCOUNT` — alwaysdata akkaunt nomingiz (masalan `small`).

## 1. Tayyorgarlik (panelda)

- **Environment → Python**: 3.10 yoki yangiroq versiyani tanlang (3.12 tavsiya).
- **Remote access → SSH**: foydalanuvchida "Enable password login" yoqilgan bo'lsin.

## 2. O'rnatish (SSH orqali)

```sh
ssh ACCOUNT@ssh-ACCOUNT.alwaysdata.net
cd ~
git clone https://github.com/Alimurod777/savebotnew.git
cd savebotnew
sh setup.sh
```

`setup.sh` hammasini o'zi qiladi: `venv` yaratadi, `requirements.txt` dagi
kutubxonalarni o'rnatadi, `.env` ni namunadan yaratadi va o'rnatishni tekshiradi.
Qayta ishga tushirish xavfsiz. Oxirida panelga kiritiladigan aniq buyruqni chiqaradi.

## 3. Sozlash

```sh
nano .env
```

`API_ID`, `API_HASH` ([my.telegram.org](https://my.telegram.org)), `BOT_TOKEN`
(@BotFather) va `OWNER_ID` (Telegram raqamli ID) ni kiriting. `DB_URI` ixtiyoriy
(MongoDB backup). Saqlash: `Ctrl+O`, `Enter`, chiqish: `Ctrl+X`.

Sinov (to'xtatish — `Ctrl+C`):

```sh
sh start-service.sh
```

Bot Telegramda `/start` ga javob bersa, hammasi to'g'ri. Doimiy ishlatishdan oldin
sinovni `Ctrl+C` bilan to'xtating.

## 4. Doimiy ishlatish

**Pullik tarif — Advanced → Services → Add a service:**

- Name: `savebotnew`
- Command: `/bin/sh /home/ACCOUNT/savebotnew/start-service.sh`
- Working directory (maydon bo'lsa): `/home/ACCOUNT/savebotnew`
- Monitoring command: bo'sh

Service to'xtab qolsa alwaysdata uni o'zi qayta ishga tushiradi.
Log: `/home/ACCOUNT/admin/logs/services/`.

**Bepul tarif (Services yo'q) — Advanced → Scheduled tasks → Add a scheduled task:**

- Type: Command
- Value: `/bin/sh /home/ACCOUNT/savebotnew/start-service.sh`
- Frequency: har 5 daqiqada (`Every 5 minutes`)

Bot ishlab turgan paytda keyingi ishga tushirishlar o'tkazib yuboriladi; bot
to'xtab qolsa, 5 daqiqa ichida qayta ko'tariladi. Log: `/home/ACCOUNT/admin/logs/jobs/`.
Bepul tarifda RAM 256 MB — katta fayllarda bot xotira yetishmasligidan
to'xtab, keyin qayta ishga tushishi mumkin.

Port kerak emas: bot Telegramga o'zi ulanadi. Bir bot tokenini bir vaqtda ikki
joyda ishlatmang (`start-service.sh` shu papkada ikkinchi nusxani ishga tushirmaydi).

## 5. Yangilash

```sh
cd ~/savebotnew
git pull
sh setup.sh
```

Keyin panelda service uchun **Restart** ni bosing (scheduled task bo'lsa:
`pkill -f "savebotnew/venv/bin/python"` — 5 daqiqa ichida o'zi qayta ko'tariladi).

## Muammolar

| Xato | Sabab va yechim |
| --- | --- |
| `Could not open requirements file: No such file or directory` | Buyruq loyiha papkasidan tashqarida ishlatilgan. `cd ~/savebotnew` qiling yoki shunchaki `sh ~/savebotnew/setup.sh` — u papkani o'zi topadi. |
| `ls ~/savebotnew` da `requirements.txt` yo'q | Loyiha to'liq yuklanmagan. `rm -rf ~/savebotnew` va 2-qadamdagi `git clone` ni qaytaring (mavjud `.env`, `sessions/`, `local_db/`, `data/` bo'lsa avval nusxalab oling). |
| `Python 3.10 yoki yangiroq kerak` | Panel → Environment → Python da 3.12 ni tanlang yoki `PYTHON_VERSION=3.12 sh setup.sh`. |
| `venv missing` | `sh setup.sh` hali ishlatilmagan. |
| `Set these in .env ...` | `.env` da ko'rsatilgan qiymatlar bo'sh. |
| Python versiyasi o'zgartirilgach bot ishlamay qoldi | `rm -rf venv && sh setup.sh`. |
| `Disk quota exceeded` | `du -sh ~/*` bilan joyni tekshiring; `rm -rf ~/.cache/pip downloads/*`. O'rnatilgan `venv` ~200 MB, birinchi video bo'lishda ffmpeg yana ~150 MB yuklaydi. |
| `Bot already running` | Bot allaqachon ishlayapti — bu xato emas. |

Mavjud `.env` faylini almashtirmang; `setup.sh` ham unga tegmaydi.

Manbalar: [Services](https://help.alwaysdata.com/en/docs/web-hosting/services/),
[Scheduled tasks](https://help.alwaysdata.com/en/docs/web-hosting/tasks/),
[Python](https://help.alwaysdata.com/en/docs/web-hosting/languages/python/configuration/).
