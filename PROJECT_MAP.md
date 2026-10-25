# PROJECT_MAP.md — Tezkor loyiha xaritasi

> Bu fayl AI modellarga (GLM, Claude va h.k.) loyihani 1 daqiqada tushunish uchun.
> **QATIY QOIDALAR `AGENTS.md`da** — har qanday o'zgartirishdan OLDIN uni o'qi.
> Bug tarixi: `data/BUGS.md` | Prompt tarixi: `data/PROMPTS.md`

---

## 1. Loyiha nima?

Telegram **save bot**: foydalanuvchi `https://t.me/...` havola yuboradi → bot postni
(media, album, topic) yuklab oladi va foydalanuvchiga qayta yuboradi.

- **Stack:** Python 3.10–3.13, Pyrogram (**Pyrofork** fork), MongoDB (ixtiyoriy, lokal fallback bor)
- **Ishga tushirish:** `python bot.py` (yoki Colab/Kaggle: `colab_runner.ipynb`, `kaggle_runner.ipynb`)
- **Config:** `.env` fayl yoki environment variables (`config.py` o'qiydi):
  `BOT_TOKEN, API_ID, API_HASH, OWNER_ID, DB_URI` (+ ixtiyoriy: `BOT_SESSION, RELAY_CHANNEL_ID`)

---

## 2. Kirish nuqtasi (start oqimi)

```
bot.py                 → BotRunner: Bot() yaradi, xatoda restart qiladi
 └─ main.py: Bot.start()
     ├─ bot client start, handlers ro'yxatdan o'tkazish
     ├─ InactivityManager
     ├─ relay_uploader.init()          ← premium relay (non-fatal)
     ├─ session_manager.init()         ← core/session_manager (non-fatal)
     └─ priority_queue.start()         ← governance workers (non-fatal)
```

Web dashboard: `app.py` (ixtiyoriy, healthcheck uchun).

---

## 3. Papkalar xaritasi

| Papka | Nima bor |
|-------|----------|
| `TechVJ/` | Asosiy biznes-logika: handler'lar, download+send, komandalar |
| `core/` | Yordamchi qatlam: xavfsiz yuborish, entity, premium, governance, downloader |
| `core/session_manager/` | Yangi sessiya tizimi (USER_OWNED/DEDICATED/BORROWABLE/GLOBAL) |
| `core/downloader/` | Download dvigateli (engine, worker, resume, progress) |
| `database/` | DB qatlami: `async_db.py`, `state_manager.py`, `sync_manager.py` |
| `data/` | Runtime fayllar: `BUGS.md`, `PROMPTS.md`, `roles/`, premium JSON'lar |
| `WOODcraft/` | Colab notebook rasmlari (not one code) |

---

## 4. Asosiy fayllar

### Eng muhim fayl

| Fayl | Qatorlar | Nima bor |
|------|----------|----------|
| `TechVJ/save.py` | ~8900 | **Botning yuragi.** `save()` handler (`save.py:4442`) barcha link'larni qabul qiladi: URL parse, governance middleware, album/topic ishlov, `download_and_send_media()` (download + premium sessiya tanlash + upload + delivery). Caption/entity helperlar ham shu yerda. |

### TechVJ/ — komandalar va logika

| Fayl | Nima bor |
|------|----------|
| `premium_commands.py` | `/premium`, `/setpremium`, `/uploadsetting`, `/split_media` komandalar |
| `session_manager_commands.py` | `/session add/list/remove/...` — yangi sessiya tizimi |
| `owner_commands.py` | `/ban /unban /setrole /stats /maintenance ...` (governance) |
| `userdata_command.py` | `/userdata` komanda (yaqin qo'shilgan) |
| `session_handler.py` | `create_user_session()` — user sessiya context manager |
| `album_collector_v2.py` | Album (media group) yig'ish |
| `file_splitter.py` | Katta fayllarni bo'lish (split fallback) |
| `task_manager.py`, `user_queue.py` | Vazifa/queue boshqaruvi |
| `progress_handler.py`, `async_progress.py` | Progress bar xabarlari |
| `inactivity_manager.py` | Noodatiy sessiyalarni tozalash |

### core/ — asosiy modullar

| Fayl | Nima bor |
|------|----------|
| `safe_send.py` | `core_safe_send`, `core_safe_reply`, `safe_edit_message` — xavfsiz yuborish wrapper'lari |
| `entity_validator.py`, `entity_builder.py`, `entity_splitter.py` | Telegram entity'lari (UTF-16 offset/length) |
| `caption_splitter.py` | `split_caption(caption, entities, is_premium)` → primary + overflow |
| `message_utils.py` | `MAX_MESSAGE_LENGTH=4096`, `MAX_CAPTION_LENGTH=1024`, `split_message` |
| `text_renderer.py`, `smart_renderer.py` | Entity → render, markdown segmentlar |
| `premium_relay.py` | `PremiumSessionPool` + `RelayUploader` (singleton `relay_uploader`) |
| `premium_logic.py` | `check_user_premium()`, `should_split()` |
| `premium_session_store.py` | Sessiya/relay JSON saqlash (`data/premium/`) |
| `user_upload_worker.py` | `UserUploadWorker` — user sessiya orqali upload (bot chat prep, flood rates) |
| `topic_extractor.py` | Forum topic'ni bulk fetch (5 so'rov / 1000 xabar) |
| `role_manager.py`, `rate_limiter.py`, `permission_guard.py`, `priority_queue.py` | Governance qatlami |
| `pyrofork_compat.py` | `get_reply_to()` monkey-patch (BUG-010) — O'CHIRMA |
| `reply_compat.py`, `copy_utils.py`, `safe_copy.py` | copy_message/reply yordamchilari |
| `restricted_channel_guard.py`, `force_subscription.py` | Kanal cheklovlari |

---

## 5. So'rov oqimi (link kelganda nima bo'ladi)

```
Foydalanuvchi t.me link yubordi (bot DM yoki group)
  → save.py: save() handler (4442)
  → Governance middleware: maintenance → ban → rate limit → permission guard
  → parse_telegram_url() → post_id/range/comma/topic
  → Album? → album_collector_v2 | Topic? → topic_extractor | Single post
  → download_and_send_media():
      1. Early premium check (check_user_premium bir marta)
      2. Download (core/downloader engine)
      3. Sessiya tanlash:
         SessionManager (USER_OWNED > DEDICATED > BORROWABLE > GLOBAL)
         → legacy pool (relay_uploader.pool) → user o'z sessiyasi → export fallback
      4. Upload: UserUploadWorker → target = bot chat → pool bo'lsa copy_message(user)
      5. Media = user session orqali, TEXT = bot orqali (QATIY QOIDA, AGENTS.md §1)
```

---

## 6. "Qayerda?" — tezkor topshiriq xaritasi

| Vazifa | Fayl |
|--------|------|
| Yangi bot komanda qo'shish | `TechVJ/` ga yangi fayl + `save.py`/`main.py` handler ro'yxati |
| Caption/format muammosi | `core/caption_splitter.py`, `core/entity_validator.py` |
| Text yuborish xatosi | `core/safe_send.py` |
| Premium sessiya/qo'shish | `core/premium_relay.py`, `TechVJ/premium_commands.py` |
| Upload tezligi / FloodWait | `core/user_upload_worker.py` (TEXT_MIN_GAP, MEDIA_MIN_GAP, BATCH_*) |
| Ro'l/limit o'zgartirish | `core/rate_limiter.py`, `core/role_manager.py`, `core/priority_queue.py` |
| >2GB split siyosati | `core/premium_logic.py: should_split()` + `save.py` >2GB blok |
| Topic fetch muammosi | `core/topic_extractor.py` |
| Comment tahlili (`/comment`, `/comments`) | `TechVJ/save.py: comment_analyzer_command` (~4226) — ikkala alias bir handler |
| DB sxemasi | `database/async_db.py` |
| Yangi bug topildi | `data/BUGS.md` ga BUG-NNN yozib qo'shish |

---

## 7. Data fayllari (runtime)

| Fayl | Maqsad |
|------|--------|
| `data/BUGS.md` | Bug tracker — yangi sessiyada O'QI |
| `data/PROMPTS.md` | Prompt tarixi — yangi sessiyada O'QI, oxirida yangila |
| `data/premium/sessions.json` | Legacy premium pool sessiyalari |
| `data/premium/relay.json` | Relay kanal config |
| `data/session_manager/sessions.json` | Yangi SessionManager sessiyalari |
| `data/premium/user_prefs/<id>.txt` | Har user upload sozlamasi |
| `data/roles/<id>.txt` | Governance ro'llari |
| `local_db/bot_storage.db` | Lokal SQLite (MongoDB bo'lmasa fallback) |

---

## 8. Oltin qoidalar (qisqacha)

1. **Media → user session (`acc`/worker), text → bot (`client`)** — hech qachon aralashtirma.
2. `str(msg.caption)` / `str(msg.text)` — Pyrogram `Str` emas, oddiy `str` ishlat.
3. Overflow caption va barcha status text — **bot orqali** yuboriladi.
4. `check_user_premium()` request boshiga **bir marta** chaqiriladi.
5. Entity muammosi → `prepare_entities_for_send()`, hech qachon `parse_mode=HTML/Markdown` qo'shma.
6. `pyrofork_compat.py` patch, `relay_uploader.init()`, `copy_message` delivery — o'chirib bo'lmaydi.
7. Yangi kod uchun sessiya: faqat `create_user_session()` (`TechVJ/session_handler.py`).
8. **Yangi komanda qo'shsangiz** — nomini `save.py` catch-all ekskluziv ro'yxatiga (~4428) qo'shish SHART, aks holda komanda jim o'ladi (BUG-055).
9. `database.db`ni modul darajasida import QILMANG — bo'sh DB_URI butun pluginni o'ldiradi (BUG-056); lazy import ishlating.
10. Batafsil qoidalar va sabablari: **`AGENTS.md`** (§1–§21).
