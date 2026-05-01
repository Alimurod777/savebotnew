# CLAUDE.md — Loyiha qoidalari va arxitekturasi

Bu fayl Claude AI uchun majburiy ko'rsatma. Har qanday o'zgartirish yoki qo'shimcha yozishdan OLDIN bu faylni o'qi va qat'iy amal qil.

---

## 1. SEND ROUTING — ENG MUHIM QOIDA

**HECH QACHON buzma:**

| Tur | Kim yuboradi |
|-----|-------------|
| Barcha media (photo, video, document, audio, album, split chunks, location, venue) | User session (`acc` yoki `_worker`) |
| Barcha text (status, xato xabarlari, overflow caption) | Bot client (`client`) |

**Sababi:** `acc` (user session) orqali `https://t.me/` havolali text yuborilsa, botning o'z save handler'i loop'ga tushib qoladi.

---

## 2. FAYL TUZILMASI

```
main.py                          — Bot class, startup/shutdown
config.py                        — BOT_TOKEN, API_ID, API_HASH, OWNER_ID, RELAY_CHANNEL_ID
TechVJ/save.py                   — Asosiy download+send logikasi (~3800 qator)
TechVJ/premium_commands.py       — Owner va user komandalar (/premium, /uploadsetting, /split_media)
TechVJ/session_handler.py        — create_user_session() context manager
TechVJ/album_collector_v2.py     — Album yig'ish logikasi
TechVJ/file_splitter.py          — Katta fayllarni bo'lish
core/premium_relay.py            — PremiumSessionPool + RelayUploader (singleton: relay_uploader)
core/premium_session_store.py    — Sessiyalar va relay kanalini disk'da saqlash
core/premium_logic.py            — Premium tekshirish, should_split(), system session
core/safe_send.py                — core_safe_send, core_safe_reply, safe_edit_message
core/message_utils.py            — MAX_MESSAGE_LENGTH=4096, MAX_CAPTION_LENGTH=1024, split_caption, split_message
core/entity_validator.py         — prepare_entities_for_send, split_caption_with_entities, utf16_length
core/text_renderer.py            — extract_to_renderer(), TextRenderer, render_chunks()
core/smart_renderer.py           — SmartRenderer, Segment.to_markdown()
core/reply_compat.py             — build_reply_kwargs_from_message()
core/repost_router.py            — load_premium_session_runtime, remove_premium_session_runtime
```

---

## 3. PREMIUM RELAY ARXITEKTURASI

```
acc (user session) yuklab oladi
    ↓
[Relay yo'li]:
  Tizim premium sessiyasi → RelayUploader → relay kanaliga yuklaydi → bot copy_message() → foydalanuvchiga

[To'g'ridan-to'g'ri yo'l]:
  Foydalanuvchining o'z premium sessiyasi → UserUploadWorker → to'g'ridan-to'g'ri yuklaydi

[Relay yo'q]:
  premium_upload() relay'siz chaqiriladi

[Relay muvaffaqiyatsiz]:
  Split fallback (faylni bo'lib yuborish)
```

**`relay_uploader`** — `core/premium_relay.py`dagi singleton. Startup'da `relay_uploader.init()` chaqiriladi.

---

## 4. DATA SAQLASH FAYLLARI

```
data/premium/sessions.json          — PremiumSessionEntry ro'yxati (relay pool uchun)
data/premium/relay.json             — RelayConfig (channel_id, enabled)
data/premium/user_prefs/<id>.txt    — Har foydalanuvchi upload sozlamasi
data/premium/system_session.json    — Yagona tizim sessiyasi (legacy, hali ishlatiladi)
data/BUGS.md                        — Bug tracker: topilgan xatolar, sabablari, yechimlari
data/PROMPTS.md                     — Prompt tarixi: sessiya buyurtmalari va yo'nalish
```

---

## 5. OWNER KOMANDALAR

```
/setpremium <session>       — Tizim Premium sessiyasini o'rnatish (legacy, yagona sessiya)
/removepremium              — Tizim Premium sessiyasini o'chirish
/premiumstatus              — Tizim Premium sessiya holati
/checkpremium <id|@user>    — MTProto orqali Premium tekshirish (faqat owner)

/premium status             — Relay holati (sessiyalar + kanal + runtime pool)
/premium add <session>      — Relay pool'ga sessiya qo'shish (hot-reload, restart kerak emas)
/premium remove <N>         — N-sessiyani olib tashlash
/premium relay <channel_id> — Relay kanalni belgilash
/premium test               — Barcha sessiyalar + bot admin tekshiruvi
/premium on                 — Relay yoqish
/premium off                — Relay o'chirish (sessiyalar saqlanadi)
```

## 6. USER KOMANDALAR

```
/uploadsetting [auto|force_split|no_split]  — Upload bo'lish usulini tanlash
/split_media [on|off|auto]                  — Caption bo'lishni boshqarish
```

---

## 7. STARTUP TARTIBI (main.py)

`Bot.start()` ichida, `InactivityManager`dan keyin:
```python
await relay_uploader.init(...)
```
Bu muvaffaqiyatsiz bo'lsa ham bot ishlaydi (non-fatal).

---

## 8. MAVJUD FUNKSIYALAR — YANGI YOZMA

Quyidagi funksiyalar allaqachon mavjud. Ularni QAYTA YAZMANG, mavjudini ishlat:

### `core/message_utils.py`
- `MAX_MESSAGE_LENGTH = 4096` — message chegarasi
- `MAX_CAPTION_LENGTH = 1024` — caption chegarasi
- `split_caption(text, limit)` → `(caption_part, overflow_or_None)` — `save.py`da `split_caption_tuple` sifatida import qilingan
- `split_message(text)` → list — xabarni 4096 ga bo'lish
- `normalize_poll_to_text(poll)` — poll → text

### `core/safe_send.py`
- `safe_send_message(client, chat_id, text, ...)` — import qilingan `core_safe_send` sifatida
- `safe_reply(message, text, ...)` — import qilingan `core_safe_reply` sifatida
- `safe_edit_message(msg, text, ...)` — import qilingan `safe_edit_message` sifatida
- `entity_safe_send_message(client, chat_id, text, entities, ...)` — entity-aware yuborish

### `core/entity_validator.py`
- `prepare_entities_for_send(text, entities)` — entity'larni tekshirib tayyorlash
- `split_caption_with_entities(caption, entities)` → `(cap_chunk, overflow_chunks)`
- `utf16_length(text)` — Telegram'ning haqiqiy o'lchovi (emoji va surrogate uchun)

### `core/text_renderer.py`
- `extract_to_renderer(text, entities)` → `TextRenderer` — entity'larni render'ga tayyorlash
- `renderer.render_chunks(limit)` → `[(text, entities), ...]` — entity'li chunklar

### `TechVJ/session_handler.py`
- `create_user_session(session_string, user_id, timeout)` — async context manager, avto-disconnect
- **MUHIM:** `create_client_session()` (`save.py` ichidagi) eski usul, hali ishlatilmoqda lekin yangisiga o'tkazilmagan. Yangi kod uchun faqat `create_user_session` ishlatil.

### `TechVJ/premium_commands.py`
- `_user_has_own_premium_session(client, user_id)` → `bool` — foydalanuvchi Premium + sessiyasi borligini tekshirish (ikkisini parallel bajaradi)

---

## 9. SAVE.PY ICHIDAGI MUHIM FUNKSIYALAR

### Caption/Text extraction
```python
get_caption_with_entities(msg) → (str, entities_list | None)
get_text_with_entities(msg)    → (str, entities_list | None)
```
**MUHIM:** Ikkalasi ham `str(msg.caption)` ishlatadi — Pyrogram `Str` subclass emas, oddiy `str`. Bu utf-16-le xatosini oldini oladi.

### Entity-safe yuborish
```python
send_text_entity_safe(client, chat_id, text, entities, reply_to_message_id)
    → Message | None

send_media_entity_safe(client, chat_id, media_type, file_path, caption, caption_entities, ...)
    → (media_msg, overflow_msg)
```

### Caption splitting
```python
split_caption_safe(caption, limit=1024) → (media_caption, overflow | None)
# Bu funksiya char-based (len()), TELEGRAM_CAPTION_LIMIT=1024 ishlatadi
# core/message_utils.split_caption utf16_length ishlatadi (to'g'riroq)
```

### Message splitting
```python
split_message_safe(text, limit=4096) → [chunk1, chunk2, ...]
# Bu funksiya char-based, hyperlink'larni buzmaslikka harakat qiladi
# core/message_utils.split_message utf16_length ishlatadi (to'g'riroq)
```

### URL parsing
```python
parse_telegram_url(url) → (ParsedURL | None, error_msg | None)
# Qo'llab-quvvatlanadigan formatlar:
# https://t.me/c/123456/101           (private, single)
# https://t.me/c/123456/101-110       (private, range)
# https://t.me/c/123456/101,102,103   (private, comma-separated)
# https://t.me/c/123456/5/101         (topic)
# https://t.me/c/123456/101?thread=5  (thread)
# https://t.me/username/101           (public)
# https://t.me/QuizBot?start=XXX      (quizbot)
```

---

## 10. O'ZGARTIRIB BO'LMAYDIGAN NARSALAR

Bu qoidalar oldingi sessiyalarda sinab ko'rilgan va to'g'ri ishlaydi:

1. **`str(msg.caption)` va `str(msg.text)`** — `get_caption_with_entities` va `get_text_with_entities`da. Bu qatorni olib tashlamang, `msg.caption` to'g'ridan-to'g'ri ishlatmang.

2. **Overflow caption bot orqali yuborish** — `send_media_entity_safe`dagi overflow text har doim `client` (bot) orqali yuboriladi, `acc` (user session) orqali EMAS.

3. **Split chunklar user session orqali** — `acc.send_video / acc.send_document` orqali yuboriladi.

4. **`relay_uploader.init()`** — `main.py`da `Bot.start()` ichida chaqiriladi. Bu qatorni olib tashlashga yoki joyini o'zgartirishga ruxsat yo'q.

5. **`_user_has_own_premium_session()`** — `asyncio.gather()` ishlatadi. Ikkala chaqiruv parallel. Ularni ketma-ket qilmang.

---

## 11. QILINMAYDIGAN ISHLAR

- `get_safe_caption()` va `get_safe_text()` funksiyalarini qayta qo'shma — ular o'chirilgan (dead code edi, markdown-based edi, entity-based variantlar mavjud).
- `TELEGRAM_CAPTION_LIMIT` va `TELEGRAM_MESSAGE_LIMIT` konstantalarini o'chirmang — `split_caption_safe` va `split_message_safe` funksiyalari default argument sifatida ishlatadi.
- `extract_hyperlinks()` funksiyasini olib tashlama — hali `save.py` ichida turli joylarda ishlatilishi mumkin.
- `create_client_session()` ni olib tashlama — hali `save.py:3010` va `save.py:3049`da ishlatilmoqda.
- Yangi `parse_mode=Markdown` yoki `parse_mode=HTML` qo'shma — entity-based yondashuv ishlatiladi (`ENTITY_BOUNDS_INVALID` xatosini oldini olish uchun).

---

## 12. MUHIM XAT0LAR VA YECHIMLAR

### `ENTITY_BOUNDS_INVALID`
**Sabab:** Entity offset/length noto'g'ri (emoji, UTF-16 surrogate muammo).
**Yechim:** `core/entity_validator.prepare_entities_for_send()` ishlatish. `utf16_length()` bilan o'lchash.

### UTF-16-le encode xatosi (SmartRenderer)
**Sabab:** Pyrogram `Str` subclass `text.encode('utf-16-le')` chaqiradi.
**Yechim:** `str(msg.caption)` yoki `str(msg.text)` — oddiy str'ga aylantirish.

### Bot save handler loop
**Sabab:** User session orqali `https://t.me/` havolali text yuborilganda, bot uni yangi link sifatida qayta ishlaydi.
**Yechim:** Barcha text xabarlarni faqat `client` (bot) orqali yuborish.

### FloodWait
**Yechim:** `getattr(e, 'value', getattr(e, 'x', 30))` — eski va yangi Pyrogram versiyalari uchun.

### Pyrofork utils.get_reply_to() TypeError (BUG-010)
**Sabab:** Pyrofork 2.3.69 (Layer 220) da `utils.get_reply_to()` noto'g'ri parametr nomlari ishlatadi:
- `reply_to_message_id` → `InputReplyToMessage` `reply_to_msg_id` kutadi
- `message_thread_id` → `top_msg_id` kutadi
- `reply_to_chat` → `reply_to_peer_id` kutadi
Bu `TypeError: got an unexpected keyword argument` chiqaradi.
**Yechim:** `core/pyrofork_compat.py` da monkey-patch qo'shildi — `_patch_pyrofork_get_reply_to()` import vaqtida to'g'ri parametr nomlarini ishlatuvchi patched versiyani qo'yadi. `core/__init__.py` da early import qilinadi. Agar `pip install` qayta qilinsa, `venv/.../pyrogram/utils.py` ham to'g'ridan tuzatilgan.
**MUHIM:** Bu patchni olib tashlama — Pyrofork yangilanmaguncha reply, copy_message va barcha send chaqiruvlari buziladi.

---

## 13. PREMIUM SESSION POOL — `PremiumSessionPool` (core/premium_relay.py)

```python
pool.count                  # @property — jami sessiyalar soni
pool.available_count()      # method — hozir foydalanib bo'ladigan sessiyalar
pool.add_session(string)    # bir sessiya qo'shish (pool reset yo'q)
pool.remove_session(string) # bir sessiya olib tashlash (pause state saqlanadi)
pool.set_sessions(strings)  # to'liq almashtirish (pause/fatal state o'chadi, index reset)

relay_uploader.is_ready     # bool — relay ishga tayyor
relay_uploader.pool         # PremiumSessionPool
relay_uploader.set_enabled(bool)
relay_uploader.reload_sessions(strings)  # pool.set_sessions + state reset
```

**Diqqat:** `reload_sessions()` barcha pause/flood state'ni o'chiradi va round-robin indeksni nolga qaytaradi. Faqat bitta sessiya qo'shish/olib tashlashda `pool.add_session()` / `pool.remove_session()` afzalroq.

---

## 16. UPLOAD PIPELINE — TO'LIQ ARXITEKTURA

### Asosiy qoida: relay yo'q, Saved Messages yo'q
```
download → premium upload → bot chat → [copy_message agar pool sessiya] → user
```

### Upload destination va delivery
- **User o'z sessiyasi:** sessiya → bot private chat (user↔bot) → to'g'ridan foydalanuvchiga
- **Pool sessiya:** sessiya → pool akkaunt↔bot chat → bot `copy_message(user_id, ...)` → foydalanuvchi

`target_chat_id = client.me.id` (bot Telegram user ID).
`_is_pool_session = (_used_pool_idx is not None)` — shu flag bilan aniqlanadi.

### Sessiya tanlash prioriteti (`download_and_send_media()`, save.py)
```
1. User o'z premium sessiyasi (_premium_session_str, early check'dan)
2. Tizim pool sessiyasi (relay_uploader.pool.get_available())
   — WorkerBotBlockedError → mark_fatal → keyingi pool sessiya
3. acc.export_session_string() — non-premium fallback
```

### Bot chat preparation (UserUploadWorker._connect())
`_prepare_bot_chat(bot_username)` har yangi worker yaratilganda BIR MARTA:
1. `unblock_user(bot)` — bloklangan bo'lsa ochadi (xato → o'tkazib yuboradi)
2. `send_message(bot, "/start")` — agar `UserIsBlocked/PeerIdInvalid` → `WorkerBotBlockedError` raise
3. `archive_chats([bot_chat_id])` — best-effort
Worker qayta ishlatilsa (uzoq muddatli) `/start` yana yuborilmaydi.

### WorkerBotBlockedError
`worker.start()` raise qilsa → save.py: `mark_fatal(pool_idx)` → keyingi `pool.get_available()`.
Pool tugasa → `acc.export_session_string()` fallback.

### Pool band bo'lganda foydalanuvchi so'rovi
Barcha pool sessiyalar flood/fatal bo'lsa `_try_upload_with_pool_fallback()` ichida:
1. `pool.next_available_in()` — qachon birinchi sessiya bo'shashini hisoblaydi
2. Foydalanuvchiga inline tugma yuboriladi:
   - **"⏳ Kutaman (N daq)"** — sessiya bo'shashini 5 daqiqagacha kutadi (10s interval poll)
   - **"⚡ Premiumsiz davom"** — `acc.export_session_string()` bilan darhol davom etadi
3. 5 daqiqa javob bo'lmasa — avtomatik "premiumsiz davom" tanlaydi
4. Barcha sessiya fatal (hech qachon tiklanmaydi) bo'lsa → to'g'ridan exception raise

### `PremiumSessionPool.next_available_in() → Optional[float]`
- `0.0` — sessiya hozir mavjud
- `N` — N soniyadan keyin birinchi sessiya bo'shaydi
- `None` — barcha sessiyalar fatal (tiklanmaydi)

### Upload tezligi (user_upload_worker.py)
```python
TEXT_MIN_GAP  = 0.3s   # (eski: 0.7s)
MEDIA_MIN_GAP = 0.5s   # (eski: 1.3s)
BATCH_SIZE    = 15     # (eski: 8)
BATCH_PAUSE   = 3.0s   # (eski: 4.0s)
```
User kamroq kutadi. Agar Telegram FloodWait bersa — sessiya rotatsiyasi ishlaydi.

### main.py startup — relay_uploader.init() YO'Q
```python
await _relay_uploader.pool.set_sessions(_strings)  # faqat shu
```
Relay kanal tekshiruvi, `RELAY_CHANNEL_ID` import — YO'Q.

---

## 17. GOVERNANCE LAYER (2026-03-08)

Additive overlay — mavjud download/upload logikasi o'zgartirilmaydi.

### Yangi fayllar

| Fayl | Maqsad |
|------|--------|
| `core/role_manager.py` | `UserRole` enum (new/normal/vip/banned), fayl kesh + MongoDB sync |
| `core/rate_limiter.py` | Sliding window rate limiter, 10s oyna, synchronous |
| `core/permission_guard.py` | Link validatsiyasi (new_user: faqat 1 post_id) |
| `core/priority_queue.py` | 3-bucket priority queue, semaphore slot reservation |
| `TechVJ/owner_commands.py` | Owner commandlar: /ban /unban /setrole /set_rate_limit va boshqalar |

### O'zgartirilgan fayllar

| Fayl | O'zgarish |
|------|-----------|
| `TechVJ/save.py` | Governance import (guarded) + middleware shim `save()` ichida |
| `main.py` | `priority_queue.start()` bot startup'da |

### Middleware zanjiri (save() ichida, URL parse'dan keyin)
```
kiruvchi URL → maintenance → ban → rate limit → permission guard → mavjud queue
```

### Rol limitleri (default)
```
new_user:    1 so'rov / 10s, faqat single post
normal_user: 3 so'rov / 10s, range/comma ruxsat
vip_user:    10 so'rov / 10s, range/comma ruxsat
banned_user: barcha so'rovlar rad
```

### Priority queue default
```
vip_user:    4 worker (50%)
normal_user: 3 worker (35%)
new_user:    1 worker (15%)
```
Spill-over: bo'sh slotlar pastki prioritetga beriladi.

### UserRole storage
- Fayl: `data/roles/<user_id>.txt` — bir qator, rol string
- In-memory kesh (asyncio.Lock + double-check pattern)
- MongoDB sync best-effort (muvaffaqiyatsiz bo'lsa bot davom etadi)
- Default: fayl yo'q → `new_user`

### Owner commandlar (TechVJ/owner_commands.py)
```
/ban <uid>                     — banned_user
/unban <uid>                   — normal_user ga qaytarish
/setrole <uid> <rol>           — new_user|normal_user|vip_user
/userinfo <uid>                — rol, rate limit, queue holati
/set_rate_limit <rol> <n>      — n req/10s (runtime, restart shart emas)
/set_parallel_limit <rol> <n>  — parallel worker soni (runtime)
/queue_status                  — kutayotgan so'rovlar soni
/stats                         — umumiy holat + sessiyalar
/maintenance <on|off>          — texnik xizmat rejimi
/enable_global_sessions        — barcha global sessiyalarni yoqish
/disable_global_sessions       — barcha global sessiyalarni o'chirish
```

### Python 3.10–3.13 moslik
- Barcha fayllarda `from __future__ import annotations`
- `asyncio.get_running_loop()` (deprecated `get_event_loop()` yo'q)
- `asyncio.Lock()` `__init__`da — 3.10+'da loop shart emas
- `sem._value` ISHLATILMAYDI — `sem.locked()` public API ishlatiladi
- `typing.Tuple/Dict/Optional` — bare `tuple/dict` emas (3.9 oldin uchun)

### Colab/Kaggle eslatma
- `data/roles/` CWD ga nisbatan yaratiladi — writable bo'lishi kerak
- `/kaggle/input` read-only → bot import paytida PermissionError beradi
- Yechim: bot `/content` (Colab) yoki `/kaggle/working` (Kaggle) dan ishlatilsin

---

## 18. PRODUCTION SESSION MANAGER (core/session_manager/)

### Fayl tuzilmasi
```
core/session_manager/
    __init__.py           — public re-exports
    models.py             — SessionType enum + SessionRecord dataclass
    session_registry.py   — CRUD + JSON persistence (data/session_manager/sessions.json)
    flood_controller.py   — per-session flood tracking (monotonic timestamps)
    borrow_manager.py     — concurrent slot acquire/release (per-session asyncio.Lock)
    premium_worker.py     — bridge to UserUploadWorker
    session_manager.py    — bosh orchestrator singleton
TechVJ/session_manager_commands.py — /session owner commandlar
```

### Sessiya turlari (SessionType)
```
USER_OWNED  — faqat egasi ishlatadi, /session disable rad etiladi
DEDICATED   — bir foydalanuvchiga bog'langan
BORROWABLE  — egasining sessiyasi, boshqalar borrow qilishi mumkin
GLOBAL      — tizim sessiyasi, barcha foydalanuvchilar uchun
```

### Tanlash prioriteti (foydalanuvchi A uchun)
```
1. USER_OWNED (owner_user_id == A)
2. DEDICATED  (owner_user_id == A)
3. BORROWABLE (allow_borrow=True, egasi A emas)
4. GLOBAL     (enabled, allow_system_use=True)
5. None → acc.export_session_string() fallback
```

### Pool sessiya delivery
Pool sessiya (BORROWABLE/GLOBAL): sessiya → bot DM ga yuklaydi → `bot.copy_message(user_id)` → foydalanuvchi

### /session commandlar
```
/session list
/session add global <session>
/session add borrowable <uid> <session>
/session add dedicated <uid> <session>
/session remove <sid_prefix>
/session disable <sid_prefix>    — USER_OWNED bo'lsa rad
/session enable <sid_prefix>
/session borrow <sid_prefix> <on|off>
/session parallel <sid_prefix> <n>
/session disableall global
/session status
```

### JSON storage (data/session_manager/sessions.json)
Runtime maydonlar (current_tasks, flood_until, _task_lock) JSONga yozilmaydi.

### main.py startup
```python
await session_manager.init(bot_id, bot_username)  # SessionManager init
await priority_queue.start()                       # PriorityQueue workers
```
Ikkalasi non-fatal try/except ichida.

---

## 19. TOPIC EXTRACTOR (core/topic_extractor.py)

### Maqsad
Forum topic'dan barcha xabarlarni bulk MTProto so'rovlar bilan olish.
`get_chat_history(message_thread_id=topic_id)` ishlatadi — server-side filter.

### API
```python
extractor = TopicExtractor(acc, TopicExtractorConfig(
    chat_id=-1001234567890,
    topic_id=456,           # topic boshlagan xabar ID si
    fetch_batch_size=200,   # MTProto max
    inter_page_delay=0.3,   # sahifalar orasida kutish
))

# Butun topicni list sifatida (kichik topiclar uchun)
msgs = await extractor.extract_all()

# Batch generator (katta topiclar uchun)
async for batch in extractor.stream(batch_size=50):
    for msg in batch:
        await process(msg)
```

### Kafolatlar
- Xronologik tartib (oldest first): double-reverse + safety sort(date, id)
- Dedup: `seen: set[int]` bilan
- Faqat shu topic xabarlari: 3-qatlamli filtrlash
  1. `msg.id == topic_id` (topic root)
  2. `reply_to_top_message_id == topic_id` (asosiy)
  3. `reply_to_message_id == topic_id` + top_id yo'q (birinchi daraja)
- FloodWait: exponential backoff (Telegram wait + min(attempt*5, 120s))

### save.py integratsiyasi
`process_topic_posts()` ichida:
- `post_ids` bo'sh bo'lsa → TopicExtractor bilan butun topicni fetch
- `post_ids` bor bo'lsa → `_topic_msg_cache` (already fetched msgs)
- Fallback: TopicExtractionError → per-ID `acc.get_messages()` loop

### Unumdorlik
| Usul | 1000 xabar MTProto so'rovlari |
|------|-------------------------------|
| Per-ID loop (eski) | 1000 |
| TopicExtractor (yangi) | 5 |

### O'ZGARTIRIB BO'LMAYDIGAN NARSALAR (§16 uchun)
- Pool sessiya uchun `copy_message` olib tashlanmasin — foydalanuvchi ko'rishi uchun shart
- `WorkerBotBlockedError` save.py da catch qilinsin — sessiya o'tkazib yuborish mexanizmi
- `sleep_threshold=0` worker clientda — FloodWait manual handle qilinadi
- `_is_pool_session` flagt o'zgartirilmasin — delivery logikasi shunga bog'liq
- `relay_uploader.upload_via_relay()` hech qachon chaqirilmasin

---

## 14. CAPTION SPLITTER — `core/caption_splitter.py`

### Konstantalar
```python
CAPTION_LIMIT = 1024          # non-premium caption max (UTF-16 units)
PREMIUM_CAPTION_LIMIT = 2048  # premium caption max (UTF-16 units)
MESSAGE_LIMIT = 4096          # overflow chunk max (UTF-16 units)
```

### `split_caption(caption, entities, is_premium=False)`
```python
from core.caption_splitter import split_caption
primary, overflow_chunks = split_caption(caption, entities, is_premium=bool(_premium_session_str))
```
- `is_premium=False` (default): custom emoji strip qilinadi, limit 1024
- `is_premium=True`: custom emoji entity'lar SAQLANADI, limit 2048
- `primary` → `CaptionChunk(text, entities)` — media captioni sifatida yuboriladi
- `overflow_chunks` → `[CaptionChunk, ...]` — `client` (bot) orqali reply sifatida yuboriladi

**MUHIM:** `is_premium` parametrini olib tashlama yoki o'zgartirma — premium user uchun custom emoji va 2048 limit shu orqali ishlaydi.

---

## 15. PREMIUM-AWARE UPLOAD LOGIKASI — `download_and_send_media()` (save.py)

### Early premium check (faylni yuklab olishdan OLDIN)
```python
user_id = message.chat.id
_user_is_premium = False
_premium_session_str = None
try:
    from core.premium_logic import check_user_premium
    _user_is_premium = await check_user_premium(client, user_id)
    if _user_is_premium:
        _udata = await async_db.find_user(user_id)
        _premium_session_str = _udata.get('session') if _udata else None
except Exception as _prem_early_err:
    logger.debug(f"Early premium check failed: {_prem_early_err}")
```
Bu blok `download_and_send_media()` ichida faylni yuklab olishdan keyin, lekin caption split va upload qaroridan OLDIN bajariladi.

### Qoidalar
1. **`check_user_premium()` faqat bir marta chaqiriladi** (early check blokida). `>2GB` blokida qayta chaqirilmaydi.
2. **`async_db.find_user()` faqat premium user uchun chaqiriladi** — `_user_is_premium` True bo'lganda.
3. **`split_caption` chaqiruvi:** `is_premium=bool(_premium_session_str)` — session string bor bo'lsa premium limit va custom emoji saqlanadi.
4. **`>2GB` blokida** `should_split()` va `get_user_upload_setting()` chaqiriladi, lekin `check_user_premium()` va `find_user()` QAYTA chaqirilmaydi — ular early check'dan olinadi.

### Premium caption stripping qoidasi
- **Non-premium:** `strip_custom_emoji_entities()` chaqiriladi → custom emoji glif saqlanadi, lekin entity o'chiriladi
- **Premium (`_premium_session_str` bor):** custom emoji entity'lar saqlanadi, limit 2048
- **Overflow text:** har doim `client` (bot) orqali yuboriladi — bu qoidadan OG'ISHMANG

---

## 20. BUG TRACKER

Barcha topilgan buglar, sabablari va yechimlari `data/BUGS.md` faylida saqlanadi.

**Yangi Claude sessiyasi boshlanganda:**
1. `data/BUGS.md` ni o'qi — oldin qanday buglar topilgan va hal qilinganini bil
2. Yangi bug topsang — `data/BUGS.md` ga qo'sh (BUG-NNN format)
3. Bugni hal qilsang — statusini ✅ ga o'zgartir va yechimni yoz

Bu fayl har xil Claude sessiyalari o'rtasida ma'lumot yo'qolishini oldini oladi.

### Prompt tarixi
Foydalanuvchi promptlari va loyiha yo'nalishi `data/PROMPTS.md` da saqlanadi.

**Yangi Claude sessiyasi boshlanganda:**
1. `data/PROMPTS.md` ni O'QI — oldingi sessiyalarda nima qilinganini bil
2. Sessiya oxirida yangi yozuvlar QO'SH — nima so'raldi, nima qilindi
3. "UMUMIY YO'NALISH" bo'limini yangilab bor

---

## 21. PREMIUM SESSION FOYDALANISH TARTIBI — TO'LIQ FLOW

### Ikki parallel tizim mavjud:

| Tizim | Joylashuv | Maqsad |
|-------|-----------|--------|
| **SessionManager** (yangi) | `core/session_manager/` | USER_OWNED/DEDICATED/BORROWABLE/GLOBAL sessiyalar |
| **Legacy pool** (relay_uploader) | `core/premium_relay.py` | `PremiumSessionPool` — round-robin premium sessiyalar |

### < 2GB fayllar uchun upload tartibi (`download_and_send_media()`, save.py)

```
1. SessionManager (avval tekshiriladi)
   → _sm_inst._initialized va registry.get_all() bo'sh emas
   → upload_for_user() → USER_OWNED > DEDICATED > BORROWABLE > GLOBAL
   → Muvaffaqiyat → legacy SKIP, overflow text yuboriladi
   → User premium bo'lishi SHART EMAS — tizim sessiyalari ishlaydi

2. Legacy yo'l (SM bo'sh yoki muvaffaqiyatsiz)
   a) System pool sessiya (relay_uploader.pool.get_available()) ← AVVAL
   b) User o'z premium sessiyasi (_premium_session_str) ← pool yo'q bo'lsa
   c) User oddiy sessiyasi (session_string / acc.export) ← oxirgi fallback

3. Worker yaratish → upload → _enqueue_and_deliver()
   → Pool: upload → copy_message (3x retry) → delete (faqat copy OK bo'lsa)
   → Non-pool: to'g'ridan-to'g'ri user↔bot chatga

4. FloodWait → _try_upload_with_pool_fallback()
   → Pool rotation → barcha band → user tanlovi (kutish/premiumsiz)
```

### > 2GB fayllar uchun upload tartibi

```
FAQAT userning O'Z premium sessiyasi bilan:
  _use_premium = file>2GB AND not_do_split AND _premium_session_str
  → User premium EMAS yoki session yo'q → SPLIT fallback (faylni bo'ladi)
  → Pool/system sessiya > 2GB uchun ISHLATILMAYDI
```

### Premium sessiya tizimni boshqarish komandlari

**Owner komandalar (`TechVJ/premium_commands.py`):**
```
/setpremium <session>        — Legacy tizim sessiyasi (yagona)
/removepremium               — Legacy sessiyani o'chirish
/premiumstatus               — Legacy sessiya holati
/checkpremium <id|@user>     — MTProto orqali user Premium tekshirish

/premium status              — Pool holati (sessiyalar + relay kanal)
/premium add <session>       — Pool'ga sessiya qo'shish (hot-reload)
/premium remove <N>          — N-sessiyani olib tashlash
/premium relay <channel_id>  — Relay kanal belgilash
/premium test                — Barcha sessiyalar + bot admin tekshirish
/premium on                  — Relay yoqish
/premium off                 — Relay o'chirish

/add_premium_session <sess>  — Hot-load premium relay (restart kerak emas)
/remove_premium_session      — Runtime'da olib tashlash
```

**SessionManager komandalar (`TechVJ/session_manager_commands.py`):**
```
/session list                       — Barcha sessiyalar
/session add global <session>       — GLOBAL sessiya (barcha userlar)
/session add borrowable <uid> <ses> — BORROWABLE (egasi bor, boshqalar ham)
/session add dedicated <uid> <ses>  — DEDICATED (faqat bir user uchun)
/session remove <sid_prefix>        — O'chirish
/session disable/enable <sid>       — Yoqish/o'chirish
/session status                     — Holat
```

**User komandalar:**
```
/uploadsetting [auto|force_split|no_split]
  auto        — Default. >2GB bo'lsa faqat o'z Premium sessiyasi bilan no-split
  force_split — Har doim split, Premium bo'lsa ham
  no_split    — Hech qachon split (FAQAT o'z Premium sessiyasi bo'lsa ishlaydi)

/split_media [on|off|auto]
  on   → force_split mode
  off  → no_split mode (FAQAT o'z Premium sessiyasi bo'lsa)
  auto → tizim qaror qiladi
```

### Muhim farqlar

| Xususiyat | SessionManager | Legacy pool (relay_uploader) |
|-----------|---------------|------|
| < 2GB upload | ✅ Ishlaydi | ⚠️ Amalda ishlamaydi (session_string bilan override) |
| > 2GB upload | ❌ Ishlatilmaydi | ❌ Ishlatilmaydi (faqat user o'z Premium) |
| User premium kerakmi? | ❌ Shart emas | ❌ Shart emas |
| Pool session delivery | copy_message + delete | copy_message + delete |
| FloodWait rotation | Ichki (upload_for_user) | _try_upload_with_pool_fallback |
| Saqlash | data/session_manager/sessions.json | data/premium/sessions.json |

### `_user_has_own_premium_session()` (premium_commands.py)
```python
async def _user_has_own_premium_session(client, user_id) -> bool:
    (user_is_premium, udata) = await asyncio.gather(
        check_user_premium(client, user_id),
        async_db.find_user(user_id),
    )
    user_session = udata.get('session') if udata else None
    return bool(user_is_premium and user_session)
```
Bu funksiya `no_split` va `split_media off` uchun tekshiradi — FAQAT userning O'Z Premium sessiyasi bo'lsa ruxsat beradi. Tizim sessiyalari hisobga olinmaydi.

### `PremiumSessionPool` (core/premium_relay.py)
```python
pool.get_available()      → (index, session_string) | None  # round-robin
pool.mark_flood(idx, sec) # FloodWait — vaqtincha to'xtatish
pool.mark_fatal(idx)      # Doimiy o'chirish (faqat runtime)
pool.next_available_in()  → 0.0 | seconds | None  # qachon bo'shaydi
pool.count                # @property — jami
pool.available_count()    # hozir foydalanib bo'ladiganlar
pool.add_session(string)  # Hot-add
pool.remove_session(n)    # 1-based index
pool.set_sessions(list)   # To'liq almashtirish
```

### `RelayUploader` (core/premium_relay.py)
```python
relay_uploader.init(sessions, channel_id, bot, ...)  # Startup
relay_uploader.is_ready           # bool
relay_uploader.pool               # PremiumSessionPool
relay_uploader.set_enabled(bool)  # on/off
relay_uploader.reload_sessions(strings)  # hot-reload
```
**MUHIM:** `relay_uploader.upload_via_relay()` — bu funksiya HECH QACHON chaqirilmasin (CLAUDE.md §19 ga qarang). Upload faqat UserUploadWorker yoki SessionManager orqali.
