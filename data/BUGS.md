# BUG TRACKER — loyiha/new Telegram Bot

Bu fayl topilgan buglar, ularning sabablari va qanday hal qilinganligini kuzatib boradi.
Yangi Claude sessiyalari bu faylni o'qib, oldingi ishlangan buglarni biladi.

---

## BUG-001: /stop cancellation race condition
**Sana:** 2026-03-23
**Holat:** ✅ Hal qilindi

**Muammo:** `/stop` yuborilganda download davom etardi. Temp papka `task_manager.cancel_all_tasks()` orqali tozalanardi, lekin download engine hali faol edi → `FileNotFoundError` → retry loop'ga kirardi (5 urinish, har biri 2-10s kutish).

**Sabab:** `stop_command()` da tartib noto'g'ri edi:
1. `task_manager.cancel_all_tasks()` — temp dir tozalaydi
2. `engine.cancel_user_downloads()` — downloadlarni bekor qiladi
Natija: download hali ishlayotganda temp dir o'chirildi.

**Yechim:**
- Tartibni o'zgartirdik: AVVAL engine cancel, KEYIN task_manager
- `FileNotFoundError` uchun maxsus catch qo'shildi (`core/downloader/resume.py`, `engine.py`) — retry qilmasdan darhol `return None`
- `cancel_user_tasks()` da asyncio wait timeout 2s → 5s ga oshirildi
- `ResumeState.save()` — temp dir o'chirilgandan keyin yozishga urinmaydigan qilinadi

**O'zgartirilgan fayllar:** `TechVJ/save.py`, `core/downloader/resume.py`, `core/downloader/engine.py`, `core/downloader/worker.py`, `TechVJ/task_manager.py`

---

## BUG-002: Media reply-to-message ishlamaydi
**Sana:** 2026-03-23
**Holat:** ✅ Hal qilindi

**Muammo:** Bot download qilgan mediayni userga yuborayotganda, userning original havola xabariga reply qilmaydi. Media standalone xabar sifatida kelardi.

**Sabab:**
1. SessionManager yo'lida: `upload_chat_id = target_chat_id = user_id` ishlatilardi → Saved Messages'ga yuborardi
2. Pool sessiyalar: `copy_message` ga `reply_to_message_id` uzatilmasdi
3. `reply_parameters` (yangi Pyrogram) strip qilinmasdi

**Yechim:**
- Barcha sessiyalar `bot_user_id` (bot Telegram user ID) ga upload qiladi
- Pool sessiyalar: `reply_to_message_id` `send_kwargs`dan olinib `copy_message`ga uzatiladi
- `reply_parameters` ham pool sessiyalarda strip qilinadi

**O'zgartirilgan fayllar:** `core/session_manager/session_manager.py`, `TechVJ/save.py`

---

## BUG-003: Pool session oraliq xabar tozalanmaydi
**Sana:** 2026-03-23
**Holat:** ✅ Hal qilindi

**Muammo:** Pool sessiya (BORROWABLE/GLOBAL) media yuborayotganda, avval pool akkaunt ↔ bot chatiga upload qilardi, keyin `copy_message` bilan userga yuborardi. Lekin pool chatdagi oraliq xabar tozalanmasdi.

**Yechim:** `copy_message` muvaffaqiyatli bo'lgandan keyin `bot_client.delete_messages()` qo'shildi.

**O'zgartirilgan fayllar:** `core/session_manager/session_manager.py`, `TechVJ/save.py`

---

## BUG-004: doc_file_name SessionManager yo'lida yo'qoladi
**Sana:** 2026-03-23
**Holat:** ✅ Hal qilindi

**Muammo:** `upload_for_user()` `doc_file_name` parametrini qabul qilardi, lekin `upload_with_session()` ga uzatmasdi.

**Yechim:** `doc_file_name=doc_file_name` parametr qo'shildi.

**O'zgartirilgan fayllar:** `core/session_manager/session_manager.py`

---

## BUG-005: struct.error login paytida
**Sana:** 2026-03-24
**Holat:** ✅ Hal qilindi

**Muammo:** User `/login` qilib muvaffaqiyatli OTP/2FA kiritgandan keyin `client.export_session_string()` da `struct.error: required argument is not an integer` xatosi chiqardi. Sababi: Pyrogram session storage'da `dc_id` yoki `user_id` `None` sifatida qolishi — eski/buzilgan `.session` fayli yoki noto'g'ri autentifikatsiya natijasi.

**Sabab:** `sessions/temp_user_X.session` fayllar oldingi login urinishlaridan tozalanmasdi. Buzilgan session fayli yangi login uchun ishlatilardi.

**Yechim:**
1. Login boshlanishida eski `.session` va `.session-journal` fayllarini o'chirish
2. `export_session_string()` chaqiruvini `try/except (struct.error, TypeError, ValueError)` bilan o'rash
3. Xato bo'lganda userga aniq xabar: "Iltimos /logout qilib qaytadan /login bilan kiring"

**O'zgartirilgan fayllar:** `TechVJ/generate.py`

---

## BUG-006: asyncio.get_event_loop() deprecated
**Sana:** 2026-03-23
**Holat:** ✅ Hal qilindi

**Muammo:** `save.py` da 4 joyda `asyncio.get_event_loop()` ishlatilardi — Python 3.12+ da deprecated.

**Yechim:** Barchasini `asyncio.get_running_loop()` ga almashtirildi.

**O'zgartirilgan fayllar:** `TechVJ/save.py`

---

## BUG-019: Audio-only media group yuborilmaydi
**Sana:** 2026-05-06
**Holat:** ✅ Hal qilindi

**Muammo:** Faqat audio (2+ item) bo'lgan media_group yuborilganda bot albomni aniqlaydi, lekin hech narsa yubormaydi.

**Sabab:** `media_group_id` ko'rilishi bilan albom pipeline ishga tushardi. Non-photo guruhlar photo-albom logikasiga tushib qolardi va fallback to'g'ri ishlamasdi.

**Yechim:**
- Photo-only bo'lsa → albom pipeline ishlaydi
- Non-photo bo'lsa → har bir xabar alohida single-send pipeline orqali yuboriladi
- get_media_group bo'sh qaytsa → fallback sifatida o'sha xabarni alohida yuboradi

**O'zgartirilgan fayllar:** `TechVJ/save.py`

---

## BUG-020: Noto'g'ri chat copy / cross-user leak
**Sana:** 2026-05-06
**Holat:** ✅ Hal qilindi

**Muammo:** Bir user link yuborganda boshqa chatdan post copy bo'lishi yoki bot API orqali noto'g'ri manba yuborilishi kuzatilgan.

**Sabab:**
- Peer resolve `access_hash=0` bilan va keshga bog'liq ishlardi
- Public post oqimida bot va user session aralash ishlatilardi (bir task ichida)
- Task konteksti (user/chat/message) qat'iy bog'lanmagan edi

**Yechim:**
- Peer resolve har taskda `resolve_peer()` orqali qayta qilinadi
- Public post oqimi bir taskda bitta client bilan ishlaydi (user session bo'lsa u bilan, bo'lmasa bot bilan)
- TaskContext qo'shildi: user_id/chat_id/message_id/task_id izolyatsiyasi

**O'zgartirilgan fayllar:** `TechVJ/session_handler.py`, `TechVJ/save.py`

---

## BUG-021: Bot block recovery yo'q
**Sana:** 2026-05-06
**Holat:** ✅ Hal qilindi

**Muammo:** User botni block qilgan bo'lsa upload worker /start ni ko'r-ko'rona yuborib, xabar yuborilmas edi.

**Sabab:** UserUploadWorker blokni aniqlagach recovery va retry qilmasdi.

**Yechim:**
- Dialog tekshiruvi qo'shildi (get_chat)
- Block bo'lsa unblock + /start bir marta yuboriladi
- Upload original send bir marta qayta urinadi

**O'zgartirilgan fayllar:** `core/user_upload_worker.py`

---

## BUG-007: Legacy pool sessiyalar < 2GB upload uchun ishlamaydi
**Sana:** 2026-03-24
**Holat:** ✅ Hal qilindi

**Muammo:** `download_and_send_media()` da `_session_string = session_string or _premium_session_str` qatori tufayli pool sessiyalar HECH QACHON tekshirilmasdi. `session_string` parametri har doim user DB sessiyasi bilan to'ldirilgan bo'lgani uchun, `if not _session_string:` sharti hech qachon True bo'lmasdi.

**Sabab:** Session tanlash tartibi noto'g'ri edi:
1. User sessiyasi (har doim bor) ← bu bilan `_session_string` to'ldirilardi
2. Pool sessiya (hech qachon tekshirilmasdi)

**Yechim:** Tartibni o'zgartirdik:
1. AVVAL pool sessiya tekshiriladi (`relay_uploader.pool.get_available()`)
2. Pool yo'q → user premium sessiyasi (`_premium_session_str`)
3. Hech narsa yo'q → user oddiy sessiyasi (`session_string` yoki `acc.export_session_string()`)

**O'zgartirilgan fayllar:** `TechVJ/save.py`

---

## BUG-008: copy_message muvaffaqiyatsiz bo'lsa ham oraliq xabar o'chirilardi
**Sana:** 2026-03-24
**Holat:** ✅ Hal qilindi

**Muammo:** Pool sessiya upload → copy_message → delete_messages ketma-ketligida copy_message xato bersa ham `delete_messages` chaqirilardi. Natijada foydalanuvchi mediayni olmay qolardi VA oraliq xabar ham yo'q qilinar edi.

**Sabab:** `copy_message` va `delete_messages` ikkalasi ham `try/except` ichida edi, ular o'rtasida bog'liqlik yo'q edi.

**Yechim:**
1. `copy_message` uchun retry (3 urinish, exponential backoff, FloodWait handle)
2. FAQAT `copy_message` muvaffaqiyatli bo'lsa → `delete_messages`
3. `copy_message` 3 marta muvaffaqiyatsiz → oraliq xabar SAQLANADI (recovery uchun)

**O'zgartirilgan fayllar:** `TechVJ/save.py`, `core/session_manager/session_manager.py`

---

## BUG-009: access_hash=0 ishlatilishi — spec buzilishi
**Sana:** 2026-03-24
**Holat:** ✅ Hal qilindi

**Muammo:** Production spec "HECH QACHON access_hash=0 ishlatma" deb belgilagan. Lekin 2 ta production faylda `access_hash=0` ishlatilardi:
1. `core/user_upload_worker.py:194` — `raw.types.InputUser(user_id=self._bot_id, access_hash=0)` bilan `GetUsers` chaqiruvi
2. `core/premium_relay.py:621` — `raw.types.InputChannel(channel_id=chan_id, access_hash=0)` bilan `GetChannels` chaqiruvi

**Sabab:** Peer resolve qilinmasdan to'g'ridan-to'g'ri raw API chaqirilardi. `access_hash=0` faqat botlar uchun ishlaydi (public peer), user sessiyalarida ishlamaydi.

**Yechim:**
1. `user_upload_worker.py`: `raw.functions.users.GetUsers(InputUser(..., access_hash=0))` → `client.resolve_peer(self._bot_id)` ga almashtirildi. `from pyrogram import raw` importi olib tashlandi.
2. `premium_relay.py`: `raw.functions.channels.GetChannels(InputChannel(..., access_hash=0))` → `client.resolve_peer(relay_channel_id)` ga almashtirildi (fallback: `get_chat`). `from pyrogram import raw` importi olib tashlandi.

**O'zgartirilgan fayllar:** `core/user_upload_worker.py`, `core/premium_relay.py`

---

## BUG-010: Pyrofork utils.get_reply_to() parametr nomi xato — reply ishlamaydi
**Sana:** 2026-03-24
**Holat:** ✅ Hal qilindi

**Muammo:** User sessiyasi orqali yuborilgan media asosiy havolaga reply qilmaydi. `reply_to_message_id` to'g'ri uzatilsa ham, Pyrofork ichida `TypeError: got an unexpected keyword argument 'reply_to_message_id'` xatosi chiqadi va media umuman yuborilmaydi yoki reply'siz yuboriladi.

**Sabab:** Pyrofork 2.3.69 (Layer 220) dagi `pyrogram/utils.py:get_reply_to()` funksiyasi `InputReplyToMessage` TLObject'ga noto'g'ri parametr nomlari uzatadi:
- `reply_to_message_id=` → TLObject `reply_to_msg_id` kutadi
- `message_thread_id=` → TLObject `top_msg_id` kutadi
- `reply_to_chat=` → TLObject `reply_to_peer_id` kutadi

Bu Pyrofork'ning ICHKI bagi — loyiha kodida xato yo'q edi.

**Ta'siri:**
- User o'z sessiyasi orqali upload: `send_video(reply_to_message_id=N)` → TypeError → upload umuman muvaffaqiyatsiz
- Pool sessiya: upload muvaffaqiyatli (reply strip qilinadi), lekin `copy_message(reply_to_message_id=N)` → TypeError → copy muvaffaqiyatsiz
- Bot'ning `message.reply()` xatosiz ishlaydi chunki private chat'da `quote=False` default → `reply_to_message_id=None` → get_reply_to() ga tushmaydi

**Yechim:**
1. `venv/Lib/site-packages/pyrogram/utils.py` va `.venv/` da to'g'ridan fix: `reply_to_msg_id=`, `top_msg_id=`, `reply_to_peer_id=`
2. `core/pyrofork_compat.py` da runtime monkey-patch qo'shildi — `_patch_pyrofork_get_reply_to()` funksiyasi import vaqtida `pyrogram.utils.get_reply_to` ni to'g'ri parametr nomlari bilan almashtiradi
3. `core/__init__.py` da `import core.pyrofork_compat` early import qo'shildi — patch barcha modullardan oldin ishga tushadi

**O'zgartirilgan fayllar:** `core/pyrofork_compat.py`, `core/__init__.py`, `venv/Lib/site-packages/pyrogram/utils.py`, `.venv/Lib/site-packages/pyrogram/utils.py`

---

## BUG-011: update_user cache invalidation yo'qolishi — logout ishlamaydi
**Sana:** 2026-03-24
**Holat:** ✅ Hal qilindi

**Muammo:** `/logout` muvaffaqiyatli bajarilgandan keyin, user `/login` qilsa "already logged in" deb javob berardi. DB da `logged_in=False` yozilgan bo'lishiga qaramay, `find_user()` eski keshdan `logged_in=True` qaytarardi.

**Sabab:** `database/async_db.py` da ikkita `update_user` metod aniqlangan edi:
1. `update_user(chat_id, **kwargs)` (line ~174) — `_USER_CACHE.pop()` bilan kesh tozalaydi
2. `update_user(chat_id, data: dict)` (line ~243) — kesh tozalaMAYDI

Python'da ikkinchi metod birinchisini shadow qiladi. Logout `update_user(user_id, {'session': None})` (dict) chaqirardi → shadowed metod → kesh tozalanmaydi → `find_user()` eski `logged_in=True` qaytaradi.

**Yechim:** Ikkala metodni BIR metod qilib birlashtirdik:
```python
async def update_user(cls, chat_id: int, data: dict = None, **kwargs):
    update_data = {}
    if data and isinstance(data, dict):
        update_data.update(data)
    if kwargs:
        update_data.update(kwargs)
    _USER_CACHE.pop(chat_id, None)  # HAR DOIM tozalaydi
```

**O'zgartirilgan fayllar:** `database/async_db.py`

---

## BUG-012: Logout to'liq tozalamaydi — file va worker qoldiqlari
**Sana:** 2026-03-24
**Holat:** ✅ Hal qilindi

**Muammo:** `/logout` faqat DB ni tozalardi. Quyidagi artefaktlar qolardi:
1. `sessions/user_{id}.session` — fayl backup sessiyasi
2. `sessions/temp_user_{id}.session` — eski login urinishlari
3. `UserUploadWorker` — MTProto ulanishlari davom etardi
4. DB kesh — stale holda qolardi

Natija: Keyingi login yoki session tekshiruvda eski ma'lumotlar ishlatilardi.

**Yechim:** Logout handler to'liq qayta yozildi:
1. DB tozalash (`session=None, logged_in=False`)
2. Session fayllarni o'chirish (`_delete_session_file()`)
3. Upload worker'larni to'xtatish (`worker_registry.remove()`)
4. DB verify — kesh stale bo'lsa forced clear

**O'zgartirilgan fayllar:** `TechVJ/generate.py`

---

## BUG-013: Login OTP ishonchliligi va state boshqaruvi
**Sana:** 2026-03-24
**Holat:** ✅ Hal qilindi

**Muammo:** Bir nechta login muammosi mavjud edi:
1. `/login` invalid sessiya bilan ham "already logged in" deb rad qilardi
2. OTP noto'g'ri formatda kelsa (bo'shliqlar, tire, nuqtalar) ishlamasdi
3. Login paytida `/start` yoki boshqa buyruqlar OTP sifatida qabul qilinardi
4. `logged_in=True` lekin `session=None` inconsistent state handle qilinmasdi
5. QR login handler'da `user_id` aniqlashdan oldin ishlatilardi (NameError)

**Sabab:** Login handler state tekshiruvi primitiv edi — faqat `logged_in` flag tekshirilardi, session validatsiyasi yo'q edi.

**Yechim:**
1. Login boshida `quick_session_check()` bilan mavjud sessiyani validate qilish
2. Invalid sessiya → clear va re-login ruxsat berish
3. `logged_in=True, session=None` → `logged_in=False` ga tuzatish
4. OTP formatidan bo'shliq, tire, nuqta strip qilish + `isdigit()` tekshiruv
5. Buyruq filter — OTP fazasida `/command` qaytariladi
6. `export_session_string()` dan keyin `get_me()` validatsiya
7. QR login'da `user_id` ni session check'dan OLDIN aniqlash

**O'zgartirilgan fayllar:** `TechVJ/generate.py`, `database/async_db.py`

---

## BUG-014: Raw auth.SignIn session storage'ni yangilamaydi — struct.error
**Sana:** 2026-03-24
**Holat:** ✅ Hal qilindi

**Muammo:** Login qilganda `export_session_string()` `struct.error: required argument is not an integer` berardi. OTP to'g'ri kiritilgan, auth muvaffaqiyatli, lekin session eksport qilib bo'lmasdi.

**Sabab:** `generate.py` da raw `client.invoke(functions.auth.SignIn(...))` ishlatilardi. Bu faqat TL natija qaytaradi, lekin session storage'ni (`user_id`, `is_bot`) yangilaMAYDI. Pyrofork'ning high-level `client.sign_in()` esa:
```python
await self.storage.user_id(r.user.id)    # ← SHART
await self.storage.is_bot(False)          # ← SHART
```
Bu ikkala qator bo'lmasa, `export_session_string()` `None` qiymatlarni `struct.pack()` ga uzatadi → `struct.error`.

Bundan tashqari, `send_code_raw()` `PhoneMigrate`/`NetworkMigrate` exceptionlarni handle qilmasdi — boshqa DC'dagi raqamlar uchun login muvaffaqiyatsiz bo'lardi.

**Yechim:**
1. `client.invoke(auth.SignIn(...))` → `client.sign_in(...)` ga almashtirildi (high-level API session storage'ni yangilaydi)
2. `send_code_raw()` ga `PhoneMigrate`/`NetworkMigrate` handling qo'shildi — DC migratsiya bilan qayta ulanadi
3. `PhoneNumberUnoccupied` exception handle qo'shildi

**O'zgartirilgan fayllar:** `TechVJ/generate.py`

---

## BUG-015: video_converter.py logged-in userlar mediasini duplikat yuboradi
**Sana:** 2026-03-24
**Holat:** ✅ Hal qilindi

**Muammo:** User t.me link yuborib, `save.py` user session orqali media yuborayotganda, `video_converter.py` handler bu mediayni ushlab, bot API orqali qayta yuborardi. Natijada user ikkita nusxa olardi — biri user session'dan, biri bot'dan.

**Sabab:** `video_converter.py` filteri faqat t.me linkli xabarlarni exclude qilardi. User session orqali yuborilgan video (t.me linksiz) filtrdan o'tardi va handler ishga tushardi.

**Yechim:**
1. `video_filter` da `outgoing` va bot xabarlarini skip qilish
2. Handler boshida logged-in userlar uchun skip (`async_db.find_user` bilan tekshirish — keshlanadi)
3. Pool session intermediate upload'larni skip (unknown sender → bot chatga video = pool session)

**O'zgartirilgan fayllar:** `TechVJ/video_converter.py`

---

## BUG-016: FloodWait cascade — bot Telegram'ga soniyada o'nlab so'rov yuboradi
**Sana:** 2026-03-30
**Holat:** ✅ Hal qilindi

**Muammo:** Katta batch (100+ post) yuklanganda bot `messages.SendMessage` uchun FloodWait (1300s) oladi. Lekin `download_and_send_media()` va `send_text_message()` FloodWait'ni umumiy `except Exception` bilan tutib, `return False` qaytaradi — FloodWait outer loop'ga yetib bormaydi. Natija: har bir keyingi post uchun bot yana Telegram'ga so'rov yuboradi, yana FloodWait oladi, va bu 100+ marta takrorlanadi. Production logda minutiga 100+ FloodWait WARNING ko'rinadi.

**Sabab:**
1. `download_and_send_media()` — FloodWait'ni `except Exception` ichida yutardi
2. `send_text_message()` — FloodWait'ni catch qilib fallback'ga o'tardi, u ham FloodWait olardi
3. `process_single_post()` — FloodWait'ni `except Exception` ichida yutardi
4. `process_single_topic_message()` — FloodWait'ni yutardi
5. `process_album_messages()` — FloodWait'ni yutardi
6. Outer loop'dagi FloodWait catch (`process_topic_posts` 2802-qator) ishlamardi chunki FloodWait unga yetib bormasdi

**Yechim:**
1. `download_and_send_media` — `except FloodWait: raise` qo'shildi (Exception'dan oldin)
2. `send_text_message` — `except FloodWait: raise` qo'shildi (asosiy va fallback'da)
3. `process_single_post` — `except FloodWait: raise` qo'shildi
4. `process_single_topic_message` — `except FloodWait: raise` qo'shildi
5. `process_album_messages` — `except FloodWait: raise` qo'shildi
6. `process_private_posts` loop — `except FloodWait` qo'shildi: >120s → batch to'xtatiladi, ≤120s → sleep
7. `process_topic_posts` loop — mavjud FloodWait catch'ni >120s → break qilish bilan yaxshilandi
8. `process_public_posts` loop — `except FloodWait` qo'shildi

**O'zgartirilgan fayllar:** `TechVJ/save.py`

---

## BUG-017: Bare except: BaseException'ni (SystemExit, KeyboardInterrupt) yutadi
**Sana:** 2026-03-30
**Holat:** ✅ Hal qilindi

**Muammo:** `TechVJ/save.py` da 17 joyda `except:` (bare except) ishlatilgan edi. Bu `SystemExit` va `KeyboardInterrupt` ni ham catch qilardi, bot to'g'ri to'xtab qolmasdi.

**Yechim:** Barcha 17 ta `except:` → `except Exception:` yoki `except Exception as e:` + debug log'ga almashtirildi.

**O'zgartirilgan fayllar:** `TechVJ/save.py`

---

## BUG-018: User session orqali yuborilgan media asosiy havolaga reply qilmaydi
**Sana:** 2026-03-30
**Holat:** ✅ Hal qilindi

**Muammo:** User t.me link yuborganda, bot media ni user session (`acc`) orqali `bot_user_id` ga yuboradi. Lekin `reply_to_message_id` user↔bot chatdan — bu message ID `bot_user_id` chatda mavjud emas. Telegram jimgina reply'ni e'tiborsiz qoldiradi. Natija: media user'ga reply'siz keladi.

**Sabab:** `_prepare_upload_send_kwargs()` faqat pool session uchun `reply_to_message_id` ni olib tashlar edi. Non-pool session uchun saqlar edi, lekin user session ham `bot_user_id` ga yuboradi — reply ishlamaydi.

**Ta'sirlangan joylar:**
1. `download_and_send_media()` — barcha media upload'lar
2. `process_single_post()` — location va venue
3. Split file upload'lar

**Yechim:**
1. `_prepare_upload_send_kwargs()` — BARCHA session'lar uchun `reply_to_message_id` strip qilindi (faqat pool emas)
2. `_enqueue_media_delivery()` — barcha session'lar uchun `copy_message` + delete pattern (reply context bot client orqali qo'shiladi)
3. Location/venue — reply olib tashlandi, o'rniga copy_message + delete pattern qo'shildi

**O'zgartirilgan fayllar:** `TechVJ/save.py`
