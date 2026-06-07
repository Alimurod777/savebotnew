# PROMPT TARIXI — Foydalanuvchi buyurtmalari va yo'nalishi

Bu fayl foydalanuvchi har sessiyada qanday so'rovlar berganini va loyiha qaysi yo'nalishda rivojlanayotganini kuzatib boradi.
Yangi Claude sessiyasi bu faylni O'QIB, kontekstni tushunishi KERAK.

---

## Sessiya 12 (2026-06-07) - Owner failure report spam filter

### So'rov:
Senior Telegram MTProto incident-response refactor: deleted/inaccessible/missing posts, message gaps, invalid old links va normal Telegram holatlari owner/bot/admin monitoring reportlariga aylanmasin. Reporting tizimi qayta yozilmasin, mavjud pipeline ichida surgical refactor qilinsin.

### Natija:
- **BUG-034**: reporting helperlari `core/failure_classifier.py` orqali 3 category bilan ishlaydi: `EXPECTED_TELEGRAM_STATE`, `USER_INPUT_ERROR`, `SYSTEM_FAILURE`.
- `_notify_owner_channel_post_failure()` endi faqat `SYSTEM_FAILURE` va reportability gate (`message_fetched` yoki `processing_started` yoki real system exception) o'tsa ownerga yuboradi.
- `_notify_realtime_post_failure()` expected Telegram state uchun owner/user realtime xabar yubormaydi; faqat statistik counterlar mavjud looplarda qoladi.
- `_notify_bot_only_post_failure()` expected Telegram state uchun owner report va `failed_downloads` retry log yozmaydi.
- Upload/relay/routing/copy/session/queue/worker/pool/topic extractor kabi real system failurelar reportable bo'lib qoldi.
- TopicExtractor failure ham shu gate orqali ulandi: expected access/missing holat jim, real extractor bug ownerga chiqadi.

### Tekshiruv:
- `python -m compileall -q TechVJ core database main.py test_governance_fixes.py` - OK
- `pytest -q test_governance_fixes.py` - 17 passed

### O'zgartirilgan fayllar:
- `TechVJ/save.py`
- `core/failure_classifier.py`
- `test_governance_fixes.py`
- `data/BUGS.md`, `data/PROMPTS.md`

---

## Sessiya 11 (2026-05-29) - Audit: barcha qilingan ishlarni ko'rib chiqish va xatolarni to'g'irlash

### So'rov:
"barcha qilingan ishlarni ko'rib chiq va xatoliklar bo'lsa to'g'irla maksimal yondash yaxshiroq va ishlaydigan yo'l bo'lsa amalga oshir"

### Topilgan asosiy xato:
- **BUG-033** — BUG-032 fix dan keyin `_enqueue_media_delivery` (save.py) va `upload_with_session` (session_manager.py) `owner_user_id` parametrini umuman uzatmasdi. Avvalgi tahrir `session_user_id` (pool akkaunt ID) ni `owner_user_id` sifatida uzatardi, bu `_run_task` da sender isolation tekshiruvi bilan har doim REJECT qilinardi. Vaqtinchalik fix `owner_user_id` ni umuman uzatmaslik bo'ldi — sender isolation o'chib qoldi.
- 2 ta regression testlari ham bu xato semantikasiga moslangan edi (`assert owner_user_id == 222/333` — pool akkaunt).

### Yechim:
1. `_enqueue_media_delivery` endi `owner_user_id=int(target_user_id)` uzatadi — so'rov yuborgan user.
2. `upload_with_session` endi `owner_user_id=user_id` uzatadi.
3. Endi `task.owner_user_id == worker._user_id` har doim mos keladi — sender isolation real ishlaydi.
4. Testlar to'g'ri semantikaga moslandi (`assert owner_user_id == 123`).
5. Stray `diff.txt`, `diff_utf8.txt` fayllari (avvalgi diff inspection qoldiqlari) o'chirildi.

### Tekshiruv:
- `python -m compileall -q TechVJ core database main.py test_governance_fixes.py` — ✅
- 13 ta test PASS, 0 FAIL

### O'zgartirilgan fayllar:
- `TechVJ/save.py`
- `core/session_manager/session_manager.py`
- `test_governance_fixes.py`
- `data/BUGS.md`, `data/PROMPTS.md`

---

## Sessiya 10 (2026-05-27) - /addchannel restricted link guard

### So'rovlar ketma-ketligi:
1. Owner tomonidan taqiqlangan kanaldan user havola yuborsa, bot "olish taqiqlangan" deb qaytaryaptimi yoki yo'qligini tekshirish.

### Natija:
- **BUG-031**: `save()` ichida `/addchannel` enabled kanal ro'yxati bo'yicha non-owner user linklari queue/downloaddan oldin bloklanadi.
- Private `/c/...` linklar numeric channel ID bilan tekshiriladi.
- Public username linklar imkon qadar bot `get_chat()` orqali numeric IDga resolve qilinib tekshiriladi.
- Javob faqat bot client orqali yuboriladi: "Bu kanaldan kontent olish owner tomonidan taqiqlangan."
- **BUG-032**: Private DM sender/bot `message_id` desync fixi kuchaytirildi; endi bot-side ID topilmasa `sent_msg.id`ga fallback qilinmaydi, uploaddan oldingi watermarkdan keyingi fingerprint-mos xabar copy/delete qilinadi.

### Tekshiruv:
- `test_governance_fixes.py` ga restricted channel guard regression testlari qo'shildi.
- Private DM copy uchun real bot-side ID va unresolved holatda copy qilmaslik regression testlari qo'shildi.

---

## Sessiya 9 (2026-05-27) - User session lifecycle va real-time failed analysis

### So'rovlar ketma-ketligi:
1. **Immediate error handling** - batch/range looplarda deleted/restricted/FloodWait postlar oxirida emas, darhol tahlil qilinishi kerak.
2. **MongoDB limit himoyasi** - failed loglar MongoDBga yozilmasin; SQLite `failed_downloads` jadvali maksimal 1000 yozuv saqlasin.
3. **/addchannel auto-disable** - access xatolari `_process_monitored_channel_message()` ichida ushlanib, kanal 3 marta xatodan keyin o'chirilsin.
4. **Retry lifecycle** - retry paytida user sessiyasi uzilgan bo'lsa yangi context manager bilan tiklanishi tekshirilsin va natija user/ownerga aniq yuborilsin.

### Natija:
- **BUG-030**: real-time failure helpers qo'shildi; topic/private/public looplarda xato post aniqlanishi bilan user+owner xabardor qilinadi.
- Failed loglar faqat SQLite `failed_downloads` jadvaliga yoziladi, insertdan keyin 1000 yozuvdan ortig'i prune qilinadi.
- Channel monitor access-failure sanog'i `_process_monitored_channel_message()` ichiga ko'chirildi.
- Retry callback private/protected loglarda user sessionni yangi `create_user_session()` bilan tekshiradi; public bot-only retry sessiyasiz davom eta oladi.

### Tekshiruv:
- `.\venv\Scripts\python.exe -m compileall -q TechVJ core database main.py test_governance_fixes.py`
- `pytest -q` -> 18 passed
- SQLite schema/read smoke -> `sqlite_failed_downloads_ok True`

---

## Sessiya 7 (2026-05-26) - `/grab <link>` chat-local va topic chronological range

### So'rovlar ketma-ketligi:
1. **Bot API orqali `/grab <link>`** - qaysi private chatga yuborilsa, link o'sha chatdagi user yuborgandek ishlashi kerak.
2. **Bitta topic xabarlarini olish** - topicdagi message IDlar oshib borishiga tayanmaslik; range ketma-ketligi topicga joylangan vaqt tartibiga qarab bo'lishi kerak.

### Natija:
- **BUG-028**: `/grab <t.me/link>` joriy chat user contextida `save()` pipelinega proxy qilinadi.
- Owner uchun eski `/grab <uid> <t.me/link>` delegated formati saqlandi.
- Topic range `FROM-TO` numeric ID range emas, anchor IDlar sifatida talqin qilinadi.
- `TopicExtractor.extract_between()` anchorlar orasini xronologik topic order bo'yicha kesadi.
- Public forum topic linklari ham topic extractor yo'liga ulandi.
- Regression test qo'shildi: non-monotonic IDlar bilan chronological range.

---

## Sessiya 8 (2026-05-26) - Activity tracker, retry diagnostika, auto-disable va failed logs

### So'rovlar ketma-ketligi:
1. **Activity Tracker integratsiyasi** - `TechVJ/activity_tracker.py`dan foydalanib asosiy handler va download/upload oqimlarida user session activity kuzatish.
2. **Owner diagnostika retry** - post failure reportida `Qayta urinish` inline tugmasi bo'lishi va callback orqali postni majburiy retry qilish.
3. **/addchannel auto-disable** - tizim sessiyasi protected kanalga kira olmay qolsa, ketma-ket 3 access xatosidan keyin monitorni avtomatik o'chirish.
4. **Failed logs** - MongoDB M0 limitlarini tejash uchun xatoliklarni SQLite `local_db/bot_storage.db` ichida saqlash va `/failed_logs` komandasi bilan ko'rsatish.

### Natija:
- **BUG-029**: `failed_downloads` SQLite jadvali, log pruning (1000), `/failed_logs`, owner-only retry callback qo'shildi.
- Owner diagnostika reportlari retry URL/log ID yaratadi va oxirgi report chunkga inline retry tugmasini qo'shadi.
- Channel monitor access xatolari ketma-ket 3 marta yuz bersa `enabled=False` qilinadi va ownerga bot client orqali xabar yuboriladi.
- `track_activity` `save`, `stop`, `comment`, monitor handlerlari hamda private/topic/public/bot/single-post/download oqimlariga ulandi.
- Batch/range xatolari endi tsikl oxirida emas, post aniqlangan zahoti userga va ownerga yuboriladi.

### Tekshiruv:
- `.\venv\Scripts\python.exe -m compileall -q TechVJ core database main.py test_governance_fixes.py`
- `pytest -q` -> 18 passed

---

## Sessiya 1 (2026-03-23) — Bug fixlar: cancellation, reply, pool cleanup

### So'rovlar ketma-ketligi:
1. **Download cancellation muammosi** — `/stop` yuborilganda download to'xtamaydi, `FileNotFoundError` retry loop'ga kiradi
2. **Reply ishlamaydi** — Bot yuborgan media userning original havolasiga reply qilmaydi
3. **Pool session oraliq xabar** — Premium pool sessiya bot chatiga yuklagan xabar tozalanmaydi
4. **Premium upload audit** — Pool sessiya orqali upload oddiy (non-premium) user uchun ishlayaptimi?

### Natija:
- 6 ta bug topildi va hal qilindi (BUG-001 dan BUG-006 gacha, `data/BUGS.md` da)
- Cancellation tartibi to'g'irlandi (engine avval, keyin task_manager)
- Reply delivery to'g'irlandi (upload_chat_id = bot_user_id)
- Pool session intermediate message cleanup qo'shildi

---

## Sessiya 2 (2026-03-24) — Login bug, premium flow audit, hujjatlashtirish

### So'rovlar ketma-ketligi:
1. **Login struct.error** — User login qilganda `export_session_string()` da `struct.error` crash. Sabab: eski `.session` fayl qolgan.
   - Fix: Login boshida eski session fayllarini tozalash + try/except
2. **Premium session foydalanish tartibi** — Hozirgi premium upload flow'ni tekshirish: qaysi tizim ishlaydi, qaysi ishlamaydi
   - Audit: SessionManager < 2GB uchun ishlaydi. Legacy pool amalda ishlamaydi (session_string override). > 2GB faqat user o'z premium sessiyasi.
3. **CLAUDE.md ga yozish** — Barcha funksiyalar, flow, komandalar CLAUDE.md ga to'liq aks etsin. Har safar "shunday qilganman" deyavermay, yangi sessiya o'zi bilsin.
4. **Prompt tarixi** — Foydalanuvchi promptlarini saqlash, ketma-ketlikdan xulosa chiqarish

### Natija:
- BUG-005 hal qilindi (`data/BUGS.md` yangilandi)
- CLAUDE.md §21 qo'shildi — Premium session to'liq flow hujjati
- `data/PROMPTS.md` yaratildi — prompt tarixi treker

---

## UMUMIY YO'NALISH

Foydalanuvchi loyihaning ishonchliligini oshirmoqda:
- **Stabilitiy:** race condition, crash, retry loop buglarni topib hal qilish
- **Premium upload:** Pool sessiyalar orqali non-premium userlarga premium upload imkoniyati
- **Hujjatlashtirish:** Har bir o'zgartish CLAUDE.md va BUGS.md ga yozilishi — yangi Claude sessiyalari uchun
- **Kontekst saqlanishi:** Har sessiya oldingi sessiyaning ishlarini bilishi kerak

### Keyingi ehtimoliy yo'nalishlar (foydalanuvchi hali so'ramagan):
- > 2GB fayllar uchun tizim pool sessiyalarini qo'llab-quvvatlash
- Login jarayonini yanada mustahkamlash (QR login fallback, session migration)

---

## Sessiya 2 davomi (2026-03-24) — Premium relay production spec

### So'rovlar ketma-ketligi:
5. **Premium relay production spec** — Foydalanuvchi to'liq MTProto engineer spesifikatsiyasi berdi:
   - Pool session → bot chat → copy_message → delete (relay flow)
   - copy muvaffaqiyatsiz → delete QILMA
   - FloodWait → retry with backoff
   - Barcha media turlarini qo'llab-quvvatlash
   - Concurrent task + caching

### Natija:
- **BUG-007 topildi va hal qilindi:** Legacy pool sessiyalar < 2GB da amalda ishlamasdi
  - Fix: session tanlash tartibi o'zgartirildi — pool AVVAL, user session OXIRIDA
- **BUG-008 topildi va hal qilindi:** copy_message muvaffaqiyatsiz bo'lsa ham delete qilinardi
  - Fix: copy 3x retry + faqat copy OK bo'lsa delete + FloodWait handle
- Ikkala fix ham `save.py` VA `session_manager.py` da qo'llanildi

---

## Sessiya 2 davomi (2026-03-24) — Production spec audit + Pyrofork bugfix

### So'rovlar ketma-ketligi:
6. **Production maintenance spec** — Foydalanuvchi ikkinchi spec berdi:
   - PeerResolver (HECH QACHON access_hash=0)
   - Upload stability, semaphore nazorati
   - API modernizatsiyasi (reply_parameters)
   - Zombie sessiya/hanging task cleanup

7. **Reply ishlamaydi** — "user o'zi bot chatiga upload qilgan lari asosiy havolaga replay qilinmayapti"

8. **Login muammosi** — "kimdir login qilgan keyin kodni orasini ochib yozgan lekin botdan javob kelmagan havola jo'natsa qaytadan ulan deyapti"

### Natija:
- **BUG-009 topildi va hal qilindi:** `access_hash=0` ishlatilishi 2 ta production faylda
  - Fix: `resolve_peer()` yoki `get_chat()` bilan almashtirildi
  - `from pyrogram import raw` importlari olib tashlandi
- **BUG-010 topildi va hal qilindi (MUHIM):** Pyrofork 2.3.69 `utils.get_reply_to()` parametr nomlari xato
  - `reply_to_message_id` → TLObject `reply_to_msg_id` kutadi → **TypeError**
  - Bu xato BARCHA `reply_to_message_id` ishlatgan chaqiruvlarni buzadi
  - Ta'sir: media reply ishlamaydi, "need_login" xabari yuborilmaydi, copy_message ishlamaydi
  - Fix: `core/pyrofork_compat.py` da monkey-patch + venv fayllarini to'g'ridan tuzatish
- **Zombie worker reaper qo'shildi:** `UserWorkerRegistry` da 10 daqiqa idle timeout
  - Har 2 daqiqada idle worker'lar tekshiriladi va to'xtatiladi
- **Stale borrow recovery qo'shildi:** `BorrowManager` da 10 daqiqa borrow timeout
  - Stuck borrow'lar avtomatik qaytariladi

### Xulosa:
Pyrofork 2.3.69 dagi `get_reply_to()` bagi ASOSIY muammo edi — bu reply ishlamasligini, ba'zi xabarlar yetib bormasligini, va ba'zi upload'lar muvaffaqiyatsiz bo'lishini tushuntiradi.

---

## Sessiya 2 davomi (2026-03-24) — Login tizimi to'liq qayta ishlash

### So'rovlar ketma-ketligi:
9. **Login muammolari** — User bir nechta login bilan bog'liq muammolarni xabar qildi:
   - Logout qilgandan keyin `/login` "already logged in" deb rad qilmoqda
   - OTP ba'zan qabul qilinmayapti
   - Login qilib, havola jo'natganda bot javob bermayapti
   - Media asosiy havolaga reply qilmayapti

10. **Login tizimi to'liq overhaul** — Foydalanuvchi ishonchli, production-ready login tizimi so'radi:
    - To'g'ri login holat boshqaruvi (per-user)
    - Session saqlash va bekor qilish
    - To'liq logout tozalash
    - Ishonchli OTP boshqaruvi
    - 2FA qo'llab-quvvatlash
    - "already logged in" soxta pozitivlarni oldini olish
    - Session validatsiya (get_me())

### Natija:
- **BUG-011 topildi va hal qilindi (KRITIK):** `async_db.py` da ikkita `update_user` metod — ikkinchisi birinchisini shadow qilardi, cache invalidation yo'qolardi
  - Bu logout ishlamasligining ASOSIY sababi edi
  - Fix: Ikkala metod bitta universal metod qilib birlashtirildi
- **BUG-012 topildi va hal qilindi:** Logout faqat DB tozalardi — fayl sessiyalar, worker'lar qolardi
  - Fix: To'liq logout — DB + file + worker + verify
- **BUG-013 topildi va hal qilindi:** Login OTP ishonchliligi — session validatsiya, format strip, buyruq filter
  - Fix: `quick_session_check()`, OTP format tozalash, `get_me()` validatsiya
- **QR login NameError tuzatildi:** `user_id` aniqlashdan oldin ishlatilardi
  - Fix: `user_id = int(message.from_user.id)` session check blokidan OLDIN ko'chirildi

### Texnik tafsilotlar:
- `database/async_db.py`: `update_user` metod birlashtirish — dict VA kwargs qabul qiladi, HAR DOIM `_USER_CACHE.pop()`
- `TechVJ/generate.py`: Login handler — session validatsiya, OTP retry loop, `get_me()` check
- `TechVJ/generate.py`: Logout handler — to'liq tozalash (DB + file + worker + verify)
- `TechVJ/generate.py`: QR login — `user_id` tartibi tuzatildi

---

## UMUMIY YO'NALISH

Foydalanuvchi loyihaning ishonchliligini oshirmoqda:
- **Stabilitiy:** race condition, crash, retry loop, cache invalidation buglarni topib hal qilish
- **Premium upload:** Pool sessiyalar orqali non-premium userlarga premium upload imkoniyati
- **Login tizimi:** Ishonchli autentifikatsiya — sessiya validatsiya, to'liq logout, OTP ishonchliligi
- **Hujjatlashtirish:** Har bir o'zgartish CLAUDE.md, BUGS.md va PROMPTS.md ga yozilishi

---

## Sessiya 6 (2026-05-25) - Owner diagnostika va ownerhelp yangilash

### So'rovlar ketma-ketligi:
1. **Ownerhelp tekshiruvi** - kanal monitor/grab komandlari `/ownerhelp`da chiqyaptimi tekshirish.
2. **User post xatolari ownerga ketsin** - user kanal contentini ola olmasa/yubora olmasa, ownerga user_id, post_id, xato sababi va post tarkibi haqida aniq report yuborish.
3. **Shovqinni kamaytirish** - report faqat session faol, kanal mavjud va user kanalga a'zo ekani tasdiqlanganda yuborilishi kerak.

### Natija:
- **BUG-027**: `save.py`ga verified owner diagnostic report qo'shildi.
- Report bot client orqali yuboriladi; user session text yubormaydi.
- Private/topic/album/user-session public xatolik yo'llari diagnostikaga ulandi.
- `/ownerhelp`ga `/addchannel`, `/removechannel`, `/channels`, `/togglechannel`, `/grab` va avto diagnostika izohi qo'shildi.

### Tekshiruv:
- `python -m compileall -q TechVJ/save.py TechVJ/owner_commands.py`

---

## Sessiya 5 (2026-05-24) - Owner command va kanal monitor integratsiyasi

### So'rovlar ketma-ketligi:
1. **Owner commandlar ishlamayapti** - ayrim owner buyruqlar, xususan t.me linkli buyruqlar asosiy save handlerga aralashmoqda.
2. **Telegram kanal kontent olish taqiqlangan ishlamayapti** - `/addchannel` bilan qo'shilgan protected kanal runtime'da postlarni olib targetga yubormayapti.
3. **Owner user nomidan havola yuborsa** - owner tomonidan user uchun yuborilgan link o'sha user yuborgandek qabul qilinishi kerak.

### Natija:
- **BUG-026**: `save()` command exclude ro'yxatiga `/grab`, `/addchannel`, `/removechannel`, `/channels`, `/togglechannel` qo'shildi.
- Channel monitor uchun `filters.channel` handler qo'shildi: monitored kanal postlari text/media/poll/album bo'yicha mavjud pipeline orqali target chatga yuboriladi.
- Protected media uchun session tanlash tartibi: SessionManager GLOBAL/BORROWABLE, legacy system premium, keyin owner DB session.
- SessionManagerga background/system upload helperlari qo'shildi.

### Tekshiruv:
- `python -m compileall -q TechVJ core`

---

## UMUMIY YO'NALISH

Foydalanuvchi owner boshqaruv qatlamini production holatiga yaqinlashtirmoqda:
- **Owner command routing:** commandlar asosiy link handler bilan urishmasligi kerak
- **Protected channel monitor:** kanal postlari runtime handler orqali real yuborilishi kerak
- **User-nomidan ishlash:** owner orchestration user context/session bilan ishlashi kerak

---

## Sessiya 4 (2026-05-18) - Governance va pool delivery bugfixlar

### So'rovlar ketma-ketligi:
1. **"juda ko'p buglar bor shularni top va to'g'irla"** - umumiy audit va yuqori ishonchli bugfixlar.

### Natija:
- **BUG-022**: `RateLimiter.check()` 0 limitda `IndexError` bermaydigan qilindi
- **BUG-023**: `permission_guard` yangi user uchun aynan 1 post talab qiladi
- **BUG-024**: `PriorityQueue` spill-over real ishlaydigan qilindi va worker capacity runtime limitlarga moslandi
- **BUG-025**: Pool delivery `copy_message` manba chatini `from_user.id` orqali aniqlaydi; `SessionManager` copy failure'da success qaytarmaydi
- Regression testlar qo'shildi: `test_governance_fixes.py`

### Tekshiruv:
- `python -m compileall -q core TechVJ test_governance_fixes.py`
- `pytest -q` -> 17 passed

---

## UMUMIY YO'NALISH

Foydalanuvchi loyihaning ishonchliligini oshirmoqda:
- **Governance qatlam:** rate limit, role guard va priority scheduling production holatiga yaqinlashtirilmoqda
- **Premium/pool delivery:** copy/delete manba chatlari va failure propagation alohida tekshirilmoqda
- **Regression testlar:** topilgan buglar qaytmasligi uchun kichik, network talab qilmaydigan testlar qo'shilmoqda
- **Hujjatlashtirish:** Har bir bug `data/BUGS.md`, har bir sessiya `data/PROMPTS.md`ga yoziladi
- **Kontekst saqlanishi:** Har sessiya oldingi sessiyaning ishlarini bilishi kerak

### Keyingi ehtimoliy yo'nalishlar (foydalanuvchi hali so'ramagan):
- > 2GB fayllar uchun tizim pool sessiyalarini qo'llab-quvvatlash
- Login jarayonini yanada mustahkamlash (QR login fallback, session migration)
- `priority_queue.py` integratsiyasi (hozir dead code)
- Global upload concurrency semaphore qo'shish

---

## Sessiya 3 (2026-05-06) — Routing izolyatsiya + album/premium/blocked fixlar

### So'rovlar ketma-ketligi:
1. **Audio-only album yuborilmasligi** — media_group non-photo bo'lsa album pipeline noto'g'ri ishga tushadi
2. **Noto'g'ri post yuborish** — bot va user session aralashuvi sababli noto'g'ri manbadan xabar kelishi
3. **Cross-user leak** — peer/access_hash kesh bilan chat konteksti aralashishi
4. **Bot blocked recovery** — USER_IS_BLOCKED holatida unblock + /start + retry
5. **VIP rol** — yuqori navbat prioriteti va premium pipeline access

### Natija:
- **BUG-019**: Audio-only media grouplar endi album pipeline emas, single-send pipeline bilan yuboriladi
- **BUG-020**: TaskContext + per-request resolve_peer; public oqimda single-client ishlaydi
- **BUG-021**: UserUploadWorker blok holatini aniqlab unblock va retry qiladi
- **PriorityQueue integratsiya**: VIP rolga prioritet scheduling qo'llandi

---

## UMUMIY YO'NALISH

Foydalanuvchi loyihaning ishonchliligini oshirmoqda:
- **Stabilitiy:** race condition, crash, retry loop, cache invalidation buglarni topib hal qilish
- **Routing izolyatsiya:** per-task context, peer resolve va client separation
- **Premium upload:** VIP foydalanuvchilar uchun premium pipeline va prioritet
- **Login tizimi:** Ishonchli autentifikatsiya — sessiya validatsiya, to'liq logout, OTP ishonchliligi
- **Hujjatlashtirish:** Har bir o'zgartish CLAUDE.md, BUGS.md va PROMPTS.md ga yozilishi
