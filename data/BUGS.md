# BUG TRACKER — loyiha/new Telegram Bot

Bu fayl topilgan buglar, ularning sabablari va qanday hal qilinganligini kuzatib boradi.
Yangi Claude sessiyalari bu faylni o'qib, oldingi ishlangan buglarni biladi.

---

## BUG-038: TopicExtractor Search va GetReplies ikkalasi ham topic xabarlarini topmaydi
**Sana:** 2026-06-20
**Holat:** Hal qilindi

**Muammo:** Production logda `https://t.me/indoc_info/1465/1476-1536` va `https://t.me/c/2346917200/15/806-877` uchun `messages.Search(top_msg_id=...)` faqat 1 ta xabar yig'di. GetReplies retry ham 1 ta xabar qaytardi. Natijada `extract_between()` ikki anchorni topmadi va `topic range anchor not found` bilan to'xtadi. Raw forum topic metadata xabarlar mavjudligini ko'rsatsa-da, server-side topic filterlar ularni bermaydi.

**Sabab:** 
1. Ayrim forum kanallarida `messages.Search` va `messages.GetReplies` to'liq topic streamni bermasligi mumkin. Server-side topic index ba'zi topiclar uchun to'liq ishlamaydi.
2. Fallback sifatida xabarlarni to'g'ridan-to'g'ri `get_messages` orqali olganda, Pyrogram forum topic xabarlariga `reply_to_top_message_id`ni qo'shmaydi, balki to'g'ridan-to'g'ri `message_thread_id` atributini to'ldiradi. Eski `_is_topic_message` filter buni inobatga olmagani uchun to'g'ri olingan xabarlarni ham tashlab yuborardi.

**Yechim:**
1. `TopicExtractor.extract_between()` ga uchinchi fallback bosqich qo'shildi: Search va GetReplies ikkalasi ham anchorlarni topmasa, `_direct_id_range_fallback()` ishga tushadi.
2. Yangi `_direct_id_range_fallback()` metodi `[min, max]` orasidagi BARCHA message IDlarni to'g'ridan-to'g'ri `channels.GetMessages` bilan oladi (maksimum 500 span).
3. `TopicExtractor._is_topic_message` filtri yangilandi: Endi u `msg.message_thread_id == topic_id` ni ham tekshiradi. Pyrogram/Pyrofork raw xabarlarini topicga ajratish shu orqali aniq ishlaydi.

**O'zgartirilgan fayllar:** `core/topic_extractor.py`, `data/BUGS.md`

---

## BUG-037: TopicExtractor `GetHistory`ni birinchi ishlatgani sabab FloodWait davom etadi
**Sana:** 2026-06-20
**Holat:** Hal qilindi

**Muammo:** Topic extraction productionda hali ham `messages.GetHistory FloodWait 20-30 seconds` keltiryapti. Sabab: `TopicExtractor` server-side topic filter sifatida `get_chat_history(message_thread_id=topic_id)`ni birinchi sinab ko'radi. Pyrogram/Pyrofork bu yo'lni baribir `messages.GetHistory` orqali bajaradi, shuning uchun katta forumlarda topic ichidagi 100 xabarni olishdan oldin chat history pressure paydo bo'ladi.

**Sabab:**
1. Raw `messages.Search(top_msg_id=topic_id)` va `messages.GetReplies` mavjud bo'lsa ham, extractor avval high-level history pathga kirardi.
2. `core/guards.py` va `/group` analiz helperlarida eski history scan/range probing fallbacklar qolgan edi.
3. Topic cache yo'q edi; bir topic qayta so'ralganda avvalgi anchor/order natijasi ishlatilmasdi.

**Yechim:**
1. `TopicExtractor` priority tartibi o'zgartirildi: raw `messages.Search(top_msg_id)` → raw `messages.GetReplies` → faqat explicit `allow_history_scan_fallback=True` bo'lsa `get_chat_history`.
2. `messages.GetDiscussionMessage` metadata/root discovery uchun best-effort qo'shildi; topic root raw `channels.GetMessages` orqali hydrate qilinadi.
3. `core/topic_cache.py` qo'shildi: `data/topic_cache/topics.json` ichida `chat_id`, `topic_id`, `root_message_id`, `known_message_ids`, `last_processed_message_id`, `fully_scanned` saqlanadi.
4. Range anchorlari cache'da bo'lsa discovery umuman qilinmaydi; IDlar batch raw `channels.GetMessages` bilan hydrate qilinadi.
5. Full topic qayta so'ralganda cache boundary (`last_processed_message_id`)gacha yangi sahifalar olinadi; known xabarlar qayta scan qilinmaydi.
6. `process_topic_posts()` extractor path loglarini (`raw_search`, `raw_replies`, `history`, `cache`) chiqaradi.
7. `core/guards.py` topic helperlari va `/group` topic analiz yo'li `TopicExtractor`ga delegatsiya qilindi; eski flood-prone history scan fallbacklar olib tashlandi yoki faqat explicit fallback sifatida qoldi.
8. Regression testlar qo'shildi: raw Search historysiz ishlashi, Search bo'sh bo'lsa GetReplies fallback, cache hit discovery qilmasligi.

**O'zgartirilgan fayllar:** `core/topic_extractor.py`, `core/topic_cache.py`, `core/guards.py`, `TechVJ/save.py`, `TechVJ/group_command.py`, `test_governance_fixes.py`, `data/BUGS.md`, `data/PROMPTS.md`

---

## BUG-036: Raw GetReplies forum topic range anchorlarini topmaydi
**Sana:** 2026-06-13
**Holat:** Hal qilindi

**Muammo:** Production logda `https://t.me/c/2234521267/8/75-171796` uchun `get_chat_history(message_thread_id=...)` yo'qligi sabab raw fallback ishga tushdi, lekin `messages.GetReplies(peer, msg_id=8)` faqat 1 ta message yig'di. Natijada `75` va `171796` anchorlari topilmadi va task `topic range anchor not found` bilan to'xtadi.

**Sabab:**
1. Telegram/Pyroforkda `messages.GetReplies` forum topic tarixini hamma chatlarda to'liq topic stream sifatida bermaydi; ayrim holatda faqat topic root yoki direct replies qaytadi.
2. Pyroforkning `search_messages(thread_id=...)` implementatsiyasi aslida raw `messages.Search(..., top_msg_id=thread_id)` ishlatadi. Bu forum topic ichini server-side filter qilish uchun to'g'riroq yo'l.
3. Search pagination `offset_id` bilan emas, `add_offset` bilan yuradi; aks holda birinchi sahifa qayta olinishi yoki anchorlar topilmasligi mumkin.
4. Pyrogram/Pyrofork history/search sahifasi amalda 100 limit bilan ishlaydi; 200 limit kutish oxirgi sahifani noto'g'ri aniqlashi mumkin edi.

**Yechim:**
1. `TopicExtractor`da `message_thread_id` TypeError bo'lsa fallback tartibi o'zgartirildi: avval raw `messages.Search(top_msg_id=topic_id)`, keyin zaxira sifatida `messages.GetReplies`.
2. Raw Search sahifalash `add_offset += len(page)` bilan qilindi; GetReplies/history esa `offset_id=page[-1].id` bilan qoldi.
3. Page limit 100 ga clamp qilindi va last-page tekshiruvi shu real limitga moslandi.
4. Agar raw Search anchorlarni topmasa, `extract_between()` bir marta `GetReplies` bilan retry qiladi.
5. Topic flow `acc.resolve_peer(channel_id)` natijasini saqlaydi va `TopicExtractor`ga beradi; bu stale/access_hash qayta-resolve muammosini kamaytiradi.
6. `get_chat().is_forum` yo'q bo'lsa fatal hisoblanmaydi. Buning o'rniga raw `messages.GetForumTopicsByID(peer=resolved_peer, topics=[topic_id])` bilan topic metadata tekshiriladi.
7. Raw forum topic `top_message` faqat diagnostika uchun loglanadi. Extractor har doim URLdagi topic root id bilan ishlaydi (`/c/<chat>/<topic>/<from>-<to>` formatida middle segment: masalan `8` yoki `1465`). `top_message` topicdagi oxirgi/yuqori xabar bo'lishi mumkin, root emas.
8. Regression testlar qo'shildi: raw Search primary fallback, raw GetReplies secondary fallback, Search pagination `add_offset`, va avvalgi FloodPremiumWait/topic anchor testlari.

**O'zgartirilgan fayllar:** `TechVJ/save.py`, `core/topic_extractor.py`, `test_governance_fixes.py`, `data/BUGS.md`, `data/PROMPTS.md`

---

## BUG-035: FloodPremiumWait stacktrace va topic range GetHistory bosimi
**Sana:** 2026-06-12
**Holat:** Hal qilindi

**Muammo:** Katta forum topic range (`/c/<chat>/<topic>/<from>-<to>`, masalan `75-171796`) ishlovida production logda `FloodPremiumWait: [420 FLOOD_PREMIUM_WAIT_X] (upload.SaveBigFilePart)` stacktrace chiqdi. Shu paytda `messages.GetHistory` ham 21-22s FloodWait berib turardi va task media yuborishga yetmasdan topic scan ichida uzoq qolardi.

**Sabab:**
1. Pyrofork 2.3.69 `FLOOD_PREMIUM_WAIT_X`ni oddiy `FloodWait` subclassi sifatida emas, alohida `FloodPremiumWait` classi sifatida chiqarishi mumkin. Mavjud `except FloodWait` bloklari uni ushlamasdi, natijada upload worker/session manager uni oddiy exception yoki stacktrace sifatida ko'rardi.
2. Pyrofork `get_chat_history(message_thread_id=...)`ni qo'llamasa, extractor butun chat history'ni scan fallbackga o'tardi. Katta forumlarda bu server-side topic filter emas, chat bo'ylab `messages.GetHistory` sahifalash bo'lib, FloodWait olib taskni media yuborishga yetkazmasdi.
3. `TopicExtractor.extract_between()` range anchorlar topilgandan keyin ham butun topicni oxirigacha yig'ardi. Katta topiclarda bu keraksiz sahifalar va FloodWait bosimini oshirardi.

**Yechim:**
1. `core/retry_utils.py`ga `is_floodwait_error()` qo'shildi: `FloodWait`, `FLOOD_WAIT`, `FLOOD_PREMIUM_WAIT_X`, class name, `ID`, `.value`, `.x` va stringdagi "wait of N seconds" variantlarini taniydi.
2. `get_floodwait_seconds()` umumiy helperga aylantirildi va `FLOOD_PREMIUM_WAIT_X` matnidan ham sekundni ajratadi.
3. `UserUploadWorker`, `SessionManager`, legacy pool fallback va `download_and_send_media()` flood-like exceptionlarni oddiy error emas, mavjud FloodWait yo'liga uzatadi: short wait bo'lsa retry, long/pool wait bo'lsa rotation/cooldown.
4. `TopicExtractor` fallback yo'li qayta yozildi: `message_thread_id` TypeError bersa, endi butun chat `get_chat_history()` scan qilinmaydi; raw MTProto `messages.GetReplies(peer, msg_id=topic_id, ...)` ishlatiladi. Bu serverdan aynan topic root replies/thread xabarlarini oladi.
5. `get_chat_history` full-scan fallback productionda default o'chirildi (`allow_history_scan_fallback=False`). Faqat explicit yoqilsa ishlaydi.
6. Raw `GetReplies` natijasi `pyrogram.utils.parse_messages()` orqali oddiy Pyrogram `Message` obyektlariga aylantiriladi; mavjud media/text yuborish pipeline o'zgarmaydi.
7. `TopicExtractorConfig.stop_after_ids` qo'shildi. Topic range anchorlari topilgach extractor sahifalashni to'xtatadi, lekin xronologik slice uchun anchorlar orasidagi allaqachon olingan xabarlarni saqlaydi.
8. `process_topic_posts()` range anchorlarini `stop_after_ids` sifatida uzatadi.
9. Regression testlar qo'shildi: raw `GetReplies` fallback ishlashi, topic range anchor topilgach erta to'xtashi va `FloodPremiumWait` matnini flood sifatida tanish.

**O'zgartirilgan fayllar:** `core/retry_utils.py`, `core/topic_extractor.py`, `core/user_upload_worker.py`, `core/session_manager/session_manager.py`, `TechVJ/save.py`, `test_governance_fixes.py`

---

## BUG-034: Deleted/missing postlar owner diagnostika spamiga aylanadi
**Sana:** 2026-06-07
**Holat:** Hal qilindi

**Muammo:** BUG-027..030 dan keyin private/topic/public batchlarda `post is deleted or inaccessible`, `message not found`, `MSG_ID_INVALID`, message gap, FloodWait va shunga o'xshash normal Telegram holatlari `_notify_realtime_post_failure()` / `_notify_bot_only_post_failure()` orqali owner chatiga va failed log retry tizimiga tushib ketardi. Bular bot nosozligi emas: post o'chirilgan, ID mavjud emas, user eski/noto'g'ri link yuborgan yoki source xabar endi o'qilmaydi.

**Sabab:** Reporting helperlari failure turini ajratmasdan barcha per-post xatolarni owner diagnostika deb hisoblagan. `topic_album` kabi stage nomlarida "album" borligi ham oddiy missing postni system failure sifatida ko'rishga sabab bo'lishi mumkin edi.

**Yechim:**
1. `core/failure_classifier.py` faol reporting qatlamiga ulandi: `EXPECTED_TELEGRAM_STATE`, `USER_INPUT_ERROR`, `SYSTEM_FAILURE`.
2. `TechVJ/save.py` ichida `_notify_owner_channel_post_failure()`, `_notify_realtime_post_failure()` va `_notify_bot_only_post_failure()` classification gate orqali ishlaydi.
3. Expected Telegram state (deleted/missing/inaccessible/message gap/FloodWait/timeout/source access restriction) ownerga yuborilmaydi, bot-only per-post notice ham yuborilmaydi, failed-download retry logga yozilmaydi; mavjud counter/statistika oqimi saqlanadi.
4. System failure (upload/relay/routing/copy mismatch/session/queue/worker/pool/topic extractor unexpected failure, `PEER_ID_INVALID`, `CHANNEL_INVALID`, corruption va hokazo) faqat message fetched yoki processing started yoki real system exception bo'lsa ownerga chiqadi.
5. `TopicExtractor` xatosi ham shu gate orqali report qilinadi: kutilgan Telegram access/missing holati jim, real extractor bug esa reportable.
6. Regression testlar qo'shildi: deleted post jim, album+message-not-found ownerga chiqmaydi, upload failure va `PEER_ID_INVALID` system failure bo'lib qoladi.

**O'zgartirilgan fayllar:** `TechVJ/save.py`, `core/failure_classifier.py`, `test_governance_fixes.py`

---

## BUG-033: Sender isolation pool upload'larda noto'g'ri owner_user_id bilan qayta ishlangan
**Sana:** 2026-05-29
**Holat:** ✅ Hal qilindi

**Muammo:** BUG-032 fix dan keyin `_enqueue_media_delivery` (save.py) va `upload_with_session` (session_manager.py) `owner_user_id` parametrini `enqueue_task` ga umuman uzatmasdi. Buning sababi: avvalgi tahrir `session_user_id` (pool akkaunt ID) ni `owner_user_id` sifatida uzatardi, lekin `UserUploadWorker._run_task` da sender isolation `task.owner_user_id == self._user_id` ni tekshiradi va `_user_id` — bu so'rov yuborayotgan user (pool akkaunt EMAS), shuning uchun har bir pool task RUN qilinmasdan REJECT qilinardi. Vaqtinchalik fix `owner_user_id` ni umuman uzatmaslik bo'ldi, bu sender isolationni o'chirib qo'ydi.

**Sabab:** Logika xato — `owner_user_id` semantikasi "kim so'rov yuborgan" (worker `_user_id` bilan mos), pool akkaunt ID emas.

**Yechim:**
1. `_enqueue_media_delivery` (save.py) endi `owner_user_id=int(target_user_id)` uzatadi — bu so'rov yuborgan user.
2. `upload_with_session` (session_manager.py) endi `owner_user_id=user_id` uzatadi — bu ham so'rov yuborgan user.
3. Sender isolation endi to'g'ri ishlaydi: `task.owner_user_id == worker._user_id` har doim mos keladi, va boshqa userdan kelgan task tasodifan o'tib ketmaydi.
4. `test_governance_fixes.py` testlari ham yangi semantikaga moslashtirildi (assert `owner_user_id == 123` — requesting user, not 222/333 — pool account).

**O'zgartirilgan fayllar:** `TechVJ/save.py`, `core/session_manager/session_manager.py`, `test_governance_fixes.py`

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

## BUG-027: Post yuborilmaganida ownerga aniq diagnostika ketmaydi
**Sana:** 2026-05-25
**Holat:** Hal qilindi

**Muammo:** User kanal postini ola olmasa yoki post yuborilmasa, owner faqat userdan kelgan umumiy shikoyat orqali bilardi. Qaysi user, qaysi kanal/post ID, qaysi bosqichda xato bo'lgani va post tarkibi haqida avtomatik ma'lumot yo'q edi.

**Yechim:**
1. `save.py`ga owner diagnostika helperlari qo'shildi.
2. Report faqat session faol (`get_me`), kanal mavjud (`get_chat`) va user kanalga a'zoligi (`get_chat_member` yoki private numeric chat access) tasdiqlangandan keyin yuboriladi.
3. Report bot client orqali owner chatiga yuboriladi: user_id, post_id, post linki, URL turi, xato bosqichi, xato sababi, kanal ma'lumoti, post turi, media hajmi/fayl nomi, caption/text preview, poll va album flaglari.
4. Private, topic, album va user-session public post xatolik yo'llariga ulab qo'yildi.
5. `/ownerhelp`ga kanal monitor/grab komandlari va avto diagnostika izohi qo'shildi.

**O'zgartirilgan fayllar:** `TechVJ/save.py`, `TechVJ/owner_commands.py`

---

## BUG-028: `/grab <link>` chat-local emas va topic range ID tartibiga bog'langan
**Sana:** 2026-05-26
**Holat:** Hal qilindi

**Muammo:** `/grab` faqat ownerning `/grab <uid> <t.me/link>` formatida ishlardi. Bot API orqali biror user chatiga `/grab <link>` yuborilganda link shu chatdagi user yuborgandek hisoblanmasdi. Forum topic range uchun esa `FROM-TO` numeric message ID range sifatida kengaytirilardi; topic ichidagi message IDlar real joylangan vaqt tartibiga doim mos kelmaydi.

**Yechim:**
1. `/grab <t.me/link>` joriy private chat useri nomidan proxy message yaratadigan qilindi; owner uchun eski `/grab <uid> <t.me/link>` formati saqlandi.
2. Bot-authored/outgoing `/grab` xabarlari Bot API orchestration uchun qabul qilinadi, lekin arbitrary userga delegated grab faqat ownerga ruxsat.
3. Topic `FROM-TO` endi numeric range emas, ikkita anchor ID sifatida saqlanadi.
4. `TopicExtractor.extract_between()` topic xabarlarini xronologik `(date, id)` tartibda yig'ib, anchorlar orasini shu tartib bo'yicha kesadi.
5. Pyrofork `get_chat_history(message_thread_id=...)` qo'llamasa, extractor chat history scan + topic membership filter fallback ishlatadi.
6. Public forum topic linklari ham `topic` routerga o'tkazildi.

**O'zgartirilgan fayllar:** `TechVJ/owner_commands.py`, `TechVJ/save.py`, `core/topic_extractor.py`, `test_governance_fixes.py`

---

## BUG-029: Owner diagnostika retry/logsiz, monitor access loop va session activity kuzatuvi yetarli emas
**Sana:** 2026-05-26
**Holat:** Hal qilindi

**Muammo:** Owner diagnostika xabarlari chatda yuqoriga chiqib ketardi va qayta urinish uchun kontekst saqlanmasdi. `/addchannel` monitor qilingan protected kanalga tizim sessiyasi kira olmay qolsa, handler xatoni qayta-qayta ishlab loop/spam xavfini tug'dirardi. User session activity tracker esa asosiy save oqimlariga ulanmagan edi.

**Yechim:**
1. `failed_downloads` SQLite jadvali qo'shildi; oxirgi 1000 yozuv saqlanadi.
2. Owner diagnostika reportlari SQLite log ID bilan yoziladi va oxirgi chunkga `Qayta urinish` inline tugmasi qo'shildi.
3. `retry_failed:<id>` callback owner-only qilindi; private user loglari `save()` proxy orqali, kanal monitor target chat loglari tizim/owner sessiyasi bilan bitta post retry qiladi.
4. `/failed_logs` owner komandasi oxirgi 10 xatolikni qisqa ko'rsatadi.
5. Channel monitor access xatolari (`USER_BANNED_IN_CHANNEL`, `CHANNEL_PRIVATE`, `CHAT_ADMIN_REQUIRED`, va boshqalar) ketma-ket 3 marta bo'lsa kanal `enabled=False` qilinadi va ownerga xabar yuboriladi.
6. `track_activity` asosiy save, command, topic/private/public/bot, single-post va download/upload oqimlariga ulandi.
7. Batch/range looplarda deleted/restricted/flood xatolar darhol owner va userga yuboriladigan real-time analizga ulandi.

**O'zgartirilgan fayllar:** `TechVJ/activity_tracker.py`, `TechVJ/save.py`, `TechVJ/owner_commands.py`, `database/local_storage.py`, `main.py`

---

## BUG-030: Batch xatolari kech tahlil qilinadi va failed log jadvali limit/sessiya retry talabi yetarli emas
**Sana:** 2026-05-27
**Holat:** Hal qilindi

**Muammo:** Range/topic/private/public batchlarda ayrim deleted/restricted/FloodWait xatolari post aniqlangan zahoti userga ko'rinmasdi. Retry paytida user sessiyasi allaqachon uzilgan bo'lishi mumkin edi. Failed loglar MongoDBga tushmasligi va SQLite jadvali 1000 yozuvdan oshmasligi qat'iy kafolatlanishi kerak edi.

**Yechim:**
1. `failed_downloads` jadvali asosiy failed log jadvali qilindi; har insertdan keyin eng yangi 1000 yozuv qoldiriladi.
2. Eski `failed_downloads_log`dan bir martalik best-effort migration qo'shildi, lekin yangi yozuvlar faqat `failed_downloads`ga ketadi.
3. `_notify_realtime_post_failure()` va `_notify_bot_only_post_failure()` qo'shilib, batch looplarda xato post aniqlanishi bilan owner va userga bot client orqali xabar yuboriladi.
4. `ping_activity()` uzun looplarda har postdan oldin va real-time notify atrofida chaqiriladi.
5. `/addchannel` access xatolari `_process_monitored_channel_message()` ichida sanaladi; 3 marta ketma-ket bo'lsa kanal auto-disable qilinadi.
6. Retry callback private/protected loglar uchun user sessiyasini yangi `create_user_session()` context manager bilan qayta tiklab tekshiradi; public loglar bot-only qayta ishlanishi mumkin.

**O'zgartirilgan fayllar:** `TechVJ/activity_tracker.py`, `TechVJ/save.py`, `TechVJ/owner_commands.py`, `database/local_storage.py`

---

## BUG-031: /addchannel ro'yxati user yuborgan havolalarni bloklamaydi
**Sana:** 2026-05-27
**Holat:** Hal qilindi

**Muammo:** `/addchannel` bilan qo'shilgan "yuklab taqiqlangan" kanal faqat monitoring handlerida ishlardi. User shu kanaldan `/c/...` yoki public havola yuborsa, `save()` darhol "olish taqiqlangan" deb qaytarmay, oddiy download queue/pipelinega kirib ketishi mumkin edi.

**Yechim:**
1. `core/restricted_channel_guard.py` qo'shildi: enabled `channel_monitor` manbalarini non-owner userlar uchun rad qiladi.
2. `save()` URL parse va post_id validatsiyasidan keyin, queue/download boshlanishidan oldin restricted channel guard chaqiradi.
3. Public username linklarda bot `get_chat()` orqali numeric channel IDni resolve qilib, `/addchannel` ro'yxati bilan solishtiradi.
4. Rad javobi bot client orqali yuboriladi; media/user session yo'liga umuman kirmaydi.

**O'zgartirilgan fayllar:** `core/restricted_channel_guard.py`, `TechVJ/save.py`, `test_governance_fixes.py`

---

## BUG-032: Private DM message_id desync fixida sender-side ID fallback xavfli
**Sana:** 2026-05-27
**Holat:** Hal qilindi

**Muammo:** Relay'siz uploadda user/pool sessiya bot DM ga media yuborganda Telegram sender-side va bot-side `message_id`lari farq qiladi. `file_unique_id` orqali bot-side ID topish qo'shilgan edi, lekin topa olmasa `sent_msg.id`ga fallback bor edi. Bu fallback eski cross-chat leakage muammosini qayta keltirishi mumkin edi. Bundan tashqari bir xil fayl qayta yuborilganda oxirgi historydan eski dublikat xabar tanlanishi xavfi bor edi.

**Yechim:**
1. Bot DMdan uploaddan oldingi high-watermark (`get_bot_latest_message_id`) olinadi.
2. Copy/delete uchun bot-side xabar faqat shu watermarkdan keyingi xabarlar ichidan media fingerprint (`file_unique_id`, media type, size, file name, caption) bilan topiladi.
3. Bot-side ID topilmasa endi `sent_msg.id`ga fallback qilinmaydi; copy/delete bloklanadi va pool yo'lida retry/rotation ishlaydi.
4. Worker sessiyaning haqiqiy account IDsi `get_me()` bilan saqlanadi va bot copy source shu ID bilan tekshiriladi.
5. SessionManager va legacy `_enqueue_media_delivery()` owner/session isolation uchun `UploadTask.owner_user_id`ni to'ldiradi.

**O'zgartirilgan fayllar:** `core/copy_utils.py`, `core/user_upload_worker.py`, `core/session_manager/premium_worker.py`, `core/session_manager/session_manager.py`, `TechVJ/save.py`, `test_governance_fixes.py`

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

## BUG-022: Rate limiter 0 limitda `IndexError` chiqaradi
**Sana:** 2026-05-18
**Holat:** ✅ Hal qilindi

**Muammo:** `core/rate_limiter.py` da role limiti `0` bo'lsa, `check()` bo'sh deque bilan `dq[0]` ga murojaat qilib `IndexError` berishi mumkin edi. Bu ban yoki runtime nol-limit holatlarida middleware'ni yiqitardi.

**Yechim:** `max_req <= 0` uchun erta `False` qaytarish qo'shildi.

**O'zgartirilgan fayllar:** `core/rate_limiter.py`

---

## BUG-023: `permission_guard` yangi userga bo'sh `post_ids`ni o'tkazadi
**Sana:** 2026-05-18
**Holat:** ✅ Hal qilindi

**Muammo:** Yangi foydalanuvchi uchun guard faqat `len(post_ids) > 1` ni tekshirardi. `post_ids = []` bo'lsa ham ruxsat berib yuborardi, bu topic/thread yoki yaroqsiz parse yo'llarini chetlab o'tishga olib kelardi.

**Yechim:** Yangi user uchun aniq `len(post_ids) == 1` talabi qo'yildi.

**O'zgartirilgan fayllar:** `core/permission_guard.py`

---

## BUG-024: PriorityQueue spill-over amalda ishlamasdi
**Sana:** 2026-05-18
**Holat:** ✅ Hal qilindi

**Muammo:** `core/priority_queue.py` da hujjatlangan spill-over bor edi, lekin kod faqat o'z role semaforini tekshirardi. Bo'sh slotlar boshqa bucketga o'tmasdi va `set_limit()` worker sonini oshirilgan limitlarga moslamasdi.

**Yechim:** Reserved capacity + spill-over skaneri qo'shildi, worker soni limitlarga moslanadigan qilindi.

**O'zgartirilgan fayllar:** `core/priority_queue.py`

---

## BUG-025: Pool copy noto'g'ri chatdan o'qiydi va failure success bo'lib ketadi
**Sana:** 2026-05-18
**Holat:** ✅ Hal qilindi

**Muammo:** Pool upload'da bot `copy_message()` va `delete_messages()` uchun `sent.chat.id`ga ishonardi. User session yuborgan xabarlarda bu bot chat bo'lib qolishi mumkin edi, natijada noto'g'ri chatdan copy/delete qilinardi. Bundan tashqari, `SessionManager` copy 3 marta yiqilganda ham uploadni muvaffaqiyat deb qaytarardi.

**Yechim:** Bot copy manbasi `from_user.id` orqali aniqlanadigan helper qo'shildi. `SessionManager` copy failure'da `None` qaytaradigan qilindi, success bo'lganda delete saqlandi.

**O'zgartirilgan fayllar:** `core/copy_utils.py`, `TechVJ/save.py`, `core/session_manager/session_manager.py`

---

## BUG-026: Owner commandlar link handlerga tushib ketadi va kanal monitor ishlamaydi
**Sana:** 2026-05-24
**Holat:** ✅ Hal qilindi

**Muammo:** Ayrim owner commandlar, ayniqsa `/grab <uid> <t.me/link>` va kanal monitor komandlari, asosiy `save()` text handleri tomonidan ham ushlanishi mumkin edi. Bundan tashqari `/addchannel` faqat kanalni JSONga yozardi, lekin kanal postlarini tinglaydigan `filters.channel` handler yo'q edi.

**Sabab:**
1. `save()` command exclude ro'yxatida `/grab`, `/addchannel`, `/removechannel`, `/channels`, `/togglechannel` yo'q edi.
2. `core/channel_monitor.py` storage va owner komandalar bor edi, ammo runtime integratsiya yo'q edi.
3. Background/channel monitor upload targeti oddiy user requestidan farq qiladi: uploader sessiya egasi target recipient bilan bir xil bo'lishi shart emas.

**Yechim:**
1. `save()` handler exclude ro'yxatiga owner kanal komandlari va `/grab` qo'shildi.
2. `TechVJ/save.py`ga `monitored_channel_handler` qo'shildi: monitored kanal postlarini ushlaydi, textni bot orqali, media/poll/albumni MTProto session orqali yuboradi.
3. Channel monitor system session tanlash tartibi qo'shildi: SessionManager GLOBAL/BORROWABLE → legacy system premium → owner DB session.
4. `SessionManager`ga background/system upload uchun `get_system_session_for_use()` va `upload_with_system_session()` qo'shildi.

**O'zgartirilgan fayllar:** `TechVJ/save.py`, `core/session_manager/session_manager.py`

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
