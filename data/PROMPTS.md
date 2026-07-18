# PROMPT TARIXI — Foydalanuvchi buyurtmalari va yo'nalishi

Bu fayl foydalanuvchi har sessiyada qanday so'rovlar berganini va loyiha qaysi yo'nalishda rivojlanayotganini kuzatib boradi.
Yangi Claude sessiyasi bu faylni O'QIB, kontekstni tushunishi KERAK.

---

## Sessiya 29 (2026-07-18) - Account-specific upload FloodPremiumWait va stuck worker fix

### So'rov:
Foydalanuvchi `user_worker_8883410579` aynan katta media uploadining oxirida `FLOOD_PREMIUM_WAIT_X` (`upload.SaveBigFilePart`, 11 soniya) olayotganini, boshqa akkauntlar floodga tushmayotganini va shu logdan keyin process uzoq vaqt siljimay qolayotganini tekshirishni so'radi. User sessiyasi production DBda mavjudligi aytildi.

### Natija:
- **BUG-052** qo'shildi va hal qilindi.
- Flood account-specific ekani, Pyrofork bitta katta fayl ichida 4 ta partni parallel yuklashi va `max_concurrent_transmissions=1` bu ichki parallellikni cheklamasligi aniqlandi.
- Pyrofork 2.3.69 `save_file()` worker exceptionlarini log qilib yutishi topildi; shu sabab tashqi adaptive throttle va session rotation floodni ko'rmasdi.
- Opt-in flood-safe `save_file()` patch qo'shildi: qisqa waitda aynan xato bergan part bounded retry qilinadi, birinchi flooddan keyin qolgan upload seriallashadi, uzun/takroriy flood tashqariga uzatiladi.
- User worker file-part parallelligi 4 dan 2 ga tushirildi; flood callback mavjud adaptive throttle'ga ulandi.
- 10 daqiqalik idle reaper faol uploadni queue bo'sh deb o'chirib yuborishi mumkinligi topildi; `_busy` guard qo'shildi.
- Regression testlar qisqa/uzun premium flood, to'liq file-part ketma-ketligi va active reaper guardni qamrab oldi.

### Tekshiruv:
- `python -m py_compile core\pyrofork_compat.py core\user_upload_worker.py test_governance_fixes.py` - OK
- Fokus regression: 6 passed

### O'zgartirilgan fayllar:
- `core/pyrofork_compat.py`
- `core/user_upload_worker.py`
- `test_governance_fixes.py`
- `data/BUGS.md`
- `data/PROMPTS.md`

### UMUMIY YO'NALISH yangilanishi:
- Katta MTProto uploadlarda message-level throttle yetarli emas; file-part concurrency, per-part flood retry va active-worker lifecycle birga boshqarilishi kerak.

---

## Sessiya 28 (2026-07-18) - Private album PEER_ID_INVALID fix

### So'rov:
Foydalanuvchi bir nechta production failure report berdi. Private kanallardagi photo album postlar mavjud, user session kanalga a'zo va postlar o'qiladi, lekin `private_album` bosqichi `status=send_failed` bilan tugaydi. Runtime logdagi aniq xato: Pyrofork `send_media_group()` uchun `[400 PEER_ID_INVALID]`.

### Natija:
- **BUG-051** qo'shildi va hal qilindi.
- `album_collector_v2.send_album()` numeric bot ID o'rniga imkon bo'lsa bot username'ini upload target sifatida ishlatadi.
- Peer invalid yoki bot blocked xatosida user session bot dialogini `get_chat`, unblock, `/start`, final resolve orqali tayyorlaydi.
- Xato bergan media-group batch username target bilan bir marta retry qilinadi.
- Media routing qoidasi saqlandi: album user session orqali, overflow caption text bot client orqali yuboriladi.
- Regression test qo'shildi va mavjud split peer-recovery testlari bilan birga o'tkazildi.

### Tekshiruv:
- `python -m py_compile TechVJ\album_collector_v2.py test_governance_fixes.py` - OK
- `python -m pytest -q test_governance_fixes.py -k "album_send_retries_peer_invalid or split_part_upload_retries_peer_invalid or split_bot_peer_resolve_is_cached"` - 3 passed

### O'zgartirilgan fayllar:
- `TechVJ/album_collector_v2.py`
- `test_governance_fixes.py`
- `data/BUGS.md`
- `data/PROMPTS.md`

### UMUMIY YO'NALISH yangilanishi:
- Task-scoped user sessiyalar botga media yuborishdan oldin bot peerini username orqali resolve qilishi yoki peer xatosida bounded retry qilishi kerak.

---

## Sessiya 27 (2026-07-08) - Comment discussion auto-join

### So'rov:
Foydalanuvchi kanal comment qismidagi post havolasi yuborilganda user session muhokama guruhiga a'zo bo'lmasa comment yuklanmayotganini aytdi. Kerakli yechim: user channel comment link yuborganda muhokama guruhiga avtomatik qo'shilib, keyin yuklab olishga urinish.

### Natija:
- **BUG-050** qo'shildi va hal qilindi.
- `parse_comment_url()` qo'shildi: native `?comment=` linklar thread flowga yo'naltiriladi va source channel post metadata saqlanadi.
- `_prepare_discussion_thread_route()` source channel postdan `get_discussion_message()` orqali linked discussion group va thread rootni aniqlaydi.
- `_join_chat_best_effort()` user sessionni discussion groupga public username/invite orqali qo'shishga urinadi; already-member OK, join-request holatida userga aniq xabar beriladi.
- `process_thread_comments()` comment/media fetchni resolved discussion chat ID orqali davom ettiradi.
- Regression testlar qo'shildi.

### Cheklov:
Faqat `/c/<discussion_group>/<comment>?thread=<root>` linkda source channel post yoki join username/invite bo'lmasa Telegram API orqali auto-join qilish uchun yetarli target bo'lmasligi mumkin. Bunday holatda userga muhokama guruhiga qo'shilib qayta urinish aytiladi.

### Tekshiruv:
- `python -m py_compile TechVJ\save.py test_governance_fixes.py` - OK
- `python -m pytest -q test_governance_fixes.py -k "native_comment_link or prepare_discussion_route or public_copy_caption_too_long or source_context_message_replies"` - 4 passed
- `git diff --check` - OK

### O'zgartirilgan fayllar:
- `TechVJ/save.py`
- `test_governance_fixes.py`
- `data/BUGS.md`
- `data/PROMPTS.md`

---

## Sessiya 26 (2026-07-08) - Public copy uzun caption fallback

### So'rov:
Foydalanuvchi production failure report berdi: `https://t.me/zapislar_efir/218` public bot-only yo'lida `MediaCaptionTooLong: [400 MEDIA_CAPTION_TOO_LONG]` xatosi chiqdi va post `No messages retrieved` deb yakunlandi.

### Natija:
- **BUG-049** qo'shildi va hal qilindi.
- Public bot-only `copy_message()` yo'liga `_copy_public_message_with_caption_fallback()` qo'shildi.
- Agar original media caption Telegram limitidan uzun bo'lsa, media `caption=""` bilan captionsiz copy qilinadi.
- Original caption alohida bot text xabarlari sifatida entity-safe chunklarga bo'linib yuboriladi.
- Reply chain saqlandi: media va caption textlar bitta `Manba` xabariga reply bo'ladi.
- Regression test qo'shildi.

### Tekshiruv:
- `python -m py_compile TechVJ\save.py test_governance_fixes.py` - OK
- `python -m pytest -q test_governance_fixes.py -k "public_copy_caption_too_long or source_context_message_replies or send_source_name_message or split_part_progress_callback or split_part_upload_retries_peer_invalid_with_bot_username or split_bot_peer_resolve_is_cached_per_session"` - 6 passed
- `git diff --check` - OK

### O'zgartirilgan fayllar:
- `TechVJ/save.py`
- `test_governance_fixes.py`
- `data/BUGS.md`
- `data/PROMPTS.md`

---

## Sessiya 25 (2026-07-05) - Source reply zanjiri va split progress

### So'rov:
Foydalanuvchi ikki bog'liq muammoni tuzatishni so'radi: 2GB+ split qilingan fayllar upload progressi qism bo'yicha real vaqt ko'rinishi kerak, va reply zanjiri `user link -> Manba xabari -> content` shaklida bo'lishi kerak. Range/comma ko'p postlarda bitta umumiy `Manba` xabari yuborilib, barcha content shu xabarga reply bo'lishi talab qilindi.

### Natija:
- **BUG-048** qo'shildi va hal qilindi.
- `send_source_name_message()` helperi qo'shildi: source nomi user session `acc.get_chat()` orqali olinadi, matn bot client orqali original link xabariga reply qilinadi.
- `_send_source_context_message()` source-only ko'rinishga o'tdi: `📡 Manba: <nom>`, request messagega reply.
- `download_and_send_media()`ga `reply_target_message=None` parametri qo'shildi; ichki status, copy, overflow va worker reply anchorlari `reply_target_message or message` orqali yuradi.
- Split uploadlar uchun `_create_split_part_progress_callback()` qo'shildi: `progress=` callback status xabarni 4s throttle bilan `Qism N/T - X/Y MB (Z%)` formatida yangilaydi.
- Standard split va premium direct failure split fallback ikkalasida video/document chunk uploadga progress callback ulandi.

### Tekshiruv:
- `python -m py_compile TechVJ\save.py test_governance_fixes.py` - OK
- `python -m pytest -q test_governance_fixes.py -k "source_context_message_replies or send_source_name_message or split_part_progress_callback or split_part_upload_retries_peer_invalid_with_bot_username or split_bot_peer_resolve_is_cached_per_session"` - 5 passed
- `git diff --check` - OK

### O'zgartirilgan fayllar:
- `TechVJ/save.py`
- `test_governance_fixes.py`
- `data/BUGS.md`
- `data/PROMPTS.md`

---

## Sessiya 24 (2026-07-05) - Split fallback PEER_ID_INVALID fix

### So'rov:
Foydalanuvchi production failure report berdi: 2.00 GB video va 3.91 GB document bitta post sifatida o'qilgan, user kanalga a'zo, lekin `download_and_send_media returned False` qaytgan. Ikkinchi reportda split fallback userga `Failed to split large file: ... [400 PEER_ID_INVALID]` ko'rsatgan.

### Natija:
- **BUG-047** qo'shildi va hal qilindi.
- 2.00 GB reportdagi fayl `2148893485` bytes bo'lgani uchun Telegram 2000 MiB limitidan katta va split fallback yo'liga tushishi kerakligi aniqlandi.
- Split fallback bevosita `acc.send_video()` / `acc.send_document()` bilan numeric bot IDga yuborayotgani, lekin worker yo'lidagi bot peer tayyorlash (`get_chat`, `/start`, username resolve) yo'qligi topildi.
- `_ensure_user_session_bot_peer()` helper qo'shildi: split loop boshlanishidan oldin user session ichida bot peer username orqali tayyorlanadi va per-session cache qilinadi.
- `_send_split_part_with_peer_retry()` helper qo'shildi: chunk send vaqtida baribir `PEER_ID_INVALID` yoki bot blocked xatosi chiqsa `force=True` bilan peer qayta tayyorlanadi va username target bilan bir marta retry qilinadi.
- Standard split path va direct premium failure'dan keyingi split fallback loopdan oldin ensure qiladi va chunk helperidan foydalanadi.

### Tekshiruv:
- `python -m py_compile TechVJ\save.py test_governance_fixes.py` - OK
- `python -m pytest -q test_governance_fixes.py -k "split_part_upload_retries_peer_invalid_with_bot_username or split_bot_peer_resolve_is_cached_per_session or media_route_guard or own_user_session_upload_skips_bot_copy"` - 5 passed

### O'zgartirilgan fayllar:
- `TechVJ/save.py`
- `test_governance_fixes.py`
- `data/BUGS.md`
- `data/PROMPTS.md`

---

## Sessiya 23 (2026-07-03) - Runtime routing guard va SessionManager reply fix

### So'rov:
Foydalanuvchi cache tozalab qayta ishlatgandan keyin ham routing almashib qolganini aytdi: progress/status user session orqali ketayotgani, media esa user session orqali bot chatiga qat'iy upload bo'lmayotganini xabar qildi va davom ettirishni so'radi.

### Natija:
- `download_and_send_media()` uchun runtime routing guard qo'shildi: `client` bot bo'lishi, `acc` user session bo'lishi shart.
- Agar `client` va `acc` tasodifan almashib kelgan bo'lsa guard lokal tarzda to'g'ri tartibga qaytaradi; boshqa xavfli holatda text/progress yoki media yuborilmaydi.
- Worker target bot ID endi guarddan chiqqan bot ID orqali olinadi, `client.me.id`ga ko'r-ko'rona tayanilmaydi.
- `SessionManager` direct user-owned uploadlarda reply anchorni user session dialogida resolve qiladi; topilmasa media replysiz yuboriladi va noto'g'ri xabarga reply qilmaydi.
- Eski/alternate `pipeline_v2.py` va `album_collector_v2.py` media sendlari user session yo'liga o'tkazildi; overflow text bot orqali qoladi.
- Regression testlar qo'shildi: routing guard reject/swap, direct reply resolve, own user-session bot copy skip.

### Tekshiruv:
- `python -m py_compile TechVJ\save.py core\session_manager\session_manager.py test_governance_fixes.py TechVJ\album_collector_v2.py TechVJ\pipeline_v2.py` - OK
- `python -m pytest -q test_governance_fixes.py -k "media_route_guard or user_owned_upload or own_user_session_upload_skips_bot_copy or session_manager_pool_copy"` - 8 passed

### O'zgartirilgan fayllar:
- `TechVJ/save.py`
- `core/session_manager/session_manager.py`
- `TechVJ/pipeline_v2.py`
- `TechVJ/album_collector_v2.py`
- `test_governance_fixes.py`
- `data/BUGS.md`
- `data/PROMPTS.md`

---

## Sessiya 22 (2026-07-01) - Bot API va user session routingini qayta ajratish

### So'rov:
Foydalanuvchi bot API yuborishi kerak bo'lgan narsalar bilan user session yuborishi kerak bo'lgan narsalar almashib qolganini aytdi. Oldingi qoida bo'yicha oddiy progress/status/text bot API orqali, media esa user session orqali ketishi kerakligini eslatdi.

### Natija:
- `TechVJ/save.py` audit qilindi: user session orqali text yuborish topilmadi, faqat bot chat tayyorlash uchun worker `/start` bor.
- Direct own user-session media yo'llarida bot `copy_message` regressiyasi topildi: album, fallback photo, location/venue, split chunk va premium-failure split fallback.
- `_is_direct_user_session_upload()` qo'shildi.
- Own user-session holatida bot watermark/history/copy/delete qilinmaydigan qilindi; media user session yuborgan holda qoladi.
- Pool/global/system yoki boshqa uploader account bo'lsa copy saqlandi, chunki target user ko'rishi uchun kerak.
- Regression test qo'shildi: own user-session media bot copy qilinmaydi.

### Tekshiruv:
- `python -m py_compile TechVJ\save.py test_governance_fixes.py core\user_upload_worker.py core\session_manager\session_manager.py`
- `python -m pytest -q test_governance_fixes.py -k "own_user_session_upload_skips_bot_copy or session_manager_user_owned_upload_does_not_use_bot_history_or_copy"`
- `git diff --check`

---

## Sessiya 21 (2026-07-01) - Telegram connected devices nomini neytrallashtirish

### So'rov:
Foydalanuvchi Telegram ulangan qurilmalar ro'yxatida `Shahzod, Pyrogram 2.3.69` chiqayotganini aytdi va `Shahzod` so'zi chiqmasligi yoki boshqa qurilma nomiga almashtirilishini so'radi.

### Natija:
- Repo va `.env` ichida `Shahzod` hardcode topilmadi.
- Pyrogram default `app_version="Pyrogram 2.3.69"` ekanligi tekshirildi.
- `config.py` desktop fingerprinti `Telegram Desktop / Windows 11 / 5.9.0 x64` qilib neytrallashtirildi.
- `album_collector_v2`, `comment_stats`, `info_command`, `database/session_manager` va `debug_chatid` helper clientlariga `get_client_params()` qo'shildi.
- Eski authorization nomi Telegram serverida re-login qilinmaguncha qolishi mumkinligi qayd qilindi; agar `Shahzod` API app name bo'lsa, uni my.telegram.org tomonda hal qilish kerak.

### Tekshiruv:
- `python -m py_compile config.py TechVJ\album_collector_v2.py TechVJ\comment_stats.py TechVJ\info_command.py database\session_manager.py debug_chatid.py TechVJ\save.py core\user_upload_worker.py core\session_manager\session_manager.py`
- `python -c "from config import get_client_params; print(get_client_params(7256358665))"`
- `git diff --check`

---

## Sessiya 20 (2026-07-01) - Upload qaysi sessiya orqali ketayotganini diagnostika qilish

### So'rov:
Foydalanuvchi `Unauthorized: Auth key not found` xatosidan keyin sessiya eskirmaganini aytdi va avval qaysi sessiya orqali upload qilinayotganini aniqlash kerakligini so'radi.
Keyingi runtime logda `route=user_session_db`, `session_fp=13467438a7`, `session_user_id=7256358665` chiqdi. Foydalanuvchi upload user sessiya bilan ketayotganini, lekin keyingi media/text source headerga emas boshqa xabarga reply bo'layotganini aytdi.

### Natija:
- Raw session string logga chiqarilmasdan xavfsiz `sha256` fingerprint diagnostikasi qo'shildi.
- `UserUploadWorker` connect bo'lganda `session_fp`, real `session_user_id` va `bot_id` loglanadi.
- Worker send xatosida ham `session_fp` va `session_user_id` chiqadi.
- Legacy upload selection loglari qo'shildi: `legacy_pool#N`, `user_premium`, `user_session_db`, `user_session_export`, pool fallback route nomlari.
- `SessionManager` upload route logi qo'shildi: session ID prefix, type, owner, `is_pool`, real uploader account ID va session fingerprint.
- Lokal tekshiruvda `data/session_manager/sessions.json` va `data/premium/sessions.json` topilmadi; `7256358665` roli `vip_user`, SQLite DB session mavjud: `logged_in=1`, `session_fp=13467438a7`.
- **BUG-043** qo'shildi va hal qilindi: user-session media uploadda bot source header IDsi user-session dialogida tekshirilmasdan reply sifatida ishlatilgani uchun noto'g'ri xabarga reply bo'lishi mumkin edi.
- `UserUploadWorker.resolve_bot_reply_message_id()` source anchorga text/date bo'yicha user-session bot chatida mos xabar topadi.
- Anchor topilmasa user-session media replysiz yuboriladi, noto'g'ri xabarga reply qilmaydi.
- `_make_reply_target_message()` synthetic reply targetlarga source message text/date metadata qo'shadi.
- `source_context_msg = ... or message` fallbacklari olib tashlandi, shuning uchun source header yaratilmasa ham media eski user link xabariga reply bo'lmaydi.

### Tekshiruv:
- `python -m py_compile TechVJ\save.py core\user_upload_worker.py core\session_manager\session_manager.py` - OK
- `git diff --check` - OK

### O'zgartirilgan fayllar:
- `TechVJ/save.py`
- `core/user_upload_worker.py`
- `core/session_manager/session_manager.py`
- `data/BUGS.md`
- `data/PROMPTS.md`

---

## Sessiya 19 (2026-06-30) - Pool-first media routing va BOT_METHOD_INVALID fix

### So'rov:
Foydalanuvchi media/photo uploadda poolni chetlab o'tmaslikni, avval tizimda pool bor-yo'qligini tekshirishni, pool bo'lmasa yoki ishlamasa user's own session fallback bo'lishini talab qildi. Real logda pool/user worker uploaddan keyin bot `messages.GetHistory` uchun `BOT_METHOD_INVALID` olayotgani ko'rsatildi.

### Natija:
- **BUG-042** qo'shildi va hal qilindi.
- Pool-first routing saqlandi: pool/global sessiya bor bo'lsa media avval user-session worker orqali shu pool akkauntdan bot DMga upload qilinadi.
- `core.copy_utils.BotUploadUpdateWaiter` qo'shildi: bot history o'qimasdan pool akkauntdan kelgan yangi media update fingerprint bilan ushlanadi va real bot-side `message_id` olinadi.
- `TechVJ/save.py` legacy pool path va `SessionManager` pool/global path update-waiter orqali copy qiladi; `get_chat_history()` faqat listener ishga tushmasa zaxira fallback.
- Non-pool/user-owned uploadlarda bot-history/copy ishlatilmaydi; reply kwargs uploadda saqlanadi.
- Pool delivery kutilmagan xato bilan yiqilsa, o'sha post user's own session bilan qayta yuboriladi.

### Tekshiruv:
- `python -m py_compile TechVJ\save.py core\copy_utils.py core\session_manager\session_manager.py core\caption_splitter.py test_governance_fixes.py` - OK
- `git diff --check` - OK
- Fokus testlar 4/4 PASS.
- To'liq `python -m pytest -q test_governance_fixes.py`: 29 PASS, 2 FAIL faqat lokal `psutil` dependency yo'qligi sabab.

### O'zgartirilgan fayllar:
- `TechVJ/save.py`
- `core/copy_utils.py`
- `core/session_manager/session_manager.py`
- `test_governance_fixes.py`
- `data/BUGS.md`, `data/PROMPTS.md`

---

## Sessiya 18 (2026-06-30) - Single post source context va photo delivery regressiyasi

### So'rov:
Foydalanuvchi bitta post yuborilganda `Manba:` yozuvi tepaga post preview/caption bilan qo'shilib ketayotganini, source alohida xabar bo'lishi va keyingi yuboriladigan content unga reply bo'lishi kerakligini aytdi. Shuningdek split/hyperlink muammosi qaytganini va real logda kichik photo post `download_and_send_media returned False` bilan yuborilmay qolganini ko'rsatdi.

### Natija:
- **BUG-041** qo'shildi va hal qilindi.
- `_send_source_context_message()` oddiy postlar uchun faqat `Manba: <title>` yuboradigan qilindi; source context endi original link xabariga reply qilmaydi.
- Keyingi media/textlar avvalgidek source context xabariga reply qiladi.
- `get_bot_copy_source_chat_id()` explicit fallback uploader IDni bot chat IDdan oldin ishlatadi; legacy delivery va SessionManager shu fallbackni uploader account ID bilan chaqiradi.
- Caption overflow chunklari bitta textga qo'shilmaydi; har chunk alohida, o'z entitylari bilan bot orqali yuboriladi.
- Caption splitter TEXT_LINK/URL entitylarini qisman bo'lak sifatida yubormaydi; link sig'sa butunicha keyingi chunkga o'tadi.
- Regression testlar qo'shildi: source context standalone, copy source fallback ustunligi, hyperlink entity split qilinmasligi.

### Tekshiruv:
- `git diff --check` - OK
- Python compile/test bajarilmadi: lokal `py` install topmadi, `venv\Scripts\python.exe` esa eski/o'chirilgan Python310 pathiga bog'langan.

### O'zgartirilgan fayllar:
- `TechVJ/save.py`
- `core/copy_utils.py`
- `core/session_manager/session_manager.py`
- `core/caption_splitter.py`
- `test_governance_fixes.py`
- `data/BUGS.md`, `data/PROMPTS.md`

---

## Sessiya 17 (2026-06-29) - Source context reply anchor va user-session copy delivery

### So'rov:
Foydalanuvchi real API tekshiruvdan keyin `171796` topic 8 emas degan xulosaga e'tiroz bildirdi va owner sessiya bilan tekshirishni so'radi. Shuningdek progress xabaridan keyin linkka oid guruh/kanal nomi, forum bo'lsa forum nomi, discussion bo'lsa asosiy post matni chiqishini va keyingi user/bot yuboradigan xabarlar shu kontekst xabariga reply bo'lishini talab qildi. Ayrim user session uploadlari bot chatida reply qilmayotgani aytildi.

### Natija:
- **BUG-040** qo'shildi va hal qilindi.
- Real API metadata xulosasi saqlandi: `171796` mavjud, lekin topic 8 emas (`message_thread_id=1`, `reply_to_top_message_id=171777`, `reply_to_message_id=171790`).
- `_send_source_context_message()` qo'shildi: bot progressdan keyin `Manba`, forum topic nomi yoki discussion root preview xabarini yuboradi.
- `ParsedURL.thread_root_id` qo'shildi; thread range linklarda ham asosiy discussion post previewi yo'qolmaydi.
- `process_thread_comments()` delivery pathlari source contextga reply qiladi.
- Topic/private/public/bot delivery pathlarida source context anchor ishlatiladi; private/public user-session flowlarda source metadata `acc.get_chat()` va birinchi post orqali olinadi.
- `_enqueue_media_delivery()` hamma sessiyalar uchun bot-side resolve/copy/delete patterniga o'tdi, shunda user-owned session upload ham reply contextni bot copy bosqichida oladi.
- Direct premium `>2GB` worker upload ham `_enqueue_media_delivery()` orqali yuradi.
- Photo-only album, album fallback photo, split chunk, premium direct failure split fallback, location va venue yo'llari ham bot-side resolved copy/delete orqali source contextga reply qiladi.
- Upload source chat endi `target_user_id` deb faraz qilinmaydi; user/session account ID `acc.get_me()` bilan kesh qilinadi, shunda system/channel monitor sessiyalarida ham copy to'g'ri DMdan olinadi.
- Overflow caption textlari ham source contextga reply qilib yuboriladi.

### Tekshiruv:
- `.\venv\Scripts\python.exe -m py_compile TechVJ\save.py core\copy_utils.py core\session_manager\session_manager.py core\topic_extractor.py core\guards.py test_governance_fixes.py test\real_topic_debug.py` - OK
- Venv manual runner bilan `test_governance_fixes.py`: 26/26 PASS
- Global `python -m pytest -q test_governance_fixes.py`: 24 PASS, 1 FAIL faqat global Python muhitida `psutil` yo'qligi sabab; venvda `psutil` bor, lekin `pytest` o'rnatilmagan.

### O'zgartirilgan fayllar:
- `TechVJ/save.py`
- `core/copy_utils.py`
- `core/session_manager/session_manager.py`
- `test_governance_fixes.py`
- `data/BUGS.md`, `data/PROMPTS.md`

---

## Sessiya 16 (2026-06-29) - Topic anchor direct hydrate va deleted sender holati

### So'rov:
Foydalanuvchi production log berdi: topic `8` uchun raw metadata `top_message=171769` va extractor `21 messages` yig'gan, lekin range anchor `171796` topilmadi deb fallbackga tushgan. Foydalanuvchi `171796` xabar mavjudligini, xabarni yuborgan account o'chganini aytdi.

### Natija:
- **BUG-039** qo'shildi va hal qilindi.
- `TopicExtractor.extract_between()` katta range'ni scan qilmasdan missing anchor IDlarni alohida hydrate qiladi (`channels.GetMessages`/`get_messages` batch).
- Direct range probe endi faqat anchor hydrate'dan keyin va faqat kichik spanlarda ishlaydi; `75..171796` kabi range kengaytirilmaydi.
- `_is_topic_message()` senderga bog'lanmaydi; `message_thread_id` mos kelsa qabul qiladi, mos kelmasa `reply_to_top_message_id`, nested `reply_to_top_id`, `reply_to_message_id`/`reply_to_msg_id` bilan tekshiradi.
- Anchor baribir inaccessible bo'lsa, extractor yig'ilgan topic xabarlarini numeric bounds ichida qaytaradi; 21 valid xabar tashlab yuborilmaydi.
- `test/real_topic_debug.py` CLI target override va katta-span direct fetch himoyasi bilan yangilandi.
- Real API smoke: `171796` mavjud, lekin API metadata bo'yicha `message_thread_id=1`, `reply_to_top_message_id=171777`, `reply_to_message_id=171790`; topic 8 emas. Extractor topic 8 ichidagi 20 ta xabarni chronological tartibda qaytardi va `171796`ni noto'g'ri topicga qo'shmadi.

### Tekshiruv:
- `python -m py_compile core\topic_extractor.py test_governance_fixes.py test\real_topic_debug.py` - OK
- TopicExtractor focused regression runner - OK
- `python test\real_topic_debug.py --help` - OK
- `python test\real_topic_debug.py --use-owner-db-session --chat-id -1002234521267 --topic-id 8 --range 75-171796 --expected-ids 75,171796 --expected-mode subset --jsonl test\artifacts\topic_8_debug.jsonl --quiet` - real API ishladi; expected `171796` topic 8 metadata'siz qaytgani uchun final expected-ID check FAIL, lekin extractor crash qilmadi va topic 8 xabarlarini qaytardi.
- `TechVJ.save` importli bitta test `psutil` yo'qligi sabab o'tkazilmadi; bu dependency muammosi topic extractor fixiga bog'liq emas.

### O'zgartirilgan fayllar:
- `core/topic_extractor.py`
- `TechVJ/save.py`
- `core/guards.py`
- `test_governance_fixes.py`
- `test/real_topic_debug.py`
- `test/README.md`
- `data/BUGS.md`, `data/PROMPTS.md`

---

## Sessiya 15 (2026-06-20) - Topic extraction raw-first + cache

### So'rov:
Foydalanuvchi senior MTProto engineer sifatida to'liq audit va refactor so'radi: forum topic extraction katta chat history range'larini scan qilmasin, `messages.GetHistory FloodWait` kamaytirilsin, `messages.GetDiscussionMessage`/`messages.GetReplies`/thread APIs ishlatilsin, topic-aware cache qo'shilsin.

### Audit topilmalari:
- Production topic download yo'li: `parse_topic_url()` → `process_topic_posts()` → `TopicExtractor`.
- `TopicExtractor` avval `get_chat_history(message_thread_id=topic_id)` chaqirardi; bu Pyroforkda `messages.GetHistory` bosimini keltiradi.
- `process_topic_posts()` range-anchor (`/c/<chat>/<topic>/<from>-<to>`) uchun extractor ishlatadi; single/comma explicit IDlar esa bounded `get_messages` bilan qoladi.
- `core/guards.py` va `/group` topic analiz helperlarida history scan yoki numeric range probing fallbacklar bor edi.
- FloodWait generatori: topic discovery ichidagi high-level `get_chat_history`/history fallbacklar.

### Natija:
- **BUG-037** qo'shildi va hal qilindi.
- `TopicExtractor` raw-first qilindi: raw `messages.Search(top_msg_id)` → raw `messages.GetReplies` → explicit ruxsat bo'lsa history fallback.
- `messages.GetDiscussionMessage` root/thread metadata uchun best-effort qo'shildi.
- `core/topic_cache.py` yaratildi; `data/topic_cache/topics.json`ga topic ID ro'yxatlari persist qilinadi.
- Cache hit bo'lsa range anchor discovery qilinmaydi; known IDlar raw `channels.GetMessages` batch bilan hydrate qilinadi.
- Full-topic repeat uchun `last_processed_message_id` boundary ishlatiladi; faqat yangi sahifalar ko'riladi.
- `process_topic_posts()` production logga extractor path flaglarini chiqaradi: raw_search/raw_replies/history/cache.
- `core/guards.py` topic helperlari va `/group` analiz yo'li `TopicExtractor`ga o'tkazildi.
- Regression testlar raw-first va cache hit uchun yangilandi.

### Tekshiruv:
- `python -m py_compile core/topic_cache.py core/topic_extractor.py core/guards.py TechVJ/group_command.py TechVJ/save.py test_governance_fixes.py` - OK
- Topic extractor focused manual tests - OK

### O'zgartirilgan fayllar:
- `core/topic_cache.py`
- `core/topic_extractor.py`
- `core/guards.py`
- `TechVJ/save.py`
- `TechVJ/group_command.py`
- `test_governance_fixes.py`
- `data/BUGS.md`, `data/PROMPTS.md`

---

## Sessiya 14 (2026-06-13) - Topic range raw Search fallback

### So'rov:
Foydalanuvchi production log berdi: `TopicExtractor: get_chat_history has no message_thread_id support; falling back to raw GetReplies`, keyin `topic=8 collected 1 messages` va `topic range anchor not found: 75, 171796`. Foydalanuvchi chat/topic mavjudligini va user kanalga a'zo ekanini aytdi.

### Natija:
- **BUG-036**: `GetReplies` ayrim forum topiclarda to'liq topic stream bermasligi aniqlandi.
- `TopicExtractor` fallback tartibi yangilandi: `get_chat_history(message_thread_id=...)` yo'q bo'lsa avval raw `messages.Search(..., top_msg_id=topic_id)` ishlatiladi.
- `messages.Search` pagination Pyroforkning o'z `search_messages(thread_id=...)` implementatsiyasiga moslab `add_offset` bilan qilindi.
- `GetReplies` zaxira fallback sifatida qoldi; agar Search anchorlarni topolmasa, `extract_between()` bir marta GetReplies bilan retry qiladi.
- Page limit 100 ga clamp qilindi, shunda last-page tekshiruvi real Pyrofork/Telegram sahifa hajmiga mos ishlaydi.
- Keyingi production logda `Chat ... may not be a forum` chiqdi; bu `get_chat()` forum flag bermayotganini anglatishi mumkin, fatal emas. Topic flow endi `acc.resolve_peer()` natijasini loglaydi va raw `messages.GetForumTopicsByID(peer=resolved_peer, topics=[topic_id])` bilan topic metadata tekshiradi.
- Raw topic `top_message` faqat diagnostika uchun loglanadi; extractor URLdagi topic root id bilan ishlaydi. `top_message` topicdagi oxirgi/yuqori xabar bo'lishi mumkin, root emas.
- Regression testlar qo'shildi: raw Search primary path, raw GetReplies secondary path, Search `add_offset` cursor, mavjud anchor/FloodPremiumWait testlari.

### Tekshiruv:
- `python -m py_compile core/topic_extractor.py test_governance_fixes.py` - OK
- `python -m py_compile TechVJ/save.py core/topic_extractor.py test_governance_fixes.py` - OK
- Topic/flood subset manual runner - OK

### O'zgartirilgan fayllar:
- `TechVJ/save.py`
- `core/topic_extractor.py`
- `test_governance_fixes.py`
- `data/BUGS.md`, `data/PROMPTS.md`

---

## Sessiya 13 (2026-06-12) - FloodPremiumWait va katta topic range bosimi

### So'rov:
Foydalanuvchi `https://t.me/c/2234521267/8/75-171796` topic range ishlovida production `journalctl` logini berdi: `FloodPremiumWait [420 FLOOD_PREMIUM_WAIT_X] (upload.SaveBigFilePart)` stacktrace, keyin `messages.GetHistory` 21-22s FloodWait warninglari takrorlangan.

### Natija:
- **BUG-035**: Pyrofork `FloodPremiumWait` oddiy `FloodWait` catchlaridan sirpanib o'tmasligi uchun `core/retry_utils.py`da `is_floodwait_error()` va kengaytirilgan `get_floodwait_seconds()` qo'shildi.
- `UserUploadWorker`, `SessionManager`, legacy pool fallback va `download_and_send_media()` flood-like exceptionlarni upload rotation/cooldown yoki existing outer FloodWait handlingga uzatadi.
- Foydalanuvchi keyingi production logda `message_thread_id` yo'qligi sabab `get_chat_history` fallback butun chatni skan qilib, task hech narsa yubormasdan FloodWait olayotganini ko'rsatdi va yangi yo'l so'radi.
- Tavsiya qilingan va amalga oshirilgan yo'l: chat-history fallback o'rniga raw MTProto `messages.GetReplies(peer, msg_id=topic_id)` ishlatish. Bu server-side faqat bitta topic/thread replies'ni olib keladi.
- `TopicExtractor` endi `message_thread_id` TypeError bersa raw `GetReplies` fallbackga o'tadi; full chat-history scan productionda default o'chirildi (`allow_history_scan_fallback=False`).
- Raw natija `pyrogram.utils.parse_messages()` orqali oddiy Pyrogram `Message` obyektiga aylantiriladi, shuning uchun mavjud yuborish pipeline o'zgarmaydi.
- `TopicExtractorConfig.stop_after_ids` qo'shildi; topic range anchorlari topilgach extractor butun topicni oxirigacha skan qilmaydi.
- `process_topic_posts()` range anchorlarni extractor'ga early-stop sifatida uzatadi.
- Regression testlar qo'shildi: raw `GetReplies` fallback, `FloodPremiumWait` string detection va topic range anchor topilgach sahifalash to'xtashi.

### Tekshiruv:
- `python -m py_compile core/retry_utils.py core/topic_extractor.py core/user_upload_worker.py core/session_manager/session_manager.py TechVJ/save.py test_governance_fixes.py` - OK
- `python -m compileall -q TechVJ core database main.py test_governance_fixes.py` - OK
- `test_governance_fixes.py` ichidagi barcha parametrsiz test funksiyalari manual runner bilan chaqirildi - OK
- Topic/flood regressionlar alohida chaqirildi, raw `GetReplies` fallback testi PASS.
- `pytest` venv'da o'rnatilmagan (`No module named pytest`), shuning uchun manual runner ishlatildi.

### O'zgartirilgan fayllar:
- `TechVJ/save.py`
- `core/retry_utils.py`
- `core/topic_extractor.py`
- `core/user_upload_worker.py`
- `core/session_manager/session_manager.py`
- `test_governance_fixes.py`
- `data/BUGS.md`, `data/PROMPTS.md`

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
- **Flood/pressure control:** Pyrofork flood variantlari (`FLOOD_PREMIUM_WAIT_X`) va katta topic/range `GetHistory` bosimini production darajada boshqarish
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
