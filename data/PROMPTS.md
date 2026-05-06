# PROMPT TARIXI — Foydalanuvchi buyurtmalari va yo'nalishi

Bu fayl foydalanuvchi har sessiyada qanday so'rovlar berganini va loyiha qaysi yo'nalishda rivojlanayotganini kuzatib boradi.
Yangi Claude sessiyasi bu faylni O'QIB, kontekstni tushunishi KERAK.

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
