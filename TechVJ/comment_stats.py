"""
DEPRECATED — bu modulning /comments handleri TechVJ/save.py dagi
`comment_analyzer_command` ga ko'chirildi (2026-10-25, BUG-058).

Sabab: Pyrogram plugin loader ikkala modulni avtomatik import qilardi va
/comment (save.py) ham /comments (shu fayl) bir xil funksiyani raqobat bilan
bajarardi — alfavit tartibi tufayli natija registratsiya tartibiga bog'liq edi.

Endi:
  /comment va /comments — IKKALASI ham save.py comment_analyzer_command.

Bu fayl atayin qoldirilgan qayd; hech qanday handler yoki helper saqlanmadi
(barchasi git tarixida: 6886245 dan keyingi o'zgarishlarga qarang).
Faylni o'chirish ham xavfsiz.
"""
