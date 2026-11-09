"""Owner-only HTML list of users with currently verified working sessions."""
from __future__ import annotations

import asyncio
import html
import tempfile
from contextlib import aclosing
from datetime import datetime, timezone
from pathlib import Path

from wzgram import Client, filters
from wzgram.enums import ParseMode

from config import OWNER_ID
from database.async_db import async_db
from TechVJ.session_handler import create_user_session

_running = False


async def verify_user(user_id, session):
    async def inspect():
        async with create_user_session(session, user_id, timeout=15, validate=False) as account:
            me = await asyncio.wait_for(account.get_me(), timeout=10)
            if not me or me.id != user_id or getattr(me, 'is_bot', False) or getattr(me, 'is_deleted', False):
                return None
            return {
                'id': user_id,
                'name': ' '.join(str(getattr(me, key, '') or '') for key in ('first_name', 'last_name')).strip(),
                'username': str(getattr(me, 'username', '') or ''),
                'premium': bool(getattr(me, 'is_premium', False)),
            }
    try:
        return await asyncio.wait_for(inspect(), timeout=45)
    except Exception:
        # Never include session strings, phone numbers or exception payloads.
        return None


async def collect_users():
    users, seen = [], set()
    checked = skipped = 0
    async with aclosing(async_db.iter_registered_users()) as registrations:
        async for row in registrations:
            uid = row.get('chat_id')
            if type(uid) is not int or uid <= 0 or uid in seen:
                continue
            seen.add(uid)
            data = await async_db.find_user(uid)
            session = data.get('session') if data else None
            if not session:
                continue
            checked += 1
            verified = await verify_user(uid, session)
            if verified:
                users.append(verified)
            else:
                skipped += 1
    return users, checked, skipped


def render_users(users, path):
    esc = lambda value: html.escape(str(value), quote=True)
    rows = ''.join(
        '<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>'.format(
            esc(user['name'] or 'Noma’lum'), esc(user['id']),
            esc('@' + user['username'] if user['username'] else 'yo‘q'),
            'Ha' if user['premium'] else 'Yo‘q')
        for user in users)
    stamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
    Path(path).write_text('''<!doctype html><html lang="uz"><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'">
<title>Ishlaydigan sessiyali foydalanuvchilar</title><style>
body{font:16px system-ui;margin:24px;background:#101827;color:#eef3ff}
table{border-collapse:collapse;width:100%}td,th{padding:12px;text-align:left;border:1px solid #536078;overflow-wrap:anywhere}
.wrap{overflow-x:auto}th{background:#24334b}</style>
<h1>Ishlaydigan sessiyali foydalanuvchilar</h1>'''
        + f'<p>Tekshirildi: {stamp} · Jami: {len(users)}</p>'
        + '<p>Sessiya va akkaunt IDsi Telegram orqali tekshirildi. Holat tekshirish vaqtiga tegishli.</p>'
        + '<div class="wrap"><table><thead><tr><th>Ism</th><th>User ID</th><th>Username</th><th>Telegram Premium</th></tr></thead>'
        + f'<tbody>{rows}</tbody></table></div></html>', encoding='utf-8')


@Client.on_message(filters.command('usersdata') & filters.private)
async def cmd_usersdata(client, message):
    global _running
    if not message.from_user or message.from_user.id != OWNER_ID:
        return
    if _running:
        await message.reply('Tekshiruv allaqachon davom etmoqda.', parse_mode=ParseMode.DISABLED)
        return
    _running = True
    try:
        status = await message.reply('Sessiyalar tekshirilmoqda. Faqat ishlaydiganlari hisobotga kiritiladi.', parse_mode=ParseMode.DISABLED)
        users, checked, skipped = await collect_users()
        with tempfile.TemporaryDirectory(prefix='usersdata-') as directory:
            path = Path(directory) / 'usersdata.html'
            render_users(users, path)
            await client.send_document(OWNER_ID, str(path),
                caption=f'Users Data — {len(users)} ta ishlaydigan sessiya\nTekshirildi: {checked}\nTasdiqlanmagan: {skipped} (hisobotga kiritilmadi)',
                parse_mode=ParseMode.DISABLED)
        await status.edit_text('Tekshiruv tugadi. HTML hisobot yuborildi.', parse_mode=ParseMode.DISABLED)
    except Exception:
        await message.reply('Tekshiruv yoki hisobot yuborish tugallanmadi. /usersdata orqali qayta urinib ko‘ring.', parse_mode=ParseMode.DISABLED)
    finally:
        _running = False
