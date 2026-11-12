"""Gate private bot interactions before login and command handlers."""
from __future__ import annotations

from wzgram import Client, filters

from config import OWNER_ID
from core.owner_settings import is_maintenance
from TechVJ.lang import get_string


@Client.on_message(filters.private & filters.incoming, group=-10001)
async def maintenance_message(client, message):
    user = message.from_user
    if not user or user.id == OWNER_ID or not is_maintenance():
        return
    # Users must still be able to cancel running work and remove their login.
    command = str(message.text or '').split(maxsplit=1)
    if command and command[0].split('@', 1)[0].lower() in ('/stop', '/cancel', '/logout'):
        return
    try:
        await message.reply(get_string(user.id, 'maintenance_notice'))
    finally:
        message.stop_propagation()


@Client.on_callback_query(group=-10001)
async def maintenance_callback(client, query):
    user = query.from_user
    if not user or user.id == OWNER_ID or not is_maintenance():
        return
    try:
        await query.answer(get_string(user.id, 'maintenance_notice'), show_alert=True)
    finally:
        query.stop_propagation()
