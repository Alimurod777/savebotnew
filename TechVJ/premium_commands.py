"""
TechVJ/premium_commands.py - Premium-related commands.

Owner commands (OWNER_ID only):
  /setpremium <session_string>  — Set system Premium session
  /removepremium                — Remove system Premium session
  /premiumstatus                — Show system Premium session status
  /checkpremium <user_id|@username> — Live MTProto Premium lookup
  /add_premium_session <session_string> — Hot-load premium relay (NO RESTART)
  /remove_premium_session       — Remove premium relay at runtime

  /premium status               — Full relay status (sessions + relay channel)
  /premium add <session_string> — Add premium relay session (hot-reload)
  /premium remove <N>           — Remove session #N from relay pool
  /premium relay <channel_id>   — Set relay channel/group
  /premium test                 — Test all sessions + bot admin check
  /premium on                   — Enable premium relay
  /premium off                  — Disable premium relay (keeps sessions)

User commands (any user):
  /uploadsetting [auto|force_split|no_split] — Set upload split preference
  /split_media [on|off]         — Toggle caption splitting per-user
"""

import asyncio
import logging

from pyrogram import Client, filters
from pyrogram.types import Message

from config import OWNER_ID
from database.async_db import async_db
from core.premium_logic import (
    set_system_session,
    remove_system_session,
    get_system_session_status,
    check_user_premium,
    lookup_user_premium,
    get_user_upload_setting,
    set_user_upload_setting,
    UPLOAD_AUTO, UPLOAD_FORCE_SPLIT, UPLOAD_NO_SPLIT, VALID_SETTINGS,
)
from core.repost_router import (
    load_premium_session_runtime,
    remove_premium_session_runtime,
)
from core.reply_compat import build_reply_kwargs_from_message
from TechVJ.session_handler import create_user_session

logger = logging.getLogger(__name__)


async def _user_has_own_premium_session(client: Client, user_id: int) -> bool:
    """Return True only if the user is Telegram Premium AND has a session stored."""
    (user_is_premium, udata) = await asyncio.gather(
        check_user_premium(client, user_id),
        async_db.find_user(user_id),
    )
    user_session = udata.get('session') if udata else None
    return bool(user_is_premium and user_session)


# ==================== OWNER COMMANDS ====================

@Client.on_message(filters.command("setpremium") & filters.private)
async def set_premium_command(client: Client, message: Message):
    """Set system Premium session. Owner only."""
    if not message.from_user or message.from_user.id != OWNER_ID:
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.reply(
            "**Usage:** `/setpremium <session_string>`\n\n"
            "Provide a Pyrogram session string for a Telegram Premium account.\n"
            "This session will be used as the system uploader for large files.",
            **build_reply_kwargs_from_message(message)
        )
        return

    session_string = parts[1].strip()
    status_msg = await message.reply("Verifying session and Premium status...")

    success, result_msg = await set_system_session(session_string)

    if success:
        await status_msg.edit_text(f"**System Premium Session Set**\n\n{result_msg}")
    else:
        await status_msg.edit_text(f"**Failed**\n\n{result_msg}")


@Client.on_message(filters.command("removepremium") & filters.private)
async def remove_premium_command(client: Client, message: Message):
    """Remove system Premium session. Owner only."""
    if not message.from_user or message.from_user.id != OWNER_ID:
        return

    success, result_msg = await remove_system_session()
    await message.reply(result_msg, **build_reply_kwargs_from_message(message))


@Client.on_message(filters.command("premiumstatus") & filters.private)
async def premium_status_command(client: Client, message: Message):
    """Show system Premium session status. Owner only."""
    if not message.from_user or message.from_user.id != OWNER_ID:
        return

    status = get_system_session_status()
    await message.reply(status, **build_reply_kwargs_from_message(message))


@Client.on_message(filters.command("checkpremium") & filters.private)
async def check_premium_command(client: Client, message: Message):
    """
    Live MTProto lookup of a user's Telegram Premium status.
    Owner only. Does NOT depend on local database.
    """
    if not message.from_user or message.from_user.id != OWNER_ID:
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.reply(
            "**Usage:** `/checkpremium <user_id or @username>`\n\n"
            "Queries Telegram directly to check Premium status.",
            **build_reply_kwargs_from_message(message)
        )
        return

    target = parts[1].strip()
    # Parse as int if numeric
    if target.lstrip('-').isdigit():
        target = int(target)
    elif target.startswith('@'):
        target = target[1:]

    status_msg = await message.reply("Checking Premium status...")

    # Use a user session for the lookup (bot tokens can't see is_premium)
    user_data = await async_db.find_user(OWNER_ID)
    if not user_data or not user_data.get('session'):
        # Fallback: try bot client (limited, but may work for contacts)
        is_premium, info = await lookup_user_premium(client, target)
    else:
        # Use owner's user session for reliable MTProto lookup
        try:
            async with create_user_session(user_data['session'], OWNER_ID, timeout=15.0) as acc:
                is_premium, info = await lookup_user_premium(acc, target)
        except Exception as e:
            await status_msg.edit_text(f"**Error:** Session lookup failed: {e}")
            return

    if info is None:
        await status_msg.edit_text("**Error:** Could not find this user.")
        return

    if is_premium:
        text = (
            f"**User is Telegram Premium** ✅\n\n"
            f"**Name:** {info.get('first_name', '')}\n"
            f"**Username:** @{info.get('username', 'N/A')}\n"
            f"**User ID:** `{info.get('user_id', '')}`"
        )
    else:
        text = (
            f"**User is NOT Telegram Premium** ❌\n\n"
            f"**Name:** {info.get('first_name', '')}\n"
            f"**Username:** @{info.get('username', 'N/A')}\n"
            f"**User ID:** `{info.get('user_id', '')}`"
        )

    await status_msg.edit_text(text)


# ==================== HOT-LOAD PREMIUM SESSION (NO RESTART) ====================

@Client.on_message(filters.command("add_premium_session") & filters.private)
async def add_premium_session_command(client: Client, message: Message):
    """
    Hot-load a Premium relay session at runtime. Owner only.
    NO RESTART REQUIRED.
    
    Usage: /add_premium_session <session_string>
    
    The system will:
    1. Validate the session by connecting
    2. Verify the account is Telegram Premium
    3. Load into memory immediately
    4. Persist to disk for next startup
    """
    if not message.from_user or message.from_user.id != OWNER_ID:
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.reply(
            "**🔥 Hot-Load Premium Session**\n\n"
            "**Usage:** `/add_premium_session <session_string>`\n\n"
            "Adds a Premium relay session **without restarting** the bot.\n"
            "The session will be validated, loaded into memory,\n"
            "and persisted for future startups.",
            **build_reply_kwargs_from_message(message)
        )
        return

    session_string = parts[1].strip()
    status_msg = await message.reply("🔄 Validating session and checking Premium status...")

    try:
        success, result_msg = await load_premium_session_runtime(session_string)
    except Exception as e:
        await status_msg.edit_text(f"**❌ Error:** {e}")
        return

    if success:
        await status_msg.edit_text(
            f"**✅ Premium Session Hot-Loaded**\n\n"
            f"{result_msg}\n\n"
            f"🔥 Active immediately — no restart needed."
        )
    else:
        await status_msg.edit_text(f"**❌ Failed**\n\n{result_msg}")


@Client.on_message(filters.command("remove_premium_session") & filters.private)
async def remove_premium_session_command(client: Client, message: Message):
    """Remove premium relay session at runtime. Owner only."""
    if not message.from_user or message.from_user.id != OWNER_ID:
        return

    try:
        success, result_msg = await remove_premium_session_runtime()
    except Exception as e:
        await message.reply(f"**❌ Error:** {e}")
        return

    if success:
        await message.reply(
            f"**✅ Premium Session Removed**\n\n{result_msg}",
            **build_reply_kwargs_from_message(message)
        )
    else:
        await message.reply(f"**❌ Failed:** {result_msg}")


# ==================== USER COMMANDS ====================

@Client.on_message(filters.command("uploadsetting") & filters.private)
async def upload_setting_command(client: Client, message: Message):
    """
    Set upload split preference. Available to all users.
    
    Options:
      auto        — Default. No split only with your own Premium session.
      force_split — Always split even if Premium.
      no_split    — Never split (requires your own Premium session).
    """
    user_id = message.from_user.id
    parts = message.text.split(maxsplit=1)

    current = get_user_upload_setting(user_id)

    if len(parts) < 2:
        # Show current setting
        await message.reply(
            f"**Upload Setting**\n\n"
            f"Current: `{current}`\n\n"
            f"**Options:**\n"
            f"• `auto` — No split only with your own Premium session\n"
            f"• `force_split` — Always split even if Premium\n"
            f"• `no_split` — Never split (requires your own Premium session)\n\n"
            f"**Usage:** `/uploadsetting <option>`",
            **build_reply_kwargs_from_message(message)
        )
        return

    new_setting = parts[1].strip().lower()

    if new_setting not in VALID_SETTINGS:
        await message.reply(
            f"Invalid option: `{new_setting}`\n"
            f"Valid options: `auto`, `force_split`, `no_split`",
            **build_reply_kwargs_from_message(message)
        )
        return

    # no_split requires user's own Premium session to be available
    if new_setting == UPLOAD_NO_SPLIT:
        if not await _user_has_own_premium_session(client, user_id):
            await message.reply(
                "**Cannot set `no_split`**\n\n"
                "This option requires your own Telegram Premium session string.\n"
                "Without a valid personal session, files >2GB will be split.",
                **build_reply_kwargs_from_message(message)
            )
            return

    if set_user_upload_setting(user_id, new_setting):
        await message.reply(
            f"Upload setting changed to `{new_setting}`",
            **build_reply_kwargs_from_message(message)
        )
    else:
        await message.reply("Failed to save setting. Please try again.")


@Client.on_message(filters.command("split_media") & filters.private)
async def split_media_command(client: Client, message: Message):
    """
    Toggle caption/media splitting per user.
    
    Usage:
      /split_media on   — Enable splitting
      /split_media off  — Disable splitting (requires your own Premium session)

    Behavior:
      on  → force_split mode (always split captions/text)
      off → no_split mode (only works with your own Premium session)
    """
    user_id = message.from_user.id
    parts = message.text.split(maxsplit=1)

    current = get_user_upload_setting(user_id)

    if len(parts) < 2:
        # Show current status
        if current == UPLOAD_FORCE_SPLIT:
            status = "**ON** ✅ — Captions will always be split"
        elif current == UPLOAD_NO_SPLIT:
            status = "**OFF** ❌ — No splitting (your Premium session)"
        else:
            status = "**AUTO** 🔄 — System decides from your Premium+session"

        await message.reply(
            f"**Split Media Status:** {status}\n\n"
            f"**Usage:**\n"
            f"• `/split_media on` — Always split captions\n"
            f"• `/split_media off` — Never split (requires your own Premium session)\n"
            f"• `/split_media auto` — Let system decide",
            **build_reply_kwargs_from_message(message)
        )
        return

    choice = parts[1].strip().lower()

    if choice == "on":
        new_setting = UPLOAD_FORCE_SPLIT
    elif choice == "off":
        # Verify user's own Premium session is available
        if not await _user_has_own_premium_session(client, user_id):
            await message.reply(
                "**Cannot disable splitting** ❌\n\n"
                "This requires your own Telegram Premium session string.\n"
                "Use `/split_media on` or `/split_media auto` instead.",
                **build_reply_kwargs_from_message(message)
            )
            return
        new_setting = UPLOAD_NO_SPLIT
    elif choice == "auto":
        new_setting = UPLOAD_AUTO
    else:
        await message.reply(
            f"Invalid option: `{choice}`\n"
            f"Use: `on`, `off`, or `auto`",
            **build_reply_kwargs_from_message(message)
        )
        return

    if set_user_upload_setting(user_id, new_setting):
        labels = {
            UPLOAD_FORCE_SPLIT: "ON ✅ — Captions will always be split",
            UPLOAD_NO_SPLIT: "OFF ❌ — No splitting (your Premium session)",
            UPLOAD_AUTO: "AUTO 🔄 — System decides from your Premium+session",
        }
        await message.reply(
            f"**Split Media:** {labels.get(new_setting, new_setting)}",
            **build_reply_kwargs_from_message(message)
        )
    else:
        await message.reply("Failed to save setting. Please try again.")


# ==================== /premium — UNIFIED RELAY MANAGEMENT ====================

_PREMIUM_USAGE = (
    "**Premium relay boshqaruvi:**\n\n"
    "• `/premium status` — Holat va sessiyalar ro'yxati\n"
    "• `/premium add <session_string>` — Yangi sessiya qo'shish\n"
    "• `/premium remove <N>` — N-sessiyani olib tashlash\n"
    "• `/premium relay <channel_id>` — Relay kanal belgilash\n"
    "• `/premium test` — Barcha sessiyalar va kanal tekshirish\n"
    "• `/premium on` — Relay yoqish\n"
    "• `/premium off` — Relay o'chirish (sessiyalar saqlanadi)\n"
)


@Client.on_message(filters.command("premium") & filters.private)
async def premium_command(client: Client, message: Message):
    """Unified premium relay management. Owner only."""
    if not message.from_user or message.from_user.id != OWNER_ID:
        return

    parts = message.text.split(maxsplit=2)
    sub = parts[1].lower() if len(parts) > 1 else "status"

    if sub == "status":
        await _premium_status(client, message)
    elif sub == "add":
        arg = parts[2].strip() if len(parts) > 2 else ""
        await _premium_add(client, message, arg)
    elif sub == "remove":
        arg = parts[2].strip() if len(parts) > 2 else ""
        await _premium_remove(client, message, arg)
    elif sub == "relay":
        arg = parts[2].strip() if len(parts) > 2 else ""
        await _premium_relay(client, message, arg)
    elif sub == "test":
        await _premium_test(client, message)
    elif sub == "on":
        await _premium_on_off(client, message, True)
    elif sub == "off":
        await _premium_on_off(client, message, False)
    else:
        await message.reply(_PREMIUM_USAGE)


async def _premium_status(client: Client, message: Message):
    from core.premium_session_store import get_all_sessions, get_relay_config
    from core.premium_relay import relay_uploader

    sessions, relay_cfg = await asyncio.gather(
        get_all_sessions(),
        get_relay_config(),
    )

    lines = ["**Premium Relay holati**\n"]

    # Sessions
    if sessions:
        lines.append(f"**Sessiyalar ({len(sessions)} ta):**")
        for i, s in enumerate(sessions, 1):
            name = s.first_name or s.username or f"user_{s.user_id}"
            uname = f" @{s.username}" if s.username else ""
            lines.append(f"  {i}. {name}{uname} (`{s.user_id}`)")
    else:
        lines.append("**Sessiyalar:** Yo'q")

    lines.append("")

    # Relay channel
    if relay_cfg.channel_id:
        lines.append(f"**Relay kanal:** `{relay_cfg.channel_id}`")
    else:
        lines.append("**Relay kanal:** Belgilanmagan")

    lines.append(f"**Kanal yoqilgan:** {'✅' if relay_cfg.enabled else '❌'}")
    lines.append("")

    # Runtime pool
    pool = relay_uploader.pool
    lines.append(f"**Runtime pool:** {pool.count} ta sessiya")
    lines.append(f"**Mavjud sessiyalar:** {pool.available_count()} ta")
    lines.append(f"**Relay tayyor:** {'✅' if relay_uploader.is_ready else '❌'}")

    await message.reply("\n".join(lines))


async def _premium_add(client: Client, message: Message, session_string: str):
    if not session_string:
        await message.reply(
            "**Foydalanish:** `/premium add <session_string>`\n\n"
            "Premium hisob Pyrogram session stringini bering."
        )
        return

    status_msg = await message.reply("🔄 Sessiya tekshirilmoqda...")

    from core.premium_logic import check_user_premium_via_session
    from core.premium_session_store import add_session, PremiumSessionEntry, get_session_strings
    from core.premium_relay import relay_uploader
    import datetime

    is_premium, info = await check_user_premium_via_session(session_string)
    if info is None:
        await status_msg.edit_text("❌ Sessiya noto'g'ri yoki ulanib bo'lmadi.")
        return
    if not is_premium:
        name = info.get("first_name", "") or info.get("username", "") or str(info.get("user_id", ""))
        await status_msg.edit_text(
            f"❌ **{name}** Telegram Premium emas.\n"
            "Faqat Telegram Premium hisoblar qo'shilishi mumkin."
        )
        return

    entry = PremiumSessionEntry(
        session_string=session_string,
        user_id=info["user_id"],
        first_name=info.get("first_name", ""),
        username=info.get("username", ""),
        added_at=datetime.datetime.utcnow().isoformat(),
    )
    idx = await add_session(entry)

    # Hot-reload the pool
    new_strings = await get_session_strings()
    await relay_uploader.reload_sessions(new_strings)

    name = info.get("first_name", "") or info.get("username", "") or str(info["user_id"])
    uname = f" @{info['username']}" if info.get("username") else ""
    await status_msg.edit_text(
        f"✅ **Sessiya #{idx} qo'shildi**\n\n"
        f"Hisob: {name}{uname} (`{info['user_id']}`)\n"
        f"🔥 Hoziroq faol — restart kerak emas."
    )


async def _premium_remove(client: Client, message: Message, arg: str):
    if not arg or not arg.isdigit():
        await message.reply("**Foydalanish:** `/premium remove <N>`\nN = sessiya raqami (`/premium status` orqali ko'ring)")
        return

    n = int(arg)
    from core.premium_session_store import remove_session, get_session_strings
    from core.premium_relay import relay_uploader

    removed = await remove_session(n)
    if not removed:
        await message.reply(f"❌ Sessiya #{n} topilmadi.")
        return

    new_strings = await get_session_strings()
    await relay_uploader.reload_sessions(new_strings)
    await message.reply(f"✅ Sessiya #{n} olib tashlandi. Jami: {len(new_strings)} ta.")


async def _premium_relay(client: Client, message: Message, arg: str):
    if not arg:
        await message.reply(
            "**Foydalanish:** `/premium relay <channel_id>`\n\n"
            "Bot shu kanal/guruhda admin bo'lishi kerak (xabar yuborish huquqi).\n"
            "Kanal ID odatda `-100xxxxxxxxxx` ko'rinishida."
        )
        return

    try:
        channel_id = int(arg)
    except ValueError:
        await message.reply("❌ Kanal ID raqam bo'lishi kerak. Masalan: `-1001234567890`")
        return

    status_msg = await message.reply("🔄 Kanal tekshirilmoqda...")

    from core.premium_session_store import set_relay_channel, get_session_strings
    from core.premium_relay import relay_uploader

    await set_relay_channel(channel_id)

    # Re-init relay uploader with new channel
    strings = await get_session_strings()
    ok = await relay_uploader.init(strings, channel_id, client, notify_owner=False)

    if ok:
        await status_msg.edit_text(
            f"✅ **Relay kanal belgilandi:** `{channel_id}`\n"
            f"Bot admin: ✅\nRelay tayyor: ✅"
        )
    else:
        await status_msg.edit_text(
            f"⚠️ **Kanal saqlandi:** `{channel_id}`\n\n"
            "Lekin relay tayyor emas:\n"
            "• Bot kanal/guruhda admin emas, YOKI\n"
            "• Premium sessiya yo'q\n\n"
            "Bot adminlikka qo'shing, so'ng `/premium test` bilan tekshiring."
        )


async def _premium_test(client: Client, message: Message):
    from core.premium_session_store import get_all_sessions, get_relay_config
    from core.premium_logic import check_user_premium_via_session

    status_msg = await message.reply("🔄 Tekshirilmoqda...")

    sessions, relay_cfg = await asyncio.gather(
        get_all_sessions(),
        get_relay_config(),
    )

    lines = ["**Premium Relay tekshiruvi**\n"]

    # Test each session
    if sessions:
        lines.append("**Sessiyalar:**")
        for i, s in enumerate(sessions, 1):
            try:
                is_premium, info = await check_user_premium_via_session(s.session_string)
                if info is None:
                    lines.append(f"  {i}. ❌ Ulanib bo'lmadi (sessiya yaroqsiz?)")
                elif not is_premium:
                    lines.append(f"  {i}. ⚠️ {info.get('first_name', '')} — Premium emas!")
                else:
                    name = info.get("first_name", "") or info.get("username", "")
                    lines.append(f"  {i}. ✅ {name} (`{info['user_id']}`) — Premium")
            except Exception as e:
                lines.append(f"  {i}. ❌ Xato: {e}")
    else:
        lines.append("**Sessiyalar:** Yo'q ❌")

    lines.append("")

    # Test relay channel
    if relay_cfg.channel_id:
        lines.append(f"**Relay kanal:** `{relay_cfg.channel_id}`")
        try:
            member = await client.get_chat_member(relay_cfg.channel_id, "me")
            can_post = getattr(member.privileges, "can_post_messages", False) or \
                       str(getattr(member, "status", "")) in ("administrator", "creator")
            if can_post:
                lines.append("**Bot admin:** ✅")
            else:
                lines.append("**Bot admin:** ⚠️ Admin lekin post ruxsati yo'q")
        except Exception as e:
            lines.append(f"**Bot admin:** ❌ ({e})")
    else:
        lines.append("**Relay kanal:** Belgilanmagan ❌")

    lines.append("")

    # Yetishmayotgan narsalar
    missing = []
    if not sessions:
        missing.append("Premium sessiya qo'shing: `/premium add <session_string>`")
    if not relay_cfg.channel_id:
        missing.append("Relay kanal belgilang: `/premium relay <channel_id>`")
    if not relay_cfg.enabled:
        missing.append("Relay yoqing: `/premium on`")

    if missing:
        lines.append("**Yetishmayotganlar:**")
        for m in missing:
            lines.append(f"  • {m}")
    else:
        lines.append("✅ Hammasi tayyor!")

    await status_msg.edit_text("\n".join(lines))


async def _premium_on_off(client: Client, message: Message, enabled: bool):
    from core.premium_session_store import set_relay_enabled
    from core.premium_relay import relay_uploader

    await set_relay_enabled(enabled)
    relay_uploader.set_enabled(enabled)

    if enabled:
        await message.reply("✅ Premium relay **yoqildi**.")
    else:
        await message.reply("🔴 Premium relay **o'chirildi** (sessiyalar saqlanmoqda).")


# NOTE: /ownerhelp handler owner_commands.py ga ko'chirildi (bitta joyda barcha komandalar).
