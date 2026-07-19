"""
TechVJ/owner_commands.py — Owner-only bot management commands.

All commands check OWNER_ID. Silent return on non-owner.

Commands:
  /ban <user_id>
  /unban <user_id>
  /setrole <user_id> <new_user|normal_user|vip_user>
  /userinfo <user_id>
  /set_parallel_limit <role> <n>
  /set_rate_limit <role> <n>
  /queue_status
  /queue_clear <user_id>   (not implemented — placeholder)
  /list_sessions           (delegates to session_manager)
  /enable_global_sessions
  /disable_global_sessions
  /stats
  /maintenance <on|off>
  /addchannel <channel_id> <target_chat_id> [label]
  /removechannel <channel_id>
  /channels
  /togglechannel <channel_id> <on|off>
  /failed_logs
  /grab <t.me/link>
  /grab <user_id> <t.me/link>
"""
from __future__ import annotations

import asyncio
import logging
from types import SimpleNamespace
from typing import Optional

from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.types import Message, CallbackQuery

from config import OWNER_ID
from database.async_db import async_db

# Governance modullarni guarded import qilish
# Agar import muvaffaqiyatsiz bo'lsa, handlerlar registratsiya qilinadi
# lekin governance-dependent komandalar xato xabar beradi
_GOVERNANCE_AVAILABLE = False
try:
    from core.role_manager import role_manager, UserRole
    from core.rate_limiter import rate_limiter
    from core.priority_queue import priority_queue
    _GOVERNANCE_AVAILABLE = True
except ImportError as _gov_err:
    logging.getLogger(__name__).warning(
        f"Governance modules not available: {_gov_err}. "
        "Commands /ban /unban /setrole /userinfo /stats /queue_status "
        "/set_rate_limit /set_parallel_limit will show error."
    )
    role_manager = None  # type: ignore
    rate_limiter = None  # type: ignore
    priority_queue = None  # type: ignore
    class UserRole:  # type: ignore
        NEW_USER = "new_user"
        NORMAL_USER = "normal_user"
        VIP_USER = "vip_user"
        BANNED = "banned_user"

logger = logging.getLogger(__name__)

# Maintenance mode flag (in-memory)
_maintenance_mode = False


def _owner(message: Message) -> bool:
    return message.from_user and message.from_user.id == OWNER_ID


async def _owner_or_bot_self(client: Client, message: Message) -> bool:
    """Allow owner commands, plus bot-authored /grab messages sent into a target chat."""
    if _owner(message):
        return True
    if getattr(message, "outgoing", False):
        return True
    from_user = getattr(message, "from_user", None)
    if not from_user:
        return False
    try:
        me = getattr(client, "me", None) or await client.get_me()
        return bool(me and from_user.id == me.id)
    except Exception:
        return False


def _parse_role(s: str):
    mapping = {
        "new_user":    UserRole.NEW_USER,
        "normal_user": UserRole.NORMAL_USER,
        "vip_user":    UserRole.VIP_USER,
    }
    return mapping.get(s.lower())


async def _collect_user_state(uid: int) -> dict:
    """Collect user state from runtime role manager + raw local/mongo/resolved DB layers."""
    state = {
        "user_id": uid,
        "runtime_role": None,
        "file_role": None,
        "db_role": None,
        "resolved_db_role": None,
        "local_db_role": None,
        "mongo_db_role": None,
        "local_banned": None,
        "legacy_banned": None,
        "resolved_role": None,
        "resolved_user": None,
        "local_user": None,
        "mongo_user": None,
        "selected_db_source": "none",
        "logged_in": False,
        "has_session": False,
        "phone": None,
        "sync_ok": True,
        "notes": [],
    }

    try:
        from database.local_storage import LocalStorage, is_local_storage_available

        if is_local_storage_available():
            state["local_user"] = await LocalStorage.find_user(uid)
            state["local_banned"] = await LocalStorage.is_banned(uid)
    except Exception as e:
        state["sync_ok"] = False
        state["notes"].append(f"local_db_read_failed={e}")

    try:
        from database.async_db import get_sessions_collection

        coll = get_sessions_collection()
        if coll is not None:
            state["mongo_user"] = await coll.find_one({"chat_id": uid})
    except Exception as e:
        state["sync_ok"] = False
        state["notes"].append(f"mongo_db_read_failed={e}")

    try:
        state["resolved_user"] = await async_db.find_user(uid)
        state["legacy_banned"] = await async_db.is_banned(uid)
    except Exception as e:
        state["sync_ok"] = False
        state["notes"].append(f"resolved_db_read_failed={e}")

    valid_roles = {"new_user", "normal_user", "vip_user", "banned_user"}
    for source_name, role_key in (
        ("local_user", "local_db_role"),
        ("mongo_user", "mongo_db_role"),
        ("resolved_user", "resolved_db_role"),
    ):
        user_data = state[source_name]
        role_value = (user_data or {}).get("role")
        if role_value in valid_roles:
            state[role_key] = role_value

    preferred_user = None
    for source_name, source_label in (
        ("local_user", "local"),
        ("mongo_user", "mongo"),
        ("resolved_user", "resolved"),
    ):
        user_data = state[source_name]
        if user_data and user_data.get("session"):
            preferred_user = user_data
            state["selected_db_source"] = source_label
            break

    if preferred_user is None:
        for source_name, source_label in (
            ("local_user", "local"),
            ("mongo_user", "mongo"),
            ("resolved_user", "resolved"),
        ):
            user_data = state[source_name]
            if user_data:
                preferred_user = user_data
                state["selected_db_source"] = source_label
                break

    if preferred_user:
        state["logged_in"] = bool(preferred_user.get("logged_in"))
        state["has_session"] = bool(preferred_user.get("session"))
        state["phone"] = preferred_user.get("phone")

    state["db_role"] = (
        state["local_db_role"]
        or state["mongo_db_role"]
        or state["resolved_db_role"]
    )

    if _GOVERNANCE_AVAILABLE:
        try:
            runtime_role = await role_manager.get_role(uid)
            state["runtime_role"] = runtime_role.value
            state["resolved_role"] = runtime_role.value
        except Exception as e:
            state["sync_ok"] = False
            state["notes"].append(f"runtime_role_failed={e}")

        try:
            if hasattr(role_manager, "_read_file"):
                file_role = role_manager._read_file(uid)
                state["file_role"] = file_role.value if file_role else None
        except Exception as e:
            state["sync_ok"] = False
            state["notes"].append(f"file_role_failed={e}")

    local_session = (state["local_user"] or {}).get("session")
    mongo_session = (state["mongo_user"] or {}).get("session")
    resolved_session = (state["resolved_user"] or {}).get("session")

    if state["local_user"] and state["mongo_user"] and local_session != mongo_session:
        state["sync_ok"] = False
        state["notes"].append("local_mongo_session_mismatch")
    if state["local_user"] and state["resolved_user"] and local_session != resolved_session:
        state["sync_ok"] = False
        state["notes"].append("local_resolved_session_mismatch")
    if state["mongo_user"] and state["resolved_user"] and mongo_session != resolved_session:
        state["sync_ok"] = False
        state["notes"].append("mongo_resolved_session_mismatch")

    if state["db_role"] and state["runtime_role"] and state["db_role"] != state["runtime_role"]:
        state["sync_ok"] = False
        state["notes"].append("db_role_mismatch")

    if state["file_role"] and state["runtime_role"] and state["file_role"] != state["runtime_role"]:
        state["sync_ok"] = False
        state["notes"].append("file_role_mismatch")

    expected_banned = state["runtime_role"] == UserRole.BANNED.value
    for key in ("local_banned", "legacy_banned"):
        if state[key] is not None and bool(state[key]) != expected_banned:
            state["sync_ok"] = False
            state["notes"].append(f"{key}_mismatch")

    if (
        state["local_db_role"]
        and state["mongo_db_role"]
        and state["local_db_role"] != state["mongo_db_role"]
    ):
        state["sync_ok"] = False
        state["notes"].append("local_mongo_role_mismatch")

    if state["resolved_role"] is None:
        state["resolved_role"] = state["db_role"] or state["file_role"] or "new_user"

    return state


def _format_user_state(state: dict, title: Optional[str] = None) -> str:
    def _describe_db(label: str, user_data: Optional[dict], role_value: Optional[str]) -> str:
        if not user_data:
            return f"{label}: `not_found`"
        return (
            f"{label}: `found, "
            f"logged_in={'yes' if user_data.get('logged_in') else 'no'}, "
            f"session={'present' if user_data.get('session') else 'missing'}, "
            f"role={role_value or 'N/A'}`"
        )

    lines = []
    if title:
        lines.append(title)
        lines.append("")

    lines.extend([
        f"User: `{state['user_id']}`",
        f"Resolved role: `{state['resolved_role']}`",
        f"Runtime role: `{state['runtime_role'] or 'N/A'}`",
        f"File role: `{state['file_role'] or 'N/A'}`",
        f"DB role: `{state['db_role'] or 'N/A'}`",
        f"DB source: `{state.get('selected_db_source', 'none')}`",
        f"Local ban: `{'yes' if state['local_banned'] else 'no'}`",
        f"Legacy ban: `{'yes' if state['legacy_banned'] else 'no'}`",
        f"Logged in: `{'yes' if state['logged_in'] else 'no'}`",
        f"Session: `{'present' if state['has_session'] else 'missing'}`",
        _describe_db("Local DB", state.get("local_user"), state.get("local_db_role")),
        _describe_db("Mongo DB", state.get("mongo_user"), state.get("mongo_db_role")),
        _describe_db("Resolved DB", state.get("resolved_user"), state.get("resolved_db_role")),
    ])

    if state["phone"]:
        lines.append(f"Phone: `{state['phone']}`")

    lines.append(f"Sync: `{'ok' if state['sync_ok'] else 'check_needed'}`")
    if state["notes"]:
        lines.append(f"Notes: `{', '.join(state['notes'])}`")

    return "\n".join(lines)


# ── /ban ─────────────────────────────────────────────────────────────────────

@Client.on_message(filters.command("ban") & filters.private)
async def cmd_ban(client: Client, message: Message):
    if not _owner(message):
        return
    if not _GOVERNANCE_AVAILABLE:
        await message.reply("❌ Governance modullari yuklanmagan. Bot loglarini tekshiring.")
        return
    parts = message.text.split()
    if len(parts) < 2:
        await message.reply("Usage: /ban <user_id>")
        return
    try:
        uid = int(parts[1])
    except ValueError:
        await message.reply("user_id must be an integer.")
        return
    if uid == OWNER_ID:
        await message.reply("Owner account cannot be banned.", parse_mode=ParseMode.DISABLED)
        return
    await role_manager.set_role(uid, UserRole.BANNED)
    role_manager.invalidate(uid)
    state = await _collect_user_state(uid)
    await message.reply(_format_user_state(state, "✅ User banned."))


# ── /unban ────────────────────────────────────────────────────────────────────

@Client.on_message(filters.command("unban") & filters.private)
async def cmd_unban(client: Client, message: Message):
    if not _owner(message):
        return
    if not _GOVERNANCE_AVAILABLE:
        await message.reply("❌ Governance modullari yuklanmagan. Bot loglarini tekshiring.")
        return
    parts = message.text.split()
    if len(parts) < 2:
        await message.reply("Usage: /unban <user_id>")
        return
    try:
        uid = int(parts[1])
    except ValueError:
        await message.reply("user_id must be an integer.")
        return
    if uid == OWNER_ID:
        await message.reply("Owner account is always vip_user.", parse_mode=ParseMode.DISABLED)
        return
    await role_manager.set_role(uid, UserRole.NORMAL_USER)
    role_manager.invalidate(uid)
    state = await _collect_user_state(uid)
    await message.reply(_format_user_state(state, "✅ User unbanned (role → normal_user)."))


# ── /setrole ──────────────────────────────────────────────────────────────────

@Client.on_message(filters.command("setrole") & filters.private)
async def cmd_setrole(client: Client, message: Message):
    if not _owner(message):
        return
    if not _GOVERNANCE_AVAILABLE:
        await message.reply("❌ Governance modullari yuklanmagan. Bot loglarini tekshiring.")
        return
    parts = message.text.split()
    if len(parts) < 3:
        await message.reply("Usage: /setrole <user_id> <new_user|normal_user|vip_user>")
        return
    try:
        uid = int(parts[1])
    except ValueError:
        await message.reply("user_id must be an integer.")
        return
    role = _parse_role(parts[2])
    if role is None:
        await message.reply("Valid roles: new_user, normal_user, vip_user")
        return
    await role_manager.set_role(uid, role)
    role_manager.invalidate(uid)
    state = await _collect_user_state(uid)
    await message.reply(_format_user_state(state, f"✅ User role updated → {role.value}"))


# ── /userinfo ─────────────────────────────────────────────────────────────────

@Client.on_message(filters.command("userinfo") & filters.private)
async def cmd_userinfo(client: Client, message: Message):
    if not _owner(message):
        return
    if not _GOVERNANCE_AVAILABLE:
        await message.reply("❌ Governance modullari yuklanmagan. Bot loglarini tekshiring.")
        return
    parts = message.text.split()
    if len(parts) < 2:
        await message.reply("Usage: /userinfo <user_id>")
        return
    try:
        uid = int(parts[1])
    except ValueError:
        await message.reply("user_id must be an integer.")
        return
    state = await _collect_user_state(uid)
    resolved_role = _parse_role(state["resolved_role"]) or UserRole.NEW_USER
    max_req, window = rate_limiter.get_limit(resolved_role)
    vip_q   = priority_queue.queue_size(UserRole.VIP_USER)
    norm_q  = priority_queue.queue_size(UserRole.NORMAL_USER)
    new_q   = priority_queue.queue_size(UserRole.NEW_USER)
    text = _format_user_state(state, "👤 User info")
    text += (
        f"\nRate limit: {max_req} req / {window}s\n"
        f"Queue sizes — vip:{vip_q} normal:{norm_q} new:{new_q}"
    )
    await message.reply(text)


# ── /set_parallel_limit ───────────────────────────────────────────────────────

@Client.on_message(filters.command("set_parallel_limit") & filters.private)
async def cmd_set_parallel(client: Client, message: Message):
    if not _owner(message):
        return
    if not _GOVERNANCE_AVAILABLE:
        await message.reply("❌ Governance modullari yuklanmagan. Bot loglarini tekshiring.")
        return
    parts = message.text.split()
    if len(parts) < 3:
        await message.reply("Usage: /set_parallel_limit <role> <n>")
        return
    role = _parse_role(parts[1])
    if role is None:
        await message.reply("Valid roles: new_user, normal_user, vip_user")
        return
    try:
        n = int(parts[2])
        if n < 1:
            raise ValueError
    except ValueError:
        await message.reply("n must be a positive integer.")
        return
    priority_queue.set_limit(role, n)
    await message.reply(f"✅ {role.value} parallel limit → {n}")


# ── /set_rate_limit ───────────────────────────────────────────────────────────

@Client.on_message(filters.command("set_rate_limit") & filters.private)
async def cmd_set_rate(client: Client, message: Message):
    if not _owner(message):
        return
    if not _GOVERNANCE_AVAILABLE:
        await message.reply("❌ Governance modullari yuklanmagan. Bot loglarini tekshiring.")
        return
    parts = message.text.split()
    if len(parts) < 3:
        await message.reply("Usage: /set_rate_limit <role> <n>  (n = max reqs per 10s)")
        return
    role = _parse_role(parts[1])
    if role is None:
        await message.reply("Valid roles: new_user, normal_user, vip_user")
        return
    try:
        n = int(parts[2])
        if n < 1:
            raise ValueError
    except ValueError:
        await message.reply("n must be a positive integer.")
        return
    rate_limiter.set_limit(role, n)
    await message.reply(f"✅ {role.value} rate limit → {n} req/10s")


# ── /queue_status ─────────────────────────────────────────────────────────────

@Client.on_message(filters.command("queue_status") & filters.private)
async def cmd_queue_status(client: Client, message: Message):
    if not _owner(message):
        return
    if not _GOVERNANCE_AVAILABLE:
        await message.reply("❌ Governance modullari yuklanmagan. Bot loglarini tekshiring.")
        return
    vip_q  = priority_queue.queue_size(UserRole.VIP_USER)
    norm_q = priority_queue.queue_size(UserRole.NORMAL_USER)
    new_q  = priority_queue.queue_size(UserRole.NEW_USER)
    lim    = priority_queue._limits
    text = (
        f"📊 Queue Status\n"
        f"VIP:    {vip_q} waiting  (max {lim.get(UserRole.VIP_USER,'?')} workers)\n"
        f"Normal: {norm_q} waiting (max {lim.get(UserRole.NORMAL_USER,'?')} workers)\n"
        f"New:    {new_q} waiting  (max {lim.get(UserRole.NEW_USER,'?')} workers)"
    )
    await message.reply(text)


# ── /enable_global_sessions / /disable_global_sessions ───────────────────────

@Client.on_message(filters.command("enable_global_sessions") & filters.private)
async def cmd_enable_global(client: Client, message: Message):
    if not _owner(message):
        return
    try:
        from core.session_manager import session_manager
        recs = session_manager.registry.get_global()
        count = 0
        for rec in recs:
            if not rec.enabled:
                ok = await session_manager.enable_session(rec.session_id)
                if ok:
                    count += 1
        await message.reply(f"✅ Enabled {count} global session(s).")
    except Exception as e:
        await message.reply(f"❌ {e}")


@Client.on_message(filters.command("disable_global_sessions") & filters.private)
async def cmd_disable_global(client: Client, message: Message):
    if not _owner(message):
        return
    try:
        from core.session_manager import session_manager
        count = await session_manager.disable_all_global()
        await message.reply(f"✅ Disabled {count} global session(s).")
    except Exception as e:
        await message.reply(f"❌ {e}")


# ── /stats ────────────────────────────────────────────────────────────────────

@Client.on_message(filters.command("stats") & filters.private)
async def cmd_stats(client: Client, message: Message):
    if not _owner(message):
        return
    if not _GOVERNANCE_AVAILABLE:
        await message.reply("❌ Governance modullari yuklanmagan. Bot loglarini tekshiring.")
        return
    vip_q  = priority_queue.queue_size(UserRole.VIP_USER)
    norm_q = priority_queue.queue_size(UserRole.NORMAL_USER)
    new_q  = priority_queue.queue_size(UserRole.NEW_USER)
    total  = vip_q + norm_q + new_q
    text = (
        f"📈 Bot Statistics\n"
        f"Active queue jobs: {total}\n"
        f"  VIP: {vip_q} | Normal: {norm_q} | New: {new_q}\n"
    )
    try:
        from core.session_manager import session_manager
        text += f"Sessions: {len(session_manager.registry.get_all())}\n"
        text += session_manager.status_text()
    except Exception:
        pass
    await message.reply(text)


# ── /maintenance ──────────────────────────────────────────────────────────────

@Client.on_message(filters.command("maintenance") & filters.private)
async def cmd_maintenance(client: Client, message: Message):
    global _maintenance_mode
    if not _owner(message):
        return
    parts = message.text.split()
    if len(parts) < 2 or parts[1].lower() not in ("on", "off"):
        await message.reply("Usage: /maintenance <on|off>")
        return
    _maintenance_mode = parts[1].lower() == "on"
    status = "🔧 Texnik xizmat rejimi YOQILDI." if _maintenance_mode else "✅ Bot normal rejimda ishlayapti."
    await message.reply(status)


def is_maintenance() -> bool:
    """Called by save.py shim to gate all requests during maintenance."""
    return _maintenance_mode


# ── /ownerhelp ────────────────────────────────────────────────────────────────

@Client.on_message(filters.command("ownerhelp") & filters.private)
async def cmd_ownerhelp(client: Client, message: Message):
    if not _owner(message):
        return
    text = (
        "📋 **Owner Kommandalar**\n\n"
        "**Governance:**\n"
        "`/ban <uid>` — Foydalanuvchini ban qilish\n"
        "`/unban <uid>` — Foydalanuvchini unban qilish\n"
        "`/setrole <uid> <rol>` — Rol o'rnatish (new_user|normal_user|vip_user)\n"
        "`/userinfo <uid>` — Foydalanuvchi ma'lumotlari\n"
        "`/set_rate_limit <rol> <n>` — Rate limit o'rnatish (req/10s)\n"
        "`/set_parallel_limit <rol> <n>` — Parallel worker soni\n"
        "`/queue_status` — Queue holati\n"
        "`/stats` — Umumiy statistika\n"
        "`/maintenance <on|off>` — Texnik xizmat rejimi\n\n"
        "**User sessiyalar (DB):**\n"
        "`/sessionupdate <uid> <session>` — User sessiyasini almashtirish\n"
        "`/sessionremove <uid>` — User sessiyasini o'chirish\n\n"
        "**Session pool:**\n"
        "`/session list` — Barcha poollar\n"
        "`/session add global <sess>` — Global pool sessiya qo'shish\n"
        "`/session add borrowable <uid> <sess>` — Borrowable sessiya\n"
        "`/session add dedicated <uid> <sess>` — Dedicated sessiya\n"
        "`/session remove <prefix>` — Pool sessiyani o'chirish\n"
        "`/session disable/enable <prefix>` — Pool sessiyani o'chir/yoq\n"
        "`/session borrow <prefix> <on|off>` — Borrow ruxsati\n"
        "`/session parallel <prefix> <n>` — Max parallel tasks\n"
        "`/session disableall global` — Barcha global poollarni o'chir\n"
        "`/session status` — Pool holati\n"
        "`/enable_global_sessions` — Barcha globallarni yoq\n"
        "`/disable_global_sessions` — Barcha globallarni o'chir\n\n"
        "**Premium relay:**\n"
        "`/premium status` — sessiyalar + pool holati\n"
        "`/premium add <session>` — pool'ga sessiya qo'shish\n"
        "`/premium remove <N>` — #N sessiyani olib tashlash\n"
        "`/premium test` — barcha sessiyalarni tekshirish\n"
        "`/premium on` — premium yoqish\n"
        "`/premium off` — premium o'chirish\n\n"
        "**Tizim sessiyasi (legacy):**\n"
        "`/setpremium <sess>` — tizim premium sessiyasini o'rnatish\n"
        "`/removepremium` — tizim sessiyasini o'chirish\n"
        "`/premiumstatus` — tizim sessiyasi holati\n"
        "`/checkpremium <id|@user>` — premium tekshirish\n\n"
        "**Kanal monitor va owner grab:**\n"
        "`/addchannel <channel_id> <target_chat_id> [label]` - Kanalni monitor qilish\n"
        "`/removechannel <channel_id>` - Monitor ro'yxatidan olib tashlash\n"
        "`/channels` - Monitor qilingan kanallar ro'yxati\n"
        "`/togglechannel <channel_id> <on|off>` - Kanal monitorini yoq/o'chir\n"
        "`/grab <t.me/link>` - Shu chat useri nomidan linkni ishlatish\n"
        "`/grab <uid> <t.me/link>` - Owner: linkni berilgan user nomidan ishlatish\n\n"
        "**Avto diagnostika:**\n"
        "Post yuborilmasa ownerga user_id, post_id, sabab va post tarkibi bo'yicha report yuboriladi.\n"
        "`/failed_logs` - Oxirgi 10 ta xatolik logini ko'rish\n"
        "Report faqat session faol, kanal mavjud va user kanalga a'zo ekani tasdiqlanganda ketadi.\n\n"
        "**Foydalanuvchi sozlamalari:**\n"
        "`/uploadsetting [auto|force_split|no_split]` — Upload rejimi\n"
        "`/split_media [on|off|auto]` — Caption bo'lish\n"
    )
    await message.reply(text, parse_mode=ParseMode.DISABLED)


def _short_failed_log_value(value, limit: int = 120) -> str:
    text = "-" if value is None else str(value)
    text = " ".join(text.split())
    if len(text) > limit:
        return text[: limit - 3] + "..."
    return text


async def _retry_failed_log_via_save(client: Client, row: dict, owner_id: int) -> None:
    retry_url = str(row.get("retry_url") or "").strip()
    target_user_id = int(row.get("user_id") or 0)
    url_type = str(row.get("url_type") or "").lower()

    anchor_id = None
    user_notice_ok = False
    try:
        anchor_msg = await client.send_message(
            target_user_id,
            f"Qayta urinilmoqda...\n{retry_url}",
            parse_mode=ParseMode.DISABLED,
        )
        anchor_id = getattr(anchor_msg, "id", None)
        user_notice_ok = True
    except Exception as anchor_err:
        logger.warning("failed-log retry: cannot send anchor to user %s: %s", target_user_id, anchor_err)

    if url_type != "public":
        user_data = await async_db.find_user(target_user_id)
        if not user_data or not user_data.get("logged_in") or not user_data.get("session"):
            text = (
                "Retry bekor qilindi: user sessiyasi faol emas.\n"
                f"Log ID: {row.get('id')}\n"
                f"User: {target_user_id}"
            )
            await client.send_message(owner_id, text, parse_mode=ParseMode.DISABLED)
            if user_notice_ok:
                try:
                    await client.send_message(
                        target_user_id,
                        "Qayta urinish uchun sessiyangiz faol emas. /login bilan qayta ulanib ko'ring.",
                        parse_mode=ParseMode.DISABLED,
                    )
                except Exception:
                    pass
            return

        try:
            from TechVJ.session_handler import create_user_session

            async with create_user_session(
                user_data["session"],
                target_user_id,
                timeout=20,
            ) as acc:
                await acc.get_me()
        except Exception as session_err:
            text = (
                "Retry bekor qilindi: user sessiyasini tiklab bo'lmadi.\n"
                f"Log ID: {row.get('id')}\n"
                f"User: {target_user_id}\n"
                f"Error: {type(session_err).__name__}: {session_err}"
            )
            await client.send_message(owner_id, text, parse_mode=ParseMode.DISABLED)
            if user_notice_ok:
                try:
                    await client.send_message(
                        target_user_id,
                        "Qayta urinishda sessiyani tiklab bo'lmadi. /login bilan qayta ulanib ko'ring.",
                        parse_mode=ParseMode.DISABLED,
                    )
                except Exception:
                    pass
            return

    proxy_message = SimpleNamespace(
        text=retry_url,
        chat=SimpleNamespace(id=target_user_id),
        from_user=SimpleNamespace(id=target_user_id, mention=f"User {target_user_id}"),
        id=anchor_id,
        message_id=anchor_id,
        reply_to_message_id=None,
    )

    async def _proxy_reply(text, **kwargs):
        kwargs.pop("quote", None)
        if anchor_id and "reply_to_message_id" not in kwargs and "reply_parameters" not in kwargs:
            kwargs["reply_to_message_id"] = anchor_id
        return await client.send_message(target_user_id, text, **kwargs)

    proxy_message.reply = _proxy_reply

    from TechVJ.save import save

    try:
        await save(client, proxy_message)
        owner_text = f"Retry yakunlandi.\nLog ID: {row.get('id')}\nUser: {target_user_id}\nNatija: save pipeline ishga tushdi"
        user_text = f"Qayta urinish yakunlandi.\nLog ID: {row.get('id')}"
    except Exception as retry_err:
        owner_text = (
            f"Retry xatolik bilan tugadi.\nLog ID: {row.get('id')}\n"
            f"User: {target_user_id}\nError: {type(retry_err).__name__}: {retry_err}"
        )
        user_text = "Qayta urinish xatolik bilan tugadi. Ownerga diagnostika yuborildi."
    finally:
        try:
            await client.send_message(owner_id, owner_text, parse_mode=ParseMode.DISABLED)
        except Exception:
            pass
        if user_notice_ok:
            try:
                await client.send_message(target_user_id, user_text, parse_mode=ParseMode.DISABLED)
            except Exception:
                pass


async def _retry_failed_log_to_chat(client: Client, row: dict, owner_id: int) -> None:
    retry_url = str(row.get("retry_url") or "").strip()
    target_chat_id = int(row.get("user_id") or 0)

    from TechVJ.save import (
        StopSafePipeline,
        create_user_session,
        parse_telegram_url,
        process_single_post,
        task_manager,
        _task_context_for_channel_monitor,
    )

    parsed, error = parse_telegram_url(retry_url)
    if error or not parsed or not parsed.post_ids:
        await client.send_message(
            owner_id,
            f"Retry bekor qilindi: URL parse xatosi.\nLog ID: {row.get('id')}\nError: {error or '-'}",
            parse_mode=ParseMode.DISABLED,
        )
        return

    source_chat_id = getattr(parsed, "channel_id", None)
    if not isinstance(source_chat_id, int):
        await client.send_message(
            owner_id,
            "Retry bekor qilindi: public username manba uchun target chat retry hozir qo'llanmaydi.\n"
            f"Log ID: {row.get('id')}",
            parse_mode=ParseMode.DISABLED,
        )
        return

    session_string = None
    session_user_id = owner_id
    try:
        from core.session_manager import session_manager as _sm_inst

        if _sm_inst._initialized:
            _sys_rec = _sm_inst.get_system_session_for_use()
            if _sys_rec:
                session_string = _sys_rec.session_string
                session_user_id = _sys_rec.owner_user_id or owner_id
    except Exception as sm_err:
        logger.debug("failed-log retry: SessionManager lookup failed: %s", sm_err)

    if not session_string:
        try:
            from core.premium_logic import get_system_session

            _legacy_system = get_system_session()
            if _legacy_system:
                session_string = _legacy_system.session_string
                session_user_id = _legacy_system.user_id or owner_id
        except Exception as legacy_err:
            logger.debug("failed-log retry: legacy system session lookup failed: %s", legacy_err)

    if not session_string:
        owner_data = await async_db.find_user(owner_id)
        if owner_data and owner_data.get("logged_in") and owner_data.get("session"):
            session_string = owner_data["session"]
            session_user_id = owner_id

    if not session_string:
        await client.send_message(
            owner_id,
            f"Retry bekor qilindi: tizim/owner sessiyasi topilmadi.\nLog ID: {row.get('id')}",
            parse_mode=ParseMode.DISABLED,
        )
        return

    post_id = int(parsed.post_ids[0])
    ctx = _task_context_for_channel_monitor(
        source_chat_id=source_chat_id,
        source_message_id=post_id,
        target_chat_id=target_chat_id,
    )

    async with StopSafePipeline(target_chat_id, task_manager) as pipeline:
        temp_dir = await pipeline.get_temp_dir()
        async with create_user_session(
            session_string,
            session_user_id,
            peers_to_resolve=[source_chat_id],
        ) as acc:
            result = await process_single_post(
                client,
                acc,
                None,
                source_chat_id,
                post_id,
                temp_dir,
                pipeline,
                target_user_id=target_chat_id,
                session_string=session_string,
                context=ctx,
            )
            ok = bool(result and result != "deleted")

    await client.send_message(
        owner_id,
        "Retry yakunlandi.\n"
        f"Log ID: {row.get('id')}\n"
        f"Target chat: {target_chat_id}\n"
        f"Natija: {'OK' if ok else 'failed'}",
        parse_mode=ParseMode.DISABLED,
    )
    try:
        await client.send_message(
            target_chat_id,
            "Qayta urinish yakunlandi.\n"
            f"Post: {post_id}\n"
            f"Natija: {'yuborildi' if ok else 'yuborilmadi'}",
            parse_mode=ParseMode.DISABLED,
        )
    except Exception as target_notice_err:
        logger.debug("failed-log retry target notice failed: %s", target_notice_err)


async def _run_failed_log_retry(client: Client, row: dict, owner_id: int) -> None:
    try:
        retry_url = str(row.get("retry_url") or "").strip()
        if not retry_url:
            await client.send_message(
                owner_id,
                f"Retry bekor qilindi: retry_url yo'q.\nLog ID: {row.get('id')}",
                parse_mode=ParseMode.DISABLED,
            )
            return

        target_id = int(row.get("user_id") or 0)
        if target_id > 0:
            await _retry_failed_log_via_save(client, row, owner_id)
        elif target_id < 0:
            await _retry_failed_log_to_chat(client, row, owner_id)
        else:
            await client.send_message(
                owner_id,
                f"Retry bekor qilindi: target user/chat ID yo'q.\nLog ID: {row.get('id')}",
                parse_mode=ParseMode.DISABLED,
            )
    except Exception as err:
        logger.warning("failed-log retry failed: %s", err)
        try:
            await client.send_message(
                owner_id,
                f"Retry xatolik bilan tugadi.\nLog ID: {row.get('id')}\nError: {type(err).__name__}: {err}",
                parse_mode=ParseMode.DISABLED,
            )
        except Exception:
            pass


@Client.on_message(filters.command("failed_logs") & filters.private)
async def cmd_failed_logs(client: Client, message: Message):
    if not _owner(message):
        return

    try:
        from database.local_storage import LocalStorage, is_local_storage_available

        if not is_local_storage_available():
            await message.reply("SQLite local storage mavjud emas.", parse_mode=ParseMode.DISABLED)
            return

        rows = await LocalStorage.get_failed_downloads(10)
        if not rows:
            await message.reply("Failed download loglari hozircha bo'sh.", parse_mode=ParseMode.DISABLED)
            return

        lines = ["Oxirgi failed download loglar:", ""]
        for row in rows:
            lines.extend([
                f"#{row.get('id')} | {row.get('created_at')}",
                f"User/chat: {row.get('user_id')} | Source: {row.get('chat_id')} | Post: {row.get('post_id')}",
                f"Stage: {_short_failed_log_value(row.get('stage'), 60)} | Reason: {_short_failed_log_value(row.get('reason'), 80)}",
                f"Retry: {row.get('retry_count') or 0} | URL: {_short_failed_log_value(row.get('retry_url'), 90)}",
                "",
            ])

        await message.reply("\n".join(lines).strip(), parse_mode=ParseMode.DISABLED)
    except Exception as err:
        await message.reply(
            f"Failed loglarni olishda xatolik: {type(err).__name__}: {err}",
            parse_mode=ParseMode.DISABLED,
        )


@Client.on_callback_query(filters.regex(r"^retry_failed:(\d+)$"))
async def cb_retry_failed_download(client: Client, callback_query: CallbackQuery):
    from_user = getattr(callback_query, "from_user", None)
    if not from_user or from_user.id != OWNER_ID:
        await callback_query.answer("Faqat owner.", show_alert=True)
        return

    try:
        log_id = int((callback_query.data or "").split(":", 1)[1])
    except Exception:
        await callback_query.answer("Log ID noto'g'ri.", show_alert=True)
        return

    from database.local_storage import LocalStorage, is_local_storage_available

    if not is_local_storage_available():
        await callback_query.answer("SQLite mavjud emas.", show_alert=True)
        return

    row = await LocalStorage.get_failed_download(log_id)
    if not row:
        await callback_query.answer("Log topilmadi.", show_alert=True)
        return
    if not row.get("retry_url"):
        await callback_query.answer("retry_url yo'q.", show_alert=True)
        return

    await LocalStorage.mark_failed_download_retry(log_id)
    await callback_query.answer("Qayta urinish boshlandi.", show_alert=False)
    asyncio.create_task(_run_failed_log_retry(client, row, int(from_user.id)))


# ── /sessionupdate ────────────────────────────────────────────────────────────

@Client.on_message(filters.command("sessionupdate") & filters.private)
async def cmd_sessionupdate(client: Client, message: Message):
    """Owner: /sessionupdate <user_id> <session_string> — replace a user's DB session."""
    if not _owner(message):
        return
    parts = message.text.split(maxsplit=2)
    if len(parts) < 3:
        await message.reply("Usage: /sessionupdate <user_id> <session_string>")
        return
    try:
        uid = int(parts[1])
    except ValueError:
        await message.reply("user_id must be an integer.")
        return
    new_session = parts[2].strip()
    if not new_session:
        await message.reply("Session string cannot be empty.")
        return
    try:
        from database.async_db import async_db
        await async_db.update_user(uid, session=new_session, logged_in=True)
        state = await _collect_user_state(uid)
        await message.reply(_format_user_state(state, "✅ User session updated."))
    except Exception as e:
        await message.reply(f"❌ Xatolik: {e}")


# ── /sessionremove ────────────────────────────────────────────────────────────

@Client.on_message(filters.command("sessionremove") & filters.private)
async def cmd_sessionremove(client: Client, message: Message):
    """Owner: /sessionremove <user_id> — clear a user's DB session."""
    if not _owner(message):
        return
    parts = message.text.split()
    if len(parts) < 2:
        await message.reply("Usage: /sessionremove <user_id>")
        return
    try:
        uid = int(parts[1])
    except ValueError:
        await message.reply("user_id must be an integer.")
        return
    try:
        from database.async_db import async_db
        await async_db.update_user(uid, session=None, logged_in=False)
        state = await _collect_user_state(uid)
        await message.reply(_format_user_state(state, "✅ User session removed."))
    except Exception as e:
        await message.reply(f"❌ Xatolik: {e}")


# ═══════════════════════════════════════════════════════════════════
# Channel Monitor Commands
# ═══════════════════════════════════════════════════════════════════

try:
    from core.channel_monitor import channel_monitor as _channel_monitor
    _CHANNEL_MONITOR_AVAILABLE = True
except ImportError:
    _CHANNEL_MONITOR_AVAILABLE = False
    _channel_monitor = None


def _parse_channel_id(raw: str) -> Optional[int]:
    """Parse channel ID from user input — supports -100xxx and plain number."""
    try:
        val = int(raw)
        # Agar foydalanuvchi qisqa ID bersa, -100 qo'shib to'liq ID yasash
        if val > 0 and len(str(val)) >= 10:
            return int(f"-100{val}")
        return val
    except ValueError:
        return None


# ── /addchannel ──────────────────────────────────────────────────

@Client.on_message(filters.command("addchannel") & filters.private)
async def cmd_addchannel(client: Client, message: Message):
    """
    Owner: /addchannel <channel_id> <target_chat_id> [label]

    Yuklab taqiqlangan kanalni kuzatishga qo'shish.
    channel_id — manba kanal (masalan -1001234567890 yoki 1234567890)
    target_chat_id — nusxa yuboriladigan chat (masalan owner'ning ID si)
    label — ixtiyoriy nom
    """
    if not _owner(message):
        return
    if not _CHANNEL_MONITOR_AVAILABLE:
        await message.reply("❌ Channel monitor moduli mavjud emas.")
        return

    parts = message.text.split(maxsplit=3)
    if len(parts) < 3:
        await message.reply(
            "**Foydalanish:**\n"
            "`/addchannel <channel_id> <target_chat_id> [label]`\n\n"
            "**Misol:**\n"
            "`/addchannel -1001234567890 123456789 Kurs kanali`"
        )
        return

    channel_id = _parse_channel_id(parts[1])
    target_id = _parse_channel_id(parts[2])
    label = parts[3] if len(parts) > 3 else ""

    if channel_id is None or target_id is None:
        await message.reply("❌ ID lar raqam bo'lishi kerak.")
        return

    ch = await _channel_monitor.add_channel(channel_id, target_id, label)
    _enabled_text = "Yoqilgan" if ch.enabled else "O'chirilgan"
    await message.reply(
        f"✅ **Kanal qo'shildi**\n\n"
        f"Manba: `{ch.channel_id}`\n"
        f"Target: `{ch.target_chat_id}`\n"
        f"Label: {ch.label or '—'}\n"
        f"Holat: {_enabled_text}"
    )


# ── /removechannel ──────────────────────────────────────────────

@Client.on_message(filters.command("removechannel") & filters.private)
async def cmd_removechannel(client: Client, message: Message):
    """Owner: /removechannel <channel_id>"""
    if not _owner(message):
        return
    if not _CHANNEL_MONITOR_AVAILABLE:
        await message.reply("❌ Channel monitor moduli mavjud emas.")
        return

    parts = message.text.split()
    if len(parts) < 2:
        await message.reply("Usage: `/removechannel <channel_id>`")
        return

    channel_id = _parse_channel_id(parts[1])
    if channel_id is None:
        await message.reply("❌ ID raqam bo'lishi kerak.")
        return

    removed = await _channel_monitor.remove_channel(channel_id)
    if removed:
        await message.reply(f"✅ Kanal `{channel_id}` o'chirildi.")
    else:
        await message.reply(f"❌ Kanal `{channel_id}` topilmadi.")


# ── /channels ────────────────────────────────────────────────────

@Client.on_message(filters.command("channels") & filters.private)
async def cmd_channels(client: Client, message: Message):
    """Owner: /channels — barcha kuzatilayotgan kanallar ro'yxati."""
    if not _owner(message):
        return
    if not _CHANNEL_MONITOR_AVAILABLE:
        await message.reply("❌ Channel monitor moduli mavjud emas.")
        return

    channels = _channel_monitor.get_all()
    if not channels:
        await message.reply("📋 Hech qanday kanal kuzatilmayapti.\n\n`/addchannel` bilan qo'shing.")
        return

    lines = ["📋 **Kuzatilayotgan kanallar**\n"]
    for i, ch in enumerate(channels, 1):
        status = "✅" if ch.enabled else "❌"
        lines.append(
            f"{i}. {status} `{ch.channel_id}`\n"
            f"   → `{ch.target_chat_id}`"
            f"{f' ({ch.label})' if ch.label else ''}\n"
            f"   Oxirgi: `{ch.last_forwarded_id}`"
        )

    await message.reply("\n".join(lines))


# ── /togglechannel ───────────────────────────────────────────────

@Client.on_message(filters.command("togglechannel") & filters.private)
async def cmd_togglechannel(client: Client, message: Message):
    """Owner: /togglechannel <channel_id> <on|off>"""
    if not _owner(message):
        return
    if not _CHANNEL_MONITOR_AVAILABLE:
        await message.reply("❌ Channel monitor moduli mavjud emas.")
        return

    parts = message.text.split()
    if len(parts) < 3 or parts[2].lower() not in ("on", "off"):
        await message.reply("Usage: `/togglechannel <channel_id> <on|off>`")
        return

    channel_id = _parse_channel_id(parts[1])
    if channel_id is None:
        await message.reply("❌ ID raqam bo'lishi kerak.")
        return

    enabled = parts[2].lower() == "on"
    ok = await _channel_monitor.toggle_channel(channel_id, enabled)
    if ok:
        state = "yoqildi ✅" if enabled else "o'chirildi ❌"
        await message.reply(f"Kanal `{channel_id}` {state}")
    else:
        await message.reply(f"❌ Kanal `{channel_id}` topilmadi.")


# ═══════════════════════════════════════════════════════════════════
# /grab — Owner user nomidan link qayta ishlash
# ═══════════════════════════════════════════════════════════════════

# Legacy implementation is intentionally left undecorated; cmd_grab_v2 below
# supports both chat-local and owner-delegated forms.
# @Client.on_message(filters.command("grab") & filters.private)
async def _cmd_grab_legacy(client: Client, message: Message):
    """
    Owner: /grab <user_id> <t.me/link>

    Shu user nomidan link'ni qayta ishlaydi — xuddi user o'zi
    yuborgan dek. User'ning session'i ishlatiladi.

    Misol:
      /grab 123456789 https://t.me/c/1234567890/101
      /grab 123456789 https://t.me/c/1234567890/101-110
    """
    if not _owner(message):
        return

    parts = message.text.split(maxsplit=2)
    if len(parts) < 3:
        await message.reply(
            "**Foydalanish:**\n"
            "`/grab <user_id> <t.me_link>`\n\n"
            "**Misol:**\n"
            "`/grab 123456789 https://t.me/c/1234567890/101`\n"
            "`/grab 123456789 https://t.me/c/1234567890/101-200`"
        )
        return

    try:
        target_user_id = int(parts[1])
    except ValueError:
        await message.reply("❌ user_id raqam bo'lishi kerak.")
        return

    url = parts[2].strip()
    if "t.me/" not in url:
        await message.reply("❌ Yaroqli t.me havolasi kerak.")
        return

    # User session borligini tekshirish
    user_data = await async_db.find_user(target_user_id)
    if not user_data or not user_data.get("session") or not user_data.get("logged_in"):
        await message.reply(
            f"❌ User `{target_user_id}` tizimga kirmagan yoki session yo'q.\n"
            "User avval /login qilishi kerak."
        )
        return

    await message.reply(
        f"⏳ User `{target_user_id}` nomidan ishlov boshlanmoqda...\n"
        f"Link: `{url}`"
    )

    # User chatiga status xabar yuborish — reply anchor sifatida
    try:
        anchor_msg = await client.send_message(
            target_user_id,
            f"⏳ Yuklanmoqda...\n`{url}`"
        )
        anchor_id = anchor_msg.id
    except Exception as _anchor_err:
        logger.warning("grab: cannot send anchor to user %s: %s", target_user_id, _anchor_err)
        anchor_id = None

    # Proxy message yaratish — xuddi user yuborgan dek
    # save() funksiyasi message.text dan URL oladi va message.chat.id dan user_id oladi
    from types import SimpleNamespace

    proxy_message = SimpleNamespace(
        text=url,
        chat=SimpleNamespace(id=target_user_id),
        from_user=SimpleNamespace(id=target_user_id, mention=f"User {target_user_id}"),
        id=anchor_id,  # user chatidagi anchor xabar — reply uchun
        reply_to_message_id=None,
    )

    # reply metodini qo'shish — save() ichida ishlatilishi mumkin
    async def _proxy_reply(text, **kwargs):
        return await client.send_message(target_user_id, text, **kwargs)

    proxy_message.reply = _proxy_reply

    # save() ni chaqirish — proxy message bilan
    try:
        from TechVJ.save import save
        await save(client, proxy_message)
    except Exception as e:
        await message.reply(f"❌ Xatolik: {type(e).__name__}: {e}")


@Client.on_message(filters.command("grab") & filters.private)
async def cmd_grab_v2(client: Client, message: Message):
    """
    Process a t.me link in the current private chat's user context.

    Supported:
      /grab <t.me/link>
      /grab <user_id> <t.me/link>   (owner only)
    """
    text = message.text or ""
    parts = text.split(maxsplit=2)
    if len(parts) < 2:
        await message.reply(
            "Foydalanish:\n"
            "/grab <t.me_link>\n"
            "/grab <user_id> <t.me_link>\n\n"
            "Misol:\n"
            "/grab https://t.me/c/1234567890/101\n"
            "/grab 123456789 https://t.me/c/1234567890/101-200",
            parse_mode=ParseMode.DISABLED,
        )
        return

    delegated = False
    target_user_id = int(message.chat.id)
    url = text.split(maxsplit=1)[1].strip()

    if len(parts) >= 3:
        try:
            target_user_id = int(parts[1])
            url = parts[2].strip()
            delegated = True
        except ValueError:
            delegated = False

    if delegated:
        if not _owner(message):
            return
    else:
        # Local /grab is equivalent to sending the raw link in this chat.
        # Bot-authored/outgoing messages are accepted for Bot API orchestration.
        if not await _owner_or_bot_self(client, message):
            from_user = getattr(message, "from_user", None)
            if not from_user or int(getattr(from_user, "id", 0)) != target_user_id:
                return

    if "t.me/" not in url:
        await message.reply(
            "Yaroqli t.me havolasi kerak.",
            parse_mode=ParseMode.DISABLED,
        )
        return

    if delegated:
        await message.reply(
            f"User {target_user_id} nomidan ishlov boshlanmoqda...\n"
            f"Link: {url}",
            parse_mode=ParseMode.DISABLED,
        )

    if target_user_id == message.chat.id:
        anchor_id = message.id
    else:
        try:
            anchor_msg = await client.send_message(
                target_user_id,
                f"Yuklanmoqda...\n{url}",
                parse_mode=ParseMode.DISABLED,
            )
            anchor_id = anchor_msg.id
        except Exception as anchor_err:
            logger.warning("grab: cannot send anchor to user %s: %s", target_user_id, anchor_err)
            anchor_id = None

    from types import SimpleNamespace

    proxy_message = SimpleNamespace(
        text=url,
        chat=SimpleNamespace(id=target_user_id),
        from_user=SimpleNamespace(id=target_user_id, mention=f"User {target_user_id}"),
        id=anchor_id,
        message_id=anchor_id,
        reply_to_message_id=None,
    )

    async def _proxy_reply(text, **kwargs):
        kwargs.pop("quote", None)
        if anchor_id and "reply_to_message_id" not in kwargs and "reply_parameters" not in kwargs:
            kwargs["reply_to_message_id"] = anchor_id
        return await client.send_message(target_user_id, text, **kwargs)

    proxy_message.reply = _proxy_reply

    async def _run_grab():
        try:
            from TechVJ.save import save
            await save(client, proxy_message)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.exception("grab: background task failed for user %s", target_user_id)
            await client.send_message(
                target_user_id,
                f"Xatolik: {type(e).__name__}: {e}",
                parse_mode=ParseMode.DISABLED,
            )

    from TechVJ.task_manager import task_manager

    await task_manager.create_task(
        target_user_id,
        _run_grab(),
        name=f"grab-{target_user_id}-{anchor_id or 'no-anchor'}",
    )
