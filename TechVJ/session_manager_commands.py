"""
TechVJ/session_manager_commands.py - /session owner command.

Owner-only sub-commands for the production-grade SessionManager.

Usage:
  /session list                              - List all sessions
  /session add global <session_string>       - Register GLOBAL session
  /session add borrowable <uid> <session>    - Register BORROWABLE session
  /session add dedicated <uid> <session>     - Register DEDICATED session
  /session remove <sid_prefix>               - Remove by 8-char UUID prefix
  /session disable <sid_prefix>              - Disable (fails for USER_OWNED)
  /session enable <sid_prefix>               - Enable
  /session borrow <sid_prefix> <on|off>      - Toggle allow_borrow
  /session parallel <sid_prefix> <n>         - Set max_parallel_tasks
  /session disableall global                 - Disable all GLOBAL sessions
  /session status                            - Full status report
"""

import json
import logging
import os
from typing import Optional

from pyrogram import Client, filters
from pyrogram.types import Message

from config import OWNER_ID
from core.session_manager import session_manager as _sm
from core.session_manager.models import SessionType

logger = logging.getLogger(__name__)
_PERSISTED_SESSIONS_FILE = os.path.join("data", "session_manager", "sessions.json")


def _owner_only(message: Message) -> bool:
    return bool(message.from_user and message.from_user.id == OWNER_ID)


def _find_by_prefix(prefix: str):
    """Return (record, None) or (None, error_str)."""
    if len(prefix) < 4:
        return None, "Prefix too short (min 4 chars)"
    rec = _sm.find_by_prefix(prefix)
    if rec is None:
        return None, f"No session found with prefix `{prefix}`"
    return rec, None


def _load_persisted_sessions() -> list:
    try:
        if not os.path.exists(_PERSISTED_SESSIONS_FILE):
            return []
        with open(_PERSISTED_SESSIONS_FILE, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        return data if isinstance(data, list) else []
    except Exception as e:
        logger.warning("session_command: failed to load persisted sessions: %s", e)
        return []


def _persisted_session_by_prefix(prefix: str) -> Optional[dict]:
    for entry in _load_persisted_sessions():
        if str(entry.get("session_id", "")).startswith(prefix):
            return entry
    return None


def _persisted_session_by_exact(session_id: str) -> Optional[dict]:
    for entry in _load_persisted_sessions():
        if entry.get("session_id") == session_id:
            return entry
    return None


async def _collect_personal_session_state(uid: int) -> dict:
    state = {
        "user_id": uid,
        "resolved": None,
        "local": None,
        "mongo": None,
        "selected": None,
        "selected_source": "none",
        "sync_ok": True,
        "notes": [],
    }

    try:
        from database.local_storage import LocalStorage, is_local_storage_available

        if is_local_storage_available():
            state["local"] = await LocalStorage.find_user(uid)
    except Exception as e:
        state["sync_ok"] = False
        state["notes"].append(f"local_read_failed={e}")

    try:
        from database.async_db import get_sessions_collection

        coll = get_sessions_collection()
        if coll is not None:
            state["mongo"] = await coll.find_one({"chat_id": uid})
    except Exception as e:
        state["sync_ok"] = False
        state["notes"].append(f"mongo_read_failed={e}")

    try:
        from database.async_db import async_db

        state["resolved"] = await async_db.find_user(uid)
    except Exception as e:
        state["sync_ok"] = False
        state["notes"].append(f"resolved_read_failed={e}")

    local_session = (state["local"] or {}).get("session")
    mongo_session = (state["mongo"] or {}).get("session")
    resolved_session = (state["resolved"] or {}).get("session")

    if state["local"] and state["mongo"] and local_session != mongo_session:
        state["sync_ok"] = False
        state["notes"].append("local_mongo_session_mismatch")

    if state["local"] and state["resolved"] and bool(local_session) != bool(resolved_session):
        state["sync_ok"] = False
        state["notes"].append("local_resolved_session_mismatch")
    if state["mongo"] and state["resolved"] and bool(mongo_session) != bool(resolved_session):
        state["sync_ok"] = False
        state["notes"].append("mongo_resolved_session_mismatch")

    for source_name in ("local", "mongo", "resolved"):
        source_data = state[source_name]
        if source_data and source_data.get("session"):
            state["selected_source"] = source_name
            state["selected"] = source_data
            break

    if state["selected"] is None:
        for source_name in ("local", "mongo", "resolved"):
            source_data = state[source_name]
            if source_data:
                state["selected_source"] = source_name
                state["selected"] = source_data
                break

    if state["selected_source"] == "mongo" and state["local"] and not local_session and mongo_session:
        state["notes"].append("using_mongo_session_fallback")
    if state["selected_source"] == "resolved" and not state["local"] and not state["mongo"] and state["resolved"]:
        state["notes"].append("using_resolved_fallback")

    return state


def _format_personal_session_state(state: dict) -> str:
    def _describe(source: Optional[dict], label: str) -> str:
        if not source:
            return f"{label}: not_found"
        return (
            f"{label}: found "
            f"(logged_in={bool(source.get('logged_in'))}, "
            f"session={'yes' if source.get('session') else 'no'}, "
            f"role={source.get('role', 'N/A')})"
        )

    lines = [
        f"User `{state['user_id']}` personal session state",
        _describe(state["local"], "local"),
        _describe(state["mongo"], "mongo"),
        _describe(state["resolved"], "resolved"),
        f"selected_source={state.get('selected_source', 'none')}",
        f"sync={'ok' if state['sync_ok'] else 'check_needed'}",
    ]
    if state["notes"]:
        lines.append(f"notes={', '.join(state['notes'])}")
    return "\n".join(lines)


def _format_pool_session_state(runtime_rec, persisted_rec: Optional[dict], prefix: str) -> str:
    sync_ok = bool(runtime_rec) == bool(persisted_rec)

    if runtime_rec and persisted_rec:
        sync_ok = (
            runtime_rec.type.value == persisted_rec.get("type")
            and runtime_rec.enabled == persisted_rec.get("enabled", True)
            and runtime_rec.allow_borrow == persisted_rec.get("allow_borrow", False)
            and runtime_rec.max_parallel_tasks == persisted_rec.get("max_parallel_tasks", 3)
        )

    lines = [
        f"Session `{prefix}` state",
        f"runtime={'found' if runtime_rec else 'missing'}",
        f"persisted={'found' if persisted_rec else 'missing'}",
        f"sync={'ok' if sync_ok else 'check_needed'}",
    ]
    if runtime_rec:
        lines.append(
            "runtime_details="
            f"type={runtime_rec.type.value}, enabled={runtime_rec.enabled}, "
            f"borrow={runtime_rec.allow_borrow}, tasks={runtime_rec.current_tasks}/{runtime_rec.max_parallel_tasks}"
        )
    if persisted_rec:
        lines.append(
            "persisted_details="
            f"type={persisted_rec.get('type')}, enabled={persisted_rec.get('enabled', True)}, "
            f"borrow={persisted_rec.get('allow_borrow', False)}, "
            f"max_parallel_tasks={persisted_rec.get('max_parallel_tasks', 3)}"
        )
    return "\n".join(lines)


@Client.on_message(filters.command("session") & filters.private)
async def session_command(client: Client, message: Message):
    """Dispatcher for all /session sub-commands."""
    if not _owner_only(message):
        return

    parts = message.text.split()
    if len(parts) < 2:
        await message.reply(
            "Usage: /session <list|add|remove|disable|enable|borrow|parallel|disableall|status>\n"
            "       /session <user_id> - foydalanuvchi DB sessiyasini ko'rish"
        )
        return

    sub = parts[1].lower()

    # /session <user_id> - fetch user's personal login session from multiple sources
    if parts[1].lstrip("-").isdigit():
        uid = int(parts[1])
        state = await _collect_personal_session_state(uid)
        user_data = state["selected"]
        if not user_data:
            await message.reply(
                f"User {uid} session sourcesda topilmadi.\n\n{_format_personal_session_state(state)}"
            )
            return

        session_str = user_data.get("session")
        if not session_str:
            await message.reply(
                f"{_format_personal_session_state(state)}\n\n"
                f"User `{uid}` yozuvi bor, lekin shaxsiy sessiyasi yo'q."
            )
            return

        await message.reply(_format_personal_session_state(state))
        await client.send_message(message.chat.id, f"`{session_str}`")
        return

    if sub == "list":
        records = _sm.registry.get_all()
        persisted = _load_persisted_sessions()
        if not records:
            if persisted:
                await message.reply(
                    f"Runtime registry empty, but persisted file has {len(persisted)} session(s)."
                )
            else:
                await message.reply("No sessions registered.")
            return

        lines = [f"Sessions ({len(records)} runtime / {len(persisted)} persisted):\n"]
        for i, rec in enumerate(records, 1):
            from core.session_manager.flood_controller import flood_controller

            flooded = flood_controller.is_flooded(rec.session_id)
            flood_str = ""
            if flooded:
                secs = flood_controller.seconds_until_free(rec.session_id)
                flood_str = f" [FLOOD {secs:.0f}s]"
            owner_str = f"owner={rec.owner_user_id}" if rec.owner_user_id else "owner=system"
            lines.append(
                f"{i}. {'OK' if rec.enabled else 'OFF'} "
                f"[{rec.type.value}] "
                f"sid={rec.session_id[:8]} "
                f"phone={rec.phone or '?'} "
                f"{owner_str} "
                f"tasks={rec.current_tasks}/{rec.max_parallel_tasks} "
                f"borrow={'Y' if rec.allow_borrow else 'N'}"
                f"{flood_str}"
            )

        runtime_ids = {rec.session_id for rec in records}
        persisted_ids = {entry.get("session_id") for entry in persisted}
        if runtime_ids != persisted_ids:
            lines.append("")
            lines.append(
                f"Registry sync check: runtime_only={len(runtime_ids - persisted_ids)}, "
                f"persisted_only={len(persisted_ids - runtime_ids)}"
            )
        await message.reply("\n".join(lines))
        return

    if sub == "status":
        persisted = _load_persisted_sessions()
        runtime = _sm.registry.get_all()
        runtime_ids = {rec.session_id for rec in runtime}
        persisted_ids = {entry.get("session_id") for entry in persisted}
        text = _sm.status_text()
        text += (
            f"\n\nRegistry sync\n"
            f"runtime={len(runtime)} persisted={len(persisted)}\n"
            f"runtime_only={len(runtime_ids - persisted_ids)} "
            f"persisted_only={len(persisted_ids - runtime_ids)}"
        )
        await message.reply(text)
        return

    if sub == "disableall":
        if len(parts) < 3 or parts[2].lower() != "global":
            await message.reply("Usage: /session disableall global")
            return
        count = await _sm.disable_all_global()
        persisted = _load_persisted_sessions()
        enabled_globals = [
            entry for entry in persisted
            if entry.get("type") == SessionType.GLOBAL.value and entry.get("enabled", True)
        ]
        await message.reply(
            f"Disabled {count} GLOBAL session(s).\n"
            f"Persisted enabled globals after write: {len(enabled_globals)}"
        )
        return

    if sub == "add":
        if len(parts) < 4:
            await message.reply(
                "Usage:\n"
                "  /session add global <session_string>\n"
                "  /session add borrowable <user_id> <session_string>\n"
                "  /session add dedicated <user_id> <session_string>"
            )
            return

        type_str = parts[2].lower()
        if type_str == "global":
            session_str = parts[3]
            owner_id = None
            stype = SessionType.GLOBAL
        elif type_str in ("borrowable", "dedicated"):
            if len(parts) < 5:
                await message.reply(f"Usage: /session add {type_str} <user_id> <session_string>")
                return
            try:
                owner_id = int(parts[3])
            except ValueError:
                await message.reply("user_id must be an integer.")
                return
            session_str = parts[4]
            stype = SessionType.BORROWABLE if type_str == "borrowable" else SessionType.DEDICATED
        else:
            await message.reply("Unknown type. Use: global, borrowable, or dedicated")
            return

        status_msg = await message.reply("Validating session (connecting to Telegram)...")
        ok, detail, rec = await _sm.register_session(
            session_string=session_str,
            session_type=stype,
            owner_user_id=owner_id,
        )
        if ok:
            persisted_rec = _persisted_session_by_exact(rec.session_id)
            await status_msg.edit(
                f"OK {detail}\n"
                f"Type: {stype.value}\n"
                f"Owner: {owner_id or 'system'}\n"
                f"Phone/name: {rec.phone or '?'}\n\n"
                f"{_format_pool_session_state(rec, persisted_rec, rec.session_id[:8])}"
            )
        else:
            await status_msg.edit(f"FAILED {detail}")
        return

    if sub == "remove":
        if len(parts) < 3:
            await message.reply("Usage: /session remove <sid_prefix>")
            return
        rec, err = _find_by_prefix(parts[2])
        if err:
            await message.reply(f"FAILED {err}")
            return
        prefix = rec.session_id[:8]
        ok = await _sm.remove_session(rec.session_id)
        await message.reply(
            (
                f"OK Removed session {prefix} ({rec.type.value})\n\n"
                f"{_format_pool_session_state(None, _persisted_session_by_prefix(prefix), prefix)}"
            ) if ok else "FAILED Remove failed (not found)"
        )
        return

    if sub == "disable":
        if len(parts) < 3:
            await message.reply("Usage: /session disable <sid_prefix>")
            return
        rec, err = _find_by_prefix(parts[2])
        if err:
            await message.reply(f"FAILED {err}")
            return
        ok, detail = await _sm.disable_session(rec.session_id)
        await message.reply(
            (
                f"OK Disabled session {rec.session_id[:8]}\n\n"
                f"{_format_pool_session_state(rec, _persisted_session_by_exact(rec.session_id), rec.session_id[:8])}"
            ) if ok else f"FAILED {detail}"
        )
        return

    if sub == "enable":
        if len(parts) < 3:
            await message.reply("Usage: /session enable <sid_prefix>")
            return
        rec, err = _find_by_prefix(parts[2])
        if err:
            await message.reply(f"FAILED {err}")
            return
        ok = await _sm.enable_session(rec.session_id)
        await message.reply(
            (
                f"OK Enabled session {rec.session_id[:8]}\n\n"
                f"{_format_pool_session_state(rec, _persisted_session_by_exact(rec.session_id), rec.session_id[:8])}"
            ) if ok else "FAILED Enable failed (not found)"
        )
        return

    if sub == "borrow":
        if len(parts) < 4:
            await message.reply("Usage: /session borrow <sid_prefix> <on|off>")
            return
        rec, err = _find_by_prefix(parts[2])
        if err:
            await message.reply(f"FAILED {err}")
            return
        toggle = parts[3].lower()
        if toggle not in ("on", "off"):
            await message.reply("Specify: on or off")
            return
        allow = toggle == "on"
        ok = await _sm.set_allow_borrow(rec.session_id, allow)
        await message.reply(
            (
                f"OK Session {rec.session_id[:8]} allow_borrow={'on' if allow else 'off'}\n\n"
                f"{_format_pool_session_state(rec, _persisted_session_by_exact(rec.session_id), rec.session_id[:8])}"
            ) if ok else "FAILED Update failed"
        )
        return

    if sub == "parallel":
        if len(parts) < 4:
            await message.reply("Usage: /session parallel <sid_prefix> <n>")
            return
        rec, err = _find_by_prefix(parts[2])
        if err:
            await message.reply(f"FAILED {err}")
            return
        try:
            n = int(parts[3])
            if n < 1:
                raise ValueError
        except ValueError:
            await message.reply("n must be a positive integer.")
            return
        ok = await _sm.set_max_parallel_tasks(rec.session_id, n)
        await message.reply(
            (
                f"OK Session {rec.session_id[:8]} max_parallel_tasks={n}\n\n"
                f"{_format_pool_session_state(rec, _persisted_session_by_exact(rec.session_id), rec.session_id[:8])}"
            ) if ok else "FAILED Update failed"
        )
        return

    await message.reply(
        "Unknown sub-command. Use: list, add, remove, disable, enable, "
        "borrow, parallel, disableall, status"
    )
