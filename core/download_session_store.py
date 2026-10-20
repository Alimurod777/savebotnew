"""
core/download_session_store.py — Per-user "download-only" session override.

Maqsad
------
Owner boshqa akkauntning sessiyasi orqali (yopiq kanal/chatlardan) yuklab olmoqchi,
lekin UPLOAD o'zining sessiyasi orqali ketishi kerak (aks holda fayl o'sha
akkauntning bot bilan chatiga yuklanadi).

Shu sababli DB'dagi asosiy `session` (upload sessiyasi) O'ZGARTIRILMAYDI.
Download uchun alohida sessiya shu yerda saqlanadi:

    data/download_sessions.json  →  {"<user_id>": {"session": "<session_string>", "account_id": <id>}}

Ishlatilishi:
    from core.download_session_store import get_download_session
    dl = get_download_session(user_id)   # None bo'lsa override yo'q

Faqat sinxron fayl I/O (kichik fayl); yozish atomik (tmp + os.replace).
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
from typing import Dict, Optional

logger = logging.getLogger(__name__)

_DATA_DIR = os.path.join("data")
_STORE_FILE = os.path.join(_DATA_DIR, "download_sessions.json")

_lock = threading.Lock()


def _store_path() -> str:
    return _STORE_FILE


def _load() -> Dict[str, dict]:
    """
    Read the store. Values are ``{"session": str, "account_id": int | missing}``.
    The legacy format (``{"<user_id>": "<session_string>"}``) is still accepted.
    """
    out: Dict[str, dict] = {}
    try:
        path = _store_path()
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                for k, v in data.items():
                    if isinstance(v, str) and v:
                        out[str(k)] = {"session": v}
                    elif isinstance(v, dict) and v.get("session"):
                        entry = {"session": str(v["session"])}
                        try:
                            if v.get("account_id") is not None:
                                entry["account_id"] = int(v["account_id"])
                        except (TypeError, ValueError):
                            pass
                        out[str(k)] = entry
    except Exception as err:
        logger.warning("download_session_store: load failed: %s", err)
    return out


def _save(data: Dict[str, dict]) -> None:
    path = _store_path()
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def get_download_session(user_id: int) -> Optional[str]:
    """Return the download-only session string for ``user_id`` or None."""
    try:
        key = str(int(user_id))
    except Exception:
        return None
    with _lock:
        entry = _load().get(key)
    return (entry or {}).get("session") or None


def get_download_account_id(user_id: int) -> Optional[int]:
    """
    Telegram id of the account behind the download-only session (None if unknown).

    Used ONLY for the download client (client name / device fingerprint), never for
    routing, queues, roles or upload — those stay bound to ``user_id``.
    """
    try:
        key = str(int(user_id))
    except Exception:
        return None
    with _lock:
        entry = _load().get(key)
    return (entry or {}).get("account_id")


def set_download_session(user_id: int, session_string: str, account_id: Optional[int] = None) -> None:
    """Store/replace the download-only session (and the id of its account) for ``user_id``."""
    session_string = (session_string or "").strip()
    if not session_string:
        raise ValueError("session_string cannot be empty")
    key = str(int(user_id))
    entry: Dict[str, object] = {"session": session_string}
    if account_id is not None:
        entry["account_id"] = int(account_id)
    with _lock:
        data = _load()
        data[key] = entry  # type: ignore[assignment]
        _save(data)


def clear_download_session(user_id: int) -> bool:
    """Remove the override. Returns True if something was removed."""
    key = str(int(user_id))
    with _lock:
        data = _load()
        if key not in data:
            return False
        data.pop(key, None)
        _save(data)
        return True


def session_fingerprint(session_string: Optional[str]) -> str:
    """Short irreversible fingerprint (safe to show in chat/logs)."""
    if not session_string:
        return "-"
    try:
        return hashlib.sha256(str(session_string).encode("utf-8")).hexdigest()[:10]
    except Exception:
        return "unknown"
