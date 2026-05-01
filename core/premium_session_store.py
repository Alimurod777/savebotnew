"""
core/premium_session_store.py — Persistent storage for premium session pool
and relay channel configuration.

Storage layout (data/premium/):
    sessions.json  — list of premium session entries
    relay.json     — relay channel ID + enabled flag

Thread/async safety: all mutations go through asyncio.Lock.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from dataclasses import asdict, dataclass
from typing import List, Optional

logger = logging.getLogger(__name__)

_DATA_DIR = os.path.join("data", "premium")
_SESSIONS_FILE = os.path.join(_DATA_DIR, "sessions.json")
_RELAY_FILE = os.path.join(_DATA_DIR, "relay.json")

_lock = asyncio.Lock()


# ── Data models ───────────────────────────────────────────────────────────────

@dataclass
class PremiumSessionEntry:
    session_string: str
    user_id: int
    first_name: str = ""
    username: str = ""
    added_at: str = ""          # ISO-8601 timestamp


@dataclass
class RelayConfig:
    channel_id: int = 0         # 0 = not configured
    enabled: bool = True        # owner can disable without removing


# ── Internal helpers ──────────────────────────────────────────────────────────

def _ensure_dirs() -> None:
    os.makedirs(_DATA_DIR, exist_ok=True)


def _load_sessions_raw() -> List[dict]:
    try:
        if os.path.exists(_SESSIONS_FILE):
            with open(_SESSIONS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                return data
    except Exception as e:
        logger.warning("premium_session_store: failed to load sessions: %s", e)
    return []


def _save_sessions_raw(entries: List[dict]) -> None:
    _ensure_dirs()
    with open(_SESSIONS_FILE, "w", encoding="utf-8") as f:
        json.dump(entries, f, ensure_ascii=False, indent=2)


def _load_relay_raw() -> dict:
    try:
        if os.path.exists(_RELAY_FILE):
            with open(_RELAY_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        logger.warning("premium_session_store: failed to load relay config: %s", e)
    return {"channel_id": 0, "enabled": True}


def _save_relay_raw(data: dict) -> None:
    _ensure_dirs()
    with open(_RELAY_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ── Public API: sessions ──────────────────────────────────────────────────────

async def get_all_sessions() -> List[PremiumSessionEntry]:
    """Return all stored premium sessions (may be empty)."""
    async with _lock:
        raw = _load_sessions_raw()
    return [
        PremiumSessionEntry(
            session_string=r["session_string"],
            user_id=r.get("user_id", 0),
            first_name=r.get("first_name", ""),
            username=r.get("username", ""),
            added_at=r.get("added_at", ""),
        )
        for r in raw
        if r.get("session_string")
    ]


async def add_session(entry: PremiumSessionEntry) -> int:
    """
    Append a new session entry.  Returns the 1-based index of the new entry.
    Duplicate session strings are silently ignored (returns existing index).
    """
    async with _lock:
        raw = _load_sessions_raw()
        # Dedup
        for i, r in enumerate(raw):
            if r.get("session_string") == entry.session_string:
                return i + 1
        raw.append(asdict(entry))
        _save_sessions_raw(raw)
        return len(raw)


async def remove_session(one_based_index: int) -> bool:
    """Remove session by 1-based index. Returns True if removed."""
    async with _lock:
        raw = _load_sessions_raw()
        idx = one_based_index - 1
        if idx < 0 or idx >= len(raw):
            return False
        raw.pop(idx)
        _save_sessions_raw(raw)
    return True


async def count_sessions() -> int:
    async with _lock:
        return len(_load_sessions_raw())


async def get_session_strings() -> List[str]:
    """Return just the session strings (used by pool at runtime)."""
    entries = await get_all_sessions()
    return [e.session_string for e in entries]


# ── Public API: relay channel ─────────────────────────────────────────────────

async def get_relay_config() -> RelayConfig:
    async with _lock:
        raw = _load_relay_raw()
    return RelayConfig(
        channel_id=int(raw.get("channel_id", 0)),
        enabled=bool(raw.get("enabled", True)),
    )


async def set_relay_channel(channel_id: int) -> None:
    async with _lock:
        raw = _load_relay_raw()
        raw["channel_id"] = channel_id
        _save_relay_raw(raw)


async def set_relay_enabled(enabled: bool) -> None:
    async with _lock:
        raw = _load_relay_raw()
        raw["enabled"] = enabled
        _save_relay_raw(raw)


async def clear_relay_channel() -> None:
    async with _lock:
        raw = _load_relay_raw()
        raw["channel_id"] = 0
        _save_relay_raw(raw)
