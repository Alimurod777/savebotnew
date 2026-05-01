"""
core/channel_monitor.py — Yuklab taqiqlangan kanal monitoring tizimi.

Owner yuklab taqiqlangan kanallarni qo'shadi, bot shu kanallardan yangi
postlarni avtomatik ko'chiradi (owner'ning user session'i orqali).

Arxitektura:
  - JSON storage: data/channel_monitor/channels.json
  - Usergroup handler: yangi xabarlarni kuzatib, user session orqali yuklab,
    target chatga ko'chirish
  - Owner commands: /addchannel, /removechannel, /channels

MUHIM:
  - Bot emas, OWNER'ning user session'i yuklab oladi
  - Bot client faqat target chatga yuboradi
  - Owner tizim sessiyalaridan birini ishlatadi (SessionManager yoki legacy pool)
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass, field, asdict
from typing import Optional, Dict, List, Set

logger = logging.getLogger(__name__)

# Storage path
_DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "channel_monitor"
)
_CHANNELS_FILE = os.path.join(_DATA_DIR, "channels.json")


# ═══════════════════════════════════════════════════════════════════
# Data model
# ═══════════════════════════════════════════════════════════════════

@dataclass
class MonitoredChannel:
    """Yuklab taqiqlangan kanal."""
    channel_id: int                    # Manba kanal ID (masalan -1001234567890)
    target_chat_id: int                # Nusxa yuboriladigan chat ID
    label: str = ""                    # Ixtiyoriy nom/label
    enabled: bool = True               # Yoqilgan/o'chirilgan
    added_at: float = field(default_factory=time.time)
    last_forwarded_id: int = 0         # Oxirgi ko'chirilgan post ID

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> MonitoredChannel:
        return cls(
            channel_id=d["channel_id"],
            target_chat_id=d["target_chat_id"],
            label=d.get("label", ""),
            enabled=d.get("enabled", True),
            added_at=d.get("added_at", 0),
            last_forwarded_id=d.get("last_forwarded_id", 0),
        )


# ═══════════════════════════════════════════════════════════════════
# Channel Monitor — storage + management
# ═══════════════════════════════════════════════════════════════════

class ChannelMonitor:
    """
    Monitored channels boshqaruvchisi.

    - JSON file storage (data/channel_monitor/channels.json)
    - Add/remove/list/toggle
    - Kanal ID bo'yicha tezkor lookup (dict)
    """

    def __init__(self):
        self._channels: Dict[int, MonitoredChannel] = {}
        self._lock = asyncio.Lock()

    # ── Persistence ──────────────────────────────────────────────

    def load(self) -> None:
        """Fayldan yuklash. Bot start'da chaqiriladi."""
        os.makedirs(_DATA_DIR, exist_ok=True)
        if not os.path.exists(_CHANNELS_FILE):
            self._channels = {}
            return

        try:
            with open(_CHANNELS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            self._channels = {}
            for item in data:
                ch = MonitoredChannel.from_dict(item)
                self._channels[ch.channel_id] = ch
            logger.info("ChannelMonitor: loaded %d channels", len(self._channels))
        except Exception as e:
            logger.error("ChannelMonitor: load error: %s", e)
            self._channels = {}

    def _save(self) -> None:
        """Faylga saqlash (sync — lock ichida chaqiriladi)."""
        os.makedirs(_DATA_DIR, exist_ok=True)
        try:
            data = [ch.to_dict() for ch in self._channels.values()]
            with open(_CHANNELS_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error("ChannelMonitor: save error: %s", e)

    # ── CRUD ─────────────────────────────────────────────────────

    async def add_channel(self, channel_id: int, target_chat_id: int,
                          label: str = "") -> MonitoredChannel:
        """Kanal qo'shish. Mavjud bo'lsa yangilaydi."""
        async with self._lock:
            ch = MonitoredChannel(
                channel_id=channel_id,
                target_chat_id=target_chat_id,
                label=label,
            )
            self._channels[channel_id] = ch
            self._save()
            logger.info("ChannelMonitor: added channel %d -> %d (%s)",
                        channel_id, target_chat_id, label)
            return ch

    async def remove_channel(self, channel_id: int) -> bool:
        """Kanalni o'chirish."""
        async with self._lock:
            if channel_id in self._channels:
                del self._channels[channel_id]
                self._save()
                logger.info("ChannelMonitor: removed channel %d", channel_id)
                return True
            return False

    async def toggle_channel(self, channel_id: int, enabled: bool) -> bool:
        """Kanalni yoqish/o'chirish."""
        async with self._lock:
            ch = self._channels.get(channel_id)
            if ch:
                ch.enabled = enabled
                self._save()
                return True
            return False

    async def update_last_forwarded(self, channel_id: int, message_id: int) -> None:
        """Oxirgi ko'chirilgan post ID ni yangilash."""
        async with self._lock:
            ch = self._channels.get(channel_id)
            if ch and message_id > ch.last_forwarded_id:
                ch.last_forwarded_id = message_id
                self._save()

    def get_channel(self, channel_id: int) -> Optional[MonitoredChannel]:
        """Kanal ma'lumotini olish."""
        return self._channels.get(channel_id)

    def get_all(self) -> List[MonitoredChannel]:
        """Barcha kanallar ro'yxati."""
        return list(self._channels.values())

    def get_enabled(self) -> List[MonitoredChannel]:
        """Faqat yoqilgan kanallar."""
        return [ch for ch in self._channels.values() if ch.enabled]

    def is_monitored(self, channel_id: int) -> bool:
        """Kanal kuzatilayaptimi?"""
        ch = self._channels.get(channel_id)
        return ch is not None and ch.enabled

    @property
    def monitored_ids(self) -> Set[int]:
        """Yoqilgan kanal ID'lari (set — tezkor lookup uchun)."""
        return {ch.channel_id for ch in self._channels.values() if ch.enabled}

    @property
    def count(self) -> int:
        return len(self._channels)


# ═══════════════════════════════════════════════════════════════════
# Singleton
# ═══════════════════════════════════════════════════════════════════

channel_monitor = ChannelMonitor()
