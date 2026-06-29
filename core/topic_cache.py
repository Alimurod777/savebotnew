from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional


DEFAULT_TOPIC_CACHE_PATH = os.path.join("data", "topic_cache", "topics.json")


@dataclass
class TopicCacheEntry:
    chat_id: Any
    topic_id: int
    root_message_id: int
    known_message_ids: List[int] = field(default_factory=list)
    last_processed_message_id: Optional[int] = None
    fully_scanned: bool = False
    updated_at: float = 0.0


class TopicCache:
    """Small JSON cache for topic discovery metadata.

    The cache stores message IDs only. Message objects are fetched fresh by the
    caller, so file references and media metadata stay current.
    """

    def __init__(self, path: str = DEFAULT_TOPIC_CACHE_PATH) -> None:
        self.path = path
        self._lock = threading.Lock()

    def get(self, chat_id: Any, topic_id: int) -> Optional[TopicCacheEntry]:
        with self._lock:
            data = self._load_unlocked()
            item = data.get("entries", {}).get(self._key(chat_id, topic_id))
            if not item:
                return None
            try:
                return TopicCacheEntry(
                    chat_id=item.get("chat_id", chat_id),
                    topic_id=int(item.get("topic_id", topic_id)),
                    root_message_id=int(item.get("root_message_id", topic_id)),
                    known_message_ids=self._normalize_ids(item.get("known_message_ids", [])),
                    last_processed_message_id=(
                        int(item["last_processed_message_id"])
                        if item.get("last_processed_message_id") is not None
                        else None
                    ),
                    fully_scanned=bool(item.get("fully_scanned", False)),
                    updated_at=float(item.get("updated_at", 0.0)),
                )
            except Exception:
                return None

    def put(self, entry: TopicCacheEntry) -> None:
        entry.known_message_ids = self._normalize_ids(entry.known_message_ids)
        entry.updated_at = time.time()
        if entry.known_message_ids and entry.last_processed_message_id is None:
            entry.last_processed_message_id = entry.known_message_ids[-1]

        with self._lock:
            data = self._load_unlocked()
            data.setdefault("entries", {})[self._key(entry.chat_id, entry.topic_id)] = asdict(entry)
            self._save_unlocked(data)

    @staticmethod
    def _key(chat_id: Any, topic_id: int) -> str:
        return f"{chat_id}:{int(topic_id)}"

    @staticmethod
    def _normalize_ids(values: List[Any]) -> List[int]:
        seen = set()
        result: List[int] = []
        for value in values or []:
            try:
                msg_id = int(value)
            except (TypeError, ValueError):
                continue
            if msg_id in seen:
                continue
            seen.add(msg_id)
            result.append(msg_id)
        return result

    def _load_unlocked(self) -> Dict[str, Any]:
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                data.setdefault("entries", {})
                return data
        except FileNotFoundError:
            pass
        except Exception:
            pass
        return {"entries": {}}

    def _save_unlocked(self, data: Dict[str, Any]) -> None:
        directory = os.path.dirname(self.path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        tmp_path = f"{self.path}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=True)
        os.replace(tmp_path, self.path)


topic_cache = TopicCache()
