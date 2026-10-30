"""
core/role_manager.py — User role management.

Storage: MongoDB (via async_db) + local file cache at data/roles/<user_id>.txt
Default role for unknown users: new_user until /setrole global changes it.
Global policy overrides legacy files/DB; later individual commands override policy.
Roles never auto-upgrade — only owner can change via /setrole.

KEY RULES:
  - Owner (OWNER_ID) is ALWAYS VIP_USER — cannot be demoted.
  - set_role() silently ignores attempts to change owner's role.
  - Mongo sync is best-effort for ALL role changes, not only ban.
  - Cache is always invalidated after a write so next get_role is fresh.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import tempfile
from enum import Enum
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

_DATA_DIR = os.path.join("data", "roles")


class UserRole(Enum):
    NEW_USER    = "new_user"
    NORMAL_USER = "normal_user"
    VIP_USER    = "vip_user"
    BANNED      = "banned_user"


_VALID_ROLES = {r.value for r in UserRole}


class RoleManager:
    def __init__(self) -> None:
        os.makedirs(_DATA_DIR, exist_ok=True)
        self._cache: Dict[int, UserRole] = {}
        self._lock = asyncio.Lock()
        # Owner ID lazy-loaded to avoid circular import at module load time
        self._owner_id: Optional[int] = None

    def _get_owner_id(self) -> Optional[int]:
        """Lazy-load OWNER_ID to avoid circular imports."""
        if self._owner_id is None:
            try:
                from config import OWNER_ID
                self._owner_id = int(OWNER_ID) if OWNER_ID else None
            except Exception:
                pass
        return self._owner_id

    def _read_policy(self) -> Optional[Dict]:
        """Read every time so other instances and restarts see committed changes.

        A broken policy must raise rather than silently restore old permissions.
        """
        try:
            with open(os.path.join(_DATA_DIR, "global.json"), encoding="utf-8") as f:
                policy = json.load(f)
        except FileNotFoundError:
            return None
        if (
            not isinstance(policy, dict)
            or policy.get("role") not in _VALID_ROLES - {UserRole.BANNED.value}
            or not isinstance(policy.get("overrides"), dict)
            or any(value not in _VALID_ROLES for value in policy["overrides"].values())
        ):
            raise ValueError("Invalid global role policy")
        return policy

    def _write_policy(self, policy: Dict) -> None:
        """Publish the whole policy atomically; failed writes keep the old policy."""
        path = os.path.join(_DATA_DIR, "global.json")
        fd, temporary = tempfile.mkstemp(prefix=".global-", dir=_DATA_DIR)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(policy, f, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temporary, path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)

    def get_policy_role(self, user_id: int) -> Optional[UserRole]:
        """Effective policy role, or None when legacy role storage is active."""
        owner_id = self._get_owner_id()
        if owner_id and user_id == owner_id:
            return UserRole.VIP_USER
        policy = self._read_policy()
        if policy is None:
            return None
        return UserRole(policy["overrides"].get(str(user_id), policy["role"]))

    def has_global_policy(self) -> bool:
        return self._read_policy() is not None

    def get_policy_banned_users(self) -> Optional[List[Dict]]:
        """Effective ban list after a global reset; None means use legacy DB."""
        policy = self._read_policy()
        if policy is None:
            return None
        owner_id = self._get_owner_id()
        return [
            {"user_id": int(uid), "banned": True}
            for uid, role in policy["overrides"].items()
            if role == UserRole.BANNED.value and int(uid) != owner_id
        ]

    async def set_global_role(self, role: UserRole) -> None:
        """Reset everyone except owner, including future users and previous bans.

        Later /setrole, /ban and /unban create individual exceptions. The next
        global command clears those exceptions. No user enumeration is needed.
        """
        if not isinstance(role, UserRole) or role == UserRole.BANNED:
            raise ValueError("Global role must be new_user, normal_user or vip_user")
        async with self._lock:
            self._write_policy({"role": role.value, "overrides": {}})
            self._cache.clear()

    def _file_path(self, user_id: int) -> str:
        return os.path.join(_DATA_DIR, f"{user_id}.txt")

    def _read_file(self, user_id: int) -> Optional[UserRole]:
        path = self._file_path(user_id)
        try:
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as f:
                    raw = f.read().strip()
                if raw in _VALID_ROLES:
                    return UserRole(raw)
        except Exception as e:
            logger.warning("RoleManager: failed to read %s: %s", path, e)
        return None

    def _write_file(self, user_id: int, role: UserRole) -> None:
        path = self._file_path(user_id)
        try:
            with open(path, "w", encoding="utf-8") as f:
                f.write(role.value)
        except Exception as e:
            logger.warning("RoleManager: failed to write %s: %s", path, e)

    async def _read_db_role(self, user_id: int) -> Optional[UserRole]:
        """Fallback to local-first DB storage for cross-process consistency."""
        try:
            from database.async_db import async_db

            data = await async_db.find_user(user_id)
            raw_role = data.get("role") if data else None
            if raw_role in _VALID_ROLES:
                return UserRole(raw_role)
        except Exception as e:
            logger.debug("RoleManager: DB role lookup failed for %s: %s", user_id, e)
        return None

    async def get_role(self, user_id: int) -> UserRole:
        """Return role for user.

        Owner → always VIP_USER (overrides any stored value).
        Others  → file-first, then local-first DB fallback, then default new_user.
        """
        # Owner is always VIP — cannot be demoted via any command
        owner_id = self._get_owner_id()
        if owner_id and user_id == owner_id:
            return UserRole.VIP_USER

        policy_role = self.get_policy_role(user_id)
        if policy_role is not None:
            return policy_role

        # Always prefer the file so role changes are visible immediately even
        # when another process updates the role and this process has stale cache.
        role = self._read_file(user_id)
        if role is not None:
            self._cache[user_id] = role
            return role

        async with self._lock:
            policy_role = self.get_policy_role(user_id)
            if policy_role is not None:
                return policy_role
            role = self._read_file(user_id)
            if role is None:
                role = await self._read_db_role(user_id)
                if role is not None:
                    self._write_file(user_id, role)
            if role is None:
                role = UserRole.NEW_USER
            self._cache[user_id] = role
            return role

    async def set_role(self, user_id: int, role: UserRole) -> None:
        """Set an individual role and persist legacy storage best-effort.

        CRITICAL: Owner's role cannot be changed — silently ignored.
        Cache is always invalidated after write so next get_role is fresh.
        """
        # Protect owner from accidental role changes
        owner_id = self._get_owner_id()
        if owner_id and user_id == owner_id:
            logger.info("RoleManager: ignoring set_role for owner (always VIP)")
            return

        async with self._lock:
            policy = self._read_policy()
            if policy is not None:
                policy["overrides"][str(user_id)] = role.value
                self._write_policy(policy)
            self._write_file(user_id, role)
            self._cache[user_id] = role

            # Persist to local-first user storage immediately so other processes that
            # read roles via LocalStorage/Mongo can see the change without waiting.
            try:
                from database.async_db import async_db
                await async_db.update_user(
                    user_id,
                    {"role": role.value},
                    immediate_sync=True,
                )
            except Exception as e:
                logger.debug("RoleManager: async_db role persist failed (non-fatal): %s", e)

            # Keep the legacy ban table in lockstep with the role. Command guards
            # still consult this table in a few compatibility paths, so this local
            # write must complete before /ban or /unban reports success.
            try:
                from database.async_db import async_db
                if role == UserRole.BANNED:
                    await async_db.ban_user(
                        user_id,
                        owner_id or 0,
                        immediate_sync=True,
                    )
                else:
                    await async_db.unban_user(user_id, immediate_sync=True)
            except Exception as e:
                logger.warning("RoleManager: legacy ban state persist failed: %s", e)

    async def is_banned(self, user_id: int) -> bool:
        return (await self.get_role(user_id)) == UserRole.BANNED

    def invalidate(self, user_id: int) -> None:
        """Remove from in-memory cache (forces file re-read next call)."""
        self._cache.pop(user_id, None)


# Module-level singleton
role_manager = RoleManager()
