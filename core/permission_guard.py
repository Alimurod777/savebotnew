"""
core/permission_guard.py — Validate ParsedURL against user role.

Rules:
  new_user    → max 1 post_id, no ranges, no comma lists
  normal_user → unlimited
  vip_user    → unlimited
  banned      → always rejected (handled before this)

Uses the existing ParsedURL from TechVJ/save.py.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Tuple

from core.role_manager import UserRole

if TYPE_CHECKING:
    # Avoid circular import; ParsedURL is checked by duck typing
    pass


_NEW_USER_MSG = (
    "❌ Yangi foydalanuvchi sifatida siz faqat bitta post havolasini "
    "yuborishingiz mumkin.\n"
    "Range va ko'p IDlar uchun botdan to'liq foydalaning yoki "
    "admin bilan bog'laning."
)


class PermissionGuard:
    def validate(self, parsed_url, role: UserRole) -> Tuple[bool, str]:
        """
        Validate parsed_url against role restrictions.
        Returns (allowed, error_message).
        error_message is empty string when allowed.
        """
        if role in (UserRole.NEW_USER,):
            post_ids = getattr(parsed_url, "post_ids", None) or []
            if len(post_ids) != 1:
                return False, _NEW_USER_MSG
        return True, ""


# Module-level singleton
permission_guard = PermissionGuard()
