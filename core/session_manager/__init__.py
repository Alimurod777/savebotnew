"""
core/session_manager/__init__.py — Public API for the session manager package.
"""

from .models import SessionType, SessionRecord
from .flood_controller import flood_controller, FloodController
from .session_registry import SessionRegistry
from .borrow_manager import borrow_manager, BorrowManager
from .premium_worker import SessionWorkerBridge
from .session_manager import session_manager, SessionManager

__all__ = [
    # Models
    "SessionType",
    "SessionRecord",
    # Singletons
    "session_manager",
    "flood_controller",
    "borrow_manager",
    # Classes (for testing / custom instances)
    "SessionManager",
    "FloodController",
    "SessionRegistry",
    "BorrowManager",
    "SessionWorkerBridge",
]
