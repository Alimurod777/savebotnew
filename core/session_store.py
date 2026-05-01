"""
core/session_store.py - Session file storage with Mongo primary + ./sessions/ fallback.

STORAGE POLICY:
  1. If DB_URI is configured and motor is available → use MongoDB (primary).
  2. If MongoDB fails or is unavailable → auto-create ./sessions/ directory
     and store Pyrogram session files there.
  3. NEVER raise sqlite3.OperationalError — SQLite locking issues on Colab/Kaggle
     are bypassed entirely because Pyrofork sessions use its own SQLite internally
     when given a file path; we manage the directory, not the SQLite file.

SESSION FILE NAMING:
  ./sessions/{user_id}.session
  Pyrofork will manage the actual SQLite content inside that file.

COMPATIBILITY:
  Works identically on Linux VPS, Windows, Google Colab, Kaggle.
  On ephemeral filesystems (Colab/Kaggle), the directory is recreated each run;
  sessions persist only as long as the runtime is alive — which is expected.

USAGE:
    from core.session_store import session_store

    # Get workdir for a user session (returns path prefix for Pyrofork Client name)
    session_path = session_store.get_session_path(user_id)

    # Create client using file-based session:
    client = Client(session_path, api_id=..., api_hash=..., ...)

    # Or continue using in_memory=True (current behavior) — this module
    # simply ensures the directory is ready as a fallback.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Directory name (NOT hidden, NOT prefixed with dot)
_SESSIONS_DIR_NAME = "sessions"


def _get_sessions_dir() -> Path:
    """
    Resolve the sessions directory for the current environment.

    Priority:
      1. SESSION_DIR env var (explicit override — for Docker/custom setups)
      2. Google Colab → /content/sessions
      3. Kaggle       → /kaggle/working/sessions
      4. Default      → <project_root>/sessions

    Never uses a hidden directory. Never uses a temp directory.
    The returned path may not exist yet — call ensure_sessions_dir() to create it.
    """
    import os

    # 1. Explicit override
    explicit = os.environ.get("SESSION_DIR", "").strip()
    if explicit:
        return Path(explicit)

    # 2. Google Colab
    if os.environ.get("COLAB_GPU") or os.environ.get("COLAB_RELEASE_TAG"):
        return Path("/content/sessions")
    try:
        import google.colab  # noqa: F401
        return Path("/content/sessions")
    except ImportError:
        pass

    # 3. Kaggle
    if os.environ.get("KAGGLE_KERNEL_RUN_TYPE") or Path("/kaggle").exists():
        return Path("/kaggle/working/sessions")

    # 4. Default: project root / sessions
    project_root = Path(__file__).resolve().parent.parent
    return project_root / _SESSIONS_DIR_NAME


def ensure_sessions_dir() -> Path:
    """
    Create the sessions directory if it doesn't exist and return its Path.

    On read-only filesystems (e.g. Kaggle input mount), falls back to
    /tmp/tgbot_sessions so the bot can still run. Logs a warning.
    Safe to call multiple times (idempotent).
    """
    import os
    sessions_dir = _get_sessions_dir()
    try:
        sessions_dir.mkdir(parents=True, exist_ok=True)
        # Quick write-test to detect read-only mount
        test_file = sessions_dir / ".writable"
        test_file.touch()
        test_file.unlink(missing_ok=True)
        logger.debug("sessions dir ready: %s", sessions_dir)
        return sessions_dir
    except OSError as exc:
        fallback = Path("/tmp/tgbot_sessions")
        logger.warning(
            "sessions dir %s not writable (%s), falling back to %s",
            sessions_dir, exc, fallback,
        )
        try:
            fallback.mkdir(parents=True, exist_ok=True)
        except OSError:
            pass
        return fallback


def get_session_path(user_id: int) -> str:
    """
    Return the full file path prefix for a user's session file.

    Pass this as the `name` parameter to Pyrofork Client when NOT using
    in_memory=True:

        client = Client(
            get_session_path(user_id),
            api_id=API_ID,
            api_hash=API_HASH,
            session_string=session_string,  # or omit to use file auth
        )

    Pyrofork will append `.session` automatically.
    """
    sessions_dir = ensure_sessions_dir()
    return str(sessions_dir / str(user_id))


def get_session_file(user_id: int) -> Path:
    """Return the Path object for a user's .session file (may not exist yet)."""
    sessions_dir = ensure_sessions_dir()
    return sessions_dir / f"{user_id}.session"


def session_file_exists(user_id: int) -> bool:
    """Return True if a .session file already exists for this user."""
    return get_session_file(user_id).is_file()


def delete_session_file(user_id: int) -> bool:
    """
    Delete a user's .session file.

    Returns True if the file was deleted, False if it didn't exist or
    deletion failed.
    """
    path = get_session_file(user_id)
    try:
        if path.is_file():
            path.unlink()
            logger.info("Deleted session file: %s", path)
            return True
    except OSError as exc:
        logger.warning("Could not delete session file %s: %s", path, exc)
    return False


class SessionStore:
    """
    Unified session storage interface.

    Primary: MongoDB (via async_db / motor) — holds session_string in DB doc.
    Fallback: ./sessions/ directory — Pyrofork file-based sessions.

    The bot currently uses in_memory=True + session_string from MongoDB,
    which is the recommended path. This class ensures the file fallback
    directory is always ready without crashing.
    """

    def __init__(self) -> None:
        self._sessions_dir: Optional[Path] = None
        self._mongo_available: Optional[bool] = None

    def initialize(self) -> None:
        """
        Prepare storage on startup.

        - Creates ./sessions/ directory.
        - Checks MongoDB availability (non-blocking, sets flag).
        """
        self._sessions_dir = ensure_sessions_dir()
        self._check_mongo()

    def _check_mongo(self) -> None:
        """Non-blocking check whether MongoDB is configured."""
        try:
            from config import DB_URI
            try:
                import motor.motor_asyncio  # noqa: F401
                self._mongo_available = bool(DB_URI)
            except ImportError:
                self._mongo_available = False
        except ImportError:
            self._mongo_available = False

        if self._mongo_available:
            logger.info("SessionStore: primary=MongoDB, fallback=./sessions/")
        else:
            logger.info("SessionStore: primary=./sessions/ (no MongoDB)")

    @property
    def sessions_dir(self) -> Path:
        if self._sessions_dir is None:
            self._sessions_dir = ensure_sessions_dir()
        return self._sessions_dir

    def get_path(self, user_id: int) -> str:
        """File path prefix for Pyrofork Client name parameter."""
        return str(self.sessions_dir / str(user_id))

    def file_exists(self, user_id: int) -> bool:
        return (self.sessions_dir / f"{user_id}.session").is_file()

    def delete(self, user_id: int) -> bool:
        return delete_session_file(user_id)


# Module-level singleton
session_store = SessionStore()

# Initialize immediately on import so the directory is always ready
try:
    session_store.initialize()
except Exception as _init_exc:
    logger.warning("session_store init warning: %s", _init_exc)
