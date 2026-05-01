"""
Helpers for import-time asyncio compatibility.

Pyrofork/Pyrogram imports ``pyrogram.sync`` at module import time and emits
``DeprecationWarning: There is no current event loop`` on Python 3.10+.
We suppress only that exact upstream warning here.

Important:
- On Windows we still force ``WindowsSelectorEventLoopPolicy``.
- On Linux/macOS we do NOT pre-create loops, so later ``uvloop.install()``
  can still take over normally in ``core.compat.configure_event_loop_policy``.
"""

import asyncio
import platform
import warnings


def ensure_current_event_loop() -> None:
    # Pyrofork 2.3.69 imports pyrogram.sync at module import time and triggers
    # this exact warning on Python 3.10+. Keep the filter narrowly scoped.
    warnings.filterwarnings(
        "ignore",
        message="There is no current event loop",
        category=DeprecationWarning,
    )

    if platform.system() == "Windows" and hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
        current_policy = asyncio.get_event_loop_policy()
        if not isinstance(current_policy, asyncio.WindowsSelectorEventLoopPolicy):
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
