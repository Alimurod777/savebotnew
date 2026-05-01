"""
Project-wide Python startup hooks.

This file is imported automatically by Python during interpreter startup
when the project root is on ``sys.path``.

We only apply a narrowly-scoped warning filter for Pyrofork/Pyrogram's
import-time sync shim and preserve platform-specific loop policy behavior.
"""

import asyncio
import platform
import warnings


def _bootstrap_event_loop() -> None:
    warnings.filterwarnings(
        "ignore",
        message="There is no current event loop",
        category=DeprecationWarning,
    )

    if platform.system() == "Windows" and hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
        current_policy = asyncio.get_event_loop_policy()
        if not isinstance(current_policy, asyncio.WindowsSelectorEventLoopPolicy):
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


_bootstrap_event_loop()
