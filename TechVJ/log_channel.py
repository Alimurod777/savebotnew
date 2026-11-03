"""Observe private conversations before commands/interactive login consume updates."""
from __future__ import annotations

from wzgram import Client, filters

from core.chat_log import chat_logger


@Client.on_message(filters.private, group=-10000)
async def log_private_message(client, message):
    chat_logger.submit(message)


@Client.on_edited_message(filters.private, group=-10000)
async def log_private_edit(client, message):
    chat_logger.submit(message)
