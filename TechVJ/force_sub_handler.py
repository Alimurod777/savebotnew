"""
TechVJ/force_sub_handler.py - Majburiy obuna handler

Bot ishga tushganda avtomatik yuklanadi va quyidagilarni ta'minlaydi:
1. Callback query handler ("Tekshirish" tugmasi)
2. Force subscription middleware

FOYDALANISH:
    # Boshqa handlerlarda:
    from TechVJ.force_sub_handler import check_force_sub
    
    @app.on_message(filters.command("start"))
    async def start(client, message):
        # Obuna tekshiruvi
        if not await check_force_sub(client, message):
            return
        
        # Asosiy kod...
"""

import logging
from pyrogram import Client, filters
from pyrogram.types import Message, CallbackQuery

from config import (
    FORCE_SUB_ENABLED,
    FORCE_SUB_CHANNELS,
    FORCE_SUB_ADMIN_IDS,
    OWNER_USERNAME,
)
from core.force_subscription import (
    ForceSubChannel,
    check_subscription,
    send_subscription_prompt,
    handle_force_sub_callback,
    configure_force_subscription,
    get_force_sub_channels,
    SubCheckResult,
)

logger = logging.getLogger(__name__)

# ==================== INITIALIZATION ====================

def init_force_subscription():
    """Force subscription ni config'dan yuklash."""
    if not FORCE_SUB_ENABLED:
        logger.info("Force subscription: O'chirilgan")
        return
    
    if not FORCE_SUB_CHANNELS:
        logger.info("Force subscription: Kanallar ro'yxati bo'sh")
        return
    
    channels = []
    for ch in FORCE_SUB_CHANNELS:
        if isinstance(ch, dict):
            channels.append(ch)
        elif isinstance(ch, (str, int)):
            channels.append({'chat_id': ch})
    
    configure_force_subscription(channels, enabled=True)
    logger.info(f"Force subscription: {len(channels)} ta kanal sozlandi")


# Modul yuklanganda avtomatik sozlash
init_force_subscription()


# ==================== MAIN CHECK FUNCTION ====================

async def check_force_sub(
    client: Client,
    message: Message,
    send_prompt: bool = True
) -> bool:
    """
    Foydalanuvchi majburiy kanallarga obuna ekanligini tekshirish.
    
    Args:
        client: Bot client
        message: Foydalanuvchi xabari
        send_prompt: Obuna so'rovini yuborishmi
    
    Returns:
        True - obuna bo'lsa yoki tekshiruv o'chirilgan
        False - obuna emas
    
    Usage:
        @app.on_message(filters.command("start"))
        async def start(client, message):
            if not await check_force_sub(client, message):
                return
            # ... davom etish
    """
    # Force sub o'chirilgan
    channels = get_force_sub_channels()
    if not channels:
        return True
    
    user = message.from_user
    if not user:
        return True
    
    user_id = user.id
    
    # Adminlar tekshirilmaydi
    if user_id in FORCE_SUB_ADMIN_IDS:
        return True
    
    # Tekshirish
    is_subscribed, results = await check_subscription(client, user_id, channels)
    
    if is_subscribed:
        return True
    
    # Obuna emas - xabar yuborish
    if send_prompt:
        await send_subscription_prompt(message, results, OWNER_USERNAME)
    
    return False


# ==================== CALLBACK HANDLER ====================

@Client.on_callback_query(filters.regex(r"^force_sub_check$"))
async def force_sub_callback_handler(client: Client, callback_query: CallbackQuery):
    """
    "Tekshirish" tugmasi bosilganda.
    
    Foydalanuvchi obunasini qayta tekshiradi va natijani ko'rsatadi.
    """
    await handle_force_sub_callback(
        client,
        callback_query,
        channels=get_force_sub_channels(),
        owner_username=OWNER_USERNAME
    )


# ==================== DECORATOR ====================

def require_subscription(func):
    """
    Handler uchun majburiy obuna decorator.
    
    Usage:
        @app.on_message(filters.command("save"))
        @require_subscription
        async def save_handler(client, message):
            ...
    """
    from functools import wraps
    
    @wraps(func)
    async def wrapper(client: Client, message: Message, *args, **kwargs):
        if not await check_force_sub(client, message):
            return None
        return await func(client, message, *args, **kwargs)
    
    return wrapper
