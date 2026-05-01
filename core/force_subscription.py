"""
core/force_subscription.py - Majburiy obuna tekshiruvi

XUSUSIYATLAR:
1. Bir yoki bir nechta kanal/guruhga majburiy obuna
2. Config orqali sozlash
3. Bot admin bo'lmasa - foydalanuvchiga xabar
4. Shaxsiy va ommaviy kanallarni qo'llab-quvvatlash
5. Kanallar ro'yxati bo'sh bo'lsa - tekshirmaydi

FOYDALANISH:
    from core.force_subscription import check_subscription, force_subscribe_decorator
    
    # Decorator bilan
    @force_subscribe_decorator
    async def my_handler(client, message):
        ...
    
    # Yoki manual
    is_subscribed, missing = await check_subscription(bot, user_id)
    if not is_subscribed:
        await send_subscription_prompt(message, missing)
"""

import logging
from typing import List, Tuple, Optional, Union
from functools import wraps
from dataclasses import dataclass
from enum import Enum

from pyrogram import Client
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, ChatMember
from pyrogram.enums import ChatMemberStatus, ChatType
from pyrogram.errors import (
    UserNotParticipant,
    ChannelPrivate,
    ChannelInvalid,
    PeerIdInvalid,
    ChatAdminRequired,
    ChatWriteForbidden,
    UsernameInvalid,
    UsernameNotOccupied,
    RPCError,
)

logger = logging.getLogger(__name__)


# ==================== CONFIGURATION ====================

@dataclass
class ForceSubChannel:
    """Majburiy obuna kanali/guruhi."""
    chat_id: Union[int, str]  # ID yoki @username
    title: str = ""           # Ko'rsatiladigan nom
    invite_link: str = ""     # Qo'shilish havolasi
    
    def __post_init__(self):
        # Username bo'lsa @ qo'shish
        if isinstance(self.chat_id, str) and not self.chat_id.startswith('@') and not self.chat_id.startswith('-'):
            self.chat_id = f"@{self.chat_id}"


class SubCheckResult(Enum):
    """Obuna tekshiruvi natijasi."""
    SUBSCRIBED = "subscribed"           # Obunachi
    NOT_SUBSCRIBED = "not_subscribed"   # Obuna emas
    BOT_NOT_ADMIN = "bot_not_admin"     # Bot admin emas
    CHANNEL_INVALID = "channel_invalid" # Kanal topilmadi
    ERROR = "error"                     # Boshqa xato


@dataclass
class ChannelCheckResult:
    """Bitta kanal tekshiruvi natijasi."""
    channel: ForceSubChannel
    status: SubCheckResult
    error_message: str = ""


# ==================== MAIN FUNCTIONS ====================

async def get_channel_info(
    bot: Client,
    chat_id: Union[int, str]
) -> Tuple[Optional[dict], Optional[str]]:
    """
    Kanal ma'lumotlarini olish.
    
    Returns:
        (info_dict, error_message)
    """
    try:
        chat = await bot.get_chat(chat_id)
        
        # Invite link olish
        invite_link = ""
        if chat.username:
            invite_link = f"https://t.me/{chat.username}"
        elif hasattr(chat, 'invite_link') and chat.invite_link:
            invite_link = chat.invite_link
        
        return {
            'id': chat.id,
            'title': chat.title or chat.first_name or str(chat.id),
            'username': chat.username,
            'type': chat.type,
            'invite_link': invite_link,
        }, None
        
    except (ChannelPrivate, ChannelInvalid, PeerIdInvalid):
        return None, "Kanal topilmadi yoki mavjud emas"
    except (UsernameInvalid, UsernameNotOccupied):
        return None, "Username noto'g'ri yoki band emas"
    except Exception as e:
        logger.error(f"get_channel_info error: {e}")
        return None, str(e)


async def check_bot_is_admin(
    bot: Client,
    chat_id: Union[int, str]
) -> Tuple[bool, str]:
    """
    Bot kanalda admin ekanligini tekshirish.
    
    Returns:
        (is_admin, error_message)
    """
    try:
        bot_user = await bot.get_me()
        member = await bot.get_chat_member(chat_id, bot_user.id)
        
        admin_statuses = {
            ChatMemberStatus.OWNER,
            ChatMemberStatus.ADMINISTRATOR,
        }
        
        is_admin = member.status in admin_statuses
        
        if not is_admin:
            return False, "Bot bu kanalda admin emas"
        
        return True, ""
        
    except (ChatAdminRequired, ChatWriteForbidden):
        return False, "Bot bu kanalda admin emas"
    except (ChannelPrivate, ChannelInvalid, PeerIdInvalid):
        return False, "Kanal topilmadi"
    except Exception as e:
        logger.error(f"check_bot_is_admin error: {e}")
        return False, str(e)


async def check_user_subscription(
    bot: Client,
    user_id: int,
    channel: ForceSubChannel
) -> ChannelCheckResult:
    """
    Foydalanuvchi kanalga obuna ekanligini tekshirish.
    
    Args:
        bot: Bot client
        user_id: Foydalanuvchi ID
        channel: Tekshiriladigan kanal
    
    Returns:
        ChannelCheckResult
    """
    chat_id = channel.chat_id
    
    try:
        # Avval bot admin ekanligini tekshirish
        is_admin, admin_error = await check_bot_is_admin(bot, chat_id)
        if not is_admin:
            return ChannelCheckResult(
                channel=channel,
                status=SubCheckResult.BOT_NOT_ADMIN,
                error_message=admin_error
            )
        
        # Foydalanuvchi statusini tekshirish
        member = await bot.get_chat_member(chat_id, user_id)
        
        # Obunachi statuslari
        subscribed_statuses = {
            ChatMemberStatus.OWNER,
            ChatMemberStatus.ADMINISTRATOR,
            ChatMemberStatus.MEMBER,
        }
        
        if member.status in subscribed_statuses:
            return ChannelCheckResult(
                channel=channel,
                status=SubCheckResult.SUBSCRIBED
            )
        else:
            # RESTRICTED, LEFT, BANNED
            return ChannelCheckResult(
                channel=channel,
                status=SubCheckResult.NOT_SUBSCRIBED
            )
    
    except UserNotParticipant:
        return ChannelCheckResult(
            channel=channel,
            status=SubCheckResult.NOT_SUBSCRIBED
        )
    
    except (ChannelPrivate, ChannelInvalid, PeerIdInvalid):
        return ChannelCheckResult(
            channel=channel,
            status=SubCheckResult.CHANNEL_INVALID,
            error_message="Kanal topilmadi yoki mavjud emas"
        )
    
    except (ChatAdminRequired, ChatWriteForbidden):
        return ChannelCheckResult(
            channel=channel,
            status=SubCheckResult.BOT_NOT_ADMIN,
            error_message="Bot bu kanalda admin emas"
        )
    
    except Exception as e:
        logger.error(f"check_user_subscription error for {chat_id}: {e}")
        return ChannelCheckResult(
            channel=channel,
            status=SubCheckResult.ERROR,
            error_message=str(e)
        )


async def check_subscription(
    bot: Client,
    user_id: int,
    channels: List[ForceSubChannel]
) -> Tuple[bool, List[ChannelCheckResult]]:
    """
    Foydalanuvchining barcha kanallarga obunasini tekshirish.
    
    Args:
        bot: Bot client
        user_id: Foydalanuvchi ID
        channels: Tekshiriladigan kanallar ro'yxati
    
    Returns:
        (all_subscribed, list_of_results)
    """
    if not channels:
        return True, []
    
    results = []
    all_subscribed = True
    
    for channel in channels:
        result = await check_user_subscription(bot, user_id, channel)
        results.append(result)
        
        if result.status != SubCheckResult.SUBSCRIBED:
            all_subscribed = False
    
    return all_subscribed, results


# ==================== UI FUNCTIONS ====================

def build_subscription_keyboard(
    results: List[ChannelCheckResult],
    check_button_text: str = "✅ Tekshirish"
) -> InlineKeyboardMarkup:
    """
    Obuna tugmalarini yaratish.
    
    Args:
        results: Tekshiruv natijalari
        check_button_text: Tekshirish tugmasi matni
    
    Returns:
        InlineKeyboardMarkup
    """
    buttons = []
    
    for result in results:
        if result.status == SubCheckResult.NOT_SUBSCRIBED:
            channel = result.channel
            
            # Havola aniqlash
            if channel.invite_link:
                url = channel.invite_link
            elif isinstance(channel.chat_id, str) and channel.chat_id.startswith('@'):
                url = f"https://t.me/{channel.chat_id[1:]}"
            else:
                # Shaxsiy kanal - invite link kerak
                continue
            
            title = channel.title or "Kanal"
            buttons.append([
                InlineKeyboardButton(f"➕ {title}", url=url)
            ])
    
    # Tekshirish tugmasi
    if buttons:
        buttons.append([
            InlineKeyboardButton(check_button_text, callback_data="force_sub_check")
        ])
    
    return InlineKeyboardMarkup(buttons) if buttons else None


def build_subscription_message(
    results: List[ChannelCheckResult],
    owner_username: str = None
) -> str:
    """
    Obuna xabarini yaratish.
    
    Args:
        results: Tekshiruv natijalari
        owner_username: Admin username (xato bo'lganda ko'rsatish uchun)
    
    Returns:
        Xabar matni
    """
    lines = ["🔒 **Botdan foydalanish uchun quyidagi kanallarga obuna bo'ling:**\n"]
    
    not_subscribed = []
    bot_not_admin = []
    errors = []
    
    for result in results:
        if result.status == SubCheckResult.NOT_SUBSCRIBED:
            not_subscribed.append(result)
        elif result.status == SubCheckResult.BOT_NOT_ADMIN:
            bot_not_admin.append(result)
        elif result.status in (SubCheckResult.CHANNEL_INVALID, SubCheckResult.ERROR):
            errors.append(result)
    
    # Obuna bo'lmagan kanallar
    if not_subscribed:
        for i, result in enumerate(not_subscribed, 1):
            title = result.channel.title or f"Kanal {i}"
            lines.append(f"  {i}. {title}")
    
    # Bot admin emas
    if bot_not_admin:
        lines.append("\n⚠️ **Bot quyidagi kanallarda admin emas:**")
        for result in bot_not_admin:
            title = result.channel.title or str(result.channel.chat_id)
            lines.append(f"  • {title}")
        
        if owner_username:
            lines.append(f"\n📞 Admin bilan bog'laning: @{owner_username}")
    
    # Xatolar
    if errors:
        lines.append("\n❌ **Xato yuz berdi:**")
        for result in errors:
            lines.append(f"  • {result.error_message}")
        
        if owner_username:
            lines.append(f"\n📞 Admin bilan bog'laning: @{owner_username}")
    
    lines.append("\n✅ Obuna bo'lgach, \"Tekshirish\" tugmasini bosing.")
    
    return "\n".join(lines)


async def send_subscription_prompt(
    message: Message,
    results: List[ChannelCheckResult],
    owner_username: str = None
) -> Optional[Message]:
    """
    Obuna so'rovini yuborish.
    
    Args:
        message: Foydalanuvchi xabari
        results: Tekshiruv natijalari
        owner_username: Admin username
    
    Returns:
        Yuborilgan xabar
    """
    text = build_subscription_message(results, owner_username)
    keyboard = build_subscription_keyboard(results)
    
    try:
        return await message.reply(
            text,
            reply_markup=keyboard,
            disable_web_page_preview=True
        )
    except Exception as e:
        logger.error(f"send_subscription_prompt error: {e}")
        return None


# ==================== DECORATOR ====================

def force_subscribe_decorator(
    channels: List[ForceSubChannel] = None,
    owner_username: str = None,
    skip_admins: bool = True,
    admin_ids: List[int] = None
):
    """
    Majburiy obuna decorator.
    
    Args:
        channels: Kanallar ro'yxati (None bo'lsa config'dan oladi)
        owner_username: Admin username
        skip_admins: Adminlarni tekshirmaslik
        admin_ids: Admin ID'lar ro'yxati
    
    Usage:
        @app.on_message(filters.command("start"))
        @force_subscribe_decorator(channels=FORCE_SUB_CHANNELS)
        async def start_handler(client, message):
            ...
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(client: Client, message: Message, *args, **kwargs):
            user_id = message.from_user.id if message.from_user else None
            
            if not user_id:
                return await func(client, message, *args, **kwargs)
            
            # Admin tekshiruvi
            if skip_admins and admin_ids and user_id in admin_ids:
                return await func(client, message, *args, **kwargs)
            
            # Kanallar ro'yxati
            check_channels = channels
            if not check_channels:
                # Config'dan olish
                from core.force_subscription import get_force_sub_channels
                check_channels = get_force_sub_channels()
            
            if not check_channels:
                # Kanallar yo'q - tekshirmaslik
                return await func(client, message, *args, **kwargs)
            
            # Tekshirish
            is_subscribed, results = await check_subscription(client, user_id, check_channels)
            
            if is_subscribed:
                return await func(client, message, *args, **kwargs)
            
            # Obuna emas
            await send_subscription_prompt(message, results, owner_username)
            return None
        
        return wrapper
    return decorator


# ==================== CONFIG HELPERS ====================

# Config'dan o'qiladigan global o'zgaruvchilar
_FORCE_SUB_CHANNELS: List[ForceSubChannel] = []
_FORCE_SUB_ENABLED: bool = False


def configure_force_subscription(
    channels: List[Union[dict, ForceSubChannel, str, int]] = None,
    enabled: bool = True
) -> None:
    """
    Majburiy obunani sozlash.
    
    Args:
        channels: Kanallar ro'yxati
            - dict: {'chat_id': ..., 'title': ..., 'invite_link': ...}
            - ForceSubChannel object
            - str: username yoki invite link
            - int: chat_id
        enabled: Yoqish/o'chirish
    
    Usage:
        configure_force_subscription([
            {'chat_id': '@mychannel', 'title': 'My Channel'},
            {'chat_id': -1001234567890, 'title': 'Private Channel', 'invite_link': 'https://t.me/+xxx'},
        ])
    """
    global _FORCE_SUB_CHANNELS, _FORCE_SUB_ENABLED
    
    _FORCE_SUB_ENABLED = enabled
    _FORCE_SUB_CHANNELS = []
    
    if not channels or not enabled:
        return
    
    for item in channels:
        if isinstance(item, ForceSubChannel):
            _FORCE_SUB_CHANNELS.append(item)
        elif isinstance(item, dict):
            _FORCE_SUB_CHANNELS.append(ForceSubChannel(**item))
        elif isinstance(item, (str, int)):
            _FORCE_SUB_CHANNELS.append(ForceSubChannel(chat_id=item))


def get_force_sub_channels() -> List[ForceSubChannel]:
    """Sozlangan kanallar ro'yxatini olish."""
    return _FORCE_SUB_CHANNELS if _FORCE_SUB_ENABLED else []


def is_force_sub_enabled() -> bool:
    """Majburiy obuna yoqilganligini tekshirish."""
    return _FORCE_SUB_ENABLED and len(_FORCE_SUB_CHANNELS) > 0


# ==================== CALLBACK HANDLER ====================

async def handle_force_sub_callback(
    client: Client,
    callback_query,
    channels: List[ForceSubChannel] = None,
    owner_username: str = None
) -> bool:
    """
    "Tekshirish" tugmasi callback handler.
    
    Args:
        client: Bot client
        callback_query: Callback query
        channels: Kanallar (None bo'lsa config'dan)
        owner_username: Admin username
    
    Returns:
        True agar obuna bo'lsa, False aks holda
    """
    user_id = callback_query.from_user.id
    
    check_channels = channels or get_force_sub_channels()
    
    if not check_channels:
        await callback_query.answer("✅ Tekshiruv o'tkazildi", show_alert=False)
        return True
    
    is_subscribed, results = await check_subscription(client, user_id, check_channels)
    
    if is_subscribed:
        await callback_query.answer("✅ Obuna tasdiqlandi! Endi botdan foydalanishingiz mumkin.", show_alert=True)
        
        # Xabarni o'chirish yoki yangilash
        try:
            await callback_query.message.delete()
        except Exception:
            try:
                await callback_query.message.edit_text("✅ Obuna tasdiqlandi!")
            except Exception:
                pass
        
        return True
    else:
        # Hali ham obuna emas
        not_sub_count = sum(1 for r in results if r.status == SubCheckResult.NOT_SUBSCRIBED)
        await callback_query.answer(
            f"❌ Hali {not_sub_count} ta kanalga obuna bo'lmagansiz!",
            show_alert=True
        )
        
        # Xabarni yangilash
        text = build_subscription_message(results, owner_username)
        keyboard = build_subscription_keyboard(results)
        
        try:
            await callback_query.message.edit_text(
                text,
                reply_markup=keyboard,
                disable_web_page_preview=True
            )
        except Exception:
            pass
        
        return False
