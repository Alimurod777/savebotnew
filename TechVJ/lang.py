"""
TechVJ/lang.py - Multi-language support for the bot.

Supports: English (en), Uzbek (uz)
Default: English
"""

import os
import logging
from typing import Dict, Optional
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery

logger = logging.getLogger(__name__)

# Language storage directory
LANG_DIR = "data/user_lang"
os.makedirs(LANG_DIR, exist_ok=True)

# Default language
DEFAULT_LANG = "en"

# ==================== LANGUAGE STRINGS ====================

STRINGS: Dict[str, Dict[str, str]] = {
    "en": {
        # General
        "lang_select": "🌐 **Select Language**\n\nCurrent: English 🇬🇧",
        "lang_changed": "✅ Language changed to **English** 🇬🇧",
        
        # Start
        "start_msg": "👋 **Welcome!**\n\nSend me a media link or forward a message to save it.",
        "start_help": "Use /help for commands list.",
        
        # Download/Upload
        "downloading": "⬇️ Downloading...",
        "uploading": "⬆️ Uploading...",
        "download_complete": "✅ Download complete!",
        "download_failed": "❌ Download failed",
        "processing": "⏳ Processing...",
        
        # Large file splitting
        "file_too_large": "📦 File is larger than 2GB ({size})\nSplitting...",
        "split_complete": "✅ Split into **{parts}** parts!\nUse file joining software to combine.",
        "splitting_ffmpeg": "✂️ Splitting with FFmpeg (quality preserved)...",
        "splitting_binary": "✂️ Splitting file...",
        "uploading_part": "⬆️ Uploading: **{part}/{total}**\n`{name}` ({size})",
        
        # Caption
        "caption_continued": "📝 Caption continued:",
        "caption_overflow": "📎 Continuation below ↓",
        
        # Progress
        "progress_download": "⬇️ Downloading\n{progress}% | {speed}/s\nETA: {eta}",
        "progress_upload": "⬆️ Uploading\n{progress}% | {speed}/s\nETA: {eta}",
        
        # Errors
        "error_generic": "❌ Error: {error}",
        "error_not_found": "❌ File not found",
        "error_timeout": "⏱️ Request timed out",
        "error_flood": "⚠️ Too many requests. Wait {seconds}s",
        "error_split": "❌ Failed to split large file: {error}",
        
        # Login/Logout
        "not_logged_in": "❌ You are not logged in.",
        "already_logged_in": "✅ You are already logged in.\n\nUse /logout first to login with another account.",
        "logout_success": "✅ **Logout Successfully** ♦",
        "login_cancelled": "❌ Login cancelled due to timeout.",
        "process_cancelled": "❌ Process cancelled!",
        "send_phone": "📱 **Send your phone number** in international format.\n\nExample: `+998901234567`",
        "invalid_phone": "❌ Phone number is invalid.",
        "sending_otp": "📤 Sending OTP...",
        "send_otp": "🔑 **Enter the OTP** sent to your phone.\n\nFormat: `1 2 3 4 5` (with spaces)",
        "invalid_otp": "❌ OTP is invalid. Please try again.",
        "otp_expired": "❌ OTP is expired. Please use /login to start again.",
        "otp_max_attempts": "❌ Maximum attempts reached.",
        "send_password": "🔐 **Two-Step Verification**\n\nEnter your password:",
        "invalid_password": "❌ Invalid password. Please try again.",
        "password_max_attempts": "❌ Maximum password attempts reached.",
        "login_success": "✅ **Successfully Logged In!**\n\nNow forward or send links to save media.",
        "login_error": "❌ Login error: {error}",
        "db_unavailable": "⚠️ Database unavailable. Session saved to file.",
        
        # QR Login
        "qr_scan": "📱 **Scan this QR code** with your Telegram app.\n\nGo to Settings → Devices → Scan QR",
        "qr_expired": "❌ QR code expired. Use /login to try again.",
        "qr_waiting": "⏳ Waiting for scan...",
        
        # Album
        "album_processing": "📸 Processing album ({count} items)...",
        "album_complete": "✅ Album sent!",
        "album_error": "❌ Album error: {error}",
        
        # Help
        "help_msg": """📚 **Commands**

/start - Start the bot
/help - Show this message
/lang - Change language 🌐
/login - Login with your account
/logout - Logout

**Usage:**
Send a media link or forward a message to download/save it.""",
        
        # Queue
        "queue_added": "📋 **Added to queue**\n\nPosition: {position}\nCurrently downloading: `{current}`\n\nCancel: /stop",
        "queue_full": "❌ Queue is full (max {max}).\nWait for current downloads to finish.",
        "queue_next": "▶️ **Starting next from queue...**\n`{link}`",
        "queue_from": "📥 From queue: {count} post(s) downloading...",
        "queue_topic": "📥 From queue: Topic {topic_id} downloading...",
        "queue_thread": "📥 From queue: Thread {thread_id} downloading...",
        "queue_public": "📥 From queue: Public channel ({count} posts)",
        "queue_bot": "📥 From queue: Bot chat downloading...",
        "queue_cleared": "📋 Queue cleared: {count}",
        "queue_done": "✅ Done: {done}/{total}",
        "queue_done_errors": "✅ Done: {done}/{total} (errors: {errors})",
        "queue_empty": "📭 Queue is empty",
        "queue_status": "📋 **Queue Status**\n\nActive: {active}\nWaiting: {waiting}",
    },
    
    "uz": {
        # General
        "lang_select": "🌐 **Tilni tanlang**\n\nHozirgi: O'zbek 🇺🇿",
        "lang_changed": "✅ Til **O'zbek**ga o'zgartirildi 🇺🇿",
        
        # Start
        "start_msg": "👋 **Xush kelibsiz!**\n\nMenga media link yuboring yoki xabarni forward qiling.",
        "start_help": "Buyruqlar ro'yxati uchun /help bosing.",
        
        # Download/Upload
        "downloading": "⬇️ Yuklanmoqda...",
        "uploading": "⬆️ Yuborilmoqda...",
        "download_complete": "✅ Yuklash tugadi!",
        "download_failed": "❌ Yuklash xatosi",
        "processing": "⏳ Qayta ishlanmoqda...",
        
        # Large file splitting
        "file_too_large": "📦 Fayl 2GB dan katta ({size})\nBo'linmoqda...",
        "split_complete": "✅ **{parts}** qismga bo'lindi!\nBirlashtirish uchun maxsus dastur ishlating.",
        "splitting_ffmpeg": "✂️ FFmpeg bilan bo'linmoqda (sifat saqlanadi)...",
        "splitting_binary": "✂️ Fayl bo'linmoqda...",
        "uploading_part": "⬆️ Yuklanmoqda: **{part}/{total}**\n`{name}` ({size})",
        
        # Caption
        "caption_continued": "📝 Caption davomi:",
        "caption_overflow": "📎 Davomi pastda ↓",
        
        # Progress
        "progress_download": "⬇️ Yuklanmoqda\n{progress}% | {speed}/s\nQolgan: {eta}",
        "progress_upload": "⬆️ Yuborilmoqda\n{progress}% | {speed}/s\nQolgan: {eta}",
        
        # Errors
        "error_generic": "❌ Xato: {error}",
        "error_not_found": "❌ Fayl topilmadi",
        "error_timeout": "⏱️ Vaqt tugadi",
        "error_flood": "⚠️ Juda ko'p so'rov. {seconds}s kuting",
        "error_split": "❌ Katta faylni bo'lib bo'lmadi: {error}",
        
        # Login/Logout
        "not_logged_in": "❌ Siz tizimga kirmagansiz.",
        "already_logged_in": "✅ Siz allaqachon tizimdasiz.\n\nBoshqa hisob bilan kirish uchun avval /logout qiling.",
        "logout_success": "✅ **Muvaffaqiyatli chiqdingiz** ♦",
        "login_cancelled": "❌ Vaqt tugagani sababli bekor qilindi.",
        "process_cancelled": "❌ Jarayon bekor qilindi!",
        "send_phone": "📱 **Telefon raqamingizni yuboring** xalqaro formatda.\n\nMisol: `+998901234567`",
        "invalid_phone": "❌ Telefon raqami noto'g'ri.",
        "sending_otp": "📤 OTP yuborilmoqda...",
        "send_otp": "🔑 **Telefoningizga kelgan kodni kiriting.**\n\nFormat: `1 2 3 4 5` (bo'sh joy bilan)",
        "invalid_otp": "❌ Kod noto'g'ri. Qaytadan urinib ko'ring.",
        "otp_expired": "❌ Kod muddati tugagan. Qaytadan /login bosing.",
        "otp_max_attempts": "❌ Maksimal urinish soni tugadi.",
        "send_password": "🔐 **Ikki bosqichli tekshiruv**\n\nParolingizni kiriting:",
        "invalid_password": "❌ Parol noto'g'ri. Qaytadan urinib ko'ring.",
        "password_max_attempts": "❌ Maksimal parol urinishi tugadi.",
        "login_success": "✅ **Muvaffaqiyatli kirdingiz!**\n\nEndi media saqlash uchun link yuboring yoki forward qiling.",
        "login_error": "❌ Kirishda xato: {error}",
        "db_unavailable": "⚠️ Database mavjud emas. Sessiya faylga saqlandi.",
        
        # QR Login
        "qr_scan": "📱 **Bu QR kodni skanerlang** Telegram ilovangiz bilan.\n\nSozlamalar → Qurilmalar → QR skanerlash",
        "qr_expired": "❌ QR kod muddati tugadi. Qaytadan /login bosing.",
        "qr_waiting": "⏳ Skanerlanishi kutilmoqda...",
        
        # Album
        "album_processing": "📸 Album qayta ishlanmoqda ({count} ta)...",
        "album_complete": "✅ Album yuborildi!",
        "album_error": "❌ Album xatosi: {error}",
        
        # Help
        "help_msg": """📚 **Buyruqlar**

/start - Botni ishga tushirish
/help - Yordam
/lang - Tilni o'zgartirish 🌐
/login - Hisobga kirish
/logout - Hisobdan chiqish

**Foydalanish:**
Media link yuboring yoki xabarni forward qiling.""",
        
        # Queue
        "queue_added": "📋 **Navbatga qo'shildi**\n\nO'rni: {position}\nHozir yuklanmoqda: `{current}`\n\nBekor qilish: /stop",
        "queue_full": "❌ Navbat to'lgan (max {max}).\nJoriy yuklanishlar tugashini kuting.",
        "queue_next": "▶️ **Navbatdan keyingi havola...**\n`{link}`",
        "queue_from": "📥 Navbatdan: {count} ta post yuklanmoqda...",
        "queue_topic": "📥 Navbatdan: Topic {topic_id} yuklanmoqda...",
        "queue_thread": "📥 Navbatdan: Thread {thread_id} yuklanmoqda...",
        "queue_public": "📥 Navbatdan: Public kanal ({count} ta post)",
        "queue_bot": "📥 Navbatdan: Bot chat yuklanmoqda...",
        "queue_cleared": "📋 Navbatdan o'chirildi: {count}",
        "queue_done": "✅ Tugadi: {done}/{total}",
        "queue_done_errors": "✅ Tugadi: {done}/{total} (xato: {errors})",
        "queue_empty": "📭 Navbat bo'sh",
        "queue_status": "📋 **Navbat holati**\n\nFaol: {active}\nKutmoqda: {waiting}",
    }
}


# ==================== LANGUAGE FUNCTIONS ====================

def get_user_lang(user_id: int) -> str:
    """Get user's selected language."""
    lang_file = os.path.join(LANG_DIR, f"{user_id}.txt")
    try:
        if os.path.exists(lang_file):
            with open(lang_file, 'r') as f:
                lang = f.read().strip()
                if lang in STRINGS:
                    return lang
    except Exception:
        pass
    return DEFAULT_LANG


def set_user_lang(user_id: int, lang: str) -> bool:
    """Set user's language."""
    if lang not in STRINGS:
        return False
    
    lang_file = os.path.join(LANG_DIR, f"{user_id}.txt")
    try:
        with open(lang_file, 'w') as f:
            f.write(lang)
        return True
    except Exception as e:
        logger.error(f"Failed to set language for {user_id}: {e}")
        return False


def get_string(user_id: int, key: str, **kwargs) -> str:
    """Get localized string for user."""
    lang = get_user_lang(user_id)
    
    # Get string from user's language
    text = STRINGS.get(lang, {}).get(key)
    
    # Fallback to English
    if text is None:
        text = STRINGS.get(DEFAULT_LANG, {}).get(key, key)
    
    # Format with kwargs
    if kwargs:
        try:
            text = text.format(**kwargs)
        except KeyError:
            pass
    
    return text


def _(user_id: int, key: str, **kwargs) -> str:
    """Shortcut for get_string."""
    return get_string(user_id, key, **kwargs)


# ==================== COMMAND HANDLERS ====================

async def lang_command(client: Client, message: Message):
    """Handle /lang command."""
    user_id = message.from_user.id
    current_lang = get_user_lang(user_id)
    
    # Build message based on current language
    if current_lang == "uz":
        text = "🌐 **Tilni tanlang**\n\nHozirgi: O'zbek 🇺🇿"
    else:
        text = "🌐 **Select Language**\n\nCurrent: English 🇬🇧"
    
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🇬🇧 English", callback_data="lang_en"),
            InlineKeyboardButton("🇺🇿 O'zbek", callback_data="lang_uz"),
        ]
    ])
    
    await message.reply(text, reply_markup=keyboard)


async def lang_callback(client: Client, callback: CallbackQuery):
    """Handle language selection callback."""
    user_id = callback.from_user.id
    lang = callback.data.replace("lang_", "")
    
    if lang not in STRINGS:
        await callback.answer("Invalid language", show_alert=True)
        return
    
    set_user_lang(user_id, lang)
    
    # Respond in selected language
    if lang == "uz":
        text = "✅ Til **O'zbek**ga o'zgartirildi 🇺🇿"
    else:
        text = "✅ Language changed to **English** 🇬🇧"
    
    await callback.message.edit_text(text)
    await callback.answer()


def register_lang_handlers(app: Client):
    """Register language handlers with the bot."""
    app.on_message(filters.command("lang"))(lang_command)
    app.on_callback_query(filters.regex(r"^lang_(en|uz)$"))(lang_callback)
    logger.info("Language handlers registered")
