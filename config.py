import os
import hashlib
from typing import Dict, Optional

# ============================================================================
# XAVFSIZ CONFIG - GitHub'ga push qilish mumkin
#
# Qiymatlar quyidagi tartibda o'qiladi (birinchi topilgani ishlatiladi):
#   1. Environment variables (Colab Secrets, Kaggle Secrets, export, ...)
#   2. .env fayl (loyiha papkasida, python-decouple orqali)
#   3. Bo'sh default (bot ishlamaydi, lekin import crashlamaydi)
#
# .env faylni GitHub'ga YUKLAMANG - .gitignore ga qo'shing!
# ============================================================================

# .env fayldan o'qish (agar mavjud bo'lsa)
try:
    from decouple import config as _env
    def _get(key, default=""):
        return os.environ.get(key) or _env(key, default=default)
except ImportError:
    def _get(key, default=""):
        return os.environ.get(key, default)

# Majburiy sozlamalar
BOT_TOKEN = _get("BOT_TOKEN")
API_ID = int(_get("API_ID", "0"))
API_HASH = _get("API_HASH")

# Bot MTProto session string (ixtiyoriy).
# Agar berilsa, upload uchun Bot API o'rniga MTProto ishlatiladi —
# 50MB chegarasi yo'q, FloodWait kam.
BOT_SESSION = _get("BOT_SESSION", "")
DB_URI = _get("DB_URI")

# Owner
OWNER_ID = int(_get("OWNER_ID", "0"))
OWNER_USERNAME = _get("OWNER_USERNAME")

# Premium relay channel/group (bot must be admin with post rights)
# Set via env var or /premium relay command
RELAY_CHANNEL_ID = int(_get("RELAY_CHANNEL_ID", "0"))

BANNED_MESSAGE = f"To use this bot, please contact the owner: @{OWNER_USERNAME}" if OWNER_USERNAME else ""

# ============================================================================
# Force Subscription (Majburiy Obuna)
# ============================================================================
FORCE_SUB_ENABLED = _get("FORCE_SUB_ENABLED", "false").lower() == "true"

FORCE_SUB_CHANNELS = [
    # {"chat_id": "@your_channel", "title": "Asosiy Kanal"},
]

FORCE_SUB_ADMIN_IDS = [OWNER_ID] if OWNER_ID else []

# ============================================================================
# Operatsion sozlamalar (secret emas, xavfsiz)
# ============================================================================
TEMP_DOWNLOAD_DIR = _get("TEMP_DOWNLOAD_DIR", "downloads/temp")

CLEANUP_INTERVAL_HOURS = float(_get("CLEANUP_INTERVAL_HOURS", "1"))
MAX_CONCURRENT_VALIDATIONS = int(_get("MAX_CONCURRENT_VALIDATIONS", "5"))
VALIDATION_DELAY_SECONDS = float(_get("VALIDATION_DELAY_SECONDS", "1.0"))
MONGODB_OPERATION_DELAY = float(_get("MONGODB_OPERATION_DELAY", "0.2"))

# ============================================================================
# Login Rate Limiting
# ============================================================================
MAX_CONCURRENT_LOGINS = int(_get("MAX_CONCURRENT_LOGINS", "2"))
LOGIN_COOLDOWN_SECONDS = float(_get("LOGIN_COOLDOWN_SECONDS", "60"))
USER_LOGIN_COOLDOWN = float(_get("USER_LOGIN_COOLDOWN", "300"))
MAX_LOGIN_ATTEMPTS_PER_HOUR = int(_get("MAX_LOGIN_ATTEMPTS_PER_HOUR", "3"))

# ============================================================================
# MTProto Client Fingerprint Configuration
# ============================================================================
# SECURITY CRITICAL: Platform parameters MUST be consistent within each family.
# Mixing platforms (e.g., Android device_model with Desktop app_version) triggers
# Telegram security resets, forced logouts, and session invalidation.
#
# Rules:
# - Desktop: device_model="Desktop", system_version=OS, app_version=Desktop version
# - Android: device_model=Android device, system_version=Android ver, app_version=Android app ver
# - iOS: device_model=iPhone/iPad, system_version=iOS ver, app_version=iOS app ver
# ============================================================================

class ClientFingerprint:
    """
    Immutable, platform-consistent MTProto client fingerprint.
    
    SECURITY: All parameters are validated to belong to the same platform family.
    Once created for a user, the fingerprint MUST remain constant for the session lifetime.
    """
    
    # Platform-consistent fingerprint presets (verified safe combinations)
    # Updated: January 2026 - Telegram Desktop 5.x/6.x series
    # WHY DESKTOP? Desktop has no hardware IDs (IMEI, UDID), making it
    # safest for multi-user bots. Corporate use of multiple accounts is normal.
    PLATFORM_PRESETS = {
        'desktop_windows': {
            'device_model': 'Desktop',
            'system_version': 'Windows 11',
            'app_version': '5.9.0 x64',  # Stable version format
            'lang_code': 'en',
        },
        'desktop_macos': {
            'device_model': 'Desktop',
            'system_version': 'macOS 14.5',
            'app_version': '5.9.0',
            'lang_code': 'en',
        },
        'desktop_linux': {
            'device_model': 'Desktop',
            'system_version': 'Linux x86_64',
            'app_version': '5.9.0 x64',
            'lang_code': 'en',
        },
        'android_samsung': {
            'device_model': 'Samsung Galaxy S24',
            'system_version': 'SDK 34',
            'app_version': '11.4.2',
            'lang_code': 'en',
        },
        'android_pixel': {
            'device_model': 'Google Pixel 8',
            'system_version': 'SDK 34',
            'app_version': '11.4.2',
            'lang_code': 'en',
        },
        'ios_iphone': {
            'device_model': 'iPhone 15 Pro',
            'system_version': 'iOS 17.4',
            'app_version': '10.8.3',
            'lang_code': 'en',
        },
    }
    
    # Default platform for all sessions (ensures consistency)
    DEFAULT_PLATFORM = 'desktop_windows'
    
    def __init__(self, device_model: str, system_version: str, app_version: str, lang_code: str = 'en'):
        self._device_model = device_model
        self._system_version = system_version
        self._app_version = app_version
        self._lang_code = lang_code
        self._validate()
    
    def _validate(self) -> None:
        """Validate platform consistency. Raises ValueError on mismatch."""
        platform = self._detect_platform()
        if platform == 'unknown':
            raise ValueError(
                f"Invalid fingerprint: device_model='{self._device_model}', "
                f"system_version='{self._system_version}', app_version='{self._app_version}' "
                "do not form a consistent platform identity"
            )
    
    def _detect_platform(self) -> str:
        """Detect platform family from parameters."""
        dm_lower = self._device_model.lower()
        sv_lower = self._system_version.lower()
        
        # Desktop detection
        if 'desktop' in dm_lower or dm_lower in ('pc', 'telegram desktop'):
            if any(x in sv_lower for x in ('windows', 'macos', 'linux', 'ubuntu')):
                return 'desktop'
        
        # Android detection
        if any(x in dm_lower for x in ('samsung', 'pixel', 'oneplus', 'xiaomi', 'huawei', 'sony', 'oppo', 'vivo')):
            if 'android' in sv_lower or 'sdk' in sv_lower:
                return 'android'
        
        # iOS detection
        if any(x in dm_lower for x in ('iphone', 'ipad')):
            if 'ios' in sv_lower or 'ipados' in sv_lower:
                return 'ios'
        
        return 'unknown'
    
    @property
    def device_model(self) -> str:
        return self._device_model
    
    @property
    def system_version(self) -> str:
        return self._system_version
    
    @property
    def app_version(self) -> str:
        return self._app_version
    
    @property
    def lang_code(self) -> str:
        return self._lang_code
    
    def to_dict(self) -> Dict[str, str]:
        """Return fingerprint as dict for Pyrogram Client kwargs."""
        return {
            'device_model': self._device_model,
            'system_version': self._system_version,
            'app_version': self._app_version,
            'lang_code': self._lang_code,
        }
    
    @classmethod
    def from_preset(cls, preset_name: str = DEFAULT_PLATFORM) -> 'ClientFingerprint':
        """Create fingerprint from a verified preset."""
        if preset_name not in cls.PLATFORM_PRESETS:
            raise ValueError(f"Unknown preset: {preset_name}. Valid: {list(cls.PLATFORM_PRESETS.keys())}")
        params = cls.PLATFORM_PRESETS[preset_name]
        return cls(**params)
    
    @classmethod
    def get_default(cls) -> 'ClientFingerprint':
        """Get the default fingerprint (Desktop Windows)."""
        return cls.from_preset(cls.DEFAULT_PLATFORM)
    
    @classmethod
    def for_user(cls, user_id: int, platform: Optional[str] = None) -> 'ClientFingerprint':
        """
        Get a deterministic, consistent fingerprint for a user.
        
        SECURITY: The same user_id ALWAYS returns the same fingerprint,
        ensuring session fingerprint immutability across restarts.
        
        Args:
            user_id: Telegram user ID
            platform: Optional platform preference ('desktop', 'android', 'ios')
                      If None, uses DEFAULT_PLATFORM for consistency
        
        Returns:
            ClientFingerprint that is deterministic for this user_id
        """
        if platform is None:
            return cls.get_default()
        
        # SECURITY FIX: Always use desktop_windows for consistency
        # Multiple desktop platforms caused confusion in Telegram devices list
        if platform == 'desktop' or platform is None:
            return cls.from_preset('desktop_windows')
        
        platform_presets = {
            'android': ['android_samsung', 'android_pixel'],
            'ios': ['ios_iphone'],
        }
        
        if platform not in platform_presets:
            return cls.from_preset('desktop_windows')
        
        presets = platform_presets[platform]
        idx = int(hashlib.md5(str(user_id).encode()).hexdigest(), 16) % len(presets)
        return cls.from_preset(presets[idx])
    
    def __repr__(self) -> str:
        return f"ClientFingerprint(device={self._device_model}, os={self._system_version}, app={self._app_version})"


# Global default fingerprint - use this for ALL client creations
DEFAULT_CLIENT_FINGERPRINT = ClientFingerprint.get_default()


def get_client_params(user_id: Optional[int] = None) -> Dict[str, str]:
    """
    Get validated client parameters for MTProto sessions.
    
    SECURITY: Always returns platform-consistent parameters.
    
    Args:
        user_id: Optional user ID for deterministic fingerprint selection
    
    Returns:
        Dict with device_model, system_version, app_version, lang_code
    """
    if user_id is not None:
        return ClientFingerprint.for_user(user_id).to_dict()
    return DEFAULT_CLIENT_FINGERPRINT.to_dict()
