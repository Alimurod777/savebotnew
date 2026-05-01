"""
Pyrofork 2.3.69 Migration Verification Script

STRICT REQUIREMENTS:
- pyrofork==2.3.69 (exact version)
- tgcrypto-pyrofork==1.2.8 (MANDATORY)

Run this after installation to verify all components work correctly.

Usage:
    python verify_migration.py
"""

import sys
import asyncio
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# Required versions
REQUIRED_PYROFORK_VERSION = "2.3.69"
REQUIRED_TGCRYPTO_VERSION = "1.2.8"


def check_pyrogram_version():
    """Check that Pyrofork 2.3.69 is installed correctly."""
    try:
        import pyrogram
        version = getattr(pyrogram, '__version__', 'unknown')
        logger.info(f"Pyrofork version: {version}")
        
        # Verify exact version
        if version != REQUIRED_PYROFORK_VERSION:
            logger.warning(f"⚠ Expected pyrofork=={REQUIRED_PYROFORK_VERSION}, got {version}")
        
        # Check for key Pyrofork features
        from pyrogram import Client, filters
        from pyrogram.types import Message
        from pyrogram.enums import ParseMode, ChatType
        from pyrogram.errors import FloodWait
        
        logger.info("✓ Core imports successful")
        return True
    except ImportError as e:
        logger.error(f"✗ Import error: {e}")
        return False


def check_tgcrypto():
    """Check that tgcrypto-pyrofork 1.2.8 is installed."""
    try:
        import tgcrypto
        version = getattr(tgcrypto, '__version__', 'unknown')
        logger.info(f"TgCrypto version: {version}")
        
        # Verify it's the pyrofork variant
        if version != REQUIRED_TGCRYPTO_VERSION:
            logger.warning(f"⚠ Expected tgcrypto-pyrofork=={REQUIRED_TGCRYPTO_VERSION}, got {version}")
            logger.warning("  Standard tgcrypto may cause MTProto failures!")
        
        # Test crypto functions work
        test_data = b"test_encryption_data_1234567890"
        key = b"0123456789abcdef0123456789abcdef"
        iv = b"0123456789abcdef"
        
        # Test IGE encryption (used by MTProto)
        encrypted = tgcrypto.ige256_encrypt(test_data, key, iv)
        decrypted = tgcrypto.ige256_decrypt(encrypted, key, iv)
        
        if decrypted == test_data:
            logger.info("✓ TgCrypto IGE encryption working")
            return True
        else:
            logger.error("✗ TgCrypto IGE encryption mismatch!")
            return False
            
    except ImportError as e:
        logger.error(f"✗ TgCrypto import error: {e}")
        logger.error("  Install with: pip install tgcrypto-pyrofork==1.2.8")
        return False
    except Exception as e:
        logger.error(f"✗ TgCrypto test failed: {e}")
        return False

def check_raw_api():
    """Check raw API availability."""
    try:
        from pyrogram.raw import functions, types
        from pyrogram.raw.base import InputFileLocation
        logger.info("✓ Raw API imports successful")
        return True
    except ImportError as e:
        logger.error(f"✗ Raw API import error: {e}")
        return False

def check_file_id():
    """Check file_id module."""
    try:
        from pyrogram.file_id import FileId, FileType, PHOTO_TYPES
        logger.info("✓ file_id module available")
        return True
    except ImportError as e:
        logger.error(f"✗ file_id import error: {e}")
        return False

def check_handlers():
    """Check handler classes."""
    try:
        from pyrogram.handlers import MessageHandler, CallbackQueryHandler
        logger.info("✓ Handlers available")
        return True
    except ImportError as e:
        logger.error(f"✗ Handler import error: {e}")
        return False

def check_errors():
    """Check error classes."""
    try:
        from pyrogram.errors import (
            FloodWait,
            AuthKeyUnregistered,
            AuthKeyDuplicated,
            SessionRevoked,
            UserDeactivated,
            MessageNotModified,
            MessageIdInvalid,
            FileReferenceExpired,
        )
        logger.info("✓ Error classes available")
        return True
    except ImportError as e:
        logger.error(f"✗ Error import error: {e}")
        return False

def check_reply_compat():
    """Check reply compatibility layer."""
    try:
        from core.reply_compat import (
            build_reply_kwargs,
            build_reply_kwargs_from_message,
            build_link_preview_kwargs,
        )
        
        # Test building kwargs
        kwargs = build_reply_kwargs(123)
        assert 'reply_to_message_id' in kwargs or 'reply_parameters' in kwargs
        
        logger.info("✓ Reply compatibility layer working")
        return True
    except Exception as e:
        logger.error(f"✗ Reply compat error: {e}")
        return False

def check_pyrofork_compat():
    """Check Pyrofork compatibility layer."""
    try:
        from core.pyrofork_compat import (
            safe_send_message,
            safe_reply,
            get_markdown_mode,
            SafeHandlerManager,
            get_pyrogram_version,
        )
        
        version = get_pyrogram_version()
        logger.info(f"✓ Pyrofork compat layer working (version: {version})")
        return True
    except Exception as e:
        logger.error(f"✗ Pyrofork compat error: {e}")
        return False

def check_config():
    """Check config and fingerprint."""
    try:
        from config import (
            API_ID, API_HASH,
            ClientFingerprint,
            DEFAULT_CLIENT_FINGERPRINT,
            get_client_params,
        )
        
        params = get_client_params()
        assert 'device_model' in params
        assert 'system_version' in params
        assert 'app_version' in params
        
        logger.info(f"✓ Config working (fingerprint: {params['device_model']})")
        return True
    except Exception as e:
        logger.error(f"✗ Config error: {e}")
        return False

def check_session_handlers():
    """Check session handling modules."""
    try:
        from TechVJ.session_handler import (
            create_user_session,
            SessionInvalidError,
            SessionConnectionError,
        )
        logger.info("✓ Session handler v1 available")
        
        from TechVJ.session_handler_v2 import (
            create_session,
            SessionStatus,
        )
        logger.info("✓ Session handler v2 available")
        return True
    except Exception as e:
        logger.error(f"✗ Session handler error: {e}")
        return False

def check_inactivity_manager():
    """Check inactivity manager."""
    try:
        from TechVJ.inactivity_manager import (
            InactivityManager,
            get_inactivity_manager,
        )
        logger.info("✓ Inactivity manager available")
        return True
    except Exception as e:
        logger.error(f"✗ Inactivity manager error: {e}")
        return False

def check_client_factory():
    """Check client factory."""
    try:
        from core.client_factory import (
            create_user_client,
            create_download_client,
            validate_session,
        )
        logger.info("✓ Client factory available")
        return True
    except Exception as e:
        logger.error(f"✗ Client factory error: {e}")
        return False

def check_downloader():
    """Check downloader engine."""
    try:
        from core.downloader import DownloadEngine, get_engine
        logger.info("✓ Downloader engine available")
        return True
    except Exception as e:
        logger.error(f"✗ Downloader error: {e}")
        return False

def main():
    """Run all verification checks."""
    logger.info("=" * 50)
    logger.info("Pyrofork 2.3.69 Migration Verification")
    logger.info("=" * 50)
    logger.info(f"Required: pyrofork=={REQUIRED_PYROFORK_VERSION}")
    logger.info(f"Required: tgcrypto-pyrofork=={REQUIRED_TGCRYPTO_VERSION}")
    logger.info("=" * 50)
    
    checks = [
        ("Pyrofork Version", check_pyrogram_version),
        ("TgCrypto (fork-specific)", check_tgcrypto),
        ("Raw API", check_raw_api),
        ("File ID Module", check_file_id),
        ("Handlers", check_handlers),
        ("Errors", check_errors),
        ("Reply Compatibility", check_reply_compat),
        ("Pyrofork Compatibility", check_pyrofork_compat),
        ("Config & Fingerprint", check_config),
        ("Session Handlers", check_session_handlers),
        ("Inactivity Manager", check_inactivity_manager),
        ("Client Factory", check_client_factory),
        ("Downloader Engine", check_downloader),
    ]
    
    results = []
    for name, check_func in checks:
        logger.info(f"\nChecking {name}...")
        try:
            result = check_func()
            results.append((name, result))
        except Exception as e:
            logger.error(f"✗ {name} check failed with exception: {e}")
            results.append((name, False))
    
    logger.info("\n" + "=" * 50)
    logger.info("VERIFICATION SUMMARY")
    logger.info("=" * 50)
    
    passed = sum(1 for _, r in results if r)
    failed = sum(1 for _, r in results if not r)
    
    for name, result in results:
        status = "✓ PASS" if result else "✗ FAIL"
        logger.info(f"  {status}: {name}")
    
    logger.info(f"\nTotal: {passed} passed, {failed} failed")
    
    if failed == 0:
        logger.info("\n✓ All checks passed! Migration verified.")
        return 0
    else:
        logger.error(f"\n✗ {failed} checks failed. Review errors above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
