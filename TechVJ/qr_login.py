# QR Code Login utilities for Telegram
# Uses auth.ExportLoginToken API for QR-based authentication

import os
import asyncio
import base64
import time
import random
import platform
import hashlib
from io import BytesIO

from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.raw import functions, types as raw_types
from pyrogram.errors import SessionPasswordNeeded
from asyncio.exceptions import TimeoutError

from config import API_ID, API_HASH, get_client_params, ClientFingerprint


# Custom ask implementation (replacement for pyromod.ask)
async def ask_message(
    client: Client,
    chat_id: int,
    text: str,
    timeout: int = 300,
    filters_arg=None
) -> Message:
    """
    Send a message and wait for a reply from the user.
    Replacement for pyromod's bot.ask() method.
    """
    await client.send_message(chat_id, text)
    
    # Try listen() if available
    if hasattr(client, 'listen'):
        try:
            return await client.listen(chat_id=chat_id, filters=filters_arg, timeout=timeout)
        except:
            pass
    
    # Fallback: Manual waiting
    response_event = asyncio.Event()
    response_message = None
    
    @client.on_message(filters.chat(chat_id) & filters.private)
    async def temp_handler(_, msg: Message):
        nonlocal response_message
        response_message = msg
        response_event.set()
    
    try:
        await asyncio.wait_for(response_event.wait(), timeout=timeout)
        return response_message
    except asyncio.TimeoutError:
        raise TimeoutError("Response timeout")
    finally:
        try:
            client.remove_handler(temp_handler)
        except:
            pass

# Try to import QR code libraries
try:
    import qrcode
    from PIL import Image
    HAS_QRCODE = True
except ImportError:
    HAS_QRCODE = False

# Directory for temporary QR images
QR_DIR = "downloads"

# DEPRECATED: Random device configs removed for security.
# All sessions now use ClientFingerprint from config.py for platform consistency.
# This prevents Telegram security resets caused by platform parameter mixing.

def get_device_info(user_id, device_type='desktop'):
    """
    Get validated, platform-consistent device information for Telegram session.
    
    SECURITY: Uses ClientFingerprint.for_user() to ensure:
    - Same user_id always gets same fingerprint (immutability)
    - Platform parameters are always consistent (no mixing)
    - Session restarts reuse the same fingerprint
    
    Args:
        user_id: User's Telegram ID (used for deterministic selection)
        device_type: Platform preference ('desktop', 'android', 'ios')
                    Note: 'auto' is no longer supported for security reasons.
    
    Returns:
        dict with device_model, system_version, app_version, lang_code
    """
    # Map old 'auto' to 'desktop' (secure default)
    if device_type == 'auto':
        device_type = 'desktop'
    
    return ClientFingerprint.for_user(user_id, platform=device_type).to_dict()


def get_system_device_info():
    """
    Get device info based on the actual system running the bot.
    SECURITY: Now returns validated fingerprint from config.py
    
    Returns:
        dict with device_model, system_version, app_version, lang_code
    """
    return get_client_params()


def ensure_qr_dir():
    """Ensure the QR directory exists"""
    os.makedirs(QR_DIR, exist_ok=True)


def get_qr_path(user_id):
    """Get the path for user's QR code image"""
    ensure_qr_dir()
    return os.path.join(QR_DIR, f"qr_{user_id}.png")


def cleanup_qr_files(user_id):
    """Remove temporary QR image for a user"""
    qr_path = get_qr_path(user_id)
    try:
        if os.path.exists(qr_path):
            os.remove(qr_path)
    except OSError:
        pass


async def generate_qr_login_token(client):
    """
    Generate a QR login token using Telegram's raw API.
    
    Args:
        client: Connected Pyrogram client
        
    Returns:
        tuple: (token_bytes, expires_timestamp) or (None, None) on error
    """
    try:
        result = await client.invoke(
            functions.auth.ExportLoginToken(
                api_id=API_ID,
                api_hash=API_HASH,
                except_ids=[]
            )
        )
        
        # Check result type
        if isinstance(result, raw_types.auth.LoginToken):
            return result.token, result.expires
        elif isinstance(result, raw_types.auth.LoginTokenMigrateTo):
            # Need to reconnect to different DC
            return None, None
        elif isinstance(result, raw_types.auth.LoginTokenSuccess):
            # Already logged in
            return "SUCCESS", None
        
        return None, None
    except Exception as e:
        print(f"Error generating QR token: {e}")
        return None, None


def create_qr_image(token_bytes, user_id):
    """
    Create a QR code image from the login token.
    
    Args:
        token_bytes: The token bytes from ExportLoginToken
        user_id: User ID for unique filename
        
    Returns:
        str: Path to the created QR image, or None on error
    """
    if not HAS_QRCODE:
        return None
    
    try:
        # Encode token as base64url (URL-safe base64 without padding)
        token_b64 = base64.urlsafe_b64encode(token_bytes).decode('utf-8').rstrip('=')
        
        # Create the tg:// login URL
        login_url = f"tg://login?token={token_b64}"
        
        # Generate QR code
        qr = qrcode.QRCode(
            version=1,
            error_correction=qrcode.constants.ERROR_CORRECT_L,
            box_size=10,
            border=4,
        )
        qr.add_data(login_url)
        qr.make(fit=True)
        
        # Create image with custom colors
        img = qr.make_image(fill_color="black", back_color="white")
        
        # Save to file
        qr_path = get_qr_path(user_id)
        img.save(qr_path)
        
        return qr_path
    except Exception as e:
        print(f"Error creating QR image: {e}")
        return None


async def check_qr_login_status(client):
    """
    Check if QR code has been scanned and login is successful.
    
    Args:
        client: Connected Pyrogram client
        
    Returns:
        tuple: (status, result)
            status: 'pending', 'success', 'migrated', 'expired', 'error'
            result: Additional data depending on status
    """
    try:
        result = await client.invoke(
            functions.auth.ExportLoginToken(
                api_id=API_ID,
                api_hash=API_HASH,
                except_ids=[]
            )
        )
        
        if isinstance(result, raw_types.auth.LoginToken):
            # Still waiting for scan
            return 'pending', result
        elif isinstance(result, raw_types.auth.LoginTokenMigrateTo):
            # Need to migrate to different DC
            return 'migrated', result.dc_id
        elif isinstance(result, raw_types.auth.LoginTokenSuccess):
            # Successfully logged in!
            return 'success', result.authorization
        
        return 'error', None
    except SessionPasswordNeeded:
        return '2fa_required', None
    except Exception as e:
        error_str = str(e).upper()
        if 'SESSION_PASSWORD_NEEDED' in error_str:
            return '2fa_required', None
        if 'AUTH_TOKEN_EXPIRED' in error_str:
            return 'expired', None
        return 'error', str(e)


async def wait_for_qr_scan(client, user_id, bot, chat_id, timeout=120, refresh_interval=30):
    """
    Wait for user to scan QR code with automatic refresh.
    
    Args:
        client: Connected Pyrogram client for QR login
        user_id: User's Telegram ID
        bot: Bot client to send messages
        chat_id: Chat ID to send updates to
        timeout: Total timeout in seconds
        refresh_interval: How often to refresh QR code
        
    Returns:
        tuple: (success, needs_2fa, error_message)
    """
    start_time = time.time()
    last_refresh = 0
    qr_message = None
    
    while time.time() - start_time < timeout:
        current_time = time.time()
        
        # Check if we need to refresh QR code
        if current_time - last_refresh >= refresh_interval or last_refresh == 0:
            # Generate new token
            token, expires = await generate_qr_login_token(client)
            
            if token == "SUCCESS":
                # Already logged in
                cleanup_qr_files(user_id)
                if qr_message:
                    try:
                        await qr_message.delete()
                    except:
                        pass
                return True, False, None
            
            if token is None:
                cleanup_qr_files(user_id)
                return False, False, "Failed to generate QR code token"
            
            # Create QR image
            qr_path = create_qr_image(token, user_id)
            if not qr_path:
                cleanup_qr_files(user_id)
                return False, False, "Failed to create QR code image"
            
            # Calculate remaining time
            remaining = int(timeout - (current_time - start_time))
            
            # Send or update QR code message
            caption = (
                "**Scan this QR code to login**\n\n"
                "**Instructions:**\n"
                "1. Open Telegram on your phone\n"
                "2. Go to **Settings** > **Devices** > **Link Desktop Device**\n"
                "3. Point your phone camera at this QR code\n\n"
                f"⏱ Time remaining: **{remaining}s**\n"
                f"🔄 QR refreshes every {refresh_interval}s\n\n"
                "_Send /cancel to cancel login_"
            )
            
            try:
                if qr_message:
                    # Delete old message and send new one (can't edit media)
                    try:
                        await qr_message.delete()
                    except:
                        pass
                
                qr_message = await bot.send_photo(
                    chat_id,
                    qr_path,
                    caption=caption
                )
            except Exception as e:
                print(f"Error sending QR: {e}")
            
            last_refresh = current_time
        
        # Check login status
        status, result = await check_qr_login_status(client)
        
        if status == 'success':
            cleanup_qr_files(user_id)
            if qr_message:
                try:
                    await qr_message.delete()
                except:
                    pass
            return True, False, None
        
        elif status == '2fa_required':
            cleanup_qr_files(user_id)
            if qr_message:
                try:
                    await qr_message.delete()
                except:
                    pass
            return True, True, None  # Success but needs 2FA
        
        elif status == 'expired':
            # Token expired, will refresh on next iteration
            pass
        
        elif status == 'error':
            cleanup_qr_files(user_id)
            if qr_message:
                try:
                    await qr_message.delete()
                except:
                    pass
            return False, False, f"Error: {result}"
        
        # Wait before next check
        await asyncio.sleep(2)
    
    # Timeout
    cleanup_qr_files(user_id)
    if qr_message:
        try:
            await qr_message.delete()
        except:
            pass
    return False, False, "Login timeout. Please try again."


async def perform_qr_login(bot, message, user_id):
    """
    Complete QR login flow for a user.
    
    Args:
        bot: Bot client
        message: User's message
        user_id: User's Telegram ID
        
    Returns:
        tuple: (success, session_string, error_message)
    """
    from database.db import database
    
    if not HAS_QRCODE:
        return False, None, "QR code library not installed. Please install: pip install qrcode[pil] Pillow"
    
    # Create sessions directory
    os.makedirs("sessions", exist_ok=True)
    session_path = f"sessions/qr_temp_{user_id}"
    
    # Get validated platform-consistent fingerprint
    # SECURITY: Use same fingerprint as normal login (desktop preset)
    fp = get_device_info(user_id, device_type='desktop')
    
    # Create client for QR login with validated fingerprint
    client = Client(
        session_path,
        api_id=API_ID,
        api_hash=API_HASH,
        device_model=fp['device_model'],
        system_version=fp['system_version'],
        app_version=fp['app_version'],
        lang_code=fp['lang_code']
    )
    
    try:
        await client.connect()
        
        # Wait for QR scan
        success, needs_2fa, error = await wait_for_qr_scan(
            client, user_id, bot, message.chat.id,
            timeout=120, refresh_interval=30
        )
        
        if not success:
            return False, None, error
        
        # Handle 2FA if needed
        if needs_2fa:
            from pyrogram.errors import PasswordHashInvalid
            
            max_2fa_attempts = 3
            for fa_attempt in range(max_2fa_attempts):
                try:
                    remaining_2fa = max_2fa_attempts - fa_attempt
                    two_step_msg = await ask_message(
                        client=bot,
                        chat_id=user_id,
                        text=f"**Your account has Two-Step Verification enabled.**\n\n"
                        f"Please enter your 2FA password:\n\n"
                        f"**Attempts remaining: {remaining_2fa}**\n"
                        f"_Send /cancel to cancel_",
                        filters_arg=filters.text,
                        timeout=300
                    )
                except TimeoutError:
                    return False, None, "Login cancelled due to timeout."
                
                if two_step_msg.text == '/cancel':
                    return False, None, "Login cancelled"
                
                try:
                    password = two_step_msg.text
                    await client.check_password(password)
                    break  # Success, exit loop
                except PasswordHashInvalid:
                    if fa_attempt < max_2fa_attempts - 1:
                        await bot.send_message(user_id, "**Invalid password. Please try again.**")
                        continue
                    else:
                        return False, None, "Invalid 2FA password. Maximum attempts reached."
                except Exception as e:
                    if "PasswordHashInvalid" in str(e) or "PASSWORD" in str(e).upper():
                        if fa_attempt < max_2fa_attempts - 1:
                            await bot.send_message(user_id, "**Invalid password. Please try again.**")
                            continue
                        else:
                            return False, None, "Invalid 2FA password. Maximum attempts reached."
                    return False, None, f"2FA error: {str(e)}"
        
        # Export session string
        session_string = await client.export_session_string()
        
        if not session_string or len(session_string) < 100:
            return False, None, "Failed to export session"
        
        return True, session_string, None
        
    except Exception as e:
        return False, None, f"Login error: {str(e)}"
    
    finally:
        # Cleanup
        try:
            await client.disconnect()
        except:
            pass
        
        cleanup_qr_files(user_id)
        
        # Remove temp session file
        try:
            if os.path.exists(f"{session_path}.session"):
                os.remove(f"{session_path}.session")
        except:
            pass


def is_qr_available():
    """Check if QR code functionality is available"""
    return HAS_QRCODE
