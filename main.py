# Don't Remove Credit Tg - @VJ_Botz
# Subscribe YouTube Channel For Amazing Bot https://youtube.com/@Tech_VJ
# Ask Doubt on telegram @KingVJ01

import asyncio
import logging
import platform
import os
from event_loop_bootstrap import ensure_current_event_loop

ensure_current_event_loop()

from pyrogram import Client, idle
from pyrogram.errors import AuthKeyUnregistered, FloodWait, BadMsgNotification
from config import API_ID, API_HASH, BOT_TOKEN, get_client_params, DEFAULT_CLIENT_FINGERPRINT

# Import StateManager for per-user state with local cache fallback
try:
    from database.state_manager import state_manager, init_state_manager
    STATE_MANAGER_AVAILABLE = True
except ImportError:
    STATE_MANAGER_AVAILABLE = False

# Import SessionManager for session validation (OPTIONAL - not for runtime sync)
try:
    from database.session_manager import scheduled_cleanup, start_scheduled_cleanup, stop_scheduled_cleanup
    SESSION_MANAGER_AVAILABLE = True
except ImportError:
    SESSION_MANAGER_AVAILABLE = False

# Import event-loop-safe cleanup manager (NO background tasks at import)
try:
    from TechVJ.cleanup_manager import cleanup_manager, on_bot_stop
    CLEANUP_MANAGER_AVAILABLE = True
except ImportError:
    CLEANUP_MANAGER_AVAILABLE = False

# Import download engine shutdown
try:
    from core.downloader import shutdown_engine
    DOWNLOAD_ENGINE_AVAILABLE = True
except ImportError:
    DOWNLOAD_ENGINE_AVAILABLE = False

# Import handler cleanup
try:
    from core.handler_manager import cleanup_global as cleanup_handlers
    HANDLER_MANAGER_AVAILABLE = True
except ImportError:
    HANDLER_MANAGER_AVAILABLE = False

# Import session_store to guarantee ./sessions/ dir is created on startup
try:
    from core.session_store import session_store as _session_store
    _SESSION_STORE_AVAILABLE = True
except ImportError:
    _SESSION_STORE_AVAILABLE = False

# Import ask_message handler setup
try:
    from TechVJ.generate import setup_ask_handler
    ASK_HANDLER_AVAILABLE = True
except ImportError:
    ASK_HANDLER_AVAILABLE = False

# NOTE: Legacy resource_calculator is NO LONGER USED for download workers.
# Download workers are managed by core.downloader.WorkerPool (32-150 adaptive).
# Pyrogram Client workers below are for update handling, not downloads.
RESOURCE_CALCULATOR_AVAILABLE = False  # Disabled - use fixed values

# Import safe reconnect module (prevents AUTH_KEY_DUPLICATED)
try:
    from TechVJ.safe_reconnect import (
        safe_reconnect, 
        safe_disconnect, 
        ConnectionMonitor,
        create_connection_monitor
    )
    SAFE_RECONNECT_AVAILABLE = True
except ImportError:
    SAFE_RECONNECT_AVAILABLE = False

# Import InactivityManager for auto-stop of idle user sessions
try:
    from TechVJ.inactivity_manager import (
        init_inactivity_manager,
        shutdown_inactivity_manager,
        get_inactivity_manager,
        INACTIVITY_TIMEOUT,
    )
    INACTIVITY_MANAGER_AVAILABLE = True
except ImportError:
    INACTIVITY_MANAGER_AVAILABLE = False

# Import premium session pool (non-critical — bot works without it)
try:
    from core.premium_relay import relay_uploader as _relay_uploader
    from core.premium_session_store import get_session_strings as _get_session_strings
    PREMIUM_RELAY_AVAILABLE = True
except ImportError:
    PREMIUM_RELAY_AVAILABLE = False

# Configure logging - both console and file
import os
os.makedirs("logs", exist_ok=True)

from logging.handlers import RotatingFileHandler

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        RotatingFileHandler(
            "logs/bot.log",
            maxBytes=50 * 1024 * 1024,  # 50 MB per file
            backupCount=3,              # Keep 3 old files (bot.log.1, .2, .3)
            encoding='utf-8'
        )
    ]
)
logger = logging.getLogger(__name__)

# Install request-scoped structured logging (enriches every log line with request context)
try:
    from core.request_context import install_request_logging
    install_request_logging()
    logger.info("Request context logging installed")
except ImportError:
    logger.debug("Request context logging not available")

# Set specific loggers
logging.getLogger("TechVJ").setLevel(logging.INFO)
logging.getLogger("core").setLevel(logging.INFO)
# Suppress noisy logs that caused 12GB log files
logging.getLogger("pymongo").setLevel(logging.WARNING)
logging.getLogger("pyrogram").setLevel(logging.WARNING)
logging.getLogger("pyrogram.session").setLevel(logging.WARNING)  # Sent/Received JSON dumps
logging.getLogger("pyrogram.connection").setLevel(logging.WARNING)  # Connection debug

# Setup uvloop for improved performance if not on Windows
def setup_event_loop():
    from core.compat import configure_event_loop_policy
    configure_event_loop_policy()

# Maximum retries for connection issues
MAX_RETRIES = 3
RETRY_DELAY = 5

class ConnectionError(Exception):
    """Custom exception for network connection errors"""
    pass

class Bot(Client):
    def __init__(self):
        # NOTE: Don't call setup_event_loop() here - let the entry point (bot.py) handle it
        # Calling it here causes "Future attached to different loop" errors

        # PATCH: Use session_store for environment-aware sessions dir.
        # On Colab → /content/sessions, on Kaggle → /kaggle/working/sessions,
        # on VPS/Windows → <project_root>/sessions.
        # Falls back to /tmp/tgbot_sessions if primary path is read-only.
        from core.session_store import ensure_sessions_dir
        _sessions_path = ensure_sessions_dir()
        _sessions_dir = str(_sessions_path)

        _base = os.path.dirname(os.path.abspath(__file__))
        _data_dir = os.path.join(_base, "data")
        os.makedirs(_data_dir, exist_ok=True)

        # Verify sessions dir is writable (prevents sqlite3 OperationalError)
        if not os.access(_sessions_dir, os.W_OK):
            raise RuntimeError(f"sessions directory not writable: {_sessions_dir}")

        # Absolute session path - independent of cwd
        _session_path = os.path.join(_sessions_dir, "techvj_login")

        logger.info(f"StorageMode: LocalSessions | SessionDir: {_sessions_dir}")
        
        # Pyrogram Client workers (for update handling, NOT downloads)
        # NOTE: Download workers are managed by core.downloader.WorkerPool (4-16 adaptive)
        # These are ONLY for Pyrogram's internal update processing
        workers = 16  # Fixed reasonable value for updates
        # CRITICAL FIX: max_concurrent_transmissions must be LOW (4) to prevent
        # FILE_PART_INVALID errors. High parallelism causes Pyrogram to send file
        # parts out-of-order → Telegram rejects them with [400 FILE_PART_INVALID].
        # Value of 4 is proven stable. Do NOT increase above 4 for uploads.
        max_concurrent = 4
        logger.info(f"Pyrogram workers={workers}, max_concurrent={max_concurrent} (downloads use core.downloader)")

        
        # SECURITY: Get validated platform-consistent fingerprint
        # This prevents Telegram security resets caused by platform parameter mixing
        fp = DEFAULT_CLIENT_FINGERPRINT.to_dict()
        logger.info(f"Using client fingerprint: device={fp['device_model']}, os={fp['system_version']}, app={fp['app_version']}")
        
        super().__init__(
            _session_path,
            api_id=API_ID,
            api_hash=API_HASH,
            bot_token=BOT_TOKEN,
            plugins=dict(root="TechVJ"),
            workdir=_sessions_dir,  # PATCH 1: Explicit workdir prevents sqlite3 errors
            workers=workers,  # Dynamically calculated based on system resources
            sleep_threshold=60,  # Higher threshold to handle rate limits gracefully
            max_concurrent_transmissions=max_concurrent,  # Dynamically calculated
            # SECURITY: Platform-consistent fingerprint (prevents security resets)
            device_model=fp['device_model'],
            system_version=fp['system_version'],
            app_version=fp['app_version'],
            lang_code=fp['lang_code'],
        )
        
        # Setup ask_message response handler
        if ASK_HANDLER_AVAILABLE:
            setup_ask_handler(self)
            logger.info("ask_message handler registered")
        
        # Setup language handlers
        try:
            from TechVJ.lang import register_lang_handlers
            register_lang_handlers(self)
            logger.info("/lang command registered")
        except ImportError as e:
            logger.warning(f"Language module not available: {e}")
        
        # Additional connection state variables
        self._connection_retries = 0
        self._is_connected = False
        self._last_ping_time = 0
        self._connection_errors = []

    async def start(self):
        """Start the bot with improved error handling"""
        for attempt in range(MAX_RETRIES):
            try:
                # Ensure we're disconnected before trying to connect
                if self.is_connected:
                    try:
                        await super().stop()
                    except Exception:
                        pass
                
                await super().start()
                self._is_connected = True
                logger.info('Bot Started Modified By WOODcraft')
                print('Bot Started Modified By WOODcraft')
                
                # Initialize StateManager (MongoDB + local cache fallback)
                if STATE_MANAGER_AVAILABLE:
                    try:
                        await init_state_manager()
                        logger.info("✅ StateManager initialized (MongoDB + local cache)")
                    except Exception as e:
                        logger.warning(f"StateManager init warning: {e}")
                
                # DISABLED: Scheduled session cleanup violates event-loop-safety
                # Background schedulers must NOT run - cleanup is now task-scoped
                # if SESSION_MANAGER_AVAILABLE:
                #     try:
                #         await start_scheduled_cleanup()
                #         logger.info("✅ Scheduled session cleanup started")
                #     except Exception as e:
                #         logger.warning(f"Session cleanup init warning: {e}")
                
                # Log event loop implementation
                loop = asyncio.get_running_loop()
                loop_class = loop.__class__.__name__
                logger.info(f"Bot running with event loop: {loop_class}")
                
                # Start the connection monitor (SAFE version)
                if SAFE_RECONNECT_AVAILABLE:
                    self._connection_monitor_obj = create_connection_monitor(self)
                    self._connection_monitor_obj.start()
                    logger.info("Safe connection monitor started")
                else:
                    # Fallback to legacy monitor (not recommended)
                    asyncio.create_task(self._connection_monitor())
                
                # Initialize InactivityManager for user session auto-stop
                # This soft-stops idle user sessions after 10 min to prevent security resets
                if INACTIVITY_MANAGER_AVAILABLE:
                    try:
                        await init_inactivity_manager()
                        logger.info(f"✅ InactivityManager initialized (timeout={INACTIVITY_TIMEOUT}s)")
                    except Exception as e:
                        logger.warning(f"InactivityManager init warning: {e}")

                # Initialize premium session pool (non-critical)
                # Bot continues working even if pool fails to init.
                # relay_uploader.pool is used by save.py for system session fallback.
                if PREMIUM_RELAY_AVAILABLE:
                    try:
                        _strings = await _get_session_strings()
                        await _relay_uploader.pool.set_sessions(_strings)
                        logger.info(
                            "PremiumPool: loaded %d session(s)",
                            len(_strings),
                        )
                    except Exception as e:
                        logger.warning(f"PremiumPool init warning (bot continues): {e}")

                # Initialize production-grade session manager (non-critical)
                try:
                    from core.session_manager import session_manager as _session_mgr
                    _me = await self.get_me()
                    await _session_mgr.init(
                        bot_id=_me.id,
                        bot_username=getattr(_me, "username", None),
                    )
                except Exception as e:
                    logger.warning("SessionManager init warning (bot continues): %s", e)

                # Initialize governance priority queue workers (non-critical)
                try:
                    from core.priority_queue import priority_queue as _pq
                    await _pq.start()
                    logger.info("PriorityQueue: started")
                except Exception as e:
                    logger.warning("PriorityQueue init warning (bot continues): %s", e)

                # Initialize local-first sync manager (Mongo → Local startup sync)
                try:
                    from database.sync_manager import sync_manager as _sync_mgr
                    await _sync_mgr.startup_sync()
                    logger.info("SyncManager: startup sync complete")
                except Exception as e:
                    logger.warning("SyncManager init warning (bot continues): %s", e)

                # Load channel monitor (non-critical)
                try:
                    from core.channel_monitor import channel_monitor as _ch_monitor
                    _ch_monitor.load()
                    logger.info("ChannelMonitor: loaded %d channels", _ch_monitor.count)
                except Exception as e:
                    logger.warning("ChannelMonitor init warning (bot continues): %s", e)

                return
            except (AuthKeyUnregistered, BadMsgNotification) as e:
                # These errors indicate a serious authentication issue, we won't retry
                logger.error(f"Critical authentication error: {e}")
                raise
            except FloodWait as e:
                # PATCH 5: Safe FloodWait handler — pause, then retry once
                wait_time = getattr(e, 'value', getattr(e, 'x', RETRY_DELAY * (attempt + 1)))
                logger.warning(f"FloodWaitHandled: {wait_time} seconds")
                await asyncio.sleep(wait_time)
            except OSError as e:
                # Handle network-related OS errors
                wait_time = RETRY_DELAY * (2 ** attempt)
                logger.warning(f"Network error: {e}. Retrying in {wait_time} seconds...")
                await asyncio.sleep(wait_time)
            except Exception as e:
                error_str = str(e).lower()
                # If already connected, try to stop first
                if "already connected" in error_str:
                    try:
                        await super().stop()
                        await asyncio.sleep(1)
                        continue  # Retry immediately after disconnecting
                    except Exception:
                        pass
                
                # For other errors, retry with exponential backoff
                wait_time = RETRY_DELAY * (2 ** attempt)
                logger.error(f"Failed to start bot: {e}")
                if attempt < MAX_RETRIES - 1:
                    logger.info(f"Retrying in {wait_time} seconds... (Attempt {attempt+1}/{MAX_RETRIES})")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"Failed to start after {MAX_RETRIES} attempts")
                    raise

    def run(self):
        """Run the bot with the event loop"""
        from core.compat import get_event_loop_safe
        loop = get_event_loop_safe()
        
        loop_class = loop.__class__.__name__
        logger.info(f"Bot run() with event loop: {loop_class}")
        
        try:
            loop.run_until_complete(self.start())
            loop.run_until_complete(idle())
        except KeyboardInterrupt:
            logger.info("Bot stopped by user")
        except Exception as e:
            logger.error(f"Bot stopped due to error: {e}")
        finally:
            loop.run_until_complete(self.stop())
            
    async def stop(self, *args):
        """Stop the bot with improved error handling"""
        try:
            self._is_connected = False
            
            # Shutdown download engine first (stops all workers)
            if DOWNLOAD_ENGINE_AVAILABLE:
                try:
                    await shutdown_engine()
                    logger.info("DownloadEngine shutdown complete")
                except Exception as e:
                    logger.warning(f"DownloadEngine shutdown warning: {e}")
            
            # Clean up all registered handlers
            if HANDLER_MANAGER_AVAILABLE:
                try:
                    await cleanup_handlers()
                    logger.info("Handler manager cleanup complete")
                except Exception as e:
                    logger.warning(f"Handler manager cleanup warning: {e}")
            
            # Run event-loop-safe cleanup (SAME loop, no threads)
            if CLEANUP_MANAGER_AVAILABLE:
                try:
                    cleaned = await on_bot_stop()
                    logger.info(f"Cleanup manager: {cleaned} resources cleaned")
                except Exception as e:
                    logger.warning(f"Cleanup manager warning: {e}")
            
            # Shutdown StateManager (save cache, sync to MongoDB)
            if STATE_MANAGER_AVAILABLE:
                try:
                    await state_manager.shutdown()
                    logger.info("StateManager shutdown complete")
                except Exception as e:
                    logger.warning(f"StateManager shutdown warning: {e}")
            
            # Shutdown InactivityManager (stops all idle user sessions safely)
            if INACTIVITY_MANAGER_AVAILABLE:
                try:
                    await shutdown_inactivity_manager()
                    logger.info("InactivityManager shutdown complete")
                except Exception as e:
                    logger.warning(f"InactivityManager shutdown warning: {e}")
            
            # Shutdown upload_queue (legacy, drains any remaining pending jobs)
            try:
                from core.upload_queue import upload_queue as _uq
                await _uq.stop_all()
                logger.info("UploadQueue shutdown complete")
            except Exception as e:
                logger.warning(f"UploadQueue shutdown warning: {e}")

            # Shutdown per-user MTProto upload workers
            try:
                from core.user_upload_worker import worker_registry as _wur
                await _wur.shutdown_all()
                logger.info("UserUploadWorker registry shutdown complete")
            except Exception as e:
                logger.warning(f"UserUploadWorker shutdown warning: {e}")

            await super().stop()
            logger.info('Bot Stopped Successfully')
            print('Bot Stopped Bye')
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
            # Force clean stop
            print('Bot Stopped with errors')

    async def restart(self):
        """
        Restart the bot using SAFE reconnect pattern.
        
        CRITICAL: Does NOT create new Client instance.
        Uses safe_reconnect() which properly waits between disconnect/connect.
        """
        try:
            if SAFE_RECONNECT_AVAILABLE:
                # Use safe reconnect (proper delays, no AUTH_KEY_DUPLICATED)
                success = await safe_reconnect(self, max_attempts=3, initial_delay=5.0)
                if success:
                    self._is_connected = True
                    logger.info("Bot successfully restarted (safe)")
                    return True
                else:
                    logger.error("Safe restart failed - may need process restart")
                    return False
            else:
                # Legacy restart (not recommended, may cause AUTH_KEY_DUPLICATED)
                await self.stop()
                await asyncio.sleep(5)  # Increased from 1s to 5s
                await self.start()
                logger.info("Bot successfully restarted (legacy)")
                return True
        except Exception as e:
            logger.error(f"Error during restart: {e}")
            return False

    async def is_bot_connected(self):
        """Check if the bot is still connected to Telegram"""
        return self._is_connected
        
    async def _connection_monitor(self):
        """
        LEGACY connection monitor - used only if safe_reconnect not available.
        
        WARNING: This method can cause AUTH_KEY_DUPLICATED if disconnect/connect
        timing is not handled properly. Prefer using SAFE_RECONNECT_AVAILABLE=True.
        """
        logger.warning("Using legacy connection monitor - AUTH_KEY_DUPLICATED risk!")
        
        while self._is_connected:
            try:
                # Perform a simple API call to check connection
                await asyncio.wait_for(self.get_me(), timeout=10.0)
                # If successful, clear any stored connection errors
                self._connection_errors.clear()
                
            except asyncio.TimeoutError:
                logger.warning("Connection check timeout")
                self._connection_errors.append("timeout")
                
            except Exception as e:
                # Track the error
                logger.warning(f"Connection check failed: {e}")
                self._connection_errors.append(str(e))
                
            # If we've accumulated too many errors, try to reconnect
            if len(self._connection_errors) >= 3:
                logger.warning("Multiple connection failures detected, attempting to reconnect...")
                try:
                    # CRITICAL FIX: Use longer delays to prevent AUTH_KEY_DUPLICATED
                    if self.is_connected:
                        await asyncio.wait_for(super().stop(), timeout=15.0)
                    
                    # Wait for Telegram to acknowledge disconnect
                    # CRITICAL: Must be >= 10 seconds to prevent AUTH_KEY_DUPLICATED
                    await asyncio.sleep(10.0)
                    
                    await asyncio.wait_for(super().start(), timeout=30.0)
                    logger.info("Reconnection successful")
                    self._connection_errors.clear()
                    
                except Exception as reconnect_error:
                    logger.error(f"Reconnection failed: {reconnect_error}")
                    # Wait even longer before trying again (exponential backoff)
                    await asyncio.sleep(20.0)
                    self._connection_errors.clear()  # Reset to allow retry
            
            # Wait before next check
            await asyncio.sleep(60)  # Check every minute

# Don't Remove Credit Tg - @VJ_Botz
# Subscribe YouTube Channel For Amazing Bot https://youtube.com/@Tech_VJ
# Ask Doubt on telegram @KingVJ01
