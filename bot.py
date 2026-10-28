# Don't Remove Credit Tg - @VJ_Botz
# Subscribe YouTube Channel For Amazing Bot https://youtube.com/@Tech_VJ
# Ask Doubt on telegram @KingVJ01

import asyncio
import signal
import time
import logging
import os
import sys
import platform
# Import Bot class directly from main.py to avoid circular import
from main import Bot

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Connection parameters
MAX_RETRIES = 10
INITIAL_RETRY_DELAY = 1
MAX_RETRY_DELAY = 60

# Held for the whole process lifetime so the OS releases it on exit/crash
_instance_lock_handle = None


def acquire_single_instance_lock():
    """Take an exclusive OS lock so two bot processes never poll the same token.

    Two instances sharing one bot token both receive every update, which makes
    the bot answer each message twice. The lock is released by the OS when the
    process exits, so a killed instance never blocks the next start.

    Returns:
        True  - lock acquired, safe to start
        False - another live instance owns the lock
        None  - lock file unavailable, start anyway (non-fatal)
    """
    global _instance_lock_handle

    lock_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "local_db", "bot.lock"
    )
    try:
        os.makedirs(os.path.dirname(lock_path), exist_ok=True)
        handle = open(lock_path, "a+")
    except OSError as e:
        logger.warning(f"Single-instance lock unavailable ({e}), starting without it")
        return None

    try:
        handle.seek(0)
        if os.name == "nt":
            import msvcrt
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        try:
            handle.seek(0)
            other_pid = handle.read().strip()
        except Exception:
            other_pid = ""
        handle.close()
        logger.error(
            f"Another bot instance is already running (pid {other_pid or 'unknown'})"
        )
        return False

    # Only write the PID once the lock is ours
    handle.seek(0)
    handle.truncate()
    handle.write(f"{os.getpid()}\n")
    handle.flush()

    _instance_lock_handle = handle
    logger.info(f"Single-instance lock acquired: {lock_path}")
    return True

def setup_event_loop():
    from core.compat import configure_event_loop_policy
    configure_event_loop_policy()

class BotRunner:
    """
    Bot lifecycle manager with SAFE restart pattern.
    
    CRITICAL CHANGES from previous version:
    1. Bot instance is created ONCE and reused
    2. On error, uses bot.restart() instead of creating new Bot()
    3. Proper cooldown between restart attempts
    4. Does NOT cause AUTH_KEY_DUPLICATED
    """
    
    def __init__(self):
        self.bot = None
        self.running = True
        self.retry_count = 0
        self.last_restart_time = 0
        self.restart_cooldown = 60  # Cooldown period between restarts in seconds
        self._restart_requested = False

    async def start_bot(self):
        """
        Start the bot with SAFE retry mechanism.
        
        KEY CHANGE: Creates Bot ONCE, then uses restart() for recovery.
        This prevents AUTH_KEY_DUPLICATED by reusing the same Client instance.
        """
        # Create bot ONCE at startup
        self.bot = Bot()
        
        while self.running:
            try:
                # Check if we need to apply cooldown
                time_since_restart = time.time() - self.last_restart_time
                if time_since_restart < self.restart_cooldown and self.retry_count > 0:
                    wait_time = self.restart_cooldown - time_since_restart
                    logger.info(f"Cooling down for {wait_time:.1f} seconds before restart...")
                    await asyncio.sleep(wait_time)

                # Start (or restart) the EXISTING bot instance
                if self.retry_count == 0 and not self._restart_requested:
                    # First start
                    await self.bot.start()
                    logger.info("Bot successfully started")
                else:
                    # Restart existing instance (does NOT create new Client)
                    logger.info(f"Attempting safe restart (attempt {self.retry_count}/{MAX_RETRIES})...")
                    success = await self.bot.restart()
                    if not success:
                        logger.error("Bot restart failed")
                        self.retry_count += 1
                        self.last_restart_time = time.time()
                        
                        if self.retry_count >= MAX_RETRIES:
                            logger.error(f"Maximum retries ({MAX_RETRIES}) reached. Stopping.")
                            self.running = False
                            break
                        
                        await asyncio.sleep(min(INITIAL_RETRY_DELAY * (2 ** self.retry_count), MAX_RETRY_DELAY))
                        continue
                    
                    logger.info("Bot successfully restarted")
                
                self.retry_count = 0  # Reset retry counter on successful start
                self._restart_requested = False

                # Keep the bot running until stopped
                restart_requested = await self._monitor_bot()
                if restart_requested and self.running:
                    # Route disconnect recovery through bot.restart() on next loop.
                    self._restart_requested = True
                    continue

            except Exception as e:
                if not self.running:
                    break
                
                # Calculate delay with exponential backoff
                delay = min(INITIAL_RETRY_DELAY * (2 ** self.retry_count), MAX_RETRY_DELAY)
                self.retry_count += 1
                self.last_restart_time = time.time()
                
                logger.error(f"Bot error: {str(e)}")
                logger.info(f"Will attempt restart in {delay} seconds... (Attempt {self.retry_count}/{MAX_RETRIES})")
                
                # NOTE: We do NOT stop and recreate the bot here anymore!
                # The restart() method will handle disconnect/reconnect safely
                
                # Stop after maximum retries
                if self.retry_count >= MAX_RETRIES:
                    logger.error(f"Maximum retries ({MAX_RETRIES}) reached. Stopping.")
                    self.running = False
                    break
                    
                await asyncio.sleep(delay)

    async def _monitor_bot(self):
        """Monitor the bot and detect connection issues"""
        try:
            # Use a simple event to keep the bot running
            stop_event = asyncio.Event()
            while self.running and self.bot:
                try:
                    # Check connection every 30 seconds
                    await asyncio.wait_for(stop_event.wait(), timeout=30)
                except asyncio.TimeoutError:
                    # Check if the bot is still connected
                    if hasattr(self.bot, "is_bot_connected") and callable(self.bot.is_bot_connected):
                        is_connected = False
                        try:
                            is_connected = await self.bot.is_bot_connected()
                        except Exception as e:
                            logger.error(f"Connection check failed: {e}")

                        if not is_connected:
                            logger.warning("Bot disconnected. Restarting...")
                            return True
        except asyncio.CancelledError:
            logger.info("Bot monitor task cancelled")
        except Exception as e:
            logger.error(f"Error in bot monitor: {e}")
        return False

    async def _safe_stop_bot(self):
        """Safely stop the bot if it's running"""
        if self.bot:
            try:
                await self.bot.stop()
            except Exception as e:
                logger.error(f"Error stopping bot: {e}")
            self.bot = None

    async def stop(self):
        """Stop the bot runner"""
        self.running = False
        await self._safe_stop_bot()
        logger.info("Bot runner stopped")

def shutdown_handler(sig, frame):
    """Handle shutdown signals"""
    print(f"Received exit signal {sig}")
    # We can't call async functions directly from a signal handler
    # Just exit and let the atexit handlers clean up
    raise KeyboardInterrupt

async def main():
    """Main entry point"""
    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)
    
    runner = BotRunner()
    try:
        await runner.start_bot()
    finally:
        await runner.stop()
        print("Bot has been shut down")

if __name__ == "__main__":
    loop = None
    try:
        # Refuse to run twice: two pollers on the same bot token double every reply
        if acquire_single_instance_lock() is False:
            print("Another bot instance is already running. Stop it first, then start again.")
            sys.exit(1)

        # Setup the appropriate event loop based on platform
        setup_event_loop()

        # Always use a new event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        # Log event loop implementation
        loop_class = loop.__class__.__name__
        logger.info(f"Using event loop: {loop_class}")

        loop.run_until_complete(main())
    except KeyboardInterrupt:
        print("Bot stopped by user")
    except Exception as e:
        print(f"Fatal error: {e}")
    finally:
        if loop is not None:
            try:
                # Clean up pending tasks
                pending = asyncio.all_tasks(loop)
                if pending:
                    print(f"Cleaning up {len(pending)} pending tasks...")
                    for task in pending:
                        task.cancel()

                    # Give tasks a chance to properly cancel
                    loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            except Exception as e:
                print(f"Error during cleanup: {e}")

            # Close the event loop properly
            if loop.is_running():
                loop.stop()
            if not loop.is_closed():
                loop.close()

        print("Bot shut down")

# Don't Remove Credit Tg - @VJ_Botz
# Subscribe YouTube Channel For Amazing Bot https://youtube.com/@Tech_VJ
# Ask Doubt on telegram @KingVJ01
