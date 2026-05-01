import asyncio
import traceback
import platform
import time
from flask import Flask, jsonify
import threading
import logging
from main import Bot

logger = logging.getLogger(__name__)

def setup_event_loop():
    from core.compat import configure_event_loop_policy
    configure_event_loop_policy()

# Initialize uvloop
setup_event_loop()

app = Flask(__name__)
bot = None
bot_thread = None
_bot_lock = threading.Lock()

@app.route('/')
def hello_world():
    bot_status = "Running" if bot_thread and bot_thread.is_alive() else "Not running"
    return jsonify({
        "status": "active",
        "message": "Telegram Save Restricted Content Bot",
        "bot_status": bot_status
    })

@app.route('/status')
def status():
    bot_status = "Running" if bot_thread and bot_thread.is_alive() else "Not running"
    return jsonify({
        "status": "active",
        "bot_status": bot_status
    })

def run_bot():
    global bot
    while True:
        try:
            # Create a new event loop for this thread
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            # Log event loop implementation
            loop_class = loop.__class__.__name__
            print(f"Bot running with event loop: {loop_class}")

            bot = Bot()
            bot.run()
            return
        except Exception as e:
            print(f"Bot crashed with error: {e}")
            print(traceback.format_exc())
            # Try to restart the bot after a delay
            time.sleep(5)

def start_bot_thread():
    global bot_thread
    with _bot_lock:
        if bot_thread is None or not bot_thread.is_alive():
            bot_thread = threading.Thread(target=run_bot)
            bot_thread.daemon = True
            bot_thread.start()
            return True
        return False

@app.route('/restart')
def restart_bot():
    global bot, bot_thread
    with _bot_lock:
        if bot_thread and bot_thread.is_alive():
            # Old thread still running — refuse to spawn a new one
            # (daemon thread will die when process exits; we can't safely kill it)
            logger.warning("/restart called but bot thread is still alive — refusing duplicate")
            return jsonify({
                "status": "already_running",
                "message": "Bot is already running. Stop it first or wait for it to exit."
            })

    success = start_bot_thread()
    return jsonify({
        "status": "success" if success else "already_running",
        "message": "Bot restart initiated" if success else "Bot is already running"
    })

if __name__ == "__main__":
    # Start the bot in a separate thread
    start_bot_thread()
    # Run Flask app
    app.run(host='0.0.0.0', port=8080)
