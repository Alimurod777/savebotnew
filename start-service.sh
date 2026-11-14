#!/bin/sh
# Alwaysdata Advanced > Services: run this file in the foreground.
set -eu

PROJECT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$PROJECT_DIR"

if [ ! -x "$PROJECT_DIR/venv/bin/python" ]; then
    echo "venv missing: create it and install requirements.txt first." >&2
    exit 1
fi

export PATH="$PROJECT_DIR/venv/bin:$PATH"
export PYTHONUNBUFFERED=1

# Validate without printing credentials or opening Telegram sessions.
"$PROJECT_DIR/venv/bin/python" -c '
import config
required = ("API_ID", "API_HASH", "BOT_TOKEN", "OWNER_ID")
missing = [name for name in required if not getattr(config, name, None)]
if missing:
    raise SystemExit("Set these in .env or service environment: " + ", ".join(missing))
'

exec "$PROJECT_DIR/venv/bin/python" -u bot.py
