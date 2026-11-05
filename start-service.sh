#!/bin/sh
# Alwaysdata service entry point: keep Python in the foreground.
set -eu
PROJECT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$PROJECT_DIR"
exec "$PROJECT_DIR/.venv/bin/python" -u bot.py
