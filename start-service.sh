#!/bin/sh
# Alias for the Wzgram launcher; use the same venv and configuration checks.
set -eu
PROJECT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
exec /bin/sh "$PROJECT_DIR/start_alwaysdata.sh"
