#!/bin/bash
set -e

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"

if [[ -x "venv/bin/python" ]]; then
    PYTHON_BIN="venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="python3"
else
    echo "Python not found. Run ./setup.sh first or install Python3."
    exit 1
fi

export FLASK_APP="${FLASK_APP:-app.server}"
export FLASK_DEBUG="${FLASK_DEBUG:-1}"

APP_HOST="${APP_HOST:-127.0.0.1}"
APP_PORT="${APP_PORT:-5000}"

echo "Starting Investment Advise Platform..."
echo "URL: http://${APP_HOST}:${APP_PORT}"
echo "Press Ctrl+C to stop."

exec "$PYTHON_BIN" -m flask run --host "$APP_HOST" --port "$APP_PORT"
