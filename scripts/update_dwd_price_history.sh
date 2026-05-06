#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="/home/ytchungaa/Documents/GitHub/Investment-Advise-Platform"
LOCK_FILE="${PROJECT_ROOT}/logs/dwd_price_history_update.lock"
MAX_WAIT_SECONDS=3600
SLEEP_SECONDS=60

cd "${PROJECT_ROOT}"
mkdir -p logs

exec 9>"${LOCK_FILE}"
if ! flock -n 9; then
    echo "$(date -Is) another DWD price-history update is already running; exiting"
    exit 0
fi

waited_seconds=0
while pgrep -f "daily_update.py" >/dev/null; do
    if (( waited_seconds >= MAX_WAIT_SECONDS )); then
        echo "$(date -Is) daily_update.py is still running after ${MAX_WAIT_SECONDS}s; skipping DWD update"
        exit 1
    fi
    echo "$(date -Is) waiting for daily_update.py to finish before DWD update"
    sleep "${SLEEP_SECONDS}"
    waited_seconds=$((waited_seconds + SLEEP_SECONDS))
done

set -a
source .env
set +a

export PGPASSWORD="${POSTGRES_DB_PASSWORD:?POSTGRES_DB_PASSWORD is not set}"

echo "$(date -Is) starting DWD price-history update"
psql \
    -h localhost \
    -p 5432 \
    -U "${POSTGRES_DB_USERNAME:?POSTGRES_DB_USERNAME is not set}" \
    -d investment_advise_platform \
    -v ON_ERROR_STOP=1 \
    -P pager=off \
    -f database/update_tables/dwd.price_history_hourly_incremental_upsert.sql

psql \
    -h localhost \
    -p 5432 \
    -U "${POSTGRES_DB_USERNAME:?POSTGRES_DB_USERNAME is not set}" \
    -d investment_advise_platform \
    -v ON_ERROR_STOP=1 \
    -P pager=off \
    -f database/update_tables/dwd.price_history_daily_incremental_upsert.sql

echo "$(date -Is) completed DWD price-history update"
