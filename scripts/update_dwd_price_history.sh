#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="/home/ytchungaa/Documents/GitHub/Investment-Advise-Platform"
LOCK_FILE="${PROJECT_ROOT}/logs/dwd_price_history_update.lock"
DWD_REFRESH_LOOKBACK_DAYS="${DWD_REFRESH_LOOKBACK_DAYS:-10}"
DWD_BUCKET_TIMEZONE="${DWD_BUCKET_TIMEZONE:-America/New_York}"

cd "${PROJECT_ROOT}"
mkdir -p logs

exec 9>"${LOCK_FILE}"
if ! flock -n 9; then
    echo "$(date -Is) another DWD price-history update is already running; exiting"
    exit 0
fi

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
    -v refresh_start_time="${DWD_REFRESH_START_TIME:-}" \
    -v refresh_end_time="${DWD_REFRESH_END_TIME:-}" \
    -v refresh_lookback_days="${DWD_REFRESH_LOOKBACK_DAYS}" \
    -v bucket_timezone="${DWD_BUCKET_TIMEZONE}" \
    -P pager=off \
    -f database/update_tables/dwd.price_history_hourly_incremental_upsert.sql

psql \
    -h localhost \
    -p 5432 \
    -U "${POSTGRES_DB_USERNAME:?POSTGRES_DB_USERNAME is not set}" \
    -d investment_advise_platform \
    -v ON_ERROR_STOP=1 \
    -v refresh_start_time="${DWD_REFRESH_START_TIME:-}" \
    -v refresh_end_time="${DWD_REFRESH_END_TIME:-}" \
    -v refresh_lookback_days="${DWD_REFRESH_LOOKBACK_DAYS}" \
    -v bucket_timezone="${DWD_BUCKET_TIMEZONE}" \
    -P pager=off \
    -f database/update_tables/dwd.price_history_daily_incremental_upsert.sql

echo "$(date -Is) completed DWD price-history update"
