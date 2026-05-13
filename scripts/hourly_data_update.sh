#!/usr/bin/env bash
set -u

PROJECT_ROOT="/home/ytchungaa/Documents/GitHub/Investment-Advise-Platform"
PYTHON_BIN="${PROJECT_ROOT}/venv/bin/python"
ACCOUNT_LOCK_FILE="${PROJECT_ROOT}/logs/hourly_account_update.lock"
MARKET_LOCK_FILE="${PROJECT_ROOT}/logs/hourly_market_update.lock"
DWD_LOCK_FILE="${PROJECT_ROOT}/logs/hourly_dwd_update.lock"
ACCOUNT_LOOKBACK_HOURS="${ACCOUNT_LOOKBACK_HOURS:-24}"
DWD_REFRESH_LOOKBACK_DAYS="${DWD_REFRESH_LOOKBACK_DAYS:-1}"
DWD_BUCKET_TIMEZONE="${DWD_BUCKET_TIMEZONE:-America/New_York}"

cd "${PROJECT_ROOT}" || exit 1
mkdir -p logs

if [[ -f .env ]]; then
    set -a
    source .env
    set +a
fi

if [[ ! -x "${PYTHON_BIN}" ]]; then
    echo "$(date -Is) Python venv not found or not executable: ${PYTHON_BIN}"
    exit 1
fi

run_locked_stage() {
    local stage_name="$1"
    local lock_file="$2"
    local lock_fd
    local status
    shift 2

    exec {lock_fd}>"${lock_file}"
    if ! flock -n "${lock_fd}"; then
        echo "$(date -Is) ${stage_name} is already running; skipping this hourly invocation"
        eval "exec ${lock_fd}>&-"
        return 0
    fi

    "$@"
    status=$?
    flock -u "${lock_fd}"
    eval "exec ${lock_fd}>&-"
    if [[ ${status} -ne 0 ]]; then
        echo "$(date -Is) ${stage_name} failed with status ${status}"
    fi
    return "${status}"
}

run_account_update() {
    echo "$(date -Is) starting hourly account update"
    "${PYTHON_BIN}" hourly_update.py account --lookback-hours "${ACCOUNT_LOOKBACK_HOURS}"
    echo "$(date -Is) completed hourly account update"
}

run_market_update() {
    echo "$(date -Is) starting hourly market update"
    "${PYTHON_BIN}" hourly_update.py market
    echo "$(date -Is) completed hourly market update"
}

run_dwd_update() {
    export PGPASSWORD="${POSTGRES_DB_PASSWORD:?POSTGRES_DB_PASSWORD is not set}"

    echo "$(date -Is) starting hourly DWD price-history refresh"
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
    echo "$(date -Is) completed hourly DWD price-history refresh"
}

overall_status=0

run_locked_stage "hourly account update" "${ACCOUNT_LOCK_FILE}" run_account_update || overall_status=1
run_locked_stage "hourly market update" "${MARKET_LOCK_FILE}" run_market_update || overall_status=1
run_locked_stage "hourly DWD update" "${DWD_LOCK_FILE}" run_dwd_update || overall_status=1

if [[ ${overall_status} -ne 0 ]]; then
    echo "$(date -Is) hourly data update completed with failures"
else
    echo "$(date -Is) hourly data update completed successfully"
fi

exit "${overall_status}"
