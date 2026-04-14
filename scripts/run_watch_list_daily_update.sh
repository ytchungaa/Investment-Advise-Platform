#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="/home/ytchungaa/Documents/GitHub/Investment-Advise-Platform"
LOG_DIR="$REPO_DIR/logs"
LOG_FILE="$LOG_DIR/watch_list_daily_update.log"

mkdir -p "$LOG_DIR"
cd "$REPO_DIR"

./venv/bin/python -c "
from daily_update import stock_list_market_data
import pandas as pd

end_date = pd.Timestamp.now(tz='UTC').normalize()
start_date = end_date - pd.Timedelta(days=2)

print(
    stock_list_market_data(
        start_date=start_date.strftime('%Y-%m-%d'),
        end_date=end_date.strftime('%Y-%m-%d'),
        period_type='day',
        period='10',
        frequency_type='minute',
        frequency='1',
        need_extended_hours_data=True,
        need_previous_close=True,
    )
)
" >> "$LOG_FILE" 2>&1
