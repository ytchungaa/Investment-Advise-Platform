import argparse
import sys
from pathlib import Path
from typing import Iterable

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from database_connect import connector
from get_market_data import schwab_api_market
from logging_config import logger


PRICE_HISTORY_UPDATE_COLUMNS = [
    "open",
    "high",
    "low",
    "close",
    "volume",
    "previous_close",
    "previous_close_time",
    "need_extended_hours_data",
]

INSTRUMENT_UPDATE_COLUMNS = [
    "cusip",
    "description",
    "exchange",
    "asset_main_type",
    "asset_sub_type",
    "quote_type",
    "ssid",
    "realtime",
    "last_seen_at",
]

VALID_DAY_PERIODS = (1, 2, 3, 4, 5, 10)
MINUTE_FREQUENCIES = ("1", "5", "10", "15", "30")
MINUTE_FREQUENCY_TYPE_ID = 1
DAILY_FREQUENCY_TYPE_ID = 2
DEFAULT_WINDOW_DAYS = 10
DEFAULT_STOP_AFTER_EMPTY_WINDOWS = 6


def _default_backfill_end_date() -> str:
    return pd.Timestamp.now(tz="UTC").normalize().strftime("%Y-%m-%d")


def _normalize_date(value: str) -> str:
    return pd.Timestamp(value, tz="UTC").strftime("%Y-%m-%d")


def _period_for_window_days(window_days: int) -> str:
    if window_days < 1 or window_days > DEFAULT_WINDOW_DAYS:
        raise ValueError(f"window_days must be between 1 and {DEFAULT_WINDOW_DAYS}")
    return str(min(period for period in VALID_DAY_PERIODS if period >= window_days))


def _iter_backward_request_windows(
    end_date: str, window_days: int = DEFAULT_WINDOW_DAYS
) -> Iterable[tuple[str, str, str, int]]:
    if window_days < 1 or window_days > DEFAULT_WINDOW_DAYS:
        raise ValueError(f"window_days must be between 1 and {DEFAULT_WINDOW_DAYS}")

    current_window_end = pd.Timestamp(end_date, tz="UTC")
    window_index = 0

    while True:
        window_start = current_window_end - pd.Timedelta(days=window_days - 1)
        days_in_window = (current_window_end - window_start).days + 1
        yield (
            window_start.strftime("%Y-%m-%d"),
            current_window_end.strftime("%Y-%m-%d"),
            _period_for_window_days(days_in_window),
            window_index,
        )
        current_window_end = window_start - pd.Timedelta(days=1)
        window_index += 1


def _ensure_price_history_frequency_types(db_ods: connector) -> None:
    db_ods.execute(
        """
        CREATE TABLE IF NOT EXISTS price_history_frequency_type (
            id SMALLINT PRIMARY KEY,
            code TEXT NOT NULL UNIQUE
        );
        """
    )
    db_ods.execute(
        """
        INSERT INTO price_history_frequency_type (id, code)
        VALUES
            (1, 'minute'),
            (2, 'daily'),
            (3, 'weekly'),
            (4, 'monthly')
        ON CONFLICT (id) DO UPDATE
        SET code = EXCLUDED.code;
        """
    )


def _load_symbols(db_ods: connector, db_dwd: connector) -> list[str]:
    watch_list_df = db_dwd.query_dataframe(
        """
        SELECT symbol
        FROM watch_list
        WHERE is_active IS TRUE
        ORDER BY symbol;
        """
    )
    if not watch_list_df.empty:
        return watch_list_df["symbol"].dropna().drop_duplicates().tolist()

    instrument_df = db_ods.query_dataframe(
        """
        SELECT DISTINCT symbol
        FROM instrument
        ORDER BY symbol;
        """
    )
    if instrument_df.empty:
        return []
    return instrument_df["symbol"].dropna().drop_duplicates().tolist()


def _filter_price_history_to_window(
    price_df: pd.DataFrame, start_date: str, end_date: str
) -> pd.DataFrame:
    if price_df.empty:
        return pd.DataFrame()

    window_start = pd.Timestamp(start_date, tz="UTC")
    window_end_exclusive = pd.Timestamp(end_date, tz="UTC") + pd.Timedelta(days=1)
    filtered_df = price_df.loc[
        (price_df["candle_time"] >= window_start)
        & (price_df["candle_time"] < window_end_exclusive)
    ].copy()
    if filtered_df.empty:
        return filtered_df

    return (
        filtered_df.drop_duplicates(
            subset=["symbol", "frequency_type", "frequency", "candle_time"]
        )
        .sort_values("candle_time")
        .reset_index(drop=True)
    )


def _fetch_best_minute_history_for_window(
    api: schwab_api_market,
    symbol: str,
    start_date: str,
    end_date: str,
    period: str,
    need_extended_hours_data: bool,
    need_previous_close: bool,
) -> tuple[pd.DataFrame, str | None]:
    for frequency in MINUTE_FREQUENCIES:
        logger.info(
            "Trying %s-minute history for %s between %s and %s.",
            frequency,
            symbol,
            start_date,
            end_date,
        )
        price_df = api.fetch_price_history(
            symbol=symbol,
            period_type="day",
            period=period,
            frequency_type="minute",
            frequency=frequency,
            start_date=start_date,
            end_date=end_date,
            need_extended_hours_data=need_extended_hours_data,
            need_previous_close=need_previous_close,
        )
        price_df = _filter_price_history_to_window(price_df, start_date, end_date)
        if not price_df.empty:
            return price_df, frequency

    return pd.DataFrame(), None


def _delete_daily_rows_covered_by_minute_data(
    db_ods: connector,
    instrument_id: int,
    start_date: str,
    end_date: str,
) -> int:
    window_start = pd.Timestamp(start_date, tz="UTC")
    window_end_exclusive = pd.Timestamp(end_date, tz="UTC") + pd.Timedelta(days=1)
    delete_cursor = db_ods.execute(
        """
        DELETE FROM price_history AS daily
        WHERE daily.instrument_id = :instrument_id
          AND daily.frequency_type = :daily_frequency_type
          AND daily.candle_time >= :window_start
          AND daily.candle_time < :window_end_exclusive
          AND EXISTS (
              SELECT 1
              FROM price_history AS minute
              WHERE minute.instrument_id = daily.instrument_id
                AND minute.frequency_type = :minute_frequency_type
                AND minute.candle_time::date = daily.candle_time::date
          );
        """,
        params={
            "instrument_id": instrument_id,
            "daily_frequency_type": DAILY_FREQUENCY_TYPE_ID,
            "minute_frequency_type": MINUTE_FREQUENCY_TYPE_ID,
            "window_start": window_start,
            "window_end_exclusive": window_end_exclusive,
        },
    )
    return max(delete_cursor.rowcount or 0, 0)


def backfill_minute_history(
    end_date: str | None = None,
    window_days: int = DEFAULT_WINDOW_DAYS,
    stop_after_empty_windows: int = DEFAULT_STOP_AFTER_EMPTY_WINDOWS,
    max_request_attempts: int = 6,
    rate_limit_backoff_cap_seconds: float = 60.0,
    rate_limit_cooldown_seconds: float = 120.0,
    need_extended_hours_data: bool = True,
    need_previous_close: bool = True,
    delete_covered_daily_data: bool = True,
) -> dict:
    if stop_after_empty_windows < 1:
        raise ValueError("stop_after_empty_windows must be at least 1")
    if max_request_attempts < 1:
        raise ValueError("max_request_attempts must be at least 1")
    if rate_limit_backoff_cap_seconds < 0:
        raise ValueError("rate_limit_backoff_cap_seconds must be non-negative")
    if rate_limit_cooldown_seconds < 0:
        raise ValueError("rate_limit_cooldown_seconds must be non-negative")

    normalized_end_date = (
        _normalize_date(end_date) if end_date is not None else _default_backfill_end_date()
    )

    db_ods = connector(schema="ods")
    db_dwd = connector(schema="dwd")
    _ensure_price_history_frequency_types(db_ods)

    symbols = _load_symbols(db_ods, db_dwd)
    if not symbols:
        logger.warning("No symbols found in dwd.watch_list or ods.instrument.")
        return {}

    api = schwab_api_market()
    api.max_request_attempts = max_request_attempts
    api.rate_limit_backoff_cap_seconds = rate_limit_backoff_cap_seconds
    api.rate_limit_cooldown_seconds = rate_limit_cooldown_seconds
    instruments_df = api.fetch_instruments(symbols)
    if instruments_df.empty:
        raise RuntimeError("Failed to fetch instruments for the requested symbols.")

    db_ods.upsert_dataframe(
        instruments_df,
        table_name="instrument",
        conflict_columns=["symbol", "asset_type"],
        update_columns=INSTRUMENT_UPDATE_COLUMNS,
        chunksize=100,
    )

    instrument_lookup = db_ods.query_dataframe(
        "SELECT id AS instrument_id, symbol, asset_type FROM instrument;"
    )
    instrument_lookup = instrument_lookup.merge(
        instruments_df[["symbol", "asset_type"]].drop_duplicates(),
        on=["symbol", "asset_type"],
        how="inner",
    )
    if instrument_lookup.empty:
        raise RuntimeError("No instrument ids were resolved for the requested symbols.")

    total_symbols = len(instrument_lookup.index)
    symbols_loaded = 0
    total_rows_loaded = 0
    total_daily_rows_deleted = 0
    rows_loaded_by_frequency = {frequency: 0 for frequency in MINUTE_FREQUENCIES}

    for index, instrument_row in instrument_lookup.reset_index(drop=True).iterrows():
        symbol = instrument_row["symbol"]
        instrument_id = int(instrument_row["instrument_id"])
        symbol_rows_loaded = 0

        logger.info(
            "Fetching %s/%s minute history for %s backward from %s.",
            index + 1,
            total_symbols,
            symbol,
            normalized_end_date,
        )

        empty_window_streak = 0
        oldest_loaded_candle_time = pd.NaT

        for window_start, window_end, period, window_index in _iter_backward_request_windows(
            normalized_end_date, window_days=window_days
        ):
            price_df, selected_frequency = _fetch_best_minute_history_for_window(
                api=api,
                symbol=symbol,
                start_date=window_start,
                end_date=window_end,
                period=period,
                need_extended_hours_data=need_extended_hours_data,
                need_previous_close=need_previous_close,
            )

            if price_df.empty or selected_frequency is None:
                empty_window_streak += 1
                logger.info(
                    "No minute history available for %s between %s and %s across fallback frequencies (empty streak %s/%s).",
                    symbol,
                    window_start,
                    window_end,
                    empty_window_streak,
                    stop_after_empty_windows,
                )
                if empty_window_streak >= stop_after_empty_windows:
                    logger.info(
                        "[stop] %s: reached %s consecutive empty windows; assuming minute history is no longer available further back",
                        symbol,
                        stop_after_empty_windows,
                    )
                    break
                continue

            empty_window_streak = 0
            price_df["instrument_id"] = instrument_id
            success = db_ods.upsert_dataframe(
                price_df.drop(columns=["symbol"]),
                table_name="price_history",
                conflict_columns=["instrument_id", "frequency_type", "frequency", "candle_time"],
                update_columns=PRICE_HISTORY_UPDATE_COLUMNS,
                chunksize=1000,
            )
            if not success:
                raise RuntimeError(
                    f"Failed to load minute price history for symbol '{symbol}' in {window_start} to {window_end}."
                )

            loaded_rows = len(price_df.index)
            symbol_rows_loaded += loaded_rows
            total_rows_loaded += loaded_rows
            rows_loaded_by_frequency[selected_frequency] += loaded_rows
            oldest_loaded_candle_time = (
                price_df["candle_time"].min()
                if pd.isna(oldest_loaded_candle_time)
                else min(oldest_loaded_candle_time, price_df["candle_time"].min())
            )

            logger.info(
                "Loaded %s rows for %s between %s and %s using %s-minute candles (window %s).",
                loaded_rows,
                symbol,
                window_start,
                window_end,
                selected_frequency,
                window_index,
            )

            if delete_covered_daily_data:
                deleted_rows = _delete_daily_rows_covered_by_minute_data(
                    db_ods=db_ods,
                    instrument_id=instrument_id,
                    start_date=window_start,
                    end_date=window_end,
                )
                total_daily_rows_deleted += deleted_rows
                if deleted_rows:
                    logger.info(
                        "Deleted %s daily rows covered by minute data for %s between %s and %s.",
                        deleted_rows,
                        symbol,
                        window_start,
                        window_end,
                    )

        if symbol_rows_loaded:
            symbols_loaded += 1
            logger.info(
                "%s backfill complete. Loaded %s minute rows; oldest loaded candle at %s.",
                symbol,
                symbol_rows_loaded,
                oldest_loaded_candle_time,
            )

    summary = {
        "symbols_requested": len(symbols),
        "symbols_resolved": total_symbols,
        "symbols_loaded": symbols_loaded,
        "price_rows_loaded": total_rows_loaded,
        "rows_loaded_by_frequency": rows_loaded_by_frequency,
        "daily_rows_deleted": total_daily_rows_deleted,
        "end_date": normalized_end_date,
        "window_days": window_days,
        "stop_after_empty_windows": stop_after_empty_windows,
        "max_request_attempts": max_request_attempts,
        "rate_limit_backoff_cap_seconds": rate_limit_backoff_cap_seconds,
        "rate_limit_cooldown_seconds": rate_limit_cooldown_seconds,
        "delete_covered_daily_data": delete_covered_daily_data,
    }
    logger.info("Minute history backfill completed: %s", summary)
    return summary


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backfill minute price history backward from today using frequency fallback."
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="Inclusive end date in YYYY-MM-DD format. Defaults to today in UTC.",
    )
    parser.add_argument(
        "--window-days",
        type=int,
        default=DEFAULT_WINDOW_DAYS,
        help="Calendar days per Schwab request window. Must be between 1 and 10.",
    )
    parser.add_argument(
        "--stop-after-empty-windows",
        type=int,
        default=DEFAULT_STOP_AFTER_EMPTY_WINDOWS,
        help="Stop after this many consecutive empty backward windows for a symbol.",
    )
    parser.add_argument(
        "--max-request-attempts",
        type=int,
        default=6,
        help="Maximum retry attempts per Schwab API request, including the first attempt.",
    )
    parser.add_argument(
        "--rate-limit-backoff-cap-seconds",
        type=float,
        default=60.0,
        help="Maximum short retry sleep after a 429 response.",
    )
    parser.add_argument(
        "--rate-limit-cooldown-seconds",
        type=float,
        default=120.0,
        help="Cooldown applied to later requests after a 429 response.",
    )
    parser.add_argument(
        "--need-extended-hours-data",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Include the Schwab extended-hours flag in the request payload.",
    )
    parser.add_argument(
        "--need-previous-close",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Request previous-close data from Schwab.",
    )
    parser.add_argument(
        "--delete-covered-daily-data",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Delete daily rows whose trade dates are covered by minute data.",
    )
    return parser


if __name__ == "__main__":
    args = _build_parser().parse_args()
    print(
        backfill_minute_history(
            end_date=args.end_date,
            window_days=args.window_days,
            stop_after_empty_windows=args.stop_after_empty_windows,
            max_request_attempts=args.max_request_attempts,
            rate_limit_backoff_cap_seconds=args.rate_limit_backoff_cap_seconds,
            rate_limit_cooldown_seconds=args.rate_limit_cooldown_seconds,
            need_extended_hours_data=args.need_extended_hours_data,
            need_previous_close=args.need_previous_close,
            delete_covered_daily_data=args.delete_covered_daily_data,
        )
    )
