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

VALID_YEAR_PERIODS = {1, 2, 3, 5, 10, 15, 20}
DEFAULT_START_DATE = "2000-01-01"
MAX_WINDOW_YEARS = 20


def _default_backfill_end_date() -> str:
    return pd.Timestamp.now(tz="UTC").normalize().strftime("%Y-%m-%d")


def _iter_request_windows(start_date: str, end_date: str) -> Iterable[tuple[str, str, str]]:
    window_start = pd.Timestamp(start_date, tz="UTC")
    final_end = pd.Timestamp(end_date, tz="UTC")

    while window_start <= final_end:
        next_window_start = window_start + pd.DateOffset(years=MAX_WINDOW_YEARS)
        window_end = min(next_window_start - pd.Timedelta(days=1), final_end)
        window_years = max(window_end.year - window_start.year + 1, 1)
        period_years = min(
            period for period in sorted(VALID_YEAR_PERIODS) if period >= window_years
        )
        yield (
            window_start.strftime("%Y-%m-%d"),
            window_end.strftime("%Y-%m-%d"),
            str(period_years),
        )
        window_start = window_end + pd.Timedelta(days=1)


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


def _truncate_price_history_if_needed(db_ods: connector) -> bool:
    existing_data_df = db_ods.query_dataframe("SELECT 1 AS has_data FROM price_history LIMIT 1;")
    if existing_data_df.empty:
        logger.info("ods.price_history is empty. Skipping truncate.")
        return False

    db_ods.execute("TRUNCATE TABLE price_history;")
    logger.info("Truncated existing rows from ods.price_history.")
    return True


def backfill_watch_list_history(
    start_date: str = DEFAULT_START_DATE,
    end_date: str | None = None,
    need_extended_hours_data: bool = True,
    need_previous_close: bool = True,
) -> dict:
    normalized_start_date = pd.Timestamp(start_date, tz="UTC").strftime("%Y-%m-%d")
    normalized_end_date = (
        pd.Timestamp(end_date, tz="UTC").strftime("%Y-%m-%d")
        if end_date is not None
        else _default_backfill_end_date()
    )
    if normalized_start_date > normalized_end_date:
        raise ValueError("start_date must be earlier than or equal to end_date")

    db_ods = connector(schema="ods")
    db_dwd = connector(schema="dwd")
    _ensure_price_history_frequency_types(db_ods)

    symbols = _load_symbols(db_ods, db_dwd)
    if not symbols:
        logger.warning("No symbols found in dwd.watch_list or ods.instrument.")
        return {}

    api = schwab_api_market()

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

    truncated = _truncate_price_history_if_needed(db_ods)

    total_price_rows = 0
    symbols_loaded = 0
    total_symbols = len(instrument_lookup.index)

    for index, instrument_row in instrument_lookup.reset_index(drop=True).iterrows():
        symbol = instrument_row["symbol"]
        instrument_id = int(instrument_row["instrument_id"])
        logger.info(
            "Fetching %s/%s daily history for %s from %s to %s.",
            index + 1,
            total_symbols,
            symbol,
            normalized_start_date,
            normalized_end_date,
        )

        price_frames: list[pd.DataFrame] = []
        for window_start, window_end, period_years in _iter_request_windows(
            normalized_start_date, normalized_end_date
        ):
            price_df = api.fetch_price_history(
                symbol=symbol,
                period_type="year",
                period=period_years,
                frequency_type="daily",
                frequency="1",
                start_date=window_start,
                end_date=window_end,
                need_extended_hours_data=need_extended_hours_data,
                need_previous_close=need_previous_close,
            )
            if price_df.empty:
                logger.warning(
                    "No daily price history returned for symbol '%s' between %s and %s.",
                    symbol,
                    window_start,
                    window_end,
                )
                continue
            price_frames.append(price_df)

        if not price_frames:
            logger.warning("No daily price history returned for symbol '%s'.", symbol)
            continue

        price_df = (
            pd.concat(price_frames, ignore_index=True)
            .drop_duplicates(subset=["symbol", "frequency_type", "frequency", "candle_time"])
            .sort_values("candle_time")
            .reset_index(drop=True)
        )
        price_df["instrument_id"] = instrument_id
        success = db_ods.upsert_dataframe(
            price_df.drop(columns=["symbol"]),
            table_name="price_history",
            conflict_columns=["instrument_id", "frequency_type", "frequency", "candle_time"],
            update_columns=PRICE_HISTORY_UPDATE_COLUMNS,
            chunksize=1000,
        )
        if not success:
            raise RuntimeError(f"Failed to load daily price history for symbol '{symbol}'.")

        total_price_rows += len(price_df.index)
        symbols_loaded += 1

    summary = {
        "symbols_requested": len(symbols),
        "symbols_resolved": total_symbols,
        "symbols_loaded": symbols_loaded,
        "price_rows_loaded": total_price_rows,
        "start_date": normalized_start_date,
        "end_date": normalized_end_date,
        "truncated_existing_price_history": truncated,
    }
    logger.info("Daily history backfill completed: %s", summary)
    return summary


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backfill daily price history for watch-list symbols."
    )
    parser.add_argument(
        "--start-date",
        default=DEFAULT_START_DATE,
        help="Inclusive start date in YYYY-MM-DD format. Defaults to 2000-01-01.",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="Inclusive end date in YYYY-MM-DD format. Defaults to today in UTC.",
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
    return parser


if __name__ == "__main__":
    args = _build_parser().parse_args()
    print(
        backfill_watch_list_history(
            start_date=args.start_date,
            end_date=args.end_date,
            need_extended_hours_data=args.need_extended_hours_data,
            need_previous_close=args.need_previous_close,
        )
    )
