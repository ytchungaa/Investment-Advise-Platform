from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

import pandas as pd

from database_connect import connector
from get_market_data import schwab_api_market
from logging_config import logger

MINUTE_FREQUENCY_TYPE_ID = 1
MINUTE_FREQUENCY = "1"
MAX_MINUTE_WINDOW_DAYS = 10
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
DEFAULT_PRICE_HISTORY_WORKERS = 4
_THREAD_LOCAL = threading.local()


def _format_timestamp_for_request(timestamp: pd.Timestamp) -> str:
    normalized_timestamp = _normalize_timestamp_utc(timestamp)
    if normalized_timestamp is None:
        raise ValueError("timestamp cannot be null")
    return normalized_timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")


def _default_price_window() -> tuple[str, str]:
    end_timestamp = pd.Timestamp.now(tz="UTC").floor("min")
    start_timestamp = end_timestamp - pd.Timedelta(days=MAX_MINUTE_WINDOW_DAYS) + pd.Timedelta(
        minutes=1
    )
    return (
        _format_timestamp_for_request(start_timestamp),
        _format_timestamp_for_request(end_timestamp),
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
    instrument_df = db_ods.query_dataframe(
        """
        SELECT DISTINCT symbol
        FROM instrument
        ORDER BY symbol;
        """
    )

    symbol_frames = [df[["symbol"]] for df in (watch_list_df, instrument_df) if not df.empty]
    if not symbol_frames:
        return []

    return (
        pd.concat(symbol_frames, ignore_index=True)["symbol"]
        .dropna()
        .drop_duplicates()
        .sort_values()
        .tolist()
    )


def _instrument_update_columns() -> list[str]:
    return [
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



def _period_for_window_days(window_days: int) -> str:
    valid_day_periods = (1, 2, 3, 4, 5, 10)
    if window_days < 1 or window_days > MAX_MINUTE_WINDOW_DAYS:
        raise ValueError(f"window_days must be between 1 and {MAX_MINUTE_WINDOW_DAYS}")
    return str(min(period for period in valid_day_periods if period >= window_days))


def _iter_forward_request_windows(
    start_timestamp: pd.Timestamp,
    end_timestamp: pd.Timestamp,
) -> list[tuple[pd.Timestamp, pd.Timestamp, str]]:
    if start_timestamp > end_timestamp:
        return []

    windows: list[tuple[pd.Timestamp, pd.Timestamp, str]] = []
    current_start = start_timestamp
    final_end = end_timestamp

    while current_start <= final_end:
        current_end = min(
            current_start + pd.Timedelta(days=MAX_MINUTE_WINDOW_DAYS) - pd.Timedelta(minutes=1),
            final_end,
        )
        window_days = max(int((current_end - current_start).ceil("D") / pd.Timedelta(days=1)), 1)
        windows.append((current_start, current_end, _period_for_window_days(window_days)))
        current_start = current_end + pd.Timedelta(minutes=1)

    return windows


def _normalize_timestamp_utc(value) -> pd.Timestamp | None:
    if pd.isna(value):
        return None
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        return timestamp.tz_localize("UTC")
    return timestamp.tz_convert("UTC")


def _latest_minute_history_by_instrument(db_ods: connector) -> pd.DataFrame:
    latest_history_df = db_ods.query_dataframe(
        """
        SELECT instrument_id, MAX(candle_time) AS latest_candle_time
        FROM price_history
        WHERE frequency_type = :frequency_type
          AND frequency = :frequency
        GROUP BY instrument_id;
        """,
        params={
            "frequency_type": MINUTE_FREQUENCY_TYPE_ID,
            "frequency": int(MINUTE_FREQUENCY),
        },
    )
    if latest_history_df.empty:
        return pd.DataFrame(columns=["instrument_id", "latest_candle_time"])

    latest_history_df["latest_candle_time"] = latest_history_df["latest_candle_time"].apply(
        _normalize_timestamp_utc
    )
    return latest_history_df


def _filter_price_history_to_time_window(
    price_df: pd.DataFrame,
    lower_bound: pd.Timestamp,
    upper_bound: pd.Timestamp,
    include_lower_bound: bool,
) -> pd.DataFrame:
    if price_df.empty:
        return pd.DataFrame()

    lower_mask = (
        price_df["candle_time"] >= lower_bound
        if include_lower_bound
        else price_df["candle_time"] > lower_bound
    )
    filtered_df = price_df.loc[
        lower_mask & (price_df["candle_time"] <= upper_bound)
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


def _get_thread_api() -> schwab_api_market:
    api = getattr(_THREAD_LOCAL, "schwab_api_market", None)
    if api is None:
        api = schwab_api_market()
        _THREAD_LOCAL.schwab_api_market = api
    return api


def _fetch_symbol_price_history(
    symbol: str,
    instrument_id: int,
    start_timestamp: pd.Timestamp,
    end_timestamp: pd.Timestamp,
    period_type: str,
    frequency: str,
    need_extended_hours_data: bool,
    need_previous_close: bool,
) -> pd.DataFrame:
    api = _get_thread_api()
    request_windows = _iter_forward_request_windows(start_timestamp, end_timestamp)
    symbol_frames: list[pd.DataFrame] = []

    for window_index, (window_start, window_end, window_period) in enumerate(request_windows):
        window_df = api.fetch_price_history(
            symbol=symbol,
            period_type=period_type,
            period=window_period,
            frequency_type="minute",
            frequency=frequency,
            start_date=_format_timestamp_for_request(window_start),
            end_date=_format_timestamp_for_request(window_end),
            need_extended_hours_data=need_extended_hours_data,
            need_previous_close=need_previous_close,
        )
        if window_df.empty:
            continue

        lower_bound = start_timestamp if window_index == 0 else window_start
        window_df = _filter_price_history_to_time_window(
            window_df,
            lower_bound=lower_bound,
            upper_bound=end_timestamp,
            include_lower_bound=window_index != 0,
        )
        if window_df.empty:
            continue

        symbol_frames.append(window_df)

    if not symbol_frames:
        return pd.DataFrame()

    price_df = pd.concat(symbol_frames, ignore_index=True)
    price_df["instrument_id"] = instrument_id
    return price_df.drop(columns=["symbol"])


def stock_list_market_data(
    start_date: str | None = None,
    end_date: str | None = None,
    period_type: str = "day",
    period: str = "10",
    frequency_type: str = "minute",
    frequency: str = "1",
    need_extended_hours_data: bool = True,
    need_previous_close: bool = True,
    max_price_history_workers: int = DEFAULT_PRICE_HISTORY_WORKERS,
):
    db_ods = connector(schema="ods")
    db_dwd = connector(schema="dwd")
    symbols = _load_symbols(db_ods, db_dwd)
    if not symbols:
        logger.warning("No symbols found in dwd.watch_list or ods.instrument.")
        return {}

    if period_type != "day":
        raise ValueError("daily_update only supports period_type='day' for minute history updates.")
    if frequency_type != "minute" or frequency != MINUTE_FREQUENCY:
        raise ValueError("daily_update only supports 1-minute price history updates.")
    if max_price_history_workers < 1:
        raise ValueError("max_price_history_workers must be at least 1.")

    default_start_date, default_end_date = _default_price_window()
    requested_start_timestamp = _normalize_timestamp_utc(
        start_date if start_date is not None else default_start_date
    )
    requested_end_timestamp = pd.Timestamp.now(tz="UTC").floor("min")
    effective_end_date = end_date if end_date is not None else default_end_date
    explicit_end_timestamp = _normalize_timestamp_utc(effective_end_date)
    if requested_start_timestamp is None or explicit_end_timestamp is None:
        raise ValueError("start_date and end_date must be valid timestamps")
    requested_end_timestamp = min(requested_end_timestamp, explicit_end_timestamp)

    api = schwab_api_market()

    instruments_df = api.fetch_instruments(symbols)
    if instruments_df.empty:
        logger.error("Failed to fetch instruments from Schwab API.")
        return {}

    db_ods.upsert_dataframe(
        instruments_df,
        table_name="instrument",
        conflict_columns=["symbol", "asset_type"],
        update_columns=_instrument_update_columns(),
        chunksize=100,
    )

    quote_instrument_updates_df, quotes_df = api.fetch_quotes(symbols)
    if not quote_instrument_updates_df.empty:
        instrument_lookup_for_update = db_ods.query_dataframe(
            "SELECT symbol, asset_type FROM instrument;"
        )
        quote_instrument_updates_df = quote_instrument_updates_df.merge(
            instrument_lookup_for_update,
            on="symbol",
            how="left",
        ).dropna(subset=["asset_type"])
        db_ods.upsert_dataframe(
            quote_instrument_updates_df,
            table_name="instrument",
            conflict_columns=["symbol", "asset_type"],
            update_columns=_instrument_update_columns(),
            chunksize=100,
        )

    instrument_lookup = db_ods.query_dataframe(
        "SELECT id AS instrument_id, symbol, asset_type FROM instrument;"
    )
    latest_minute_history_df = _latest_minute_history_by_instrument(db_ods)
    instrument_lookup = instrument_lookup.merge(
        latest_minute_history_df,
        on="instrument_id",
        how="left",
    )

    fundamentals_df = api.fetch_instrument_fundamentals(symbols)
    if not fundamentals_df.empty:
        fundamentals_df = fundamentals_df.merge(
            instrument_lookup,
            on=["symbol", "asset_type"],
            how="left",
        ).dropna(subset=["instrument_id"])
        fundamentals_df["instrument_id"] = fundamentals_df["instrument_id"].astype("int64")
        db_ods.insert_dataframe(
            fundamentals_df.drop(columns=["symbol", "asset_type"]),
            table_name="instrument_fundamental_history",
            chunksize=100,
        )

    if not quotes_df.empty:
        quotes_df = quotes_df.merge(
            instrument_lookup[["instrument_id", "symbol"]],
            on="symbol",
            how="left",
        ).dropna(subset=["instrument_id"])
        quotes_df["instrument_id"] = quotes_df["instrument_id"].astype("int64")
        db_ods.insert_dataframe(
            quotes_df.drop(columns=["symbol"]),
            table_name="quote_history",
            chunksize=100,
        )

    price_frames: list[pd.DataFrame] = []
    symbols_already_current = 0
    symbols_with_existing_minute_history = 0
    price_history_jobs: list[dict] = []
    for instrument_row in instrument_lookup.itertuples(index=False):
        symbol = instrument_row.symbol
        instrument_id = int(instrument_row.instrument_id)
        latest_minute_history = _normalize_timestamp_utc(
            getattr(instrument_row, "latest_candle_time", None)
        )
        if latest_minute_history is not None:
            symbols_with_existing_minute_history += 1
            start_timestamp = max(requested_start_timestamp, latest_minute_history)
        else:
            start_timestamp = requested_start_timestamp

        request_windows = _iter_forward_request_windows(start_timestamp, requested_end_timestamp)
        if not request_windows:
            symbols_already_current += 1
            logger.info(
                "Minute history for %s is already current through %s. Skipping price-history fetch.",
                symbol,
                latest_minute_history,
            )
            continue

        price_history_jobs.append(
            {
                "symbol": symbol,
                "instrument_id": instrument_id,
                "start_timestamp": start_timestamp,
            }
        )

    if price_history_jobs:
        worker_count = min(max_price_history_workers, len(price_history_jobs))
        logger.info(
            "Fetching minute price history for %s symbols with %s workers.",
            len(price_history_jobs),
            worker_count,
        )
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            future_to_job = {
                executor.submit(
                    _fetch_symbol_price_history,
                    symbol=job["symbol"],
                    instrument_id=job["instrument_id"],
                    start_timestamp=job["start_timestamp"],
                    end_timestamp=requested_end_timestamp,
                    period_type=period_type,
                    frequency=MINUTE_FREQUENCY,
                    need_extended_hours_data=need_extended_hours_data,
                    need_previous_close=need_previous_close,
                ): job
                for job in price_history_jobs
            }
            for future in as_completed(future_to_job):
                job = future_to_job[future]
                symbol = job["symbol"]
                try:
                    price_df = future.result()
                except Exception as exc:
                    raise RuntimeError(
                        f"Failed to fetch 1-minute price history for symbol '{symbol}'."
                    ) from exc

                if price_df.empty:
                    logger.warning(f"No 1-minute price history returned for symbol '{symbol}'.")
                    continue
                price_frames.append(price_df)

    if price_frames:
        price_history_df = pd.concat(price_frames, ignore_index=True)
        success = db_ods.upsert_dataframe(
            price_history_df,
            table_name="price_history",
            conflict_columns=["instrument_id", "frequency_type", "frequency", "candle_time"],
            update_columns=PRICE_HISTORY_UPDATE_COLUMNS,
            chunksize=500,
        )
        if not success:
            raise RuntimeError("Failed to upsert price history during daily update.")
    else:
        price_history_df = pd.DataFrame()

    summary = {
        "symbols": len(symbols),
        "instrument_rows": len(instruments_df),
        "quote_rows": len(quotes_df),
        "fundamental_rows": len(fundamentals_df),
        "price_rows": len(price_history_df),
        "price_window_end": str(requested_end_timestamp),
        "symbols_with_existing_minute_history": symbols_with_existing_minute_history,
        "symbols_already_current": symbols_already_current,
        "price_history_workers": min(max_price_history_workers, max(len(price_history_jobs), 1)),
    }
    logger.info(f"Market data update completed: {summary}")
    return summary


if __name__ == "__main__":
    print(stock_list_market_data())
