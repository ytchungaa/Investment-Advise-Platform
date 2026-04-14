import pandas as pd

from database_connect import connector
from get_market_data import schwab_api_market
from logging_config import logger


def _default_price_window() -> tuple[str, str]:
    end_date = pd.Timestamp.now(tz="UTC").normalize()
    start_date = end_date - pd.Timedelta(days=30)
    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")


def _load_watch_list(db_dwd: connector) -> list[str]:
    watch_list_df = db_dwd.query_dataframe(
        """
        SELECT symbol
        FROM watch_list
        WHERE is_active IS TRUE
        ORDER BY symbol;
        """
    )
    if watch_list_df.empty:
        return []
    return watch_list_df["symbol"].dropna().drop_duplicates().tolist()


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
        "source_payload",
    ]


def stock_list_market_data(
    start_date: str | None = None,
    end_date: str | None = None,
    period_type: str = "day",
    period: str = "10",
    frequency_type: str = "minute",
    frequency: str = "1",
    need_extended_hours_data: bool = True,
    need_previous_close: bool = True,
):
    db_ods = connector(schema="ods")
    db_dwd = connector(schema="dwd")
    symbols = _load_watch_list(db_dwd)
    if not symbols:
        logger.warning("Watch list is empty. No symbols to fetch market data for.")
        return {}

    if start_date is None or end_date is None:
        start_date, end_date = _default_price_window()

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
    for _, instrument_row in instrument_lookup.iterrows():
        symbol = instrument_row["symbol"]
        instrument_id = int(instrument_row["instrument_id"])
        price_df = api.fetch_price_history(
            symbol=symbol,
            period_type=period_type,
            period=period,
            frequency_type=frequency_type,
            frequency=frequency,
            start_date=start_date,
            end_date=end_date,
            need_extended_hours_data=need_extended_hours_data,
            need_previous_close=need_previous_close,
        )
        if price_df.empty:
            logger.warning(f"No price history returned for symbol '{symbol}'.")
            continue

        price_df["instrument_id"] = instrument_id
        price_frames.append(price_df.drop(columns=["symbol"]))

    if price_frames:
        price_history_df = pd.concat(price_frames, ignore_index=True)
        db_ods.upsert_dataframe(
            price_history_df,
            table_name="price_history",
            conflict_columns=["instrument_id", "frequency_type", "frequency", "candle_time"],
            update_columns=[
                "open",
                "high",
                "low",
                "close",
                "volume",
                "previous_close",
                "previous_close_time",
                "need_extended_hours_data",
                "source_payload",
            ],
            chunksize=500,
        )
    else:
        price_history_df = pd.DataFrame()

    summary = {
        "symbols": len(symbols),
        "instrument_rows": len(instruments_df),
        "quote_rows": len(quotes_df),
        "fundamental_rows": len(fundamentals_df),
        "price_rows": len(price_history_df),
    }
    logger.info(f"Market data update completed: {summary}")
    return summary


if __name__ == "__main__":
    print(stock_list_market_data())
