from __future__ import annotations

import calendar
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from database_connect import connector


DAILY_FREQUENCY_TYPE = 2
DAILY_FREQUENCY = 1
SYMBOL_SUGGESTION_LIMIT = 8


class StockNotFoundError(ValueError):
    """Raised when the requested symbol is not available."""


@dataclass(frozen=True)
class InstrumentLookup:
    instrument_id: int
    symbol: str
    description: str | None
    available_start: date | None
    available_end: date | None


def _normalize_symbol(symbol: str) -> str:
    return (symbol or "").strip().upper()


def _utc_today() -> date:
    return datetime.now(timezone.utc).date()


def _parse_optional_date(value: str | None, field_name: str) -> date | None:
    if value in (None, ""):
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"{field_name} must use YYYY-MM-DD format.") from exc


def _date_to_str(value: date | None) -> str | None:
    return value.isoformat() if value else None


def _resolve_selected_range(
    requested_start: date | None,
    requested_end: date | None,
    available_start: date | None,
    available_end: date | None,
) -> tuple[date, date]:
    if requested_start and requested_end and requested_start > requested_end:
        raise ValueError("start_date must be on or before end_date.")

    if requested_start and requested_end:
        return requested_start, requested_end

    default_end = _utc_today()
    default_start = default_end - timedelta(days=365)

    selected_start = requested_start or default_start
    selected_end = requested_end or default_end

    if available_start:
        selected_start = max(selected_start, available_start)
    if available_end:
        selected_end = min(selected_end, available_end)

    if selected_start > selected_end:
        return selected_end, selected_end

    return selected_start, selected_end


def _get_instrument_lookup(db: connector, normalized_symbol: str) -> InstrumentLookup:
    instrument_df = db.query_dataframe(
        """
        SELECT
            i.id AS instrument_id,
            i.symbol,
            i.description,
            MIN(ph.candle_time)::date AS available_start,
            MAX(ph.candle_time)::date AS available_end
        FROM instrument AS i
        LEFT JOIN price_history AS ph
            ON ph.instrument_id = i.id
           AND ph.frequency_type = :frequency_type
           AND ph.frequency = :frequency
        WHERE UPPER(i.symbol) = :symbol
        GROUP BY i.id, i.symbol, i.description
        ORDER BY MAX(ph.candle_time) DESC NULLS LAST, i.id ASC
        LIMIT 1;
        """,
        params={
            "symbol": normalized_symbol,
            "frequency_type": DAILY_FREQUENCY_TYPE,
            "frequency": DAILY_FREQUENCY,
        },
    )

    if instrument_df.empty:
        raise StockNotFoundError(f"Symbol '{normalized_symbol}' was not found.")

    row = instrument_df.iloc[0]
    return InstrumentLookup(
        instrument_id=int(row["instrument_id"]),
        symbol=str(row["symbol"]),
        description=row["description"] if pd.notna(row["description"]) else None,
        available_start=row["available_start"] if pd.notna(row["available_start"]) else None,
        available_end=row["available_end"] if pd.notna(row["available_end"]) else None,
    )


def search_symbols(query: str, limit: int = SYMBOL_SUGGESTION_LIMIT) -> list[dict[str, Any]]:
    normalized_query = _normalize_symbol(query)
    if not normalized_query:
        return []

    db = connector(schema="ods")
    suggestions_df = db.query_dataframe(
        """
        SELECT
            i.symbol,
            i.description
        FROM instrument AS i
        INNER JOIN price_history AS ph
            ON ph.instrument_id = i.id
           AND ph.frequency_type = :frequency_type
           AND ph.frequency = :frequency
        WHERE UPPER(i.symbol) LIKE :symbol_contains
           OR UPPER(COALESCE(i.description, '')) LIKE :description_contains
        GROUP BY i.id, i.symbol, i.description
        ORDER BY
            CASE
                WHEN UPPER(i.symbol) = :exact_symbol THEN 0
                WHEN UPPER(i.symbol) LIKE :symbol_prefix THEN 1
                WHEN UPPER(i.symbol) LIKE :symbol_contains THEN 2
                ELSE 3
            END,
            i.symbol ASC
        LIMIT :limit;
        """,
        params={
            "frequency_type": DAILY_FREQUENCY_TYPE,
            "frequency": DAILY_FREQUENCY,
            "exact_symbol": normalized_query,
            "symbol_prefix": f"{normalized_query}%",
            "symbol_contains": f"%{normalized_query}%",
            "description_contains": f"%{normalized_query}%",
            "limit": limit,
        },
    )

    if suggestions_df.empty:
        return []

    suggestions: list[dict[str, Any]] = []
    for row in suggestions_df.itertuples(index=False):
        suggestions.append(
            {
                "symbol": str(row.symbol),
                "description": row.description if pd.notna(row.description) else None,
            }
        )
    return suggestions


def _load_price_history(
    db: connector,
    instrument_id: int,
    selected_start: date,
    selected_end: date,
) -> pd.DataFrame:
    end_exclusive = selected_end + timedelta(days=1)
    price_df = db.query_dataframe(
        """
        SELECT
            candle_time::date AS trade_date,
            open,
            high,
            low,
            close,
            volume
        FROM price_history
        WHERE instrument_id = :instrument_id
          AND frequency_type = :frequency_type
          AND frequency = :frequency
          AND candle_time >= :selected_start
          AND candle_time < :selected_end_exclusive
        ORDER BY candle_time ASC;
        """,
        params={
            "instrument_id": instrument_id,
            "frequency_type": DAILY_FREQUENCY_TYPE,
            "frequency": DAILY_FREQUENCY,
            "selected_start": selected_start,
            "selected_end_exclusive": end_exclusive,
        },
    )

    if price_df.empty:
        return price_df

    price_df["trade_date"] = pd.to_datetime(price_df["trade_date"]).dt.date
    return price_df


def _build_timeseries(price_df: pd.DataFrame) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for row in price_df.itertuples(index=False):
        records.append(
            {
                "date": row.trade_date.isoformat(),
                "close": float(row.close) if pd.notna(row.close) else None,
                "open": float(row.open) if pd.notna(row.open) else None,
                "high": float(row.high) if pd.notna(row.high) else None,
                "low": float(row.low) if pd.notna(row.low) else None,
                "volume": int(row.volume) if pd.notna(row.volume) else None,
            }
        )
    return records


def _build_monthly_return_boxes(price_df: pd.DataFrame) -> list[dict[str, Any]]:
    if len(price_df) < 2:
        return []

    working_df = price_df.copy()
    working_df["daily_return"] = working_df["close"].div(working_df["close"].shift(1)) - 1
    working_df = working_df.dropna(subset=["daily_return"])
    if working_df.empty:
        return []

    working_df["month_number"] = pd.to_datetime(working_df["trade_date"]).dt.month

    boxes: list[dict[str, Any]] = []
    for month_number in range(1, 13):
        month_returns = working_df.loc[
            working_df["month_number"] == month_number, "daily_return"
        ].tolist()
        if not month_returns:
            continue
        boxes.append(
            {
                "month_number": month_number,
                "month_label": calendar.month_abbr[month_number],
                "returns": [float(value) for value in month_returns],
            }
        )
    return boxes


def get_stock_visualization_data(
    symbol: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, Any]:
    normalized_symbol = _normalize_symbol(symbol)
    if not normalized_symbol:
        raise ValueError("symbol is required.")

    requested_start = _parse_optional_date(start_date, "start_date")
    requested_end = _parse_optional_date(end_date, "end_date")

    db = connector(schema="ods")
    instrument = _get_instrument_lookup(db, normalized_symbol)
    selected_start, selected_end = _resolve_selected_range(
        requested_start,
        requested_end,
        instrument.available_start,
        instrument.available_end,
    )

    price_df = _load_price_history(
        db=db,
        instrument_id=instrument.instrument_id,
        selected_start=selected_start,
        selected_end=selected_end,
    )

    timeseries = _build_timeseries(price_df)
    monthly_return_boxes = _build_monthly_return_boxes(price_df)

    message: str | None = None
    if price_df.empty:
        message = "No daily price history is available for the selected range."
    elif len(price_df) < 2:
        message = (
            "Return distribution is unavailable because fewer than two price points "
            "were found in the selected range."
        )

    return {
        "symbol": instrument.symbol,
        "description": instrument.description,
        "available_range": {
            "start": _date_to_str(instrument.available_start),
            "end": _date_to_str(instrument.available_end),
        },
        "selected_range": {
            "start": _date_to_str(selected_start),
            "end": _date_to_str(selected_end),
        },
        "timeseries": timeseries,
        "monthly_return_boxes": monthly_return_boxes,
        "summary": {
            "price_points": len(timeseries),
            "return_points": sum(len(box["returns"]) for box in monthly_return_boxes),
        },
        "message": message,
    }
