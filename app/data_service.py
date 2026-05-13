from __future__ import annotations

from calendar import monthrange
import math
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


DEFAULT_INTERVAL = "hourly"
DWD_BUCKET_TIMEZONE = "America/New_York"
INTERVAL_TABLES = {
    "hourly": "dwd.price_history_hourly",
    "daily": "dwd.price_history_daily",
}
SYMBOL_SUGGESTION_LIMIT = 8
DWD_REFRESH_SQL_FILES = (
    ROOT_DIR / "database/update_tables/dwd.price_history_hourly_incremental_upsert.sql",
    ROOT_DIR / "database/update_tables/dwd.price_history_daily_incremental_upsert.sql",
)
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
PORTFOLIO_BENCHMARK_SYMBOL = "SPY"
PORTFOLIO_LOOKBACK_DAYS = 365
PORTFOLIO_CATEGORIES = ("Stocks", "ETF", "Bonds", "Cash")
PORTFOLIO_SCHEMA_SQL_FILES = (
    ROOT_DIR / "database/create_tables/ods.instrument.sql",
    ROOT_DIR / "database/create_tables/ods.account_data.sql",
    ROOT_DIR / "database/create_tables/ods.orders_data.sql",
    ROOT_DIR / "database/create_tables/dwd.account_data.sql",
)


class StockNotFoundError(ValueError):
    """Raised when the requested symbol is not available."""


@dataclass(frozen=True)
class InstrumentLookup:
    instrument_id: int
    symbol: str
    description: str | None
    asset_type: str | None
    exchange: str | None
    asset_main_type: str | None
    asset_sub_type: str | None
    quote_type: str | None
    realtime: bool | None
    last_seen_at: Any | None
    available_start: date | None
    available_end: date | None


def _normalize_symbol(symbol: str) -> str:
    return (symbol or "").strip().upper()


def _normalize_interval(interval: str | None) -> str:
    normalized_interval = (interval or DEFAULT_INTERVAL).strip().lower()
    if normalized_interval not in INTERVAL_TABLES:
        allowed_intervals = ", ".join(sorted(INTERVAL_TABLES))
        raise ValueError(f"interval must be one of: {allowed_intervals}.")
    return normalized_interval


def _price_table_for_interval(interval: str) -> str:
    return INTERVAL_TABLES[interval]


def _utc_today() -> date:
    return datetime.now(timezone.utc).date()


def _utc_now_minute() -> pd.Timestamp:
    return pd.Timestamp.now(tz="UTC").floor("min")


def _subtract_one_calendar_month(value: date) -> date:
    target_year = value.year if value.month > 1 else value.year - 1
    target_month = value.month - 1 if value.month > 1 else 12
    target_day = min(value.day, monthrange(target_year, target_month)[1])
    return date(target_year, target_month, target_day)


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
    default_start = _subtract_one_calendar_month(default_end)

    selected_start = requested_start or default_start
    selected_end = requested_end or default_end

    if available_start:
        selected_start = max(selected_start, available_start)
    if available_end:
        selected_end = min(selected_end, available_end)

    if selected_start > selected_end:
        return selected_end, selected_end

    return selected_start, selected_end


def _get_instrument_lookup(
    db: connector,
    normalized_symbol: str,
    interval: str,
) -> InstrumentLookup:
    price_table = _price_table_for_interval(interval)
    instrument_df = db.query_dataframe(
        f"""
        SELECT
            i.id AS instrument_id,
            i.symbol,
            i.description,
            i.asset_type,
            i.exchange,
            i.asset_main_type,
            i.asset_sub_type,
            i.quote_type,
            i.realtime,
            i.last_seen_at,
            MIN(ph.bucket_start AT TIME ZONE :bucket_timezone)::date AS available_start,
            MAX(ph.bucket_start AT TIME ZONE :bucket_timezone)::date AS available_end
        FROM ods.instrument AS i
        LEFT JOIN {price_table} AS ph
            ON ph.instrument_id = i.id
           AND ph.source_candle_count > 0
        WHERE UPPER(i.symbol) = :symbol
        GROUP BY
            i.id,
            i.symbol,
            i.description,
            i.asset_type,
            i.exchange,
            i.asset_main_type,
            i.asset_sub_type,
            i.quote_type,
            i.realtime,
            i.last_seen_at
        ORDER BY MAX(ph.bucket_start) DESC NULLS LAST, i.id ASC
        LIMIT 1;
        """,
        params={
            "symbol": normalized_symbol,
            "bucket_timezone": DWD_BUCKET_TIMEZONE,
        },
    )

    if instrument_df.empty:
        raise StockNotFoundError(f"Symbol '{normalized_symbol}' was not found.")

    row = instrument_df.iloc[0]
    return InstrumentLookup(
        instrument_id=int(row["instrument_id"]),
        symbol=str(row["symbol"]),
        description=row["description"] if pd.notna(row["description"]) else None,
        asset_type=row.get("asset_type") if pd.notna(row.get("asset_type")) else None,
        exchange=row.get("exchange") if pd.notna(row.get("exchange")) else None,
        asset_main_type=row.get("asset_main_type")
        if pd.notna(row.get("asset_main_type"))
        else None,
        asset_sub_type=row.get("asset_sub_type") if pd.notna(row.get("asset_sub_type")) else None,
        quote_type=row.get("quote_type") if pd.notna(row.get("quote_type")) else None,
        realtime=bool(row.get("realtime")) if pd.notna(row.get("realtime")) else None,
        last_seen_at=row.get("last_seen_at") if pd.notna(row.get("last_seen_at")) else None,
        available_start=row["available_start"] if pd.notna(row["available_start"]) else None,
        available_end=row["available_end"] if pd.notna(row["available_end"]) else None,
    )


def search_symbols(
    query: str,
    limit: int = SYMBOL_SUGGESTION_LIMIT,
    interval: str | None = None,
) -> list[dict[str, Any]]:
    normalized_query = _normalize_symbol(query)
    if not normalized_query:
        return []

    normalized_interval = _normalize_interval(interval)
    price_table = _price_table_for_interval(normalized_interval)
    db = connector(schema="dwd")
    suggestions_df = db.query_dataframe(
        f"""
        SELECT
            i.symbol,
            i.description
        FROM ods.instrument AS i
        INNER JOIN {price_table} AS ph
            ON ph.instrument_id = i.id
           AND ph.source_candle_count > 0
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
    interval: str,
) -> pd.DataFrame:
    end_exclusive = selected_end + timedelta(days=1)
    price_table = _price_table_for_interval(interval)
    price_df = db.query_dataframe(
        f"""
        SELECT
            bucket_start AS time_bucket,
            open,
            high,
            low,
            close,
            volume
        FROM {price_table}
        WHERE instrument_id = :instrument_id
          AND source_candle_count > 0
          AND bucket_start >= (CAST(:selected_start AS DATE)::timestamp AT TIME ZONE :bucket_timezone)
          AND bucket_start < (CAST(:selected_end_exclusive AS DATE)::timestamp AT TIME ZONE :bucket_timezone)
        ORDER BY bucket_start ASC;
        """,
        params={
            "instrument_id": instrument_id,
            "selected_start": selected_start,
            "selected_end_exclusive": end_exclusive,
            "bucket_timezone": DWD_BUCKET_TIMEZONE,
        },
    )

    if price_df.empty:
        return price_df

    price_df["time_bucket"] = pd.to_datetime(price_df["time_bucket"])
    return price_df


def _format_time_bucket(value: Any, interval: str) -> str:
    timestamp = pd.Timestamp(value)
    if interval == "daily":
        return timestamp.date().isoformat()
    return timestamp.isoformat()


def _build_timeseries(price_df: pd.DataFrame, interval: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for row in price_df.itertuples(index=False):
        records.append(
            {
                "date": _format_time_bucket(row.time_bucket, interval),
                "close": float(row.close) if pd.notna(row.close) else None,
                "open": float(row.open) if pd.notna(row.open) else None,
                "high": float(row.high) if pd.notna(row.high) else None,
                "low": float(row.low) if pd.notna(row.low) else None,
                "volume": int(row.volume) if pd.notna(row.volume) else None,
            }
        )
    return records


def _to_json_value(value: Any) -> Any:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, float) and math.isnan(value):
        return None
    if hasattr(value, "item"):
        return value.item()
    return value


def _dataframe_first_record(df: pd.DataFrame) -> dict[str, Any] | None:
    if df.empty:
        return None
    row = df.iloc[0]
    return {column: _to_json_value(row[column]) for column in df.columns}


def _close_connector(db: connector) -> None:
    try:
        db.connection.close()
    except Exception:
        pass
    try:
        db.engine.dispose()
    except Exception:
        pass


def _relation_exists(db: connector, relation_name: str) -> bool:
    relation_df = db.query_dataframe(
        "SELECT to_regclass(:relation_name) IS NOT NULL AS relation_exists;",
        params={"relation_name": relation_name},
    )
    if relation_df.empty:
        return False
    return bool(relation_df.iloc[0]["relation_exists"])


def _relation_count(
    db: connector,
    relation_name: str,
    timestamp_column: str | None = None,
) -> dict[str, Any]:
    if not _relation_exists(db, relation_name):
        return {
            "exists": False,
            "row_count": 0,
            "min_time": None,
            "max_time": None,
        }

    if timestamp_column:
        count_df = db.query_dataframe(
            f"""
            SELECT
                COUNT(*) AS row_count,
                MIN({timestamp_column}) AS min_time,
                MAX({timestamp_column}) AS max_time
            FROM {relation_name};
            """
        )
    else:
        count_df = db.query_dataframe(f"SELECT COUNT(*) AS row_count FROM {relation_name};")

    if count_df.empty:
        return {
            "exists": True,
            "row_count": 0,
            "min_time": None,
            "max_time": None,
        }

    row = count_df.iloc[0]
    return {
        "exists": True,
        "row_count": int(row["row_count"]) if pd.notna(row["row_count"]) else 0,
        "min_time": _to_json_value(row.get("min_time")) if "min_time" in row else None,
        "max_time": _to_json_value(row.get("max_time")) if "max_time" in row else None,
    }


def ensure_portfolio_schema() -> list[str]:
    executed_files: list[str] = []
    for sql_file in PORTFOLIO_SCHEMA_SQL_FILES:
        _execute_sql_file(sql_file)
        executed_files.append(sql_file.name)
    return executed_files


def get_portfolio_storage_status() -> dict[str, Any]:
    db = connector(schema="dwd")
    try:
        relations = {
            "ods_position": _relation_count(db, "ods.position", "as_of_time"),
            "ods_account_current_balances": _relation_count(
                db,
                "ods.account_current_balances",
                "as_of_time",
            ),
            "ods_orders": _relation_count(db, "ods.orders", "entered_time"),
            "ods_account_transaction": _relation_count(
                db,
                "ods.account_transaction",
                "transaction_time",
            ),
            "dwd_fact_position_latest": _relation_count(
                db,
                "dwd.fact_position_latest",
                "as_of_time",
            ),
            "dwd_fact_portfolio_balance_latest": _relation_count(
                db,
                "dwd.fact_portfolio_balance_latest",
                "as_of_time",
            ),
        }
        return {
            "relations": relations,
            "has_portfolio_snapshots": (
                relations["ods_position"]["row_count"] > 0
                and relations["ods_account_current_balances"]["row_count"] > 0
            ),
            "has_order_history": relations["ods_orders"]["row_count"] > 0,
            "has_transaction_history": relations["ods_account_transaction"]["row_count"] > 0,
        }
    finally:
        _close_connector(db)


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return float(value)


def _portfolio_category(asset_type: Any) -> str:
    normalized_asset_type = str(asset_type or "").strip().upper()
    if normalized_asset_type in {"ETF", "COLLECTIVE_INVESTMENT"}:
        return "ETF"
    if normalized_asset_type in {"FIXED_INCOME", "BOND"}:
        return "Bonds"
    if normalized_asset_type in {"CASH", "CASH_EQUIVALENT", "MONEY_MARKET"}:
        return "Cash"
    return "Stocks"


def _portfolio_weight_value(value: Any) -> float:
    numeric_value = _to_float(value)
    if numeric_value is None or numeric_value <= 0:
        return 0.0
    return numeric_value


def _holding_total_cost(row: Any) -> float | None:
    net_quantity = _to_float(getattr(row, "net_quantity", None))
    average_price = first_available_number(
        getattr(row, "average_price", None),
        getattr(row, "average_long_price", None),
        getattr(row, "taxlot_average_long_price", None),
    )
    if net_quantity is not None and average_price is not None:
        total_cost = abs(net_quantity) * average_price
        if total_cost > 0:
            return total_cost

    current_day_cost = _to_float(getattr(row, "current_day_cost", None))
    if current_day_cost is not None and current_day_cost > 0:
        return current_day_cost
    return None


def _portfolio_cost_value(row: Any) -> float:
    total_cost = _holding_total_cost(row)
    return total_cost if total_cost is not None and total_cost > 0 else 0.0


def _load_portfolio_holdings(db: connector) -> pd.DataFrame:
    holdings_df = db.query_dataframe(
        """
        SELECT
            as_of_time,
            account_id,
            masked_account_number,
            account_type,
            instrument_id,
            symbol,
            description,
            asset_type,
            long_quantity,
            short_quantity,
            net_quantity,
            average_price,
            average_long_price,
            taxlot_average_long_price,
            market_value,
            current_day_cost,
            current_day_profit_loss,
            current_day_profit_loss_percentage
        FROM dwd.fact_position_latest
        ORDER BY market_value DESC NULLS LAST, symbol ASC;
        """
    )
    return holdings_df


def _load_portfolio_balance(db: connector) -> dict[str, Any]:
    balance_df = db.query_dataframe(
        """
        SELECT
            as_of_time,
            account_count,
            total_liquidation_value,
            total_cash_balance,
            total_cash,
            total_long_market_value,
            total_short_market_value,
            total_money_market_fund,
            total_mutual_fund_value,
            total_bond_value,
            total_cash_available_for_trading,
            total_cash_available_for_withdrawal
        FROM dwd.fact_portfolio_balance_latest
        LIMIT 1;
        """
    )
    return _dataframe_first_record(balance_df) or {}


def _load_portfolio_price_history(
    db: connector,
    symbols: list[str],
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    if not symbols:
        return pd.DataFrame()
    normalized_symbols = sorted({_normalize_symbol(symbol) for symbol in symbols if symbol})
    if not normalized_symbols:
        return pd.DataFrame()

    return db.query_dataframe(
        """
        SELECT
            UPPER(i.symbol) AS symbol,
            (ph.bucket_start AT TIME ZONE :bucket_timezone)::date AS price_date,
            ph.close
        FROM dwd.price_history_daily ph
        JOIN ods.instrument i
            ON i.id = ph.instrument_id
        WHERE UPPER(i.symbol) = ANY(:symbols)
          AND ph.source_candle_count > 0
          AND ph.bucket_start >= (CAST(:start_date AS DATE)::timestamp AT TIME ZONE :bucket_timezone)
          AND ph.bucket_start < ((CAST(:end_date AS DATE) + INTERVAL '1 day')::timestamp AT TIME ZONE :bucket_timezone)
        ORDER BY i.symbol, ph.bucket_start ASC;
        """,
        params={
            "symbols": normalized_symbols,
            "start_date": start_date,
            "end_date": end_date,
            "bucket_timezone": DWD_BUCKET_TIMEZONE,
        },
    )


def _load_portfolio_balance_history(db: connector) -> pd.DataFrame:
    if not _relation_exists(db, "dwd.fact_portfolio_balance_history"):
        return pd.DataFrame()
    return db.query_dataframe(
        """
        SELECT
            as_of_time::date AS history_date,
            as_of_time,
            account_count,
            total_liquidation_value,
            total_cash,
            total_long_market_value,
            total_short_market_value
        FROM dwd.fact_portfolio_balance_history
        ORDER BY as_of_time ASC;
        """
    )


def _load_realized_gain_loss_events(db: connector) -> pd.DataFrame:
    if not _relation_exists(db, "ods.account_transaction") or not _relation_exists(
        db,
        "ods.transaction_transfer_item",
    ):
        return pd.DataFrame()
    return db.query_dataframe(
        """
        WITH closing_transactions AS (
            SELECT DISTINCT
                at.transaction_id,
                at.transaction_time,
                at.net_amount,
                COALESCE(NULLIF(tti.symbol, ''), 'UNKNOWN') AS symbol
            FROM ods.account_transaction at
            JOIN ods.transaction_transfer_item tti
                ON tti.transaction_id = at.transaction_id
            WHERE at.transaction_type = 'TRADE'
              AND tti.position_effect = 'CLOSING'
              AND COALESCE(tti.asset_type, '') <> 'CURRENCY'
              AND at.net_amount IS NOT NULL
        )
        SELECT
            transaction_time::date AS event_date,
            symbol,
            SUM(net_amount) AS realized_amount,
            COUNT(*) AS transaction_count
        FROM closing_transactions
        GROUP BY transaction_time::date, symbol
        ORDER BY event_date ASC, symbol ASC;
        """
    )


def _holding_price_return(price_history_df: pd.DataFrame, symbol: str) -> float | None:
    if price_history_df.empty:
        return None
    symbol_prices = price_history_df.loc[
        price_history_df["symbol"].str.upper() == symbol.upper()
    ].copy()
    symbol_prices = symbol_prices.dropna(subset=["close"]).sort_values("price_date")
    if len(symbol_prices) < 2:
        return None
    first_close = float(symbol_prices.iloc[0]["close"])
    last_close = float(symbol_prices.iloc[-1]["close"])
    if first_close <= 0:
        return None
    return (last_close / first_close) - 1


def _daily_returns(price_history_df: pd.DataFrame, symbol: str) -> pd.Series:
    if price_history_df.empty:
        return pd.Series(dtype="float64")
    symbol_prices = price_history_df.loc[
        price_history_df["symbol"].str.upper() == symbol.upper(),
        ["price_date", "close"],
    ].copy()
    if symbol_prices.empty:
        return pd.Series(dtype="float64")
    symbol_prices = symbol_prices.dropna(subset=["close"]).sort_values("price_date")
    symbol_prices["price_date"] = pd.to_datetime(symbol_prices["price_date"]).dt.date
    returns = symbol_prices.set_index("price_date")["close"].astype("float64").pct_change()
    return returns.dropna()


def _holding_beta(
    price_history_df: pd.DataFrame,
    symbol: str,
    benchmark_returns: pd.Series,
) -> float | None:
    holding_returns = _daily_returns(price_history_df, symbol)
    if holding_returns.empty or benchmark_returns.empty:
        return None
    aligned_returns = pd.concat(
        [holding_returns.rename("holding"), benchmark_returns.rename("benchmark")],
        axis=1,
        join="inner",
    ).dropna()
    if len(aligned_returns) < 2:
        return None
    benchmark_variance = aligned_returns["benchmark"].var()
    if benchmark_variance is None or benchmark_variance == 0 or pd.isna(benchmark_variance):
        return None
    covariance = aligned_returns["holding"].cov(aligned_returns["benchmark"])
    if pd.isna(covariance):
        return None
    return float(covariance / benchmark_variance)


def _build_allocation(
    holdings_df: pd.DataFrame,
    balance: dict[str, Any],
) -> tuple[list[dict[str, Any]], float]:
    category_values = {category: 0.0 for category in PORTFOLIO_CATEGORIES}
    if not holdings_df.empty:
        for row in holdings_df.itertuples(index=False):
            category = _portfolio_category(getattr(row, "asset_type", None))
            category_values[category] += _portfolio_weight_value(
                getattr(row, "market_value", None)
            )

    cash_value = first_available_number(
        balance.get("total_cash"),
        balance.get("total_cash_balance"),
        balance.get("total_cash_available_for_trading"),
    )
    if cash_value is not None and cash_value > 0:
        category_values["Cash"] += cash_value

    total_value = sum(category_values.values())
    allocation = []
    for category in PORTFOLIO_CATEGORIES:
        market_value = category_values[category]
        allocation.append(
            {
                "category": category,
                "market_value": market_value,
                "weight": market_value / total_value if total_value > 0 else None,
            }
        )
    return allocation, total_value


def first_available_number(*values: Any) -> float | None:
    for value in values:
        numeric_value = _to_float(value)
        if numeric_value is not None:
            return numeric_value
    return None


def _build_holdings_payload(
    holdings_df: pd.DataFrame,
    price_history_df: pd.DataFrame,
    total_cost_value: float,
) -> list[dict[str, Any]]:
    holdings: list[dict[str, Any]] = []
    if holdings_df.empty:
        return holdings

    for row in holdings_df.itertuples(index=False):
        market_value = _to_float(getattr(row, "market_value", None))
        total_cost = _holding_total_cost(row)
        symbol = str(getattr(row, "symbol", "") or "").upper()
        category = _portfolio_category(getattr(row, "asset_type", None))
        holdings.append(
            {
                "symbol": symbol,
                "description": _to_json_value(getattr(row, "description", None)),
                "category": category,
                "asset_type": _to_json_value(getattr(row, "asset_type", None)),
                "quantity": _to_json_value(getattr(row, "net_quantity", None)),
                "average_price": _to_json_value(getattr(row, "average_price", None)),
                "total_cost": _to_json_value(total_cost),
                "market_value": _to_json_value(market_value),
                "weight": (total_cost / total_cost_value)
                if total_cost is not None and total_cost_value > 0
                else None,
                "day_profit_loss": _to_json_value(
                    getattr(row, "current_day_profit_loss", None)
                ),
                "day_profit_loss_percentage": _to_json_value(
                    getattr(row, "current_day_profit_loss_percentage", None)
                ),
                "return_1y": _to_json_value(_holding_price_return(price_history_df, symbol)),
                "as_of_time": _to_json_value(getattr(row, "as_of_time", None)),
            }
        )
    return sorted(
        holdings,
        key=lambda holding: holding["market_value"] or 0,
        reverse=True,
    )


def _build_portfolio_risk_metrics(
    holdings: list[dict[str, Any]],
    price_history_df: pd.DataFrame,
) -> tuple[dict[str, Any], list[str]]:
    warnings: list[str] = []
    benchmark_return = _holding_price_return(price_history_df, PORTFOLIO_BENCHMARK_SYMBOL)
    benchmark_returns = _daily_returns(price_history_df, PORTFOLIO_BENCHMARK_SYMBOL)

    weighted_return_numerator = 0.0
    weighted_beta_numerator = 0.0
    return_weight_total = 0.0
    beta_weight_total = 0.0

    for holding in holdings:
        symbol = holding["symbol"]
        market_value = _to_float(holding["market_value"]) or 0.0
        if market_value <= 0 or holding["category"] == "Cash":
            continue
        holding_return = _to_float(holding.get("return_1y"))
        if holding_return is not None:
            weighted_return_numerator += holding_return * market_value
            return_weight_total += market_value
        holding_beta = _holding_beta(price_history_df, symbol, benchmark_returns)
        if holding_beta is not None:
            weighted_beta_numerator += holding_beta * market_value
            beta_weight_total += market_value

    portfolio_return = (
        weighted_return_numerator / return_weight_total if return_weight_total > 0 else None
    )
    beta = weighted_beta_numerator / beta_weight_total if beta_weight_total > 0 else None
    alpha = (
        portfolio_return - benchmark_return
        if portfolio_return is not None and benchmark_return is not None
        else None
    )

    if benchmark_return is None or benchmark_returns.empty:
        warnings.append(f"{PORTFOLIO_BENCHMARK_SYMBOL} price history is unavailable.")
    if portfolio_return is None:
        warnings.append("Holding price history is insufficient for 1-year return.")
    if beta is None:
        warnings.append("Holding and benchmark daily returns are insufficient for beta.")

    return (
        {
            "benchmark_symbol": PORTFOLIO_BENCHMARK_SYMBOL,
            "portfolio_return_1y": _to_json_value(portfolio_return),
            "benchmark_return_1y": _to_json_value(benchmark_return),
            "alpha_1y": _to_json_value(alpha),
            "beta_1y": _to_json_value(beta),
            "priced_holding_weight": return_weight_total,
            "beta_holding_weight": beta_weight_total,
        },
        warnings,
    )


def _build_portfolio_history_payload(
    balance_history_df: pd.DataFrame,
    realized_events_df: pd.DataFrame,
) -> dict[str, Any]:
    balance_series: list[dict[str, Any]] = []
    if not balance_history_df.empty:
        for row in balance_history_df.itertuples(index=False):
            balance_series.append(
                {
                    "date": _to_json_value(getattr(row, "history_date", None)),
                    "as_of_time": _to_json_value(getattr(row, "as_of_time", None)),
                    "total_value": _to_json_value(
                        getattr(row, "total_liquidation_value", None)
                    ),
                    "cash": _to_json_value(getattr(row, "total_cash", None)),
                    "long_market_value": _to_json_value(
                        getattr(row, "total_long_market_value", None)
                    ),
                    "short_market_value": _to_json_value(
                        getattr(row, "total_short_market_value", None)
                    ),
                    "account_count": _to_json_value(getattr(row, "account_count", None)),
                }
            )

    realized_events: list[dict[str, Any]] = []
    if not realized_events_df.empty:
        for row in realized_events_df.itertuples(index=False):
            realized_amount = _to_float(getattr(row, "realized_amount", None))
            realized_events.append(
                {
                    "date": _to_json_value(getattr(row, "event_date", None)),
                    "symbol": _to_json_value(getattr(row, "symbol", None)),
                    "realized_amount": _to_json_value(realized_amount),
                    "transaction_count": _to_json_value(
                        getattr(row, "transaction_count", None)
                    ),
                    "direction": "gain"
                    if realized_amount is not None and realized_amount >= 0
                    else "loss",
                }
            )

    return {
        "balance_series": balance_series,
        "realized_events": realized_events,
    }


def get_portfolio_data() -> dict[str, Any]:
    db = connector(schema="dwd")
    try:
        holdings_df = _load_portfolio_holdings(db)
        balance = _load_portfolio_balance(db)
        end_date = _utc_today()
        start_date = end_date - timedelta(days=PORTFOLIO_LOOKBACK_DAYS)

        symbols = [PORTFOLIO_BENCHMARK_SYMBOL]
        if not holdings_df.empty:
            symbols.extend(
                str(symbol)
                for symbol in holdings_df["symbol"].dropna().drop_duplicates().tolist()
            )
        price_history_df = _load_portfolio_price_history(db, symbols, start_date, end_date)
        balance_history_df = _load_portfolio_balance_history(db)
        realized_events_df = _load_realized_gain_loss_events(db)

        allocation, total_weight_value = _build_allocation(holdings_df, balance)
        total_cost_value = (
            sum(_portfolio_cost_value(row) for row in holdings_df.itertuples(index=False))
            if not holdings_df.empty
            else 0.0
        )
        holdings = _build_holdings_payload(holdings_df, price_history_df, total_cost_value)
        risk_metrics, warnings = _build_portfolio_risk_metrics(holdings, price_history_df)
        history = _build_portfolio_history_payload(balance_history_df, realized_events_df)

        total_portfolio_value = first_available_number(
            balance.get("total_liquidation_value"),
            total_weight_value,
        )
        total_cash = first_available_number(
            balance.get("total_cash"),
            balance.get("total_cash_balance"),
            balance.get("total_cash_available_for_trading"),
        )

        if not holdings:
            warnings.append("No latest portfolio holdings are available in DWD.")

        return {
            "as_of_time": _to_json_value(balance.get("as_of_time")),
            "benchmark_symbol": PORTFOLIO_BENCHMARK_SYMBOL,
            "summary": {
                "total_portfolio_value": _to_json_value(total_portfolio_value),
                "total_cash": _to_json_value(total_cash),
                "account_count": _to_json_value(balance.get("account_count")),
                **risk_metrics,
            },
            "balance": {key: _to_json_value(value) for key, value in balance.items()},
            "allocation": allocation,
            "holdings": holdings,
            "history": history,
            "warnings": warnings,
        }
    finally:
        _close_connector(db)


def refresh_portfolio_data(force_refresh: bool = False) -> dict[str, Any]:
    schema_files = ensure_portfolio_schema()
    before_status = get_portfolio_storage_status()
    should_fetch = force_refresh or not before_status["has_portfolio_snapshots"]
    summary: dict[str, Any] | None = None

    if should_fetch:
        from api_data_to_database import save_to_database

        end_date = _utc_today()
        start_date = end_date - timedelta(days=PORTFOLIO_LOOKBACK_DAYS)
        summary = save_to_database(
            start_date=start_date,
            end_date=end_date,
            transaction_types=None,
            fetch_history=True,
        )
        schema_files = ensure_portfolio_schema()

    after_status = get_portfolio_storage_status()
    return {
        "refreshed": should_fetch,
        "schema_files": schema_files,
        "before": before_status,
        "after": after_status,
        "summary": summary,
    }


def _load_company_info(db: connector, instrument: InstrumentLookup) -> dict[str, Any]:
    quote_df = db.query_dataframe(
        """
        SELECT
            as_of_time,
            last_price,
            mark,
            mark_change,
            mark_percent_change,
            net_change,
            net_percent_change,
            open_price,
            high_price,
            low_price,
            close_price,
            bid_price,
            ask_price,
            total_volume,
            security_status,
            quote_time,
            trade_time
        FROM ods.quote_history
        WHERE instrument_id = :instrument_id
        ORDER BY as_of_time DESC
        LIMIT 1;
        """,
        params={"instrument_id": instrument.instrument_id},
    )
    fundamentals_df = db.query_dataframe(
        """
        SELECT
            as_of_time,
            market_cap,
            market_cap_float,
            shares_outstanding,
            pe_ratio,
            peg_ratio,
            pb_ratio,
            pr_ratio,
            pcf_ratio,
            eps,
            eps_ttm,
            eps_change_percent_ttm,
            rev_change_ttm,
            beta,
            dividend_amount,
            dividend_yield,
            dividend_pay_amount,
            dividend_pay_date,
            next_dividend_date,
            next_dividend_pay_date,
            week_52_high,
            week_52_low,
            gross_margin_ttm,
            net_profit_margin_ttm,
            operating_margin_ttm,
            return_on_equity,
            return_on_assets,
            return_on_investment,
            current_ratio,
            quick_ratio,
            total_debt_to_equity,
            total_debt_to_capital,
            book_value_per_share,
            avg_10_days_volume,
            avg_3_month_volume
        FROM ods.instrument_fundamental_history
        WHERE instrument_id = :instrument_id
        ORDER BY as_of_time DESC
        LIMIT 1;
        """,
        params={"instrument_id": instrument.instrument_id},
    )

    return {
        "profile": {
            "asset_type": _to_json_value(instrument.asset_type),
            "exchange": _to_json_value(instrument.exchange),
            "asset_main_type": _to_json_value(instrument.asset_main_type),
            "asset_sub_type": _to_json_value(instrument.asset_sub_type),
            "quote_type": _to_json_value(instrument.quote_type),
            "realtime": _to_json_value(instrument.realtime),
            "last_seen_at": _to_json_value(instrument.last_seen_at),
        },
        "quote": _dataframe_first_record(quote_df),
        "fundamentals": _dataframe_first_record(fundamentals_df),
    }


def _format_timestamp_for_market_data(value: pd.Timestamp) -> str:
    return value.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ")


def _execute_sql_file(path: Path) -> None:
    db = connector(schema="dwd")
    raw_connection = db.engine.raw_connection()
    try:
        with raw_connection.cursor() as cursor:
            cursor.execute(path.read_text())
        raw_connection.commit()
    except Exception:
        raw_connection.rollback()
        raise
    finally:
        raw_connection.close()
        _close_connector(db)


def _refresh_dwd_price_tables() -> list[str]:
    refreshed_files: list[str] = []
    for sql_file in DWD_REFRESH_SQL_FILES:
        _execute_sql_file(sql_file)
        refreshed_files.append(sql_file.name)
    return refreshed_files


def refresh_stock_data(symbol: str) -> dict[str, Any]:
    normalized_symbol = _normalize_symbol(symbol)
    if not normalized_symbol:
        raise ValueError("symbol is required.")

    from get_market_data import schwab_api_market

    db_ods = connector(schema="ods")
    api = schwab_api_market()

    instruments_df = api.fetch_instruments(normalized_symbol)
    if instruments_df.empty:
        raise StockNotFoundError(f"Symbol '{normalized_symbol}' was not found.")

    if not db_ods.upsert_dataframe(
        instruments_df,
        table_name="instrument",
        conflict_columns=["symbol", "asset_type"],
        update_columns=INSTRUMENT_UPDATE_COLUMNS,
        chunksize=100,
    ):
        raise RuntimeError("Failed to upsert instrument data.")

    instrument_lookup = db_ods.query_dataframe(
        """
        SELECT id AS instrument_id, symbol, asset_type
        FROM instrument
        WHERE UPPER(symbol) = :symbol
        ORDER BY id ASC
        LIMIT 1;
        """,
        params={"symbol": normalized_symbol},
    )
    if instrument_lookup.empty:
        raise StockNotFoundError(f"Symbol '{normalized_symbol}' was not found.")

    instrument_row = instrument_lookup.iloc[0]
    instrument_id = int(instrument_row["instrument_id"])
    resolved_symbol = str(instrument_row["symbol"])

    quote_instrument_updates_df, quotes_df = api.fetch_quotes(resolved_symbol)
    instrument_asset_lookup = db_ods.query_dataframe(
        """
        SELECT symbol, asset_type
        FROM instrument
        WHERE UPPER(symbol) = :symbol;
        """,
        params={"symbol": normalized_symbol},
    )

    if not quote_instrument_updates_df.empty and not instrument_asset_lookup.empty:
        quote_instrument_updates_df = quote_instrument_updates_df.merge(
            instrument_asset_lookup,
            on="symbol",
            how="left",
        ).dropna(subset=["asset_type"])
        if not quote_instrument_updates_df.empty:
            db_ods.upsert_dataframe(
                quote_instrument_updates_df,
                table_name="instrument",
                conflict_columns=["symbol", "asset_type"],
                update_columns=INSTRUMENT_UPDATE_COLUMNS,
                chunksize=100,
            )

    if not quotes_df.empty:
        quotes_df = quotes_df.merge(
            instrument_lookup[["instrument_id", "symbol"]],
            on="symbol",
            how="left",
        ).dropna(subset=["instrument_id"])
        if not quotes_df.empty:
            quotes_df["instrument_id"] = quotes_df["instrument_id"].astype("int64")
            db_ods.insert_dataframe(
                quotes_df.drop(columns=["symbol"]),
                table_name="quote_history",
                chunksize=100,
            )

    fundamentals_df = api.fetch_instrument_fundamentals(resolved_symbol)
    if not fundamentals_df.empty:
        fundamentals_df = fundamentals_df.merge(
            instrument_lookup[["instrument_id", "symbol", "asset_type"]],
            on=["symbol", "asset_type"],
            how="left",
        ).dropna(subset=["instrument_id"])
        if not fundamentals_df.empty:
            fundamentals_df["instrument_id"] = fundamentals_df["instrument_id"].astype("int64")
            db_ods.insert_dataframe(
                fundamentals_df.drop(columns=["symbol", "asset_type"]),
                table_name="instrument_fundamental_history",
                chunksize=100,
            )

    end_timestamp = _utc_now_minute()
    start_timestamp = end_timestamp - pd.Timedelta(days=10) + pd.Timedelta(minutes=1)
    latest_history_df = db_ods.query_dataframe(
        """
        SELECT MAX(candle_time) AS latest_candle_time
        FROM price_history
        WHERE instrument_id = :instrument_id
          AND frequency_type = 1
          AND frequency = 1;
        """,
        params={"instrument_id": instrument_id},
    )
    if not latest_history_df.empty and pd.notna(latest_history_df.iloc[0]["latest_candle_time"]):
        latest_timestamp = pd.Timestamp(latest_history_df.iloc[0]["latest_candle_time"])
        if latest_timestamp.tzinfo is None:
            latest_timestamp = latest_timestamp.tz_localize("UTC")
        else:
            latest_timestamp = latest_timestamp.tz_convert("UTC")
        start_timestamp = max(start_timestamp, latest_timestamp + pd.Timedelta(minutes=1))

    price_rows = 0
    if start_timestamp <= end_timestamp:
        price_df = api.fetch_price_history(
            symbol=resolved_symbol,
            period_type="day",
            period="10",
            frequency_type="minute",
            frequency="1",
            start_date=_format_timestamp_for_market_data(start_timestamp),
            end_date=_format_timestamp_for_market_data(end_timestamp),
            need_extended_hours_data=True,
            need_previous_close=True,
        )
        if not price_df.empty:
            price_df["instrument_id"] = instrument_id
            price_df = price_df.drop(columns=["symbol"])
            price_rows = len(price_df)
            if not db_ods.upsert_dataframe(
                price_df,
                table_name="price_history",
                conflict_columns=["instrument_id", "frequency_type", "frequency", "candle_time"],
                update_columns=PRICE_HISTORY_UPDATE_COLUMNS,
                chunksize=500,
            ):
                raise RuntimeError("Failed to upsert price history data.")

    refreshed_files = _refresh_dwd_price_tables()
    return {
        "symbol": resolved_symbol,
        "instrument_rows": len(instruments_df),
        "quote_rows": len(quotes_df),
        "fundamental_rows": len(fundamentals_df),
        "price_rows": price_rows,
        "dwd_refresh_files": refreshed_files,
        "refreshed_at": _format_timestamp_for_market_data(end_timestamp),
    }


def get_stock_visualization_data(
    symbol: str,
    start_date: str | None = None,
    end_date: str | None = None,
    interval: str | None = None,
) -> dict[str, Any]:
    normalized_symbol = _normalize_symbol(symbol)
    if not normalized_symbol:
        raise ValueError("symbol is required.")

    normalized_interval = _normalize_interval(interval)
    requested_start = _parse_optional_date(start_date, "start_date")
    requested_end = _parse_optional_date(end_date, "end_date")

    db = connector(schema="dwd")
    instrument = _get_instrument_lookup(db, normalized_symbol, normalized_interval)
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
        interval=normalized_interval,
    )

    timeseries = _build_timeseries(price_df, normalized_interval)
    company_info = _load_company_info(db, instrument)

    message: str | None = None
    if price_df.empty:
        message = f"No {normalized_interval} price history is available for the selected range."

    return {
        "symbol": instrument.symbol,
        "description": instrument.description,
        "interval": normalized_interval,
        "available_range": {
            "start": _date_to_str(instrument.available_start),
            "end": _date_to_str(instrument.available_end),
        },
        "selected_range": {
            "start": _date_to_str(selected_start),
            "end": _date_to_str(selected_end),
        },
        "company_info": company_info,
        "timeseries": timeseries,
        "summary": {
            "price_points": len(timeseries),
        },
        "message": message,
    }
