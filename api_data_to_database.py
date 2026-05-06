from __future__ import annotations

import datetime as dt
import json
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

import pandas as pd

from database_connect import connector
from logging_config import logger
from schwab_api import SchwabApi


SECURITIES_ACCOUNT_MAPPING = {
    "accountNumber": "account_number",
    "type": "account_type",
    "roundTrips": "round_trips",
    "isDayTrader": "is_day_trader",
    "isClosingOnlyRestricted": "is_closing_only_restricted",
    "pfcbFlag": "pfcb_flag",
    "hashValue": "hash_value",
}

POSITION_MAPPING = {
    "longQuantity": "long_quantity",
    "shortQuantity": "short_quantity",
    "averagePrice": "average_price",
    "averageLongPrice": "average_long_price",
    "taxLotAverageLongPrice": "taxlot_average_long_price",
    "currentDayProfitLoss": "current_day_profit_loss",
    "currentDayProfitLossPercentage": "current_day_profit_loss_percentage",
    "longOpenProfitLoss": "long_open_profit_loss",
    "marketValue": "market_value",
    "maintenanceRequirement": "maintenance_requirement",
    "previousSessionLongQuantity": "previous_session_long_quantity",
    "currentDayCost": "current_day_cost",
}

INSTRUMENT_MAPPING = {
    "assetType": "asset_type",
    "cusip": "cusip",
    "symbol": "symbol",
    "description": "description",
    "type": "account_instrument_type",
}

BALANCE_TABLES = {
    "initialBalances": "account_initial_balances",
    "currentBalances": "account_current_balances",
    "projectedBalances": "account_projected_balances",
}

BALANCE_FIELD_MAPPING = {
    "accruedInterest": "accrued_interest",
    "cashAvailableForTrading": "cash_available_for_trading",
    "cashAvailableForWithdrawal": "cash_available_for_withdrawal",
    "cashBalance": "cash_balance",
    "bondValue": "bond_value",
    "cashReceipts": "cash_receipts",
    "liquidationValue": "liquidation_value",
    "longOptionMarketValue": "long_option_market_value",
    "longStockValue": "long_stock_value",
    "longMarketValue": "long_market_value",
    "moneyMarketFund": "money_market_fund",
    "mutualFundValue": "mutual_fund_value",
    "shortOptionMarketValue": "short_option_market_value",
    "shortStockValue": "short_stock_value",
    "shortMarketValue": "short_market_value",
    "isInCall": "is_in_call",
    "unsettledCash": "unsettled_cash",
    "cashDebitCallValue": "cash_debit_call_value",
    "pendingDeposits": "pending_deposits",
    "accountValue": "account_value",
    "savings": "savings",
    "cashCall": "cash_call",
    "longNonMarginableMarketValue": "long_non_marginable_market_value",
    "totalCash": "total_cash",
}

TRANSACTION_TYPES = [
    "TRADE",
    "RECEIVE_AND_DELIVER",
    "DIVIDEND_OR_INTEREST",
    "ACH_RECEIPT",
    "ACH_DISBURSEMENT",
    "CASH_RECEIPT",
    "CASH_DISBURSEMENT",
    "ELECTRONIC_FUND",
    "WIRE_OUT",
    "WIRE_IN",
    "JOURNAL",
    "MEMORANDUM",
    "MARGIN_CALL",
    "MONEY_MARKET",
    "SMA_ADJUSTMENT",
]

ORDER_ENUMS = {
    "session": {"NORMAL", "AM", "PM", "SEAMLESS"},
    "duration": {
        "DAY",
        "GOOD_TILL_CANCEL",
        "FILL_OR_KILL",
        "IMMEDIATE_OR_CANCEL",
        "END_OF_WEEK",
        "END_OF_MONTH",
        "NEXT_END_OF_MONTH",
        "UNKNOWN",
    },
    "order_type": {
        "MARKET",
        "LIMIT",
        "STOP",
        "STOP_LIMIT",
        "TRAILING_STOP",
        "CABINET",
        "NON_MARKETABLE",
        "MARKET_ON_CLOSE",
        "EXERCISE",
        "TRAILING_STOP_LIMIT",
        "NET_DEBIT",
        "NET_CREDIT",
        "NET_ZERO",
        "LIMIT_ON_CLOSE",
        "UNKNOWN",
    },
    "complex_strategy": {
        "NONE",
        "COVERED",
        "VERTICAL",
        "BACK_RATIO",
        "CALENDAR",
        "DIAGONAL",
        "STRADDLE",
        "STRANGLE",
        "COLLAR_SYNTHETIC",
        "BUTTERFLY",
        "CONDOR",
        "IRON_CONDOR",
        "VERTICAL_ROLL",
        "COLLAR_WITH_STOCK",
        "DOUBLE_DIAGONAL",
        "UNBALANCED_BUTTERFLY",
        "UNBALANCED_CONDOR",
        "UNBALANCED_IRON_CONDOR",
        "UNBALANCED_VERTICAL_ROLL",
        "MUTUAL_FUND_SWAP",
        "CUSTOM",
    },
    "requested_destination": {
        "INET",
        "ECN_ARCA",
        "CBOE",
        "AMEX",
        "PHLX",
        "ISE",
        "BOX",
        "NYSE",
        "NASDAQ",
        "BATS",
        "C2",
        "AUTO",
    },
    "order_strategy": {
        "SINGLE",
        "CANCEL",
        "RECALL",
        "PAIR",
        "FLATTEN",
        "TWO_DAY_SWAP",
        "BLAST_ALL",
        "OCO",
        "TRIGGER",
    },
    "status": {
        "AWAITING_PARENT_ORDER",
        "AWAITING_CONDITION",
        "AWAITING_STOP_CONDITION",
        "AWAITING_MANUAL_REVIEW",
        "ACCEPTED",
        "AWAITING_UR_OUT",
        "PENDING_ACTIVATION",
        "QUEUED",
        "WORKING",
        "REJECTED",
        "PENDING_CANCEL",
        "CANCELED",
        "PENDING_REPLACE",
        "REPLACED",
        "FILLED",
        "EXPIRED",
        "NEW",
        "AWAITING_RELEASE_TIME",
        "PENDING_ACKNOWLEDGEMENT",
        "PENDING_RECALL",
        "UNKNOWN",
    },
    "order_leg_type": {
        "EQUITY",
        "OPTION",
        "INDEX",
        "MUTUAL_FUND",
        "CASH_EQUIVALENT",
        "FIXED_INCOME",
        "CURRENCY",
        "COLLECTIVE_INVESTMENT",
    },
    "instruction": {
        "BUY",
        "SELL",
        "BUY_TO_COVER",
        "SELL_SHORT",
        "BUY_TO_OPEN",
        "BUY_TO_CLOSE",
        "SELL_TO_OPEN",
        "SELL_TO_CLOSE",
        "EXCHANGE",
        "SELL_SHORT_EXEMPT",
    },
    "position_effect": {"OPENING", "CLOSING", "AUTOMATIC"},
    "quantity_type": {"ALL_SHARES", "DOLLARS", "SHARES"},
    "div_cap_gains": {"REINVEST", "PAYOUT"},
    "activity_type": {"EXECUTION", "ORDER_ACTION"},
    "execution_type": {"FILL"},
}

SECURITIES_ACCOUNT_UPDATE_COLUMNS = [
    "hash_value",
    "account_type",
    "round_trips",
    "is_day_trader",
    "is_closing_only_restricted",
    "pfcb_flag",
    "raw_payload",
    "last_seen_at",
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

ORDER_UPDATE_COLUMNS = [
    "account_id",
    "session",
    "duration",
    "order_type",
    "complex_strategy",
    "quantity",
    "filled_quantity",
    "remaining_quantity",
    "requested_destination",
    "destination_link_name",
    "price",
    "tax_lot_method",
    "order_strategy",
    "cancelable",
    "editable",
    "status",
    "entered_time",
    "close_time",
    "tag",
    "raw_payload",
    "last_seen_at",
]

COMMON_LAST_SEEN_UPDATE_COLUMNS = ["raw_payload", "last_seen_at"]


@dataclass
class AccountIngestionFrames:
    securities_account: pd.DataFrame
    instruments: pd.DataFrame
    positions: pd.DataFrame
    balances: dict[str, pd.DataFrame]
    aggregated_balance: pd.DataFrame
    orders: pd.DataFrame
    order_legs: pd.DataFrame
    order_activities: pd.DataFrame
    execution_legs: pd.DataFrame
    transactions: pd.DataFrame
    transaction_transfer_items: pd.DataFrame


def _get_as_of_time() -> pd.Timestamp:
    return pd.Timestamp.now(tz="UTC")


def _normalize_asset_type(asset_type: str | None, instrument_type: str | None = None) -> str | None:
    if asset_type == "COLLECTIVE_INVESTMENT" and instrument_type == "EXCHANGE_TRADED_FUND":
        return "ETF"
    return asset_type


def _valid_enum(value: Any, column_name: str) -> Any:
    allowed_values = ORDER_ENUMS.get(column_name)
    if value is None or allowed_values is None or value in allowed_values:
        return value
    if "UNKNOWN" in allowed_values:
        return "UNKNOWN"
    return None


def _to_int(value: Any) -> int | None:
    if value in (None, "") or pd.isna(value):
        return None
    return int(value)


def _normalize_timestamp(value: Any) -> pd.Timestamp | None:
    if value in (None, "") or pd.isna(value):
        return None
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        return timestamp.tz_localize("UTC")
    return timestamp.tz_convert("UTC")


def _normalize_date(value: Any) -> dt.date | None:
    timestamp = _normalize_timestamp(value)
    if timestamp is None:
        return None
    return timestamp.date()


def _format_request_datetime(timestamp: pd.Timestamp) -> str:
    return timestamp.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ")


def _coerce_history_window(
    start_date: str | dt.date | dt.datetime | None,
    end_date: str | dt.date | dt.datetime | None,
) -> tuple[pd.Timestamp, pd.Timestamp]:
    end_timestamp = _normalize_timestamp(end_date) or pd.Timestamp.now(tz="UTC")
    start_timestamp = _normalize_timestamp(start_date) or (end_timestamp - pd.Timedelta(days=60))
    if start_timestamp > end_timestamp:
        raise ValueError("start_date must be before end_date")
    return start_timestamp, end_timestamp


def _iter_60_day_windows(
    start_timestamp: pd.Timestamp,
    end_timestamp: pd.Timestamp,
) -> Iterable[tuple[pd.Timestamp, pd.Timestamp]]:
    current_start = start_timestamp
    while current_start <= end_timestamp:
        current_end = min(current_start + pd.Timedelta(days=60), end_timestamp)
        yield current_start, current_end
        current_start = current_end + pd.Timedelta(seconds=1)


def _account_number_lookup(account_numbers_payload: list[dict[str, Any]]) -> dict[str, str]:
    return {
        account_info.get("hashValue"): account_info.get("accountNumber")
        for account_info in account_numbers_payload
        if account_info.get("hashValue") and account_info.get("accountNumber")
    }


def fetch_account_payloads(api: SchwabApi, fields: str | None = "positions") -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
]:
    account_numbers_payload = api.get_account_numbers()
    account_payloads: list[dict[str, Any]] = []
    for account_info in account_numbers_payload:
        account_hash = account_info.get("hashValue")
        if not account_hash:
            continue
        account_payload = api.get_account(account_hash, fields=fields)
        account_payload["_accountHash"] = account_hash
        account_payloads.append(account_payload)
    return account_numbers_payload, account_payloads


def fetch_account_history(
    api: SchwabApi,
    account_numbers_payload: list[dict[str, Any]],
    start_date: str | dt.date | dt.datetime | None = None,
    end_date: str | dt.date | dt.datetime | None = None,
    transaction_types: list[str] | None = None,
    symbol: str | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    start_timestamp, end_timestamp = _coerce_history_window(start_date, end_date)
    orders_by_id: dict[int, dict[str, Any]] = {}
    transactions_by_id: dict[str, dict[str, Any]] = {}
    transaction_type_values = transaction_types or TRANSACTION_TYPES

    for account_info in account_numbers_payload:
        account_hash = account_info.get("hashValue")
        account_number = account_info.get("accountNumber")
        if not account_hash:
            continue
        for window_start, window_end in _iter_60_day_windows(start_timestamp, end_timestamp):
            orders = api.get_orders(
                account_hash=account_hash,
                from_entered_time=_format_request_datetime(window_start),
                to_entered_time=_format_request_datetime(window_end),
            )
            for order in orders or []:
                order_id = _to_int(order.get("orderId"))
                if order_id is None:
                    continue
                order["_accountNumber"] = account_number
                orders_by_id[order_id] = order

            for transaction_type in transaction_type_values:
                transactions = api.get_transactions(
                    account_hash=account_hash,
                    start_date=_format_request_datetime(window_start),
                    end_date=_format_request_datetime(window_end),
                    types=transaction_type,
                    symbol=symbol,
                )
                for transaction in transactions or []:
                    transaction_id = _transaction_id(transaction)
                    if transaction_id is None:
                        continue
                    transaction["_accountNumber"] = transaction.get("accountNumber") or account_number
                    transactions_by_id[transaction_id] = transaction

    return list(orders_by_id.values()), list(transactions_by_id.values())


def _build_securities_account_df(
    account_payloads: list[dict[str, Any]],
    account_numbers_payload: list[dict[str, Any]],
    as_of_time: pd.Timestamp,
) -> pd.DataFrame:
    account_numbers_df = pd.json_normalize(account_numbers_payload)
    records: list[dict[str, Any]] = []
    for account_payload in account_payloads:
        securities_account = account_payload.get("securitiesAccount", {})
        if not securities_account:
            continue
        record = {
            "raw_payload": securities_account,
            "last_seen_at": as_of_time,
            "_accountHash": account_payload.get("_accountHash"),
            **securities_account,
        }
        records.append(record)

    if not records:
        return pd.DataFrame()

    df = pd.json_normalize(records, max_level=0)
    if not account_numbers_df.empty:
        df = df.merge(account_numbers_df, on="accountNumber", how="left")
    if "hashValue" not in df.columns and "_accountHash" in df.columns:
        df["hashValue"] = df["_accountHash"]
    elif "_accountHash" in df.columns:
        df["hashValue"] = df["hashValue"].fillna(df["_accountHash"])
    df = df.rename(columns=SECURITIES_ACCOUNT_MAPPING)
    df["is_day_trader"] = df.get("is_day_trader", False).fillna(False)
    df["is_closing_only_restricted"] = df.get("is_closing_only_restricted", False).fillna(False)
    df["pfcb_flag"] = df.get("pfcb_flag", False).fillna(False)
    return df[
        [
            "account_number",
            "hash_value",
            "account_type",
            "round_trips",
            "is_day_trader",
            "is_closing_only_restricted",
            "pfcb_flag",
            "raw_payload",
            "last_seen_at",
        ]
    ].drop_duplicates(subset=["account_number"])


def _build_positions_df(account_payloads: list[dict[str, Any]], as_of_time: pd.Timestamp) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for account_payload in account_payloads:
        securities_account = account_payload.get("securitiesAccount", {})
        account_number = securities_account.get("accountNumber")
        for position in securities_account.get("positions", []) or []:
            record = {
                "account_number": account_number,
                "as_of_time": as_of_time,
                "instrument": position.get("instrument", {}),
                "raw_payload": position,
            }
            record.update(position)
            records.append(record)

    if not records:
        return pd.DataFrame()

    return pd.json_normalize(records, max_level=0).rename(columns=POSITION_MAPPING)


def _iter_instrument_payloads(
    positions_df: pd.DataFrame,
    orders_payload: list[dict[str, Any]],
    transactions_payload: list[dict[str, Any]],
) -> Iterable[dict[str, Any]]:
    if not positions_df.empty:
        for instrument in positions_df["instrument"].dropna().tolist():
            if isinstance(instrument, dict):
                yield instrument

    for order in orders_payload:
        for order_leg in order.get("orderLegCollection", []) or []:
            instrument = order_leg.get("instrument")
            if isinstance(instrument, dict):
                yield instrument

    for transaction in transactions_payload:
        for transfer_item in transaction.get("transferItems", []) or []:
            instrument = transfer_item.get("instrument")
            if isinstance(instrument, dict):
                yield instrument


def _build_instrument_df(
    positions_df: pd.DataFrame,
    orders_payload: list[dict[str, Any]],
    transactions_payload: list[dict[str, Any]],
    as_of_time: pd.Timestamp,
) -> pd.DataFrame:
    records = []
    for instrument in _iter_instrument_payloads(positions_df, orders_payload, transactions_payload):
        record = {INSTRUMENT_MAPPING.get(key, key): value for key, value in instrument.items()}
        record["asset_type"] = _normalize_asset_type(
            record.get("asset_type"), record.get("account_instrument_type")
        )
        record["asset_sub_type"] = record.get("asset_sub_type") or record.get(
            "account_instrument_type"
        )
        records.append(record)

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    df = df.dropna(subset=["symbol", "asset_type"])
    if df.empty:
        return df

    for column in [
        "cusip",
        "description",
        "exchange",
        "asset_main_type",
        "asset_sub_type",
        "quote_type",
        "ssid",
        "realtime",
    ]:
        if column not in df.columns:
            df[column] = pd.NA
    df["first_seen_at"] = as_of_time
    df["last_seen_at"] = as_of_time
    return df[
        [
            "symbol",
            "asset_type",
            "cusip",
            "description",
            "exchange",
            "asset_main_type",
            "asset_sub_type",
            "quote_type",
            "ssid",
            "realtime",
            "first_seen_at",
            "last_seen_at",
        ]
    ].drop_duplicates(subset=["symbol", "asset_type"])


def _build_balance_df(
    account_payloads: list[dict[str, Any]],
    block_name: str,
    as_of_time: pd.Timestamp,
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for account_payload in account_payloads:
        securities_account = account_payload.get("securitiesAccount", {})
        balance_payload = securities_account.get(block_name)
        if not balance_payload:
            continue
        record = {
            BALANCE_FIELD_MAPPING.get(key, key): value
            for key, value in balance_payload.items()
        }
        record["account_number"] = securities_account.get("accountNumber")
        record["as_of_time"] = as_of_time
        record["raw_payload"] = balance_payload
        records.append(record)
    return pd.DataFrame(records)


def _build_aggregated_balance_df(
    account_payloads: list[dict[str, Any]], as_of_time: pd.Timestamp
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for account_payload in account_payloads:
        securities_account = account_payload.get("securitiesAccount", {})
        aggregated_balance = account_payload.get("aggregatedBalance")
        if not aggregated_balance:
            continue
        records.append(
            {
                "account_number": securities_account.get("accountNumber"),
                "as_of_time": as_of_time,
                "current_liquidation_value": aggregated_balance.get(
                    "currentLiquidationValue"
                ),
                "liquidation_value": aggregated_balance.get("liquidationValue"),
                "raw_payload": aggregated_balance,
            }
        )
    return pd.DataFrame(records)


def _instrument_identity(instrument: dict[str, Any] | None) -> dict[str, Any]:
    instrument = instrument or {}
    asset_type = _normalize_asset_type(instrument.get("assetType"), instrument.get("type"))
    return {
        "symbol": instrument.get("symbol"),
        "asset_type": asset_type,
        "cusip": instrument.get("cusip"),
        "schwab_instrument_id": _to_int(instrument.get("instrumentId")),
    }


def _build_orders_df(orders_payload: list[dict[str, Any]], as_of_time: pd.Timestamp) -> pd.DataFrame:
    records = []
    for order in orders_payload:
        order_id = _to_int(order.get("orderId"))
        if order_id is None:
            continue
        records.append(
            {
                "order_id": order_id,
                "account_number": order.get("_accountNumber"),
                "session": _valid_enum(order.get("session"), "session"),
                "duration": _valid_enum(order.get("duration"), "duration"),
                "order_type": _valid_enum(order.get("orderType"), "order_type"),
                "complex_strategy": _valid_enum(
                    order.get("complexOrderStrategyType"), "complex_strategy"
                ),
                "quantity": order.get("quantity"),
                "filled_quantity": order.get("filledQuantity"),
                "remaining_quantity": order.get("remainingQuantity"),
                "requested_destination": _valid_enum(
                    order.get("requestedDestination"), "requested_destination"
                ),
                "destination_link_name": order.get("destinationLinkName"),
                "price": order.get("price"),
                "tax_lot_method": order.get("taxLotMethod"),
                "order_strategy": _valid_enum(order.get("orderStrategyType"), "order_strategy"),
                "cancelable": order.get("cancelable"),
                "editable": order.get("editable"),
                "status": _valid_enum(order.get("status"), "status"),
                "entered_time": _normalize_timestamp(order.get("enteredTime")),
                "close_time": _normalize_timestamp(order.get("closeTime")),
                "tag": order.get("tag"),
                "raw_payload": order,
                "last_seen_at": as_of_time,
            }
        )
    return pd.DataFrame(records)


def _build_order_legs_df(
    orders_payload: list[dict[str, Any]], as_of_time: pd.Timestamp
) -> pd.DataFrame:
    records = []
    for order in orders_payload:
        order_id = _to_int(order.get("orderId"))
        if order_id is None:
            continue
        for order_leg in order.get("orderLegCollection", []) or []:
            leg_id = _to_int(order_leg.get("legId"))
            if leg_id is None:
                continue
            instrument_identity = _instrument_identity(order_leg.get("instrument"))
            records.append(
                {
                    "order_id": order_id,
                    "leg_id": leg_id,
                    "order_leg_type": _valid_enum(
                        order_leg.get("orderLegType"), "order_leg_type"
                    ),
                    "instruction": _valid_enum(order_leg.get("instruction"), "instruction"),
                    "position_effect": _valid_enum(
                        order_leg.get("positionEffect"), "position_effect"
                    ),
                    "quantity": order_leg.get("quantity"),
                    "quantity_type": _valid_enum(
                        order_leg.get("quantityType"), "quantity_type"
                    ),
                    "div_cap_gains": _valid_enum(order_leg.get("divCapGains"), "div_cap_gains"),
                    "to_symbol": order_leg.get("toSymbol"),
                    "raw_payload": order_leg,
                    "last_seen_at": as_of_time,
                    **instrument_identity,
                }
            )
    return pd.DataFrame(records)


def _build_order_activities_df(
    orders_payload: list[dict[str, Any]], as_of_time: pd.Timestamp
) -> pd.DataFrame:
    records = []
    for order in orders_payload:
        order_id = _to_int(order.get("orderId"))
        if order_id is None:
            continue
        for activity in order.get("orderActivityCollection", []) or []:
            activity_id = _to_int(activity.get("activityId"))
            if activity_id is None:
                continue
            records.append(
                {
                    "order_id": order_id,
                    "activity_id": activity_id,
                    "activity_type": _valid_enum(activity.get("activityType"), "activity_type"),
                    "execution_type": _valid_enum(
                        activity.get("executionType"), "execution_type"
                    ),
                    "quantity": activity.get("quantity"),
                    "order_remaining_quantity": activity.get("orderRemainingQuantity"),
                    "raw_payload": activity,
                    "last_seen_at": as_of_time,
                }
            )
    return pd.DataFrame(records)


def _build_execution_legs_df(
    orders_payload: list[dict[str, Any]], as_of_time: pd.Timestamp
) -> pd.DataFrame:
    records = []
    for order in orders_payload:
        for activity in order.get("orderActivityCollection", []) or []:
            activity_id = _to_int(activity.get("activityId"))
            if activity_id is None:
                continue
            for execution_leg in activity.get("executionLegs", []) or []:
                leg_id = _to_int(execution_leg.get("legId"))
                if leg_id is None:
                    continue
                records.append(
                    {
                        "activity_id": activity_id,
                        "leg_id": leg_id,
                        "price": execution_leg.get("price"),
                        "quantity": execution_leg.get("quantity"),
                        "mismarked_quantity": execution_leg.get("mismarkedQuantity"),
                        "schwab_instrument_id": _to_int(
                            execution_leg.get("instrumentId")
                        ),
                        "exec_time": _normalize_timestamp(execution_leg.get("time")),
                        "raw_payload": execution_leg,
                        "last_seen_at": as_of_time,
                    }
                )
    return pd.DataFrame(records)


def _transaction_id(transaction: dict[str, Any]) -> str | None:
    value = (
        transaction.get("activityId")
        or transaction.get("transactionId")
        or transaction.get("id")
    )
    if value in (None, ""):
        return None
    return str(value)


def _build_transactions_df(
    transactions_payload: list[dict[str, Any]],
    as_of_time: pd.Timestamp,
) -> pd.DataFrame:
    records = []
    for transaction in transactions_payload:
        transaction_id = _transaction_id(transaction)
        if transaction_id is None:
            continue
        records.append(
            {
                "transaction_id": transaction_id,
                "activity_id": str(transaction.get("activityId"))
                if transaction.get("activityId") is not None
                else None,
                "account_number": transaction.get("_accountNumber")
                or transaction.get("accountNumber"),
                "transaction_time": _normalize_timestamp(transaction.get("time")),
                "transaction_type": transaction.get("type"),
                "status": transaction.get("status"),
                "sub_account": transaction.get("subAccount"),
                "trade_date": _normalize_date(transaction.get("tradeDate")),
                "settlement_date": _normalize_date(transaction.get("settlementDate")),
                "position_id": str(transaction.get("positionId"))
                if transaction.get("positionId") is not None
                else None,
                "order_id": _to_int(transaction.get("orderId")),
                "net_amount": transaction.get("netAmount"),
                "description": transaction.get("description"),
                "raw_payload": transaction,
                "last_seen_at": as_of_time,
            }
        )
    return pd.DataFrame(records)


def _build_transaction_transfer_items_df(
    transactions_payload: list[dict[str, Any]],
    as_of_time: pd.Timestamp,
) -> pd.DataFrame:
    records = []
    for transaction in transactions_payload:
        transaction_id = _transaction_id(transaction)
        if transaction_id is None:
            continue
        for sequence, transfer_item in enumerate(
            transaction.get("transferItems", []) or [], start=1
        ):
            instrument = transfer_item.get("instrument") or {}
            instrument_identity = _instrument_identity(instrument)
            records.append(
                {
                    "transaction_id": transaction_id,
                    "item_sequence": sequence,
                    "symbol": instrument_identity["symbol"] or transfer_item.get("symbol"),
                    "cusip": instrument_identity["cusip"] or transfer_item.get("cusip"),
                    "asset_type": instrument_identity["asset_type"]
                    or transfer_item.get("assetType"),
                    "quantity": transfer_item.get("quantity"),
                    "price": transfer_item.get("price"),
                    "cost": transfer_item.get("cost"),
                    "amount": transfer_item.get("amount"),
                    "fee_type": transfer_item.get("feeType"),
                    "position_effect": transfer_item.get("positionEffect"),
                    "instruction": transfer_item.get("instruction"),
                    "raw_payload": transfer_item,
                    "last_seen_at": as_of_time,
                }
            )
    return pd.DataFrame(records)


def build_account_ingestion_frames(
    account_payloads: list[dict[str, Any]],
    account_numbers_payload: list[dict[str, Any]],
    orders_payload: list[dict[str, Any]] | None = None,
    transactions_payload: list[dict[str, Any]] | None = None,
    as_of_time: pd.Timestamp | None = None,
) -> AccountIngestionFrames:
    if as_of_time is None:
        as_of_time = _get_as_of_time()
    orders_payload = orders_payload or []
    transactions_payload = transactions_payload or []
    positions_df = _build_positions_df(account_payloads, as_of_time)
    return AccountIngestionFrames(
        securities_account=_build_securities_account_df(
            account_payloads, account_numbers_payload, as_of_time
        ),
        instruments=_build_instrument_df(
            positions_df, orders_payload, transactions_payload, as_of_time
        ),
        positions=positions_df,
        balances={
            table_name: _build_balance_df(account_payloads, block_name, as_of_time)
            for block_name, table_name in BALANCE_TABLES.items()
        },
        aggregated_balance=_build_aggregated_balance_df(account_payloads, as_of_time),
        orders=_build_orders_df(orders_payload, as_of_time),
        order_legs=_build_order_legs_df(orders_payload, as_of_time),
        order_activities=_build_order_activities_df(orders_payload, as_of_time),
        execution_legs=_build_execution_legs_df(orders_payload, as_of_time),
        transactions=_build_transactions_df(transactions_payload, as_of_time),
        transaction_transfer_items=_build_transaction_transfer_items_df(
            transactions_payload, as_of_time
        ),
    )


def _merge_account_id(df: pd.DataFrame, account_lookup: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    merged_df = df.merge(account_lookup, on="account_number", how="left")
    return merged_df.dropna(subset=["account_id"]).assign(
        account_id=lambda frame: frame["account_id"].astype("int64")
    )


def _merge_instrument_id(df: pd.DataFrame, instrument_lookup: pd.DataFrame) -> pd.DataFrame:
    if df.empty or not {"symbol", "asset_type"}.issubset(df.columns):
        return df
    merged_df = df.merge(
        instrument_lookup[["instrument_id", "symbol", "asset_type"]],
        on=["symbol", "asset_type"],
        how="left",
    )
    if "instrument_id" in merged_df.columns:
        merged_df["instrument_id"] = pd.to_numeric(
            merged_df["instrument_id"], errors="coerce"
        )
    return merged_df


def _start_load_audit(
    ods_db: connector,
    dataset_name: str,
    request_params: dict[str, Any],
) -> int | None:
    try:
        cursor = ods_db.execute(
            """
            INSERT INTO load_audit (dataset_name, request_params)
            VALUES (:dataset_name, CAST(:request_params AS jsonb))
            RETURNING id;
            """,
            {"dataset_name": dataset_name, "request_params": json.dumps(request_params)},
        )
        row = cursor.fetchone()
        return row[0] if row else None
    except Exception as exc:
        logger.warning("Unable to start load audit for %s: %s", dataset_name, exc)
        return None


def _finish_load_audit(
    ods_db: connector,
    audit_id: int | None,
    status: str,
    notes: str | None = None,
) -> None:
    if audit_id is None:
        return
    try:
        ods_db.execute(
            """
            UPDATE load_audit
            SET completed_at = now(),
                status = :status,
                notes = :notes
            WHERE id = :audit_id;
            """,
            {"audit_id": audit_id, "status": status, "notes": notes},
        )
    except Exception as exc:
        logger.warning("Unable to finish load audit %s: %s", audit_id, exc)


def _upsert_if_not_empty(
    ods_db: connector,
    df: pd.DataFrame,
    table_name: str,
    conflict_columns: list[str],
    update_columns: list[str] | None = None,
    chunksize: int | None = None,
) -> bool:
    if df.empty:
        return True
    return ods_db.upsert_dataframe(
        df,
        table_name=table_name,
        conflict_columns=conflict_columns,
        update_columns=update_columns,
        chunksize=chunksize,
    )


def _insert_if_not_empty(
    ods_db: connector,
    df: pd.DataFrame,
    table_name: str,
    chunksize: int | None = None,
) -> None:
    if df.empty:
        return
    ods_db.insert_dataframe(df, table_name, if_exists="append", chunksize=chunksize)


def get_account_data(
    api: SchwabApi | None = None,
    start_date: str | dt.date | dt.datetime | None = None,
    end_date: str | dt.date | dt.datetime | None = None,
    transaction_types: list[str] | None = None,
    fetch_history: bool = True,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], AccountIngestionFrames]:
    api = api or SchwabApi()
    as_of_time = _get_as_of_time()
    account_numbers_payload, account_payloads = fetch_account_payloads(api)
    if fetch_history:
        orders_payload, transactions_payload = fetch_account_history(
            api,
            account_numbers_payload,
            start_date=start_date,
            end_date=end_date,
            transaction_types=transaction_types,
        )
    else:
        orders_payload, transactions_payload = [], []
    frames = build_account_ingestion_frames(
        account_payloads,
        account_numbers_payload,
        orders_payload=orders_payload,
        transactions_payload=transactions_payload,
        as_of_time=as_of_time,
    )
    return account_numbers_payload, account_payloads, frames


def save_to_database(
    start_date: str | dt.date | dt.datetime | None = None,
    end_date: str | dt.date | dt.datetime | None = None,
    transaction_types: list[str] | None = None,
    fetch_history: bool = True,
) -> dict[str, int]:
    ods_db = connector(schema="ods")
    audit_id = _start_load_audit(
        ods_db,
        dataset_name="schwab_account",
        request_params={
            "start_date": str(start_date) if start_date is not None else None,
            "end_date": str(end_date) if end_date is not None else None,
            "transaction_types": transaction_types,
            "fetch_history": fetch_history,
        },
    )
    try:
        _account_numbers_payload, _account_payloads, frames = get_account_data(
            start_date=start_date,
            end_date=end_date,
            transaction_types=transaction_types,
            fetch_history=fetch_history,
        )

        _upsert_if_not_empty(
            ods_db,
            frames.securities_account,
            table_name="securities_account",
            conflict_columns=["account_number"],
            update_columns=SECURITIES_ACCOUNT_UPDATE_COLUMNS,
            chunksize=100,
        )

        if not frames.instruments.empty:
            _upsert_if_not_empty(
                ods_db,
                frames.instruments,
                table_name="instrument",
                conflict_columns=["symbol", "asset_type"],
                update_columns=INSTRUMENT_UPDATE_COLUMNS,
                chunksize=100,
            )

        account_lookup = ods_db.query_dataframe(
            "SELECT id AS account_id, account_number FROM securities_account;"
        )
        instrument_lookup = ods_db.query_dataframe(
            "SELECT id AS instrument_id, symbol, asset_type FROM instrument;"
        )

        positions_df = _merge_account_id(frames.positions, account_lookup)
        if not positions_df.empty:
            raw_instruments = pd.json_normalize(positions_df["instrument"].tolist()).rename(
                columns=INSTRUMENT_MAPPING
            )
            raw_instruments["asset_type"] = raw_instruments.apply(
                lambda row: _normalize_asset_type(
                    row.get("asset_type"), row.get("account_instrument_type")
                ),
                axis=1,
            )
            positions_df["symbol"] = raw_instruments["symbol"]
            positions_df["asset_type"] = raw_instruments["asset_type"]
            positions_df = _merge_instrument_id(positions_df, instrument_lookup)
            positions_df = positions_df.dropna(subset=["instrument_id"])
            positions_df["instrument_id"] = positions_df["instrument_id"].astype("int64")
            _insert_if_not_empty(
                ods_db,
                positions_df.drop(
                    columns=[
                        column
                        for column in ["account_number", "instrument", "symbol", "asset_type"]
                        if column in positions_df.columns
                    ]
                ),
                "position",
                chunksize=100,
            )

        for table_name, balance_df in frames.balances.items():
            balance_df = _merge_account_id(balance_df, account_lookup)
            _insert_if_not_empty(
                ods_db,
                balance_df.drop(columns=["account_number"], errors="ignore"),
                table_name,
                chunksize=100,
            )

        aggregated_balance_df = _merge_account_id(frames.aggregated_balance, account_lookup)
        _insert_if_not_empty(
            ods_db,
            aggregated_balance_df.drop(columns=["account_number"], errors="ignore"),
            "aggregated_balance",
            chunksize=100,
        )

        orders_df = _merge_account_id(frames.orders, account_lookup)
        _upsert_if_not_empty(
            ods_db,
            orders_df.drop(columns=["account_number"], errors="ignore"),
            table_name="orders",
            conflict_columns=["order_id"],
            update_columns=ORDER_UPDATE_COLUMNS,
            chunksize=100,
        )

        order_legs_df = _merge_instrument_id(frames.order_legs, instrument_lookup)
        _upsert_if_not_empty(
            ods_db,
            order_legs_df.drop(columns=["symbol", "asset_type", "cusip"], errors="ignore"),
            table_name="order_leg",
            conflict_columns=["order_id", "leg_id"],
            update_columns=[
                "order_leg_type",
                "instrument_id",
                "schwab_instrument_id",
                "instruction",
                "position_effect",
                "quantity",
                "quantity_type",
                "div_cap_gains",
                "to_symbol",
                *COMMON_LAST_SEEN_UPDATE_COLUMNS,
            ],
            chunksize=100,
        )

        _upsert_if_not_empty(
            ods_db,
            frames.order_activities,
            table_name="order_activity",
            conflict_columns=["activity_id"],
            update_columns=[
                "order_id",
                "activity_type",
                "execution_type",
                "quantity",
                "order_remaining_quantity",
                *COMMON_LAST_SEEN_UPDATE_COLUMNS,
            ],
            chunksize=100,
        )

        _upsert_if_not_empty(
            ods_db,
            frames.execution_legs,
            table_name="execution_leg",
            conflict_columns=["activity_id", "leg_id"],
            update_columns=[
                "price",
                "quantity",
                "mismarked_quantity",
                "schwab_instrument_id",
                "exec_time",
                *COMMON_LAST_SEEN_UPDATE_COLUMNS,
            ],
            chunksize=100,
        )

        transactions_df = _merge_account_id(frames.transactions, account_lookup)
        _upsert_if_not_empty(
            ods_db,
            transactions_df.drop(columns=["account_number"], errors="ignore"),
            table_name="account_transaction",
            conflict_columns=["transaction_id"],
            update_columns=[
                "account_id",
                "activity_id",
                "transaction_time",
                "transaction_type",
                "status",
                "sub_account",
                "trade_date",
                "settlement_date",
                "position_id",
                "order_id",
                "net_amount",
                "description",
                *COMMON_LAST_SEEN_UPDATE_COLUMNS,
            ],
            chunksize=100,
        )

        transaction_transfer_items_df = _merge_instrument_id(
            frames.transaction_transfer_items, instrument_lookup
        )
        _upsert_if_not_empty(
            ods_db,
            transaction_transfer_items_df,
            table_name="transaction_transfer_item",
            conflict_columns=["transaction_id", "item_sequence"],
            update_columns=[
                "instrument_id",
                "symbol",
                "cusip",
                "asset_type",
                "quantity",
                "price",
                "cost",
                "amount",
                "fee_type",
                "position_effect",
                "instruction",
                *COMMON_LAST_SEEN_UPDATE_COLUMNS,
            ],
            chunksize=100,
        )

        summary = {
            "accounts": len(frames.securities_account),
            "positions": len(frames.positions),
            "balance_rows": sum(len(df) for df in frames.balances.values()),
            "orders": len(frames.orders),
            "transactions": len(frames.transactions),
            "transaction_transfer_items": len(frames.transaction_transfer_items),
        }
        _finish_load_audit(ods_db, audit_id, "completed", notes=str(summary))
        logger.info("Account data saved successfully: %s", summary)
        return summary
    except Exception as exc:
        _finish_load_audit(ods_db, audit_id, "failed", notes=str(exc))
        raise


if __name__ == "__main__":
    print(save_to_database())
