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
    "accountNumber": "account_number",
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


def _normalize_asset_type(asset_type: str | None, instrument_type: str | None = None) -> str | None:
    if asset_type == "COLLECTIVE_INVESTMENT" and instrument_type == "EXCHANGE_TRADED_FUND":
        return "ETF"
    return asset_type


def _get_as_of_time() -> pd.Timestamp:
    return pd.Timestamp.now(tz="UTC")


def _build_securities_account_df(
    account_payload: dict, account_numbers_payload: dict
) -> pd.DataFrame:
    df = pd.json_normalize(account_payload["securitiesAccount"])[
        [
            "accountNumber",
            "type",
            "roundTrips",
            "isDayTrader",
            "isClosingOnlyRestricted",
            "pfcbFlag",
        ]
    ]
    account_numbers_df = pd.json_normalize(account_numbers_payload)
    df = df.merge(account_numbers_df, on="accountNumber", how="left")
    return df.rename(columns=SECURITIES_ACCOUNT_MAPPING)


def _build_instrument_df(positions_df: pd.DataFrame, as_of_time: pd.Timestamp) -> pd.DataFrame:
    if positions_df.empty:
        return pd.DataFrame()

    raw_instruments = positions_df["instrument"].tolist()
    instrument_df = pd.json_normalize(raw_instruments).rename(columns=INSTRUMENT_MAPPING)
    instrument_df["asset_type"] = instrument_df.apply(
        lambda row: _normalize_asset_type(
            row.get("asset_type"), row.get("account_instrument_type")
        ),
        axis=1,
    )
    instrument_df["exchange"] = pd.NA
    instrument_df["asset_main_type"] = pd.NA
    instrument_df["asset_sub_type"] = instrument_df.get("account_instrument_type")
    instrument_df["quote_type"] = pd.NA
    instrument_df["ssid"] = pd.NA
    instrument_df["realtime"] = pd.NA
    instrument_df["first_seen_at"] = as_of_time
    instrument_df["last_seen_at"] = as_of_time
    instrument_df["source_payload"] = raw_instruments
    return instrument_df[
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
            "source_payload",
        ]
    ].drop_duplicates(subset=["symbol", "asset_type"])


def _build_positions_df(account_payload: dict, as_of_time: pd.Timestamp) -> pd.DataFrame:
    positions = account_payload["securitiesAccount"].get("positions", [])
    if not positions:
        return pd.DataFrame()

    df = pd.json_normalize(
        account_payload,
        record_path=["securitiesAccount", "positions"],
        meta=[["securitiesAccount", "accountNumber"]],
        max_level=0,
    ).rename(columns=POSITION_MAPPING)
    df["as_of_time"] = as_of_time
    return df


def _build_balance_df(
    securities_account_payload: dict,
    block_name: str,
    as_of_time: pd.Timestamp,
) -> pd.DataFrame:
    balance_payload = securities_account_payload.get(block_name)
    if not balance_payload:
        return pd.DataFrame()

    record = {
        BALANCE_FIELD_MAPPING.get(key, key): value
        for key, value in balance_payload.items()
    }
    record["account_number"] = securities_account_payload["accountNumber"]
    record["as_of_time"] = as_of_time
    return pd.DataFrame([record])


def get_account_data():
    api = SchwabApi()
    ods_db = connector(schema="ods")
    as_of_time = _get_as_of_time()
    account_payload = api.get_api_data("accounts")[0]
    account_numbers_payload = api.get_api_data("account_numbers")[0]

    securities_account_df = _build_securities_account_df(
        account_payload, account_numbers_payload
    )
    positions_df = _build_positions_df(account_payload, as_of_time)
    instruments_df = _build_instrument_df(positions_df, as_of_time)

    ods_db.upsert_dataframe(
        securities_account_df,
        table_name="securities_account",
        conflict_columns=["account_number"],
        update_columns=[
            "hash_value",
            "account_type",
            "round_trips",
            "is_day_trader",
            "is_closing_only_restricted",
            "pfcb_flag",
        ],
    )

    if not instruments_df.empty:
        ods_db.upsert_dataframe(
            instruments_df,
            table_name="instrument",
            conflict_columns=["symbol", "asset_type"],
            update_columns=[
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
            ],
        )

    account_lookup = ods_db.query_dataframe(
        "SELECT id AS account_id, account_number FROM securities_account;"
    )
    instrument_lookup = ods_db.query_dataframe(
        "SELECT id AS instrument_id, symbol, asset_type, cusip FROM instrument;"
    )

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
        positions_df["cusip"] = raw_instruments["cusip"]
        positions_df = positions_df.merge(account_lookup, on="account_number", how="left")
        positions_df = positions_df.merge(
            instrument_lookup[["instrument_id", "symbol", "asset_type"]],
            on=["symbol", "asset_type"],
            how="left",
        )
        positions_df = positions_df.dropna(subset=["account_id", "instrument_id"])
        positions_df["account_id"] = positions_df["account_id"].astype("int64")
        positions_df["instrument_id"] = positions_df["instrument_id"].astype("int64")

    balance_dfs = {
        table_name: _build_balance_df(
            account_payload["securitiesAccount"], block_name, as_of_time
        )
        for block_name, table_name in BALANCE_TABLES.items()
    }

    aggregated_balance_df = pd.DataFrame()
    if account_payload.get("aggregatedBalance"):
        aggregated_balance_df = pd.DataFrame(
            [
                {
                    "as_of_time": as_of_time,
                    "current_liquidation_value": account_payload["aggregatedBalance"].get(
                        "currentLiquidationValue"
                    ),
                    "liquidation_value": account_payload["aggregatedBalance"].get(
                        "liquidationValue"
                    ),
                }
            ]
        )

    return (
        ods_db,
        securities_account_df,
        instruments_df,
        positions_df,
        balance_dfs,
        aggregated_balance_df,
    )


def save_to_database():
    (
        ods_db,
        _securities_account_df,
        _instruments_df,
        positions_df,
        balance_dfs,
        aggregated_balance_df,
    ) = get_account_data()

    if not positions_df.empty:
        ods_db.insert_dataframe(
            positions_df[
                [
                    "as_of_time",
                    "account_id",
                    "instrument_id",
                    "long_quantity",
                    "short_quantity",
                    "average_price",
                    "average_long_price",
                    "taxlot_average_long_price",
                    "current_day_profit_loss",
                    "current_day_profit_loss_percentage",
                    "market_value",
                    "maintenance_requirement",
                    "long_open_profit_loss",
                    "previous_session_long_quantity",
                    "current_day_cost",
                ]
            ],
            "position",
            if_exists="append",
            chunksize=100,
        )

    account_lookup = ods_db.query_dataframe(
        "SELECT id AS account_id, account_number FROM securities_account;"
    )
    for table_name, balance_df in balance_dfs.items():
        if balance_df.empty:
            continue
        balance_df = balance_df.merge(account_lookup, on="account_number", how="left")
        balance_df = balance_df.dropna(subset=["account_id"])
        balance_df["account_id"] = balance_df["account_id"].astype("int64")
        ods_db.insert_dataframe(
            balance_df.drop(columns=["account_number"]),
            table_name,
            if_exists="append",
            chunksize=10,
        )

    if not aggregated_balance_df.empty:
        ods_db.insert_dataframe(
            aggregated_balance_df,
            "aggregated_balance",
            if_exists="append",
            chunksize=10,
        )

    logger.info("Account data saved successfully.")


if __name__ == "__main__":
    save_to_database()
