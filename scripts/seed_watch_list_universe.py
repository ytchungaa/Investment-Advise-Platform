#!/usr/bin/env python3

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

REPO_DIR = Path(__file__).resolve().parents[1]
if str(REPO_DIR) not in sys.path:
    sys.path.insert(0, str(REPO_DIR))

from database_connect import connector
from get_market_data import schwab_api_market


SP500_WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
REQUEST_HEADERS = {
    "User-Agent": "InvestmentAdvisePlatform/1.0 (local watch list seed script)"
}

COMMON_INDEX_SYMBOLS = [
    "$SPX",
    "$DJI",
    "$COMPX",
    "$NDX",
    "$RUT",
    "$VIX",
]

COMMON_ETF_SYMBOLS = [
    "SPY",
    "IVV",
    "VOO",
    "QQQ",
    "DIA",
    "IWM",
    "VTI",
    "VT",
    "BND",
    "AGG",
    "GLD",
    "SLV",
    "TLT",
]


def normalize_symbol(symbol: str) -> str:
    return (symbol or "").strip().upper().replace(".", "/")


def fetch_sp500_constituents() -> pd.DataFrame:
    response = requests.get(SP500_WIKI_URL, headers=REQUEST_HEADERS, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table", id="constituents")
    if table is None:
        raise RuntimeError("Could not find the S&P 500 constituents table on Wikipedia.")

    rows: list[dict[str, str]] = []
    body_rows = table.find("tbody").find_all("tr")
    for tr in body_rows[1:]:
        cols = tr.find_all("td")
        if len(cols) < 2:
            continue
        symbol = normalize_symbol(cols[0].get_text(strip=True))
        name = cols[1].get_text(" ", strip=True)
        if not symbol:
            continue
        rows.append(
            {
                "symbol": symbol,
                "symbol_name": name,
                "asset_type": "EQUITY",
            }
        )

    constituents_df = pd.DataFrame(rows).drop_duplicates(subset=["symbol"])
    if constituents_df.empty:
        raise RuntimeError("No S&P 500 constituents were parsed from Wikipedia.")
    return constituents_df


def fetch_instrument_metadata(api: schwab_api_market, symbols: list[str]) -> pd.DataFrame:
    metadata_df = api.fetch_instruments(symbols, chunksize=500)
    if metadata_df.empty:
        return pd.DataFrame(columns=["symbol", "symbol_name", "asset_type"])

    metadata_df = metadata_df.rename(columns={"description": "symbol_name"})
    metadata_df["symbol"] = metadata_df["symbol"].map(normalize_symbol)
    return metadata_df[["symbol", "symbol_name", "asset_type"]].drop_duplicates(
        subset=["symbol"], keep="first"
    )


def build_watch_list_rows() -> pd.DataFrame:
    api = schwab_api_market()

    sp500_df = fetch_sp500_constituents()
    sp500_metadata_df = fetch_instrument_metadata(api, sp500_df["symbol"].tolist())
    merged_sp500_df = sp500_df.merge(
        sp500_metadata_df,
        on="symbol",
        how="left",
        suffixes=("_wiki", "_api"),
    )
    merged_sp500_df["symbol_name"] = merged_sp500_df["symbol_name_api"].fillna(
        merged_sp500_df["symbol_name_wiki"]
    )
    merged_sp500_df["asset_type"] = merged_sp500_df["asset_type_api"].fillna(
        merged_sp500_df["asset_type_wiki"]
    )
    sp500_rows_df = merged_sp500_df[["symbol", "symbol_name", "asset_type"]]

    extra_symbols = sorted(set(COMMON_INDEX_SYMBOLS + COMMON_ETF_SYMBOLS))
    extras_df = fetch_instrument_metadata(api, extra_symbols)
    missing_extra_symbols = sorted(set(extra_symbols) - set(extras_df["symbol"].tolist()))
    if missing_extra_symbols:
        raise RuntimeError(
            "Failed to resolve these default symbols from Schwab: "
            + ", ".join(missing_extra_symbols)
        )

    watch_list_df = pd.concat([sp500_rows_df, extras_df], ignore_index=True)
    watch_list_df["symbol"] = watch_list_df["symbol"].map(normalize_symbol)
    watch_list_df["is_active"] = True
    watch_list_df = watch_list_df.drop_duplicates(subset=["symbol"], keep="first")
    return watch_list_df[["symbol", "symbol_name", "asset_type", "is_active"]]


def main() -> None:
    watch_list_df = build_watch_list_rows()

    db_dwd = connector(schema="dwd")
    db_dwd.upsert_dataframe(
        watch_list_df,
        table_name="watch_list",
        conflict_columns=["symbol"],
        update_columns=["symbol_name", "asset_type", "is_active"],
        chunksize=500,
    )

    asset_counts = (
        watch_list_df.groupby("asset_type")["symbol"].count().sort_index().to_dict()
    )
    print(
        f"Seeded watch list with {len(watch_list_df)} symbols: "
        + ", ".join(f"{asset_type}={count}" for asset_type, count in asset_counts.items())
    )


if __name__ == "__main__":
    main()
