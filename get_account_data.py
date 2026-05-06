from __future__ import annotations

import pandas as pd

from api_data_to_database import get_account_data as build_account_frames
from api_data_to_database import save_to_database
from schwab_api import SchwabApi as _SchwabApi


class SchwabApi(_SchwabApi):
    """Compatibility wrapper around the canonical Schwab account ingestion client."""

    def get_account_data(self) -> pd.DataFrame:
        _account_numbers, _payloads, frames = build_account_frames(
            api=self, fetch_history=False
        )
        return frames.securities_account

    def get_account_balances(self) -> pd.DataFrame:
        _account_numbers, _payloads, frames = build_account_frames(
            api=self, fetch_history=False
        )
        balance_frames = [
            balance_df.assign(balance_table=table_name)
            for table_name, balance_df in frames.balances.items()
            if not balance_df.empty
        ]
        if not balance_frames:
            return pd.DataFrame()
        return pd.concat(balance_frames, ignore_index=True)

    def get_account_orders(self, start_date: str, end_date: str) -> list[dict]:
        account_numbers = self.get_account_numbers()
        orders: list[dict] = []
        for account_info in account_numbers:
            account_hash = account_info.get("hashValue")
            if not account_hash:
                continue
            account_orders = self.get_orders(
                account_hash=account_hash,
                from_entered_time=start_date,
                to_entered_time=end_date,
            )
            orders.extend(account_orders or [])
        return orders

    def get_account_transactions(self, start_date: str, end_date: str) -> pd.DataFrame:
        account_numbers = self.get_account_numbers()
        transactions: list[dict] = []
        for account_info in account_numbers:
            account_hash = account_info.get("hashValue")
            if not account_hash:
                continue
            account_transactions = self.get_transactions(
                account_hash=account_hash,
                start_date=start_date,
                end_date=end_date,
                types="TRADE",
            )
            transactions.extend(account_transactions or [])
        return pd.json_normalize(transactions)


if __name__ == "__main__":
    print(save_to_database())
