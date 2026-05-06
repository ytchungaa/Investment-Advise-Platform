from __future__ import annotations

import os
from pathlib import Path
import unittest
from unittest.mock import patch

import pandas as pd

from api_data_to_database import (
    build_account_ingestion_frames,
    fetch_account_history,
    fetch_account_payloads,
)
from get_market_data import schwab_api_market
from schwab_api import SchwabApi
from schwab_auth import SchwabAuth


ROOT_DIR = Path(__file__).resolve().parents[1]


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload
        self.status_code = 200
        self.text = ""

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self):
        self.calls = []

    def get(self, url, headers=None, params=None):
        self.calls.append({"url": url, "headers": headers, "params": params or {}})
        return FakeResponse([{"accountNumber": "123", "hashValue": "hash-123"}])


class SchwabAuthTests(unittest.TestCase):
    @patch("schwab_auth.dotenv.set_key")
    def test_refresh_updates_process_environment(self, mocked_set_key):
        with patch.dict(
            os.environ,
            {
                "SCHWAB_CLIENT_ID": "client-id",
                "SCHWAB_CLIENT_SECRET": "client-secret",
                "SCHWAB_REFRESH_TOKEN": "refresh-token",
                "SCHWAB_ACCESS_TOKEN": "expired-token",
                "SCHWAB_ACCESS_TOKEN_EXPIRES_TIMES": "2026-01-01 00:00:00",
                "SCHWAB_REFRESH_TOKEN_EXPIRES_TIMES": "2026-01-08 00:00:00",
            },
            clear=False,
        ):
            auth = SchwabAuth()
            with patch.object(
                auth,
                "_post_token",
                return_value=({"access_token": "fresh-token", "expires_in": 1800}, True),
            ):
                refreshed = auth.get_access_token()
                self.assertEqual(os.environ["SCHWAB_ACCESS_TOKEN"], "fresh-token")

        self.assertTrue(refreshed)
        self.assertEqual(auth.access_token, "fresh-token")
        mocked_set_key.assert_any_call(auth.dotenv_path, "SCHWAB_ACCESS_TOKEN", "fresh-token")


class SchwabApiMarketTests(unittest.TestCase):
    def test_explicit_access_token_skips_initial_refresh(self):
        with patch("get_market_data.SchwabAuth") as mocked_auth_cls:
            mocked_auth = mocked_auth_cls.return_value
            api = schwab_api_market(access_token="shared-token")

        self.assertEqual(api.access_token, "shared-token")
        mocked_auth.get_token.assert_not_called()


class SchwabTraderClientTests(unittest.TestCase):
    def test_client_builds_account_urls_with_bearer_token_and_params(self):
        session = FakeSession()
        api = SchwabApi(
            access_token="token-123",
            base_url="https://example.test/trader/v1",
            session=session,
        )

        api.get_account_numbers()
        api.get_account("hash-123", fields="positions")
        api.get_orders(
            "hash-123",
            from_entered_time="2026-01-01T00:00:00Z",
            to_entered_time="2026-01-02T00:00:00Z",
            status="FILLED",
        )
        api.get_transactions(
            "hash-123",
            start_date="2026-01-01T00:00:00Z",
            end_date="2026-01-02T00:00:00Z",
            types="TRADE",
            symbol="AAPL",
        )

        self.assertEqual(
            session.calls[0]["url"],
            "https://example.test/trader/v1/accounts/accountNumbers",
        )
        self.assertEqual(session.calls[0]["headers"]["Authorization"], "Bearer token-123")
        self.assertEqual(
            session.calls[1]["url"],
            "https://example.test/trader/v1/accounts/hash-123",
        )
        self.assertEqual(session.calls[1]["params"], {"fields": "positions"})
        self.assertEqual(
            session.calls[2]["url"],
            "https://example.test/trader/v1/accounts/hash-123/orders",
        )
        self.assertEqual(session.calls[2]["params"]["maxResults"], 3000)
        self.assertEqual(session.calls[2]["params"]["status"], "FILLED")
        self.assertEqual(
            session.calls[3]["url"],
            "https://example.test/trader/v1/accounts/hash-123/transactions",
        )
        self.assertEqual(session.calls[3]["params"]["types"], "TRADE")
        self.assertEqual(session.calls[3]["params"]["symbol"], "AAPL")

    def test_account_scoped_methods_require_hash(self):
        api = SchwabApi(access_token="token-123", session=FakeSession())

        with self.assertRaises(ValueError):
            api.get_account("")
        with self.assertRaises(ValueError):
            api.get_orders("", "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z")
        with self.assertRaises(ValueError):
            api.get_transactions("", "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z")


class FakeAccountApi:
    def __init__(self):
        self.account_calls = []

    def get_account_numbers(self):
        return [
            {"accountNumber": "111", "hashValue": "hash-111"},
            {"accountNumber": "222", "hashValue": "hash-222"},
        ]

    def get_account(self, account_hash, fields="positions"):
        self.account_calls.append((account_hash, fields))
        return {
            "securitiesAccount": {
                "accountNumber": "111" if account_hash == "hash-111" else "222",
                "type": "MARGIN",
                "roundTrips": 0,
                "isDayTrader": False,
                "isClosingOnlyRestricted": False,
                "pfcbFlag": False,
            }
        }


class FakeHistoryApi:
    def __init__(self):
        self.order_calls = []
        self.transaction_calls = []

    def get_orders(
        self,
        account_hash,
        from_entered_time,
        to_entered_time,
        max_results=3000,
        status=None,
    ):
        self.order_calls.append((account_hash, from_entered_time, to_entered_time))
        return [{"orderId": 9001, "status": "FILLED"}]

    def get_transactions(self, account_hash, start_date, end_date, types=None, symbol=None):
        self.transaction_calls.append((account_hash, start_date, end_date, types))
        return [{"activityId": 7001, "type": types, "transferItems": []}]


class AccountIngestionTests(unittest.TestCase):
    def test_fetch_account_payloads_iterates_all_linked_hashes(self):
        api = FakeAccountApi()

        account_numbers, account_payloads = fetch_account_payloads(api)

        self.assertEqual(len(account_numbers), 2)
        self.assertEqual(len(account_payloads), 2)
        self.assertEqual(
            api.account_calls,
            [("hash-111", "positions"), ("hash-222", "positions")],
        )
        self.assertEqual(account_payloads[0]["_accountHash"], "hash-111")

    def test_history_fetch_uses_60_day_windows_and_dedupes(self):
        api = FakeHistoryApi()

        orders, transactions = fetch_account_history(
            api,
            [{"accountNumber": "111", "hashValue": "hash-111"}],
            start_date="2026-01-01T00:00:00Z",
            end_date="2026-03-05T00:00:00Z",
            transaction_types=["TRADE"],
        )

        self.assertEqual(len(api.order_calls), 2)
        self.assertEqual(len(api.transaction_calls), 2)
        self.assertEqual(len(orders), 1)
        self.assertEqual(len(transactions), 1)
        self.assertEqual(orders[0]["_accountNumber"], "111")
        self.assertEqual(transactions[0]["_accountNumber"], "111")

    def test_builds_multi_account_snapshots_orders_and_transactions(self):
        as_of_time = pd.Timestamp("2026-05-06T12:00:00Z")
        account_numbers = [
            {"accountNumber": "111", "hashValue": "hash-111"},
            {"accountNumber": "222", "hashValue": "hash-222"},
        ]
        account_payloads = [
            {
                "_accountHash": "hash-111",
                "securitiesAccount": {
                    "accountNumber": "111",
                    "type": "MARGIN",
                    "roundTrips": 1,
                    "isDayTrader": False,
                    "isClosingOnlyRestricted": False,
                    "pfcbFlag": False,
                    "positions": [
                        {
                            "longQuantity": 5,
                            "shortQuantity": 0,
                            "averagePrice": 180.0,
                            "marketValue": 1000.0,
                            "instrument": {
                                "symbol": "AAPL",
                                "assetType": "EQUITY",
                                "cusip": "037833100",
                                "description": "Apple Inc.",
                            },
                        }
                    ],
                    "currentBalances": {
                        "cashBalance": 100.0,
                        "liquidationValue": 1200.0,
                    },
                    "projectedBalances": {
                        "cashAvailableForTrading": 100.0,
                        "cashAvailableForWithdrawal": 50.0,
                    },
                },
                "aggregatedBalance": {
                    "currentLiquidationValue": 1200.0,
                    "liquidationValue": 1200.0,
                },
            },
            {
                "_accountHash": "hash-222",
                "securitiesAccount": {
                    "accountNumber": "222",
                    "type": "CASH",
                    "roundTrips": 0,
                    "isDayTrader": False,
                    "isClosingOnlyRestricted": False,
                    "pfcbFlag": False,
                },
            },
        ]
        orders = [
            {
                "_accountNumber": "111",
                "orderId": 9001,
                "session": "NORMAL",
                "duration": "DAY",
                "orderType": "LIMIT",
                "complexOrderStrategyType": "NONE",
                "quantity": 1,
                "filledQuantity": 1,
                "remainingQuantity": 0,
                "requestedDestination": "AUTO",
                "orderStrategyType": "SINGLE",
                "status": "FILLED",
                "enteredTime": "2026-05-05T14:00:00Z",
                "orderLegCollection": [
                    {
                        "legId": 1,
                        "orderLegType": "EQUITY",
                        "instruction": "BUY",
                        "positionEffect": "OPENING",
                        "quantity": 1,
                        "instrument": {
                            "symbol": "MSFT",
                            "assetType": "EQUITY",
                            "instrumentId": 12345,
                        },
                    }
                ],
                "orderActivityCollection": [
                    {
                        "activityId": 8001,
                        "activityType": "EXECUTION",
                        "executionType": "FILL",
                        "quantity": 1,
                        "executionLegs": [
                            {
                                "legId": 1,
                                "price": 420.0,
                                "quantity": 1,
                                "instrumentId": 12345,
                                "time": "2026-05-05T14:01:00Z",
                            }
                        ],
                    }
                ],
            }
        ]
        transactions = [
            {
                "_accountNumber": "111",
                "activityId": 7001,
                "accountNumber": "111",
                "time": "2026-05-05T15:00:00Z",
                "type": "TRADE",
                "tradeDate": "2026-05-05",
                "orderId": 9001,
                "netAmount": -420.0,
                "transferItems": [
                    {
                        "quantity": 1,
                        "price": 420.0,
                        "instrument": {"symbol": "MSFT", "assetType": "EQUITY"},
                    },
                    {"amount": -420.0, "feeType": "COMMISSION"},
                ],
            }
        ]

        frames = build_account_ingestion_frames(
            account_payloads,
            account_numbers,
            orders_payload=orders,
            transactions_payload=transactions,
            as_of_time=as_of_time,
        )

        self.assertEqual(len(frames.securities_account), 2)
        self.assertEqual(set(frames.securities_account["hash_value"]), {"hash-111", "hash-222"})
        self.assertEqual(len(frames.positions), 1)
        self.assertEqual(len(frames.balances["account_current_balances"]), 1)
        self.assertEqual(len(frames.aggregated_balance), 1)
        self.assertEqual(set(frames.instruments["symbol"]), {"AAPL", "MSFT"})
        self.assertEqual(len(frames.orders), 1)
        self.assertEqual(len(frames.order_legs), 1)
        self.assertEqual(len(frames.order_activities), 1)
        self.assertEqual(len(frames.execution_legs), 1)
        self.assertEqual(len(frames.transactions), 1)
        self.assertEqual(len(frames.transaction_transfer_items), 2)
        self.assertEqual(frames.order_legs.iloc[0]["schwab_instrument_id"], 12345)
        self.assertEqual(frames.transaction_transfer_items.iloc[0]["symbol"], "MSFT")

    def test_account_and_order_sql_are_idempotent_and_include_latest_views(self):
        account_sql = (ROOT_DIR / "database/create_tables/ods.account_data.sql").read_text()
        order_sql = (ROOT_DIR / "database/create_tables/ods.orders_data.sql").read_text()

        self.assertIn("IF NOT EXISTS (SELECT 1 FROM pg_type", account_sql)
        self.assertIn("ADD COLUMN IF NOT EXISTS raw_payload JSONB", account_sql)
        self.assertIn("CREATE OR REPLACE VIEW ods.v_position_latest", account_sql)
        self.assertIn("CREATE OR REPLACE VIEW ods.v_account_current_balances_latest", account_sql)
        self.assertIn("IF NOT EXISTS (SELECT 1 FROM pg_type", order_sql)
        self.assertIn("CREATE TABLE IF NOT EXISTS ods.account_transaction", order_sql)
        self.assertIn("CREATE TABLE IF NOT EXISTS ods.transaction_transfer_item", order_sql)
        self.assertIn("raw_payload JSONB", order_sql)


class DwdAccountSqlTests(unittest.TestCase):
    def test_account_dwd_views_cover_portfolio_reporting_shapes(self):
        sql = (ROOT_DIR / "database/create_tables/dwd.account_data.sql").read_text()

        self.assertIn("CREATE OR REPLACE VIEW dwd.dim_account", sql)
        self.assertIn("CREATE OR REPLACE VIEW dwd.fact_position_latest", sql)
        self.assertIn("CREATE OR REPLACE VIEW dwd.fact_portfolio_composition_latest", sql)
        self.assertIn("CREATE OR REPLACE VIEW dwd.fact_account_balance_latest", sql)
        self.assertIn("CREATE OR REPLACE VIEW dwd.fact_portfolio_balance_latest", sql)
        self.assertIn("CREATE OR REPLACE VIEW dwd.fact_position_history", sql)
        self.assertIn("CREATE OR REPLACE VIEW dwd.fact_account_balance_history", sql)
        self.assertIn("CREATE OR REPLACE VIEW dwd.fact_portfolio_balance_history", sql)

    def test_account_dimension_masks_account_number_without_hash_value(self):
        sql = (ROOT_DIR / "database/create_tables/dwd.account_data.sql").read_text()
        dim_account_sql = sql.split("CREATE OR REPLACE VIEW dwd.fact_position_latest")[0]

        self.assertIn("masked_account_number", dim_account_sql)
        self.assertIn("right(account_number, 4)", dim_account_sql)
        self.assertNotIn("hash_value", dim_account_sql)

    def test_portfolio_composition_uses_positive_market_value_by_symbol_and_asset_type(self):
        sql = (ROOT_DIR / "database/create_tables/dwd.account_data.sql").read_text()

        self.assertIn("WHERE market_value > 0", sql)
        self.assertIn("'symbol'::TEXT AS composition_type", sql)
        self.assertIn("'asset_type'::TEXT AS composition_type", sql)
        self.assertIn("COALESCE(asset_type, 'UNKNOWN') AS asset_type", sql)
        self.assertIn("SUM(market_value) AS market_value", sql)
        self.assertIn("c.market_value / t.total_market_value", sql)

    def test_portfolio_balance_aggregates_latest_account_current_balances(self):
        sql = (ROOT_DIR / "database/create_tables/dwd.account_data.sql").read_text()

        self.assertIn("FROM ods.v_account_current_balances_latest b", sql)
        self.assertIn("FROM dwd.fact_account_balance_latest", sql)
        self.assertIn("COUNT(DISTINCT account_id) AS account_count", sql)
        self.assertIn("SUM(COALESCE(liquidation_value, 0)) AS total_liquidation_value", sql)


if __name__ == "__main__":
    unittest.main()
