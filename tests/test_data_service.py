from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from app import data_service


class FakeConnector:
    def __init__(
        self,
        instrument_df: pd.DataFrame,
        price_df: pd.DataFrame,
        quote_df: pd.DataFrame | None = None,
        fundamental_df: pd.DataFrame | None = None,
    ):
        self.instrument_df = instrument_df
        self.price_df = price_df
        self.quote_df = quote_df if quote_df is not None else pd.DataFrame()
        self.fundamental_df = fundamental_df if fundamental_df is not None else pd.DataFrame()
        self.calls: list[tuple[str, dict]] = []

    def query_dataframe(self, query, params=None):
        normalized_query = " ".join(str(query).split())
        self.calls.append((normalized_query, params or {}))
        if "FROM ods.instrument AS i" in normalized_query:
            if "SELECT i.symbol, i.description" in normalized_query:
                return self.instrument_df.copy()
            return self.instrument_df.copy()
        if "FROM dwd.price_history_" in normalized_query:
            return self.price_df.copy()
        if "FROM ods.quote_history" in normalized_query:
            return self.quote_df.copy()
        if "FROM ods.instrument_fundamental_history" in normalized_query:
            return self.fundamental_df.copy()
        raise AssertionError(f"Unexpected query: {normalized_query}")


class PortfolioFakeConnector:
    def __init__(
        self,
        holdings_df: pd.DataFrame,
        balance_df: pd.DataFrame,
        price_df: pd.DataFrame,
        balance_history_df: pd.DataFrame | None = None,
        realized_events_df: pd.DataFrame | None = None,
    ):
        self.holdings_df = holdings_df
        self.balance_df = balance_df
        self.price_df = price_df
        self.balance_history_df = (
            balance_history_df if balance_history_df is not None else pd.DataFrame()
        )
        self.realized_events_df = (
            realized_events_df if realized_events_df is not None else pd.DataFrame()
        )
        self.calls: list[tuple[str, dict]] = []

    def query_dataframe(self, query, params=None):
        normalized_query = " ".join(str(query).split())
        self.calls.append((normalized_query, params or {}))
        if "to_regclass" in normalized_query:
            return pd.DataFrame([{"relation_exists": True}])
        if "FROM dwd.fact_position_latest" in normalized_query:
            return self.holdings_df.copy()
        if "FROM dwd.fact_portfolio_balance_latest" in normalized_query:
            return self.balance_df.copy()
        if "FROM dwd.price_history_daily" in normalized_query:
            return self.price_df.copy()
        if "FROM dwd.fact_portfolio_balance_history" in normalized_query:
            return self.balance_history_df.copy()
        if "FROM ods.account_transaction" in normalized_query:
            return self.realized_events_df.copy()
        raise AssertionError(f"Unexpected query: {normalized_query}")


class DataServiceTests(unittest.TestCase):
    def test_default_interval_is_hourly_and_range_clips_to_available_data(self):
        instrument_df = pd.DataFrame(
            [
                {
                    "instrument_id": 7,
                    "symbol": "AAPL",
                    "description": "Apple Inc.",
                    "available_start": pd.Timestamp("2026-03-20").date(),
                    "available_end": pd.Timestamp("2026-04-01").date(),
                }
            ]
        )
        price_df = pd.DataFrame(
            [
                {
                    "time_bucket": pd.Timestamp("2026-03-31 10:00:00-04:00"),
                    "open": 220.0,
                    "high": 225.0,
                    "low": 219.0,
                    "close": 224.0,
                    "volume": 100,
                },
                {
                    "time_bucket": pd.Timestamp("2026-04-01 11:00:00-04:00"),
                    "open": 224.0,
                    "high": 226.0,
                    "low": 223.0,
                    "close": 225.0,
                    "volume": 150,
                },
            ]
        )
        fake_db = FakeConnector(instrument_df=instrument_df, price_df=price_df)

        with patch("app.data_service.connector", return_value=fake_db), patch(
            "app.data_service._utc_today", return_value=pd.Timestamp("2026-04-15").date()
        ):
            payload = data_service.get_stock_visualization_data("aapl")

        self.assertEqual(payload["selected_range"]["start"], "2026-03-20")
        self.assertEqual(payload["selected_range"]["end"], "2026-04-01")
        self.assertEqual(payload["interval"], "hourly")
        self.assertEqual(payload["summary"]["price_points"], 2)
        self.assertIn("T10:00:00", payload["timeseries"][0]["date"])
        self.assertNotIn("monthly_return_boxes", payload)
        self.assertIn("company_info", payload)
        self.assertTrue(
            any("FROM dwd.price_history_hourly" in query for query, _ in fake_db.calls)
        )
        self.assertTrue(
            any("source_candle_count > 0" in query for query, _ in fake_db.calls)
        )

    def test_stock_visualization_payload_includes_latest_company_info(self):
        instrument_df = pd.DataFrame(
            [
                {
                    "instrument_id": 7,
                    "symbol": "AAPL",
                    "description": "Apple Inc.",
                    "asset_type": "EQUITY",
                    "exchange": "NASDAQ",
                    "asset_main_type": "EQUITY",
                    "asset_sub_type": None,
                    "quote_type": "NBBO",
                    "realtime": True,
                    "last_seen_at": pd.Timestamp("2026-05-05T14:00:00Z"),
                    "available_start": pd.Timestamp("2026-04-01").date(),
                    "available_end": pd.Timestamp("2026-04-01").date(),
                }
            ]
        )
        price_df = pd.DataFrame(
            [
                {
                    "time_bucket": pd.Timestamp("2026-04-01 00:00:00-04:00"),
                    "open": 224.0,
                    "high": 226.0,
                    "low": 223.0,
                    "close": 225.0,
                    "volume": 150,
                }
            ]
        )
        quote_df = pd.DataFrame(
            [
                {
                    "as_of_time": pd.Timestamp("2026-05-05T14:00:00Z"),
                    "last_price": 225.5,
                    "mark": 225.4,
                    "mark_change": 1.1,
                    "mark_percent_change": 0.49,
                    "net_change": 1.0,
                    "net_percent_change": 0.45,
                    "open_price": 224.0,
                    "high_price": 226.0,
                    "low_price": 223.0,
                    "close_price": 224.5,
                    "bid_price": 225.4,
                    "ask_price": 225.6,
                    "total_volume": 1000,
                    "security_status": "Normal",
                    "quote_time": pd.Timestamp("2026-05-05T13:59:00Z"),
                    "trade_time": pd.NaT,
                }
            ]
        )
        fundamental_df = pd.DataFrame(
            [
                {
                    "as_of_time": pd.Timestamp("2026-05-05T14:00:00Z"),
                    "market_cap": 3500000000000.0,
                    "market_cap_float": 3200000000000.0,
                    "shares_outstanding": 15000000000.0,
                    "pe_ratio": 30.5,
                    "peg_ratio": 2.1,
                    "pb_ratio": 44.0,
                    "pr_ratio": 8.2,
                    "pcf_ratio": 25.0,
                    "eps": 7.1,
                    "eps_ttm": 7.0,
                    "eps_change_percent_ttm": 4.5,
                    "rev_change_ttm": 6.0,
                    "beta": 1.2,
                    "dividend_amount": 1.04,
                    "dividend_yield": 0.5,
                    "dividend_pay_amount": 0.26,
                    "dividend_pay_date": pd.Timestamp("2026-05-15T00:00:00Z"),
                    "next_dividend_date": pd.NaT,
                    "next_dividend_pay_date": pd.NaT,
                    "week_52_high": 240.0,
                    "week_52_low": 170.0,
                    "gross_margin_ttm": 46.0,
                    "net_profit_margin_ttm": 24.0,
                    "operating_margin_ttm": 31.0,
                    "return_on_equity": 150.0,
                    "return_on_assets": 25.0,
                    "return_on_investment": 40.0,
                    "current_ratio": 0.9,
                    "quick_ratio": 0.8,
                    "total_debt_to_equity": 180.0,
                    "total_debt_to_capital": 65.0,
                    "book_value_per_share": 4.5,
                    "avg_10_days_volume": 50000000,
                    "avg_3_month_volume": 55000000,
                }
            ]
        )
        fake_db = FakeConnector(
            instrument_df=instrument_df,
            price_df=price_df,
            quote_df=quote_df,
            fundamental_df=fundamental_df,
        )

        with patch("app.data_service.connector", return_value=fake_db):
            payload = data_service.get_stock_visualization_data(
                "aapl",
                start_date="2026-04-01",
                end_date="2026-04-01",
                interval="daily",
            )

        company_info = payload["company_info"]
        self.assertEqual(company_info["profile"]["exchange"], "NASDAQ")
        self.assertTrue(company_info["profile"]["realtime"])
        self.assertEqual(company_info["quote"]["last_price"], 225.5)
        self.assertEqual(company_info["fundamentals"]["pe_ratio"], 30.5)
        self.assertIsNone(company_info["quote"]["trade_time"])
        self.assertIn("T14:00:00+00:00", company_info["fundamentals"]["as_of_time"])
        self.assertTrue(
            any("FROM ods.quote_history" in query for query, _ in fake_db.calls)
        )
        self.assertTrue(
            any("FROM ods.instrument_fundamental_history" in query for query, _ in fake_db.calls)
        )

    def test_daily_interval_reads_daily_dwd_table_and_returns_date_values(self):
        instrument_df = pd.DataFrame(
            [
                {
                    "instrument_id": 7,
                    "symbol": "AAPL",
                    "description": "Apple Inc.",
                    "available_start": pd.Timestamp("2026-03-20").date(),
                    "available_end": pd.Timestamp("2026-04-01").date(),
                }
            ]
        )
        price_df = pd.DataFrame(
            [
                {
                    "time_bucket": pd.Timestamp("2026-04-01 00:00:00-04:00"),
                    "open": 224.0,
                    "high": 226.0,
                    "low": 223.0,
                    "close": 225.0,
                    "volume": 150,
                }
            ]
        )
        fake_db = FakeConnector(instrument_df=instrument_df, price_df=price_df)

        with patch("app.data_service.connector", return_value=fake_db):
            payload = data_service.get_stock_visualization_data(
                "aapl",
                start_date="2026-04-01",
                end_date="2026-04-01",
                interval="daily",
            )

        self.assertEqual(payload["interval"], "daily")
        self.assertEqual(payload["timeseries"][0]["date"], "2026-04-01")
        self.assertTrue(
            any("FROM dwd.price_history_daily" in query for query, _ in fake_db.calls)
        )

    def test_invalid_date_format_raises_value_error(self):
        with self.assertRaises(ValueError):
            data_service.get_stock_visualization_data("AAPL", start_date="04-15-2026")

    def test_invalid_interval_raises_value_error(self):
        with self.assertRaises(ValueError):
            data_service.get_stock_visualization_data("AAPL", interval="weekly")

    def test_empty_selected_range_returns_no_data_message(self):
        instrument_df = pd.DataFrame(
            [
                {
                    "instrument_id": 7,
                    "symbol": "AAPL",
                    "description": "Apple Inc.",
                    "available_start": pd.Timestamp("2026-03-01").date(),
                    "available_end": pd.Timestamp("2026-04-01").date(),
                }
            ]
        )
        fake_db = FakeConnector(instrument_df=instrument_df, price_df=pd.DataFrame())

        with patch("app.data_service.connector", return_value=fake_db):
            payload = data_service.get_stock_visualization_data(
                "AAPL",
                start_date="2026-01-01",
                end_date="2026-01-31",
            )

        self.assertEqual(payload["timeseries"], [])
        self.assertEqual(
            payload["message"], "No hourly price history is available for the selected range."
        )
        self.assertEqual(payload["summary"]["price_points"], 0)

    def test_search_symbols_returns_matching_results_from_selected_dwd_table(self):
        instrument_df = pd.DataFrame(
            [
                {"symbol": "AAPL", "description": "Apple Inc."},
                {"symbol": "AAP", "description": "Advance Auto Parts"},
            ]
        )
        fake_db = FakeConnector(instrument_df=instrument_df, price_df=pd.DataFrame())

        with patch("app.data_service.connector", return_value=fake_db):
            results = data_service.search_symbols("aa")

        self.assertEqual(results[0]["symbol"], "AAPL")
        self.assertEqual(len(results), 2)
        self.assertTrue(
            any("dwd.price_history_hourly" in query for query, _ in fake_db.calls)
        )

        fake_db.calls.clear()
        with patch("app.data_service.connector", return_value=fake_db):
            data_service.search_symbols("aa", interval="daily")

        self.assertTrue(
            any("dwd.price_history_daily" in query for query, _ in fake_db.calls)
        )

    def test_refresh_stock_data_requests_after_latest_minute_candle(self):
        class RefreshConnector:
            def __init__(self):
                self.upserts: list[tuple[str, pd.DataFrame]] = []
                self.inserts: list[tuple[str, pd.DataFrame]] = []

            def upsert_dataframe(self, df, table_name, **kwargs):
                self.upserts.append((table_name, df.copy()))
                return True

            def insert_dataframe(self, df, table_name, **kwargs):
                self.inserts.append((table_name, df.copy()))

            def query_dataframe(self, query, params=None):
                normalized_query = " ".join(str(query).split())
                if "SELECT id AS instrument_id, symbol, asset_type" in normalized_query:
                    return pd.DataFrame(
                        [{"instrument_id": 7, "symbol": "AAPL", "asset_type": "EQUITY"}]
                    )
                if "SELECT symbol, asset_type" in normalized_query:
                    return pd.DataFrame([{"symbol": "AAPL", "asset_type": "EQUITY"}])
                if "SELECT MAX(candle_time)" in normalized_query:
                    return pd.DataFrame(
                        [{"latest_candle_time": pd.Timestamp("2026-05-05T14:00:00Z")}]
                    )
                raise AssertionError(f"Unexpected query: {normalized_query}")

        fake_db = RefreshConnector()
        fake_api = MagicMock()
        fake_api.fetch_instruments.return_value = pd.DataFrame(
            [
                {
                    "symbol": "AAPL",
                    "asset_type": "EQUITY",
                    "cusip": "037833100",
                    "description": "Apple Inc.",
                    "exchange": "NASDAQ",
                    "asset_main_type": "EQUITY",
                    "asset_sub_type": None,
                    "quote_type": "NBBO",
                    "ssid": 1,
                    "realtime": True,
                    "last_seen_at": pd.Timestamp("2026-05-05T14:10:00Z"),
                }
            ]
        )
        fake_api.fetch_quotes.return_value = (pd.DataFrame(), pd.DataFrame())
        fake_api.fetch_instrument_fundamentals.return_value = pd.DataFrame()
        fake_api.fetch_price_history.return_value = pd.DataFrame(
            [
                {
                    "symbol": "AAPL",
                    "frequency_type": 1,
                    "frequency": 1,
                    "candle_time": pd.Timestamp("2026-05-05T14:01:00Z"),
                    "open": 1.0,
                    "high": 1.0,
                    "low": 1.0,
                    "close": 1.0,
                    "volume": 1,
                    "previous_close": 1.0,
                    "previous_close_time": pd.Timestamp("2026-05-04T20:00:00Z"),
                    "need_extended_hours_data": True,
                }
            ]
        )

        with patch("app.data_service.connector", return_value=fake_db), patch(
            "get_market_data.schwab_api_market", return_value=fake_api
        ), patch("app.data_service._refresh_dwd_price_tables", return_value=[]), patch(
            "app.data_service._utc_now_minute",
            return_value=pd.Timestamp("2026-05-05T14:10:00Z"),
        ):
            data_service.refresh_stock_data("AAPL")

        _, kwargs = fake_api.fetch_price_history.call_args
        self.assertEqual(kwargs["start_date"], "2026-05-05T14:01:00Z")

    def test_portfolio_data_maps_categories_cash_and_weighted_risk(self):
        holdings_df = pd.DataFrame(
            [
                {
                    "as_of_time": pd.Timestamp("2026-05-06T13:00:00Z"),
                    "account_id": 1,
                    "masked_account_number": "****1111",
                    "account_type": "MARGIN",
                    "instrument_id": 101,
                    "symbol": "AAPL",
                    "description": "Apple Inc.",
                    "asset_type": "EQUITY",
                    "long_quantity": 3,
                    "short_quantity": 0,
                    "net_quantity": 3,
                    "average_price": 150.0,
                    "average_long_price": 150.0,
                    "market_value": 600.0,
                    "current_day_profit_loss": 12.0,
                    "current_day_profit_loss_percentage": 2.0,
                },
                {
                    "as_of_time": pd.Timestamp("2026-05-06T13:00:00Z"),
                    "account_id": 1,
                    "masked_account_number": "****1111",
                    "account_type": "MARGIN",
                    "instrument_id": 102,
                    "symbol": "VOO",
                    "description": "Vanguard S&P 500 ETF",
                    "asset_type": "ETF",
                    "long_quantity": 2,
                    "short_quantity": 0,
                    "net_quantity": 2,
                    "average_price": 190.0,
                    "average_long_price": 190.0,
                    "market_value": 400.0,
                    "current_day_profit_loss": -4.0,
                    "current_day_profit_loss_percentage": -1.0,
                },
            ]
        )
        balance_df = pd.DataFrame(
            [
                {
                    "as_of_time": pd.Timestamp("2026-05-06T13:00:00Z"),
                    "account_count": 1,
                    "total_liquidation_value": 1200.0,
                    "total_cash_balance": 200.0,
                    "total_cash": 200.0,
                    "total_long_market_value": 1000.0,
                    "total_short_market_value": 0.0,
                    "total_money_market_fund": 0.0,
                    "total_mutual_fund_value": 0.0,
                    "total_bond_value": 0.0,
                    "total_cash_available_for_trading": 200.0,
                    "total_cash_available_for_withdrawal": 150.0,
                }
            ]
        )
        price_df = pd.DataFrame(
            [
                {"symbol": "AAPL", "price_date": pd.Timestamp("2025-05-06").date(), "close": 100.0},
                {"symbol": "AAPL", "price_date": pd.Timestamp("2025-11-06").date(), "close": 110.0},
                {"symbol": "AAPL", "price_date": pd.Timestamp("2026-05-06").date(), "close": 120.0},
                {"symbol": "VOO", "price_date": pd.Timestamp("2025-05-06").date(), "close": 100.0},
                {"symbol": "VOO", "price_date": pd.Timestamp("2025-11-06").date(), "close": 102.0},
                {"symbol": "VOO", "price_date": pd.Timestamp("2026-05-06").date(), "close": 105.0},
                {"symbol": "SPY", "price_date": pd.Timestamp("2025-05-06").date(), "close": 100.0},
                {"symbol": "SPY", "price_date": pd.Timestamp("2025-11-06").date(), "close": 105.0},
                {"symbol": "SPY", "price_date": pd.Timestamp("2026-05-06").date(), "close": 110.0},
            ]
        )
        balance_history_df = pd.DataFrame(
            [
                {
                    "history_date": pd.Timestamp("2026-05-06").date(),
                    "as_of_time": pd.Timestamp("2026-05-06T13:00:00Z"),
                    "account_count": 1,
                    "total_liquidation_value": 1200.0,
                    "total_cash": 200.0,
                    "total_long_market_value": 1000.0,
                    "total_short_market_value": 0.0,
                }
            ]
        )
        realized_events_df = pd.DataFrame(
            [
                {
                    "event_date": pd.Timestamp("2026-05-05").date(),
                    "symbol": "AAPL",
                    "realized_amount": 281.19,
                    "transaction_count": 1,
                }
            ]
        )
        fake_db = PortfolioFakeConnector(
            holdings_df,
            balance_df,
            price_df,
            balance_history_df=balance_history_df,
            realized_events_df=realized_events_df,
        )

        with patch("app.data_service.connector", return_value=fake_db), patch(
            "app.data_service._utc_today", return_value=pd.Timestamp("2026-05-06").date()
        ):
            payload = data_service.get_portfolio_data()

        allocation = {row["category"]: row for row in payload["allocation"]}
        self.assertEqual(payload["benchmark_symbol"], "SPY")
        self.assertEqual(payload["summary"]["total_portfolio_value"], 1200.0)
        self.assertAlmostEqual(allocation["Stocks"]["market_value"], 600.0)
        self.assertAlmostEqual(allocation["ETF"]["market_value"], 400.0)
        self.assertAlmostEqual(allocation["Cash"]["market_value"], 200.0)
        self.assertAlmostEqual(allocation["Stocks"]["weight"], 0.5)
        self.assertEqual(payload["holdings"][0]["symbol"], "AAPL")
        self.assertEqual(payload["holdings"][0]["category"], "Stocks")
        self.assertEqual(payload["holdings"][1]["category"], "ETF")
        self.assertAlmostEqual(payload["holdings"][0]["weight"], 0.5)
        self.assertAlmostEqual(payload["summary"]["portfolio_return_1y"], 0.14)
        self.assertAlmostEqual(payload["summary"]["benchmark_return_1y"], 0.10)
        self.assertAlmostEqual(payload["summary"]["alpha_1y"], 0.04)
        self.assertIsNotNone(payload["summary"]["beta_1y"])
        self.assertEqual(payload["history"]["balance_series"][0]["total_value"], 1200.0)
        self.assertEqual(payload["history"]["realized_events"][0]["symbol"], "AAPL")
        self.assertEqual(payload["history"]["realized_events"][0]["direction"], "gain")
        self.assertEqual(payload["warnings"], [])
        self.assertTrue(
            any("dwd.fact_position_latest" in query for query, _ in fake_db.calls)
        )
        self.assertTrue(
            any("dwd.price_history_daily" in query for query, _ in fake_db.calls)
        )

    def test_portfolio_data_warns_when_spy_history_is_missing(self):
        holdings_df = pd.DataFrame(
            [
                {
                    "as_of_time": pd.Timestamp("2026-05-06T13:00:00Z"),
                    "account_id": 1,
                    "masked_account_number": "****1111",
                    "account_type": "MARGIN",
                    "instrument_id": 101,
                    "symbol": "AAPL",
                    "description": "Apple Inc.",
                    "asset_type": "EQUITY",
                    "long_quantity": 3,
                    "short_quantity": 0,
                    "net_quantity": 3,
                    "average_price": 150.0,
                    "average_long_price": 150.0,
                    "market_value": 600.0,
                    "current_day_profit_loss": 12.0,
                    "current_day_profit_loss_percentage": 2.0,
                }
            ]
        )
        balance_df = pd.DataFrame(
            [
                {
                    "as_of_time": pd.Timestamp("2026-05-06T13:00:00Z"),
                    "account_count": 1,
                    "total_liquidation_value": 800.0,
                    "total_cash_balance": 200.0,
                    "total_cash": 200.0,
                    "total_long_market_value": 600.0,
                    "total_short_market_value": 0.0,
                    "total_money_market_fund": 0.0,
                    "total_mutual_fund_value": 0.0,
                    "total_bond_value": 0.0,
                    "total_cash_available_for_trading": 200.0,
                    "total_cash_available_for_withdrawal": 150.0,
                }
            ]
        )
        price_df = pd.DataFrame(
            [
                {"symbol": "AAPL", "price_date": pd.Timestamp("2025-05-06").date(), "close": 100.0},
                {"symbol": "AAPL", "price_date": pd.Timestamp("2026-05-06").date(), "close": 120.0},
            ]
        )
        fake_db = PortfolioFakeConnector(holdings_df, balance_df, price_df)

        with patch("app.data_service.connector", return_value=fake_db), patch(
            "app.data_service._utc_today", return_value=pd.Timestamp("2026-05-06").date()
        ):
            payload = data_service.get_portfolio_data()

        self.assertIsNone(payload["summary"]["benchmark_return_1y"])
        self.assertIsNone(payload["summary"]["alpha_1y"])
        self.assertTrue(
            any("SPY price history is unavailable" in warning for warning in payload["warnings"])
        )

    def test_refresh_portfolio_data_fetches_when_snapshots_are_missing(self):
        before_status = {
            "relations": {},
            "has_portfolio_snapshots": False,
            "has_order_history": False,
            "has_transaction_history": False,
        }
        after_status = {
            "relations": {},
            "has_portfolio_snapshots": True,
            "has_order_history": True,
            "has_transaction_history": True,
        }

        with patch("app.data_service.ensure_portfolio_schema", return_value=["ods.account_data.sql"]), patch(
            "app.data_service.get_portfolio_storage_status",
            side_effect=[before_status, after_status],
        ), patch("api_data_to_database.save_to_database", return_value={"positions": 2}) as mocked_save, patch(
            "app.data_service._utc_today", return_value=pd.Timestamp("2026-05-06").date()
        ):
            payload = data_service.refresh_portfolio_data()

        self.assertTrue(payload["refreshed"])
        self.assertEqual(payload["summary"]["positions"], 2)
        mocked_save.assert_called_once()
        _, kwargs = mocked_save.call_args
        self.assertEqual(kwargs["start_date"], pd.Timestamp("2025-05-06").date())
        self.assertEqual(kwargs["end_date"], pd.Timestamp("2026-05-06").date())
        self.assertTrue(kwargs["fetch_history"])

    def test_refresh_portfolio_data_skips_api_when_snapshots_exist(self):
        status = {
            "relations": {},
            "has_portfolio_snapshots": True,
            "has_order_history": True,
            "has_transaction_history": True,
        }

        with patch("app.data_service.ensure_portfolio_schema", return_value=["dwd.account_data.sql"]), patch(
            "app.data_service.get_portfolio_storage_status",
            side_effect=[status, status],
        ), patch("api_data_to_database.save_to_database") as mocked_save:
            payload = data_service.refresh_portfolio_data()

        self.assertFalse(payload["refreshed"])
        self.assertIsNone(payload["summary"])
        mocked_save.assert_not_called()


if __name__ == "__main__":
    unittest.main()
