from __future__ import annotations

import unittest
from unittest.mock import patch

import pandas as pd

from app import data_service


class FakeConnector:
    def __init__(self, instrument_df: pd.DataFrame, price_df: pd.DataFrame):
        self.instrument_df = instrument_df
        self.price_df = price_df
        self.calls: list[tuple[str, dict]] = []

    def query_dataframe(self, query, params=None):
        normalized_query = " ".join(str(query).split())
        self.calls.append((normalized_query, params or {}))
        if "FROM instrument AS i" in normalized_query:
            if "SELECT i.symbol, i.description" in normalized_query:
                return self.instrument_df.copy()
            return self.instrument_df.copy()
        if "FROM price_history" in normalized_query:
            return self.price_df.copy()
        raise AssertionError(f"Unexpected query: {normalized_query}")


class DataServiceTests(unittest.TestCase):
    def test_default_date_range_uses_one_month_window_and_clips_to_available_data(self):
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
                    "trade_date": pd.Timestamp("2026-03-31").date(),
                    "open": 220.0,
                    "high": 225.0,
                    "low": 219.0,
                    "close": 224.0,
                    "volume": 100,
                },
                {
                    "trade_date": pd.Timestamp("2026-04-01").date(),
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
        self.assertEqual(payload["summary"]["price_points"], 2)
        self.assertNotIn("monthly_return_boxes", payload)

    def test_invalid_date_format_raises_value_error(self):
        with self.assertRaises(ValueError):
            data_service.get_stock_visualization_data("AAPL", start_date="04-15-2026")

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
            payload["message"], "No daily price history is available for the selected range."
        )
        self.assertEqual(payload["summary"]["price_points"], 0)

    def test_search_symbols_returns_matching_results(self):
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


if __name__ == "__main__":
    unittest.main()
