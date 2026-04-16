from __future__ import annotations

import unittest
from unittest.mock import patch

from app.data_service import StockNotFoundError
from app.server import create_app


class ServerApiTests(unittest.TestCase):
    def setUp(self):
        self.app = create_app()
        self.client = self.app.test_client()

    @patch("app.server.get_stock_visualization_data")
    def test_valid_symbol_with_daily_data(self, mocked_service):
        mocked_service.return_value = {
            "symbol": "AAPL",
            "description": "Apple Inc.",
            "available_range": {"start": "2025-01-01", "end": "2026-04-15"},
            "selected_range": {"start": "2025-04-15", "end": "2026-04-15"},
            "timeseries": [
                {
                    "date": "2026-04-14",
                    "close": 210.5,
                    "open": 208.2,
                    "high": 211.0,
                    "low": 207.9,
                    "volume": 1234,
                },
                {
                    "date": "2026-04-15",
                    "close": 212.0,
                    "open": 210.0,
                    "high": 213.5,
                    "low": 209.8,
                    "volume": 1400,
                },
            ],
            "monthly_return_boxes": [
                {"month_number": 4, "month_label": "Apr", "returns": [0.01, -0.002]}
            ],
            "summary": {"price_points": 2, "return_points": 2},
            "message": None,
        }

        response = self.client.get(
            "/api/stock-data?symbol=AAPL&start_date=2025-04-15&end_date=2026-04-15"
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["symbol"], "AAPL")
        mocked_service.assert_called_once_with(
            symbol="AAPL", start_date="2025-04-15", end_date="2026-04-15"
        )

    @patch("app.server.get_stock_visualization_data")
    def test_missing_symbol_returns_404(self, mocked_service):
        mocked_service.side_effect = StockNotFoundError("Symbol 'MISSING' was not found.")

        response = self.client.get("/api/stock-data?symbol=MISSING")

        self.assertEqual(response.status_code, 404)
        self.assertIn("not found", response.get_json()["message"])

    @patch("app.server.get_stock_visualization_data")
    def test_valid_symbol_with_no_rows_in_selected_range(self, mocked_service):
        mocked_service.return_value = {
            "symbol": "AAPL",
            "description": "Apple Inc.",
            "available_range": {"start": "2023-01-03", "end": "2026-04-15"},
            "selected_range": {"start": "2020-01-01", "end": "2020-12-31"},
            "timeseries": [],
            "monthly_return_boxes": [],
            "summary": {"price_points": 0, "return_points": 0},
            "message": "No daily price history is available for the selected range.",
        }

        response = self.client.get(
            "/api/stock-data?symbol=AAPL&start_date=2020-01-01&end_date=2020-12-31"
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["timeseries"], [])
        self.assertIsNotNone(response.get_json()["message"])

    @patch("app.server.get_stock_visualization_data")
    def test_one_row_result_has_empty_monthly_return_boxes(self, mocked_service):
        mocked_service.return_value = {
            "symbol": "AAPL",
            "description": "Apple Inc.",
            "available_range": {"start": "2026-04-15", "end": "2026-04-15"},
            "selected_range": {"start": "2026-04-15", "end": "2026-04-15"},
            "timeseries": [
                {
                    "date": "2026-04-15",
                    "close": 212.0,
                    "open": 210.0,
                    "high": 213.5,
                    "low": 209.8,
                    "volume": 1400,
                }
            ],
            "monthly_return_boxes": [],
            "summary": {"price_points": 1, "return_points": 0},
            "message": "Return distribution is unavailable because fewer than two price points were found in the selected range.",
        }

        response = self.client.get(
            "/api/stock-data?symbol=AAPL&start_date=2026-04-15&end_date=2026-04-15"
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["monthly_return_boxes"], [])

    @patch("app.server.get_stock_visualization_data")
    def test_default_date_range_is_used_when_query_params_are_omitted(self, mocked_service):
        mocked_service.return_value = {
            "symbol": "AAPL",
            "description": "Apple Inc.",
            "available_range": {"start": "2025-01-01", "end": "2026-04-15"},
            "selected_range": {"start": "2025-04-15", "end": "2026-04-15"},
            "timeseries": [],
            "monthly_return_boxes": [],
            "summary": {"price_points": 0, "return_points": 0},
            "message": None,
        }

        response = self.client.get("/api/stock-data?symbol=AAPL")

        self.assertEqual(response.status_code, 200)
        mocked_service.assert_called_once_with(symbol="AAPL", start_date=None, end_date=None)

    @patch("app.server.search_symbols")
    def test_symbol_search_returns_suggestions(self, mocked_search):
        mocked_search.return_value = [
            {"symbol": "AAPL", "description": "Apple Inc."},
            {"symbol": "AAP", "description": "Advance Auto Parts"},
        ]

        response = self.client.get("/api/symbol-search?q=aa")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["results"][0]["symbol"], "AAPL")
        mocked_search.assert_called_once_with(query="aa")


if __name__ == "__main__":
    unittest.main()
