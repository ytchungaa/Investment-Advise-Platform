from __future__ import annotations

from pathlib import Path
import unittest
from unittest.mock import patch

from app.data_service import StockNotFoundError
from app.server import create_app


ROOT_DIR = Path(__file__).resolve().parents[1]


class ServerApiTests(unittest.TestCase):
    def setUp(self):
        self.app = create_app()
        self.client = self.app.test_client()

    @patch("app.server.get_stock_visualization_data")
    def test_valid_symbol_with_daily_data(self, mocked_service):
        mocked_service.return_value = {
            "symbol": "AAPL",
            "description": "Apple Inc.",
            "interval": "daily",
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
            "summary": {"price_points": 2},
            "message": None,
        }

        response = self.client.get(
            "/api/stock-data?symbol=AAPL&start_date=2025-04-15&end_date=2026-04-15&interval=daily"
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["symbol"], "AAPL")
        mocked_service.assert_called_once_with(
            symbol="AAPL",
            start_date="2025-04-15",
            end_date="2026-04-15",
            interval="daily",
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
            "interval": "daily",
            "available_range": {"start": "2023-01-03", "end": "2026-04-15"},
            "selected_range": {"start": "2020-01-01", "end": "2020-12-31"},
            "timeseries": [],
            "summary": {"price_points": 0},
            "message": "No daily price history is available for the selected range.",
        }

        response = self.client.get(
            "/api/stock-data?symbol=AAPL&start_date=2020-01-01&end_date=2020-12-31"
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["timeseries"], [])
        self.assertIsNotNone(response.get_json()["message"])

    @patch("app.server.get_stock_visualization_data")
    def test_default_date_range_is_used_when_query_params_are_omitted(self, mocked_service):
        mocked_service.return_value = {
            "symbol": "AAPL",
            "description": "Apple Inc.",
            "interval": "hourly",
            "available_range": {"start": "2025-01-01", "end": "2026-04-15"},
            "selected_range": {"start": "2025-04-15", "end": "2026-04-15"},
            "timeseries": [],
            "summary": {"price_points": 0},
            "message": None,
        }

        response = self.client.get("/api/stock-data?symbol=AAPL")

        self.assertEqual(response.status_code, 200)
        mocked_service.assert_called_once_with(
            symbol="AAPL",
            start_date=None,
            end_date=None,
            interval=None,
        )

    @patch("app.server.get_stock_visualization_data")
    def test_invalid_interval_returns_400(self, mocked_service):
        mocked_service.side_effect = ValueError("interval must be one of: daily, hourly.")

        response = self.client.get("/api/stock-data?symbol=AAPL&interval=weekly")

        self.assertEqual(response.status_code, 400)
        self.assertIn("interval", response.get_json()["message"])

    @patch("app.server.refresh_stock_data")
    def test_refresh_stock_data_returns_summary(self, mocked_refresh):
        mocked_refresh.return_value = {
            "symbol": "AAPL",
            "instrument_rows": 1,
            "quote_rows": 1,
            "fundamental_rows": 1,
            "price_rows": 24,
            "dwd_refresh_files": [
                "dwd.price_history_hourly_incremental_upsert.sql",
                "dwd.price_history_daily_incremental_upsert.sql",
            ],
            "refreshed_at": "2026-05-05T14:00:00Z",
        }

        response = self.client.post("/api/refresh-stock-data", json={"symbol": "aapl"})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["summary"]["symbol"], "AAPL")
        self.assertIn("Latest data loaded", response.get_json()["message"])
        mocked_refresh.assert_called_once_with(symbol="aapl")

    @patch("app.server.refresh_stock_data")
    def test_refresh_missing_symbol_returns_404(self, mocked_refresh):
        mocked_refresh.side_effect = StockNotFoundError("Symbol 'MISSING' was not found.")

        response = self.client.post("/api/refresh-stock-data", json={"symbol": "MISSING"})

        self.assertEqual(response.status_code, 404)
        self.assertIn("not found", response.get_json()["message"])

    @patch("app.server.search_symbols")
    def test_symbol_search_returns_suggestions(self, mocked_search):
        mocked_search.return_value = [
            {"symbol": "AAPL", "description": "Apple Inc."},
            {"symbol": "AAP", "description": "Advance Auto Parts"},
        ]

        response = self.client.get("/api/symbol-search?q=aa&interval=daily")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["results"][0]["symbol"], "AAPL")
        mocked_search.assert_called_once_with(query="aa", interval="daily")

    @patch("app.server.search_symbols")
    def test_symbol_search_invalid_interval_returns_400(self, mocked_search):
        mocked_search.side_effect = ValueError("interval must be one of: daily, hourly.")

        response = self.client.get("/api/symbol-search?q=aa&interval=weekly")

        self.assertEqual(response.status_code, 400)
        self.assertIn("interval", response.get_json()["message"])

    @patch("app.server.get_portfolio_data")
    def test_portfolio_data_returns_dashboard_payload(self, mocked_portfolio):
        mocked_portfolio.return_value = {
            "as_of_time": "2026-05-06T13:00:00+00:00",
            "benchmark_symbol": "SPY",
            "summary": {
                "total_portfolio_value": 1200.0,
                "total_cash": 200.0,
                "portfolio_return_1y": 0.14,
                "benchmark_return_1y": 0.10,
                "alpha_1y": 0.04,
                "beta_1y": 1.1,
            },
            "balance": {"account_count": 1},
            "allocation": [{"category": "Stocks", "market_value": 600.0, "weight": 0.5}],
            "holdings": [{"symbol": "AAPL", "market_value": 600.0}],
            "warnings": [],
        }

        response = self.client.get("/api/portfolio-data")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["benchmark_symbol"], "SPY")
        self.assertEqual(response.get_json()["summary"]["alpha_1y"], 0.04)
        mocked_portfolio.assert_called_once_with()

    @patch("app.server.get_portfolio_data")
    def test_portfolio_data_returns_502_for_service_failure(self, mocked_portfolio):
        mocked_portfolio.side_effect = RuntimeError("database unavailable")

        response = self.client.get("/api/portfolio-data")

        self.assertEqual(response.status_code, 502)
        self.assertIn("Failed to load portfolio data", response.get_json()["message"])

    @patch("app.server.refresh_portfolio_data")
    def test_refresh_portfolio_data_returns_summary(self, mocked_refresh):
        mocked_refresh.return_value = {
            "refreshed": True,
            "schema_files": ["ods.account_data.sql", "dwd.account_data.sql"],
            "before": {"has_portfolio_snapshots": False},
            "after": {"has_portfolio_snapshots": True},
            "summary": {"positions": 2},
        }

        response = self.client.post(
            "/api/refresh-portfolio-data",
            json={"force_refresh": True},
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn("refreshed from Schwab", response.get_json()["message"])
        self.assertEqual(response.get_json()["summary"]["summary"]["positions"], 2)
        mocked_refresh.assert_called_once_with(force_refresh=True)

    @patch("app.server.refresh_portfolio_data")
    def test_refresh_portfolio_data_returns_502_for_failure(self, mocked_refresh):
        mocked_refresh.side_effect = RuntimeError("Schwab unavailable")

        response = self.client.post("/api/refresh-portfolio-data", json={})

        self.assertEqual(response.status_code, 502)
        self.assertIn("Failed to refresh portfolio data", response.get_json()["message"])


class StaticPortfolioUiTests(unittest.TestCase):
    def test_portfolio_tab_shell_exists(self):
        html = (ROOT_DIR / "app/static/index.html").read_text()

        self.assertIn('data-app-tab="portfolio"', html)
        self.assertIn('id="portfolio-kpi-grid"', html)
        self.assertIn('id="portfolio-allocation-chart"', html)
        self.assertIn('id="portfolio-history-chart"', html)
        self.assertIn('id="portfolio-risk-panel"', html)
        self.assertIn('id="portfolio-balance-panel"', html)
        self.assertIn('id="portfolio-holdings-body"', html)
        self.assertIn('data-holdings-sort="market_value"', html)
        self.assertNotIn("<th>Name</th>", html)
        self.assertNotIn("<th>As Of</th>", html)
        self.assertIn("Refresh data", html)

    def test_portfolio_frontend_fetches_and_renders_dashboard_regions(self):
        js = (ROOT_DIR / "app/static/main.js").read_text()

        self.assertIn('fetch("/api/portfolio-data")', js)
        self.assertIn('fetch("/api/refresh-portfolio-data"', js)
        self.assertIn("function loadPortfolioData", js)
        self.assertIn("function refreshPortfolioData", js)
        self.assertIn("function renderPortfolioHistory", js)
        self.assertIn("function sortedPortfolioHoldings", js)
        self.assertIn('portfolioHoldingsSort = { key: "category_weight", direction: "desc" }', js)
        self.assertIn('compareHoldingValues(left, right, "weight")', js)
        self.assertIn("Plotly.newPlot", js)
        self.assertIn("portfolioAllocationChart", js)
        self.assertIn("portfolioHistoryChart", js)
        self.assertIn("portfolioRiskPanel", js)
        self.assertIn("portfolioBalancePanel", js)
        self.assertIn("portfolioHoldingsBody", js)


if __name__ == "__main__":
    unittest.main()
