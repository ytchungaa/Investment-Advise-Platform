from __future__ import annotations

from flask import Flask, jsonify, request, send_from_directory

try:
    from .data_service import (
        StockNotFoundError,
        get_portfolio_data,
        get_stock_visualization_data,
        refresh_portfolio_data,
        refresh_stock_data,
        search_symbols,
    )
except ImportError:  # pragma: no cover - supports `python3 app/server.py`
    from data_service import (
        StockNotFoundError,
        get_portfolio_data,
        get_stock_visualization_data,
        refresh_portfolio_data,
        refresh_stock_data,
        search_symbols,
    )


def create_app() -> Flask:
    app = Flask(__name__, static_folder="static", static_url_path="/static")

    @app.get("/")
    def index():
        return send_from_directory(app.static_folder, "index.html")

    @app.get("/api/stock-data")
    def stock_data():
        symbol = request.args.get("symbol", "")
        start_date = request.args.get("start_date") or None
        end_date = request.args.get("end_date") or None
        interval = request.args.get("interval") or None

        try:
            payload = get_stock_visualization_data(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                interval=interval,
            )
        except StockNotFoundError as exc:
            return jsonify({"message": str(exc)}), 404
        except ValueError as exc:
            return jsonify({"message": str(exc)}), 400

        return jsonify(payload)

    @app.post("/api/refresh-stock-data")
    def refresh_stock():
        payload = request.get_json(silent=True) or {}
        symbol = payload.get("symbol") or request.args.get("symbol", "")

        try:
            summary = refresh_stock_data(symbol=symbol)
        except StockNotFoundError as exc:
            return jsonify({"message": str(exc)}), 404
        except ValueError as exc:
            return jsonify({"message": str(exc)}), 400
        except RuntimeError as exc:
            return jsonify({"message": str(exc)}), 502
        except Exception as exc:
            return jsonify({"message": f"Failed to refresh market data: {exc}"}), 502

        return jsonify(
            {
                "message": f"Latest data loaded for {summary['symbol']}.",
                "summary": summary,
            }
        )

    @app.get("/api/symbol-search")
    def symbol_search():
        query = request.args.get("q", "")
        interval = request.args.get("interval") or None
        try:
            results = search_symbols(query=query, interval=interval)
        except ValueError as exc:
            return jsonify({"message": str(exc)}), 400
        return jsonify({"results": results})

    @app.get("/api/portfolio-data")
    def portfolio_data():
        try:
            payload = get_portfolio_data()
        except ValueError as exc:
            return jsonify({"message": str(exc)}), 400
        except Exception as exc:
            return jsonify({"message": f"Failed to load portfolio data: {exc}"}), 502
        return jsonify(payload)

    @app.post("/api/refresh-portfolio-data")
    def refresh_portfolio():
        payload = request.get_json(silent=True) or {}
        force_refresh = bool(payload.get("force_refresh", False))
        try:
            summary = refresh_portfolio_data(force_refresh=force_refresh)
        except ValueError as exc:
            return jsonify({"message": str(exc)}), 400
        except Exception as exc:
            return jsonify({"message": f"Failed to refresh portfolio data: {exc}"}), 502
        return jsonify(
            {
                "message": (
                    "Portfolio data refreshed from Schwab."
                    if summary["refreshed"]
                    else "Portfolio data already exists in ODS/DWD."
                ),
                "summary": summary,
            }
        )

    return app


app = create_app()


if __name__ == "__main__":
    app.run(debug=True)
