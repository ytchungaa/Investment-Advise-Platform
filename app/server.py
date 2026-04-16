from __future__ import annotations

from flask import Flask, jsonify, request, send_from_directory

try:
    from .data_service import (
        StockNotFoundError,
        get_stock_visualization_data,
        search_symbols,
    )
except ImportError:  # pragma: no cover - supports `python3 app/server.py`
    from data_service import StockNotFoundError, get_stock_visualization_data, search_symbols


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

        try:
            payload = get_stock_visualization_data(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
            )
        except StockNotFoundError as exc:
            return jsonify({"message": str(exc)}), 404
        except ValueError as exc:
            return jsonify({"message": str(exc)}), 400

        return jsonify(payload)

    @app.get("/api/symbol-search")
    def symbol_search():
        query = request.args.get("q", "")
        return jsonify({"results": search_symbols(query=query)})

    return app


app = create_app()


if __name__ == "__main__":
    app.run(debug=True)
