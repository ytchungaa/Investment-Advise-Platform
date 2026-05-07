from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime, timedelta
from typing import Any

from api_data_to_database import save_to_database
from daily_update import DEFAULT_PRICE_HISTORY_WORKERS, stock_list_market_data
from logging_config import logger


DEFAULT_ACCOUNT_LOOKBACK_HOURS = 24


def _format_utc_timestamp(timestamp: datetime) -> str:
    return timestamp.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def run_account_update(lookback_hours: int = DEFAULT_ACCOUNT_LOOKBACK_HOURS) -> dict[str, int]:
    if lookback_hours < 1:
        raise ValueError("lookback_hours must be at least 1.")

    end_timestamp = datetime.now(tz=UTC)
    start_timestamp = end_timestamp - timedelta(hours=lookback_hours)
    logger.info(
        "Starting hourly account update with %s-hour history overlap: %s to %s",
        lookback_hours,
        _format_utc_timestamp(start_timestamp),
        _format_utc_timestamp(end_timestamp),
    )
    return save_to_database(
        start_date=start_timestamp,
        end_date=end_timestamp,
        fetch_history=True,
    )


def run_market_update(max_price_history_workers: int = DEFAULT_PRICE_HISTORY_WORKERS) -> dict[str, Any]:
    logger.info(
        "Starting hourly market update with %s price-history workers.",
        max_price_history_workers,
    )
    return stock_list_market_data(max_price_history_workers=max_price_history_workers)


def _print_summary(summary: dict[str, Any]) -> None:
    print(json.dumps(summary, indent=2, sort_keys=True, default=str))


def main() -> None:
    parser = argparse.ArgumentParser(description="Hourly account and market data update entrypoint.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    account_parser = subparsers.add_parser("account", help="Update account snapshots and history.")
    account_parser.add_argument(
        "--lookback-hours",
        type=int,
        default=DEFAULT_ACCOUNT_LOOKBACK_HOURS,
        help="Hours of overlapping order and transaction history to retrieve.",
    )

    market_parser = subparsers.add_parser("market", help="Update market ODS data.")
    market_parser.add_argument(
        "--max-price-history-workers",
        type=int,
        default=DEFAULT_PRICE_HISTORY_WORKERS,
        help="Maximum worker threads for minute price-history retrieval.",
    )

    args = parser.parse_args()
    if args.command == "account":
        _print_summary(run_account_update(lookback_hours=args.lookback_hours))
    elif args.command == "market":
        _print_summary(
            run_market_update(max_price_history_workers=args.max_price_history_workers)
        )


if __name__ == "__main__":
    main()
