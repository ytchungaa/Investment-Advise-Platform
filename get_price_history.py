from get_market_data import (
    get_price_history_frequency_type_id,
    schwab_api_market as _CanonicalSchwabApiMarket,
)


class schwab_api_market(_CanonicalSchwabApiMarket):
    """Compatibility wrapper for older imports.

    Use get_market_data.schwab_api_market for new code.
    """


if __name__ == "__main__":
    api = schwab_api_market()
    df = api.get_price_history(
        symbol="AAPL",
        period_type="month",
        period="1",
        frequency_type="daily",
        frequency="1",
        start_date="2025-01-01",
        end_date="2025-01-31",
        need_extended_hours_data=True,
        need_previous_close=True,
    )
    print(df)
