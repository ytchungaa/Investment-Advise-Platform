import datetime
from typing import Iterable

import pandas as pd
import requests

from logging_config import logger
from schwab_auth import SchwabAuth


class schwab_api_market:
    def __init__(self):
        self.auth = SchwabAuth()
        self.access_token = self.auth.get_token()
        self.server_link = "https://api.schwabapi.com/marketdata/v1"

    def _request(self, path: str, params: dict) -> dict:
        for attempt in range(2):
            response = requests.get(
                f"{self.server_link}{path}",
                headers={"Authorization": f"Bearer {self.access_token}"},
                params=params,
                timeout=30,
            )
            if response.status_code == 401 and attempt == 0:
                logger.warning(f"Received 401 for {path}. Refreshing token and retrying once.")
                self.access_token = self.auth.get_token()
                continue
            if response.status_code != 200:
                logger.error(f"Error fetching {path}: {response.status_code} - {response.text}")
                return {}
            return response.json()
        return {}

    @staticmethod
    def _chunk_symbols(symbols: str | list[str], chunksize: int) -> Iterable[list[str]]:
        normalized_symbols = symbols if isinstance(symbols, list) else [symbols]
        for i in range(0, len(normalized_symbols), chunksize):
            yield normalized_symbols[i : i + chunksize]

    @staticmethod
    def _now_utc() -> pd.Timestamp:
        return pd.Timestamp.now(tz="UTC")

    @staticmethod
    def _epoch_millis_to_timestamp(value) -> pd.Timestamp | None:
        if value is None:
            return pd.NaT
        if pd.isna(value):
            return pd.NaT
        if value == "":
            return pd.NaT
        return pd.to_datetime(value, unit="ms", utc=True, errors="coerce")

    @staticmethod
    def _parse_timestamp(value) -> pd.Timestamp | None:
        if value is None:
            return pd.NaT
        if pd.isna(value):
            return pd.NaT
        if value == "":
            return pd.NaT
        return pd.to_datetime(value, utc=True, errors="coerce")

    @staticmethod
    def _normalize_asset_type(asset_type: str | None, instrument_type: str | None = None) -> str | None:
        if asset_type == "COLLECTIVE_INVESTMENT" and instrument_type == "EXCHANGE_TRADED_FUND":
            return "ETF"
        return asset_type

    def fetch_instruments(self, symbols: str | list[str], chunksize: int = 500) -> pd.DataFrame:
        records: list[dict] = []
        chunksize = min(chunksize, 500)

        for chunk in self._chunk_symbols(symbols, chunksize):
            fetched_at = self._now_utc()
            payload = self._request(
                "/instruments",
                {"symbol": ",".join(chunk), "projection": "symbol-search"},
            )
            for instrument in payload.get("instruments", []):
                records.append(
                    {
                        "symbol": instrument.get("symbol"),
                        "asset_type": instrument.get("assetType"),
                        "cusip": instrument.get("cusip"),
                        "description": instrument.get("description"),
                        "exchange": instrument.get("exchange"),
                        "asset_main_type": None,
                        "asset_sub_type": None,
                        "quote_type": None,
                        "ssid": None,
                        "realtime": None,
                        "first_seen_at": fetched_at,
                        "last_seen_at": fetched_at,
                        "source_payload": instrument,
                    }
                )

        return pd.DataFrame(records)

    def fetch_instrument_fundamentals(
        self, symbols: str | list[str], chunksize: int = 500
    ) -> pd.DataFrame:
        records: list[dict] = []
        chunksize = min(chunksize, 500)

        for chunk in self._chunk_symbols(symbols, chunksize):
            fetched_at = self._now_utc()
            payload = self._request(
                "/instruments",
                {"symbol": ",".join(chunk), "projection": "fundamental"},
            )
            for instrument in payload.get("instruments", []):
                fundamental = instrument.get("fundamental", {})
                if not fundamental:
                    continue

                records.append(
                    {
                        "symbol": instrument.get("symbol"),
                        "asset_type": instrument.get("assetType"),
                        "as_of_time": fetched_at,
                        "week_52_high": fundamental.get("high52"),
                        "week_52_low": fundamental.get("low52"),
                        "dividend_amount": fundamental.get("dividendAmount"),
                        "dividend_yield": fundamental.get("dividendYield"),
                        "dividend_date": self._parse_timestamp(fundamental.get("dividendDate")),
                        "pe_ratio": fundamental.get("peRatio"),
                        "peg_ratio": fundamental.get("pegRatio"),
                        "pb_ratio": fundamental.get("pbRatio"),
                        "pr_ratio": fundamental.get("prRatio"),
                        "pcf_ratio": fundamental.get("pcfRatio"),
                        "gross_margin_ttm": fundamental.get("grossMarginTTM"),
                        "gross_margin_mrq": fundamental.get("grossMarginMRQ"),
                        "net_profit_margin_ttm": fundamental.get("netProfitMarginTTM"),
                        "net_profit_margin_mrq": fundamental.get("netProfitMarginMRQ"),
                        "operating_margin_ttm": fundamental.get("operatingMarginTTM"),
                        "operating_margin_mrq": fundamental.get("operatingMarginMRQ"),
                        "return_on_equity": fundamental.get("returnOnEquity"),
                        "return_on_assets": fundamental.get("returnOnAssets"),
                        "return_on_investment": fundamental.get("returnOnInvestment"),
                        "quick_ratio": fundamental.get("quickRatio"),
                        "current_ratio": fundamental.get("currentRatio"),
                        "interest_coverage": fundamental.get("interestCoverage"),
                        "total_debt_to_capital": fundamental.get("totalDebtToCapital"),
                        "lt_debt_to_equity": fundamental.get("ltDebtToEquity"),
                        "total_debt_to_equity": fundamental.get("totalDebtToEquity"),
                        "eps_ttm": fundamental.get("epsTTM"),
                        "eps_change_percent_ttm": fundamental.get("epsChangePercentTTM"),
                        "eps_change_year": fundamental.get("epsChangeYear"),
                        "eps_change": fundamental.get("epsChange"),
                        "rev_change_year": fundamental.get("revChangeYear"),
                        "rev_change_ttm": fundamental.get("revChangeTTM"),
                        "rev_change_in": fundamental.get("revChangeIn"),
                        "shares_outstanding": fundamental.get("sharesOutstanding"),
                        "market_cap_float": fundamental.get("marketCapFloat"),
                        "market_cap": fundamental.get("marketCap"),
                        "book_value_per_share": fundamental.get("bookValuePerShare"),
                        "short_int_to_float": fundamental.get("shortIntToFloat"),
                        "short_int_day_to_cover": fundamental.get("shortIntDayToCover"),
                        "div_growth_rate_3_year": fundamental.get("divGrowthRate3Year"),
                        "dividend_pay_amount": fundamental.get("dividendPayAmount"),
                        "dividend_pay_date": self._parse_timestamp(
                            fundamental.get("dividendPayDate")
                        ),
                        "beta": fundamental.get("beta"),
                        "vol_1_day_avg": fundamental.get("vol1DayAvg"),
                        "vol_10_day_avg": fundamental.get("vol10DayAvg"),
                        "vol_3_month_avg": fundamental.get("vol3MonthAvg"),
                        "avg_10_days_volume": fundamental.get("avg10DaysVolume"),
                        "avg_1_day_volume": fundamental.get("avg1DayVolume"),
                        "avg_3_month_volume": fundamental.get("avg3MonthVolume"),
                        "declaration_date": self._parse_timestamp(
                            fundamental.get("declarationDate")
                        ),
                        "dividend_freq": fundamental.get("dividendFreq"),
                        "eps": fundamental.get("eps"),
                        "dtn_volume": fundamental.get("dtnVolume"),
                        "next_dividend_pay_date": self._parse_timestamp(
                            fundamental.get("nextDividendPayDate")
                        ),
                        "next_dividend_date": self._parse_timestamp(
                            fundamental.get("nextDividendDate")
                        ),
                        "fund_leverage_factor": fundamental.get("fundLeverageFactor"),
                        "source_payload": fundamental,
                    }
                )

        return pd.DataFrame(records)

    def fetch_quotes(
        self, symbols: str | list[str], chunksize: int = 500
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        quote_records: list[dict] = []
        instrument_updates: list[dict] = []
        chunksize = min(chunksize, 500)

        for chunk in self._chunk_symbols(symbols, chunksize):
            fetched_at = self._now_utc()
            payload = self._request("/quotes", {"symbols": ",".join(chunk), "fields": "quote"})
            for symbol, quote_payload in payload.items():
                quote = quote_payload.get("quote", {})
                quote_time = self._epoch_millis_to_timestamp(quote.get("quoteTime"))
                trade_time = self._epoch_millis_to_timestamp(quote.get("tradeTime"))
                ask_time = self._epoch_millis_to_timestamp(quote.get("askTime"))
                bid_time = self._epoch_millis_to_timestamp(quote.get("bidTime"))

                instrument_updates.append(
                    {
                        "symbol": quote_payload.get("symbol", symbol),
                        "asset_main_type": quote_payload.get("assetMainType"),
                        "asset_sub_type": quote_payload.get("assetSubType"),
                        "quote_type": quote_payload.get("quoteType"),
                        "ssid": quote_payload.get("ssid"),
                        "realtime": quote_payload.get("realtime"),
                        "last_seen_at": fetched_at,
                        "source_payload": quote_payload,
                    }
                )

                quote_records.append(
                    {
                        "symbol": quote_payload.get("symbol", symbol),
                        "as_of_time": fetched_at,
                        "week_52_high": quote.get("52WeekHigh"),
                        "week_52_low": quote.get("52WeekLow"),
                        "ask_mic_id": quote.get("askMICId"),
                        "ask_price": quote.get("askPrice"),
                        "ask_size": quote.get("askSize"),
                        "ask_time": ask_time,
                        "bid_mic_id": quote.get("bidMICId"),
                        "bid_price": quote.get("bidPrice"),
                        "bid_size": quote.get("bidSize"),
                        "bid_time": bid_time,
                        "close_price": quote.get("closePrice"),
                        "high_price": quote.get("highPrice"),
                        "last_mic_id": quote.get("lastMICId"),
                        "last_price": quote.get("lastPrice"),
                        "last_size": quote.get("lastSize"),
                        "low_price": quote.get("lowPrice"),
                        "mark": quote.get("mark"),
                        "mark_change": quote.get("markChange"),
                        "mark_percent_change": quote.get("markPercentChange"),
                        "net_change": quote.get("netChange"),
                        "net_percent_change": quote.get("netPercentChange"),
                        "open_price": quote.get("openPrice"),
                        "post_market_change": quote.get("postMarketChange"),
                        "post_market_percent_change": quote.get("postMarketPercentChange"),
                        "quote_time": quote_time,
                        "trade_time": trade_time,
                        "security_status": quote.get("securityStatus"),
                        "total_volume": quote.get("totalVolume"),
                        "source_payload": quote_payload,
                    }
                )

        return pd.DataFrame(instrument_updates), pd.DataFrame(quote_records)

    def fetch_price_history(
        self,
        symbol: str,
        period_type: str,
        period: str,
        frequency_type: str,
        frequency: str,
        start_date: str,
        end_date: str,
        need_extended_hours_data: bool = True,
        need_previous_close: bool = True,
    ) -> pd.DataFrame:
        valid_periods = {
            "day": ["1", "2", "3", "4", "5", "10"],
            "month": ["1", "2", "3", "6"],
            "year": ["1", "2", "3", "5", "10", "15", "20"],
            "ytd": ["1"],
        }
        valid_frequencies = {
            "minute": ["1", "5", "10", "15", "30"],
            "daily": ["1"],
            "weekly": ["1"],
            "monthly": ["1"],
        }

        if period_type not in valid_periods or period not in valid_periods[period_type]:
            raise ValueError("period_type must be valid for the requested period")
        if frequency_type not in valid_frequencies or frequency not in valid_frequencies[frequency_type]:
            raise ValueError("frequency_type must be valid for the requested frequency")
        if frequency_type == "minute" and period_type != "day":
            raise ValueError("Schwab minute history requires period_type='day'")

        start_timestamp = int(
            datetime.datetime.strptime(start_date, "%Y-%m-%d").timestamp() * 1000
        )
        end_timestamp = int(
            datetime.datetime.strptime(end_date, "%Y-%m-%d").timestamp() * 1000
        )
        fetched_at = self._now_utc()
        payload = self._request(
            "/pricehistory",
            {
                "symbol": symbol,
                "periodType": period_type,
                "period": period,
                "frequencyType": frequency_type,
                "frequency": frequency,
                "startDate": start_timestamp,
                "endDate": end_timestamp,
                "needExtendedHoursData": need_extended_hours_data,
                "needPreviousClose": need_previous_close,
            },
        )

        candles = payload.get("candles", [])
        if not candles:
            return pd.DataFrame()

        records = []
        previous_close_time = self._epoch_millis_to_timestamp(payload.get("previousCloseDate"))
        for candle in candles:
            records.append(
                {
                    "symbol": payload.get("symbol", symbol),
                    "frequency_type": frequency_type,
                    "frequency": int(frequency),
                    "candle_time": self._epoch_millis_to_timestamp(candle.get("datetime")),
                    "open": candle.get("open"),
                    "high": candle.get("high"),
                    "low": candle.get("low"),
                    "close": candle.get("close"),
                    "volume": candle.get("volume"),
                    "previous_close": payload.get("previousClose"),
                    "previous_close_time": previous_close_time,
                    "need_extended_hours_data": need_extended_hours_data,
                    "source_payload": {
                        "symbol": payload.get("symbol", symbol),
                        "candle": candle,
                        "previousClose": payload.get("previousClose"),
                        "previousCloseDate": payload.get("previousCloseDate"),
                        "fetchedAt": fetched_at.isoformat(),
                    },
                }
            )

        return pd.DataFrame(records)

    def get_instruments(self, symbols: str | list[str], chunksize: int = 500) -> pd.DataFrame:
        return self.fetch_instruments(symbols=symbols, chunksize=chunksize)

    def get_instrument_fundamental(
        self, symbols: str | list[str], chunksize: int = 500
    ) -> pd.DataFrame:
        return self.fetch_instrument_fundamentals(symbols=symbols, chunksize=chunksize)

    def get_price_history(
        self,
        symbol: str,
        period_type: str,
        period: str,
        frequency_type: str,
        frequency: str,
        start_date: str,
        end_date: str,
        need_extended_hours_data: bool = True,
        need_previous_close: bool = True,
    ) -> pd.DataFrame:
        return self.fetch_price_history(
            symbol=symbol,
            period_type=period_type,
            period=period,
            frequency_type=frequency_type,
            frequency=frequency,
            start_date=start_date,
            end_date=end_date,
            need_extended_hours_data=need_extended_hours_data,
            need_previous_close=need_previous_close,
        )


if __name__ == "__main__":
    api = schwab_api_market()
    df = api.fetch_price_history(
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
    print(df.head())
