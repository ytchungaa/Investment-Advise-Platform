import datetime as dt
from typing import Any

import requests

from logging_config import logger
from schwab_auth import SchwabAuth


class SchwabApi:
    """Read-only Schwab Trader API client for account data ingestion."""

    def __init__(
        self,
        access_token: str | None = None,
        auth: SchwabAuth | None = None,
        base_url: str = "https://api.schwabapi.com/trader/v1",
        session: requests.Session | None = None,
    ):
        self.auth = auth or SchwabAuth()
        self.access_token = access_token or self.auth.get_token()
        self.base_url = base_url.rstrip("/")
        self.session = session or requests.Session()

    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.access_token}"}

    def _format_datetime(self, value: str | dt.date | dt.datetime | None) -> str | None:
        if value is None:
            return None
        if isinstance(value, str):
            return value
        if isinstance(value, dt.datetime):
            timestamp = value
        elif isinstance(value, dt.date):
            timestamp = dt.datetime.combine(value, dt.time.min)
        else:
            timestamp = value

        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=dt.timezone.utc)
        timestamp = timestamp.astimezone(dt.timezone.utc)
        return timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")

    def _get_json(self, path: str, params: dict[str, Any] | None = None) -> Any:
        url = f"{self.base_url}/{path.lstrip('/')}"
        request_params = {
            key: value for key, value in (params or {}).items() if value is not None
        }
        response = self.session.get(url, headers=self._headers(), params=request_params)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            logger.error(
                "Schwab API request failed: %s %s - %s",
                response.status_code,
                url,
                response.text,
            )
            raise
        return response.json()

    def get_account_numbers(self) -> list[dict[str, Any]]:
        return self._get_json("/accounts/accountNumbers")

    def get_accounts(self, fields: str | None = "positions") -> list[dict[str, Any]]:
        return self._get_json("/accounts", params={"fields": fields})

    def get_account(
        self,
        account_hash: str,
        fields: str | None = "positions",
    ) -> dict[str, Any]:
        if not account_hash:
            raise ValueError("account_hash is required")
        return self._get_json(f"/accounts/{account_hash}", params={"fields": fields})

    def get_orders(
        self,
        account_hash: str,
        from_entered_time: str | dt.date | dt.datetime,
        to_entered_time: str | dt.date | dt.datetime,
        max_results: int = 3000,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        if not account_hash:
            raise ValueError("account_hash is required")
        return self._get_json(
            f"/accounts/{account_hash}/orders",
            params={
                "fromEnteredTime": self._format_datetime(from_entered_time),
                "toEnteredTime": self._format_datetime(to_entered_time),
                "maxResults": max_results,
                "status": status,
            },
        )

    def get_transactions(
        self,
        account_hash: str,
        start_date: str | dt.date | dt.datetime,
        end_date: str | dt.date | dt.datetime,
        types: str | None = None,
        symbol: str | None = None,
    ) -> list[dict[str, Any]]:
        if not account_hash:
            raise ValueError("account_hash is required")
        return self._get_json(
            f"/accounts/{account_hash}/transactions",
            params={
                "startDate": self._format_datetime(start_date),
                "endDate": self._format_datetime(end_date),
                "types": types,
                "symbol": symbol,
            },
        )

    def get_api_data(
        self,
        endpoint: str = "",
        params: dict[str, Any] | None = None,
        url: str = "",
    ) -> Any:
        """Compatibility shim for older scripts.

        New code should call the explicit get_* methods above. Account-scoped
        endpoints must pass account_hash in params to avoid empty-hash URLs.
        """
        params = dict(params or {})
        if url:
            return self.session.get(url, headers=self._headers(), params=params).json()
        if endpoint == "account_numbers":
            return self.get_account_numbers()
        if endpoint == "accounts":
            account_hash = params.pop("account_hash", None)
            fields = params.pop("fields", "positions")
            if account_hash:
                return self.get_account(account_hash, fields=fields)
            return self.get_accounts(fields=fields)
        if endpoint == "orders":
            return self.get_orders(
                account_hash=params["account_hash"],
                from_entered_time=params["fromEnteredTime"],
                to_entered_time=params["toEnteredTime"],
                max_results=params.get("maxResults", 3000),
                status=params.get("status"),
            )
        if endpoint == "transactions":
            return self.get_transactions(
                account_hash=params["account_hash"],
                start_date=params["startDate"],
                end_date=params["endDate"],
                types=params.get("types"),
                symbol=params.get("symbol"),
            )

        logger.error("Invalid endpoint: %s", endpoint)
        return {
            "error": "Invalid endpoint",
            "status_code": 400,
            "message": "Endpoint not found",
        }


if __name__ == "__main__":
    api = SchwabApi()
    print(api.get_account_numbers())
