from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from get_market_data import schwab_api_market
from schwab_auth import SchwabAuth


class SchwabAuthTests(unittest.TestCase):
    @patch("schwab_auth.dotenv.set_key")
    def test_refresh_updates_process_environment(self, mocked_set_key):
        with patch.dict(
            os.environ,
            {
                "SCHWAB_CLIENT_ID": "client-id",
                "SCHWAB_CLIENT_SECRET": "client-secret",
                "SCHWAB_REFRESH_TOKEN": "refresh-token",
                "SCHWAB_ACCESS_TOKEN": "expired-token",
                "SCHWAB_ACCESS_TOKEN_EXPIRES_TIMES": "2026-01-01 00:00:00",
                "SCHWAB_REFRESH_TOKEN_EXPIRES_TIMES": "2026-01-08 00:00:00",
            },
            clear=False,
        ):
            auth = SchwabAuth()
            with patch.object(
                auth,
                "_post_token",
                return_value=({"access_token": "fresh-token", "expires_in": 1800}, True),
            ):
                refreshed = auth.get_access_token()
                self.assertEqual(os.environ["SCHWAB_ACCESS_TOKEN"], "fresh-token")

        self.assertTrue(refreshed)
        self.assertEqual(auth.access_token, "fresh-token")
        mocked_set_key.assert_any_call(auth.dotenv_path, "SCHWAB_ACCESS_TOKEN", "fresh-token")


class SchwabApiMarketTests(unittest.TestCase):
    def test_explicit_access_token_skips_initial_refresh(self):
        with patch("get_market_data.SchwabAuth") as mocked_auth_cls:
            mocked_auth = mocked_auth_cls.return_value
            api = schwab_api_market(access_token="shared-token")

        self.assertEqual(api.access_token, "shared-token")
        mocked_auth.get_token.assert_not_called()


if __name__ == "__main__":
    unittest.main()
