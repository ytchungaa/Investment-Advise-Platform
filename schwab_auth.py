import requests
import os
import base64
import urllib.parse
from datetime import datetime, timedelta
import dotenv
from requests.exceptions import HTTPError
from logging_config import logger
import json

class SchwabAuth:
    def __init__(self, redirect_uri = "https://127.0.0.1"):
        dotenv.load_dotenv(override=True)
        self.dotenv_path = dotenv.find_dotenv()
        self.client_id = os.getenv("SCHWAB_CLIENT_ID")
        self.client_secret = os.getenv("SCHWAB_CLIENT_SECRET")
        self.refresh_token = os.getenv("SCHWAB_REFRESH_TOKEN")
        self.access_token = os.getenv("SCHWAB_ACCESS_TOKEN")
        self.access_token_expire = os.getenv("SCHWAB_ACCESS_TOKEN_EXPIRES_TIMES")
        self.refresh_token_expire = os.getenv("SCHWAB_REFRESH_TOKEN_EXPIRES_TIMES")
        self.redirect_uri = redirect_uri

    def _persist_env_value(self, key: str, value: str) -> None:
        dotenv.set_key(self.dotenv_path, key, value)
        os.environ[key] = value

    def update_client_id_secret(self,) -> bool:
        client_id = input("Enter your Schwab Client ID: ").strip()
        client_secret = input("Enter your Schwab Client Secret: ").strip()
        self._persist_env_value("SCHWAB_CLIENT_ID", client_id)
        self._persist_env_value("SCHWAB_CLIENT_SECRET", client_secret)
        self.client_id = client_id
        self.client_secret = client_secret
        return True
    
    def _post_token(self, url: str, data: dict) -> tuple[dict, bool]:
        resp = None
        try:
            resp = requests.post(url, data=data, headers={
                'Authorization': f'Basic {base64.b64encode(bytes(f"{self.client_id}:{self.client_secret}", "utf-8")).decode("utf-8")}',
                'Content-Type': 'application/x-www-form-urlencoded'
            }, timeout=(5, 20))
            resp.raise_for_status()  # <-- fail fast on non-2xx
            return resp.json(), True     # safe to parse now
        except HTTPError as e:
            logger.error(f"HTTP error occurred: {e.response.status_code} - {e.response.text}")
            return json.loads(e.response.text), False
        
    def get_refresh_token(self) -> bool:

        # 1. Construct the authorization URL with response_type=code
        auth_url = f"https://api.schwabapi.com/v1/oauth/authorize?client_id={self.client_id}&redirect_uri={self.redirect_uri}"

        logger.info(f"Navigate to this URL to authenticate: {auth_url}")

        # 2. Capture the redirected URL from user input
        returned_link = input("Paste the redirect URL here:")

        # 3. Parse the 'code' parameter from the redirect
        parsed_url = urllib.parse.urlparse(returned_link)
        parsed_query = urllib.parse.parse_qs(parsed_url.query)
        auth_code = parsed_query.get('code', [None])[0]

        if not auth_code:
            logger.error("Could not find 'code' parameter in the redirect URL.")
            return False

        # 4. Exchange the authorization code for access tokens
        token_url = "https://api.schwabapi.com/v1/oauth/token"

        # Option A: Pass client_id and client_secret in the POST body
        data = {
            "grant_type": "authorization_code",
            "code": auth_code,
            "redirect_uri": self.redirect_uri,
        }
        
        post_result, to_prase = self._post_token(token_url, data)

        # 5. Extract tokens
        
        if to_prase:
            access_token_expire = (datetime.now() + timedelta(seconds=post_result["expires_in"])).strftime("%Y-%m-%d %H:%M:%S")
            self._persist_env_value("SCHWAB_ACCESS_TOKEN", post_result["access_token"])
            self._persist_env_value("SCHWAB_ACCESS_TOKEN_EXPIRES_TIMES", access_token_expire)
            self.access_token = post_result["access_token"]
            self.access_token_expire = access_token_expire

            refresh_token_expire = (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
            self._persist_env_value("SCHWAB_REFRESH_TOKEN", post_result["refresh_token"])
            self._persist_env_value("SCHWAB_REFRESH_TOKEN_EXPIRES_TIMES", refresh_token_expire)
            self.refresh_token = post_result["refresh_token"]
            self.refresh_token_expire = refresh_token_expire
            logger.info(f"Access token obtained successfully. Expires at {access_token_expire}")
            logger.info(f"Refresh token obtained successfully. Expires at {refresh_token_expire}")
            return True
        else:
            logger.error("Error exchanging authorization code. Please check your credentials and try again.")
            return False

    def get_access_token(self,) -> bool:

        token_url = "https://api.schwabapi.com/v1/oauth/token"

        data = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
        }

        post_result, to_prase = self._post_token(token_url, data)

        if to_prase:
            access_token_expire = (datetime.now() + timedelta(seconds=post_result['expires_in'])).strftime("%Y-%m-%d %H:%M:%S")
            self._persist_env_value("SCHWAB_ACCESS_TOKEN", post_result['access_token'])
            self._persist_env_value("SCHWAB_ACCESS_TOKEN_EXPIRES_TIMES", access_token_expire)
            self.access_token = post_result['access_token']
            self.access_token_expire = access_token_expire
            logger.info(f"Access token refreshed successfully. Expires at {access_token_expire}")
            return True
        elif post_result.get("error") == "unsupported_token_type":
            logger.error("Unsupported token type. Please re-authenticate to get a new access token.")
            return self.get_refresh_token()
        else:
            logger.error("Error refreshing token")
            return False

    def get_token(self):
        """
        Refreshes tokens if expired and returns the latest access token.
        """
        client_id_check = False
        refresh_token_check = False
        access_token_check = False
        
        if not self.client_id or not self.client_secret:
            logger.info("Client ID or Secret missing. Updating...")
            client_id_check = self.update_client_id_secret()
        else:
            client_id_check = True
            
        if self.refresh_token_expire is not None:
            expire_time = datetime.strptime(self.refresh_token_expire, "%Y-%m-%d %H:%M:%S")
            if datetime.now() > expire_time:
                logger.info("Refresh token is about to expire. Refreshing...")
                refresh_token_check = self.get_refresh_token()
            elif datetime.now() > expire_time - timedelta(days=2):
                logger.warning(f"Refresh token expires soon: {expire_time}")
                refresh_token_check = True
            else:
                logger.info(f"Refresh token is valid until {expire_time}")
                refresh_token_check = True
        else:
            logger.info("Refresh token expiration time is missing. Refreshing...")
            refresh_token_check = self.get_refresh_token()

        if self.access_token_expire is not None:
            expire_time = datetime.strptime(self.access_token_expire, "%Y-%m-%d %H:%M:%S")
            if datetime.now() > expire_time - timedelta(seconds=180):
                logger.info("Access token is about to expire. Refreshing...")
                access_token_check = self.get_access_token()
            else:
                logger.info(f"Access token is valid until {expire_time}")
                access_token_check = True
        else:
            logger.info("Access token expiration time is missing. Refreshing...")
            access_token_check = self.get_access_token()

        if not (client_id_check and refresh_token_check and access_token_check):
            logger.error("Failed to get access tokens. Please check your credentials and try again.")
            return None
        else:
            logger.info("Tokens are valid and up-to-date.")
            return self.access_token

if __name__ == "__main__":
    # Example usage
    auth = SchwabAuth()
    token = auth.get_token()
    print(token)
    
    # print(os.getenv("SCHWAB_CLIENT_ID"))
    # print(os.getenv("SCHWAB_CLIENT_SECRET"))
    # print(os.getenv("SCHWAB_REFRESH_TOKEN"))
    
    # Uncomment below to manually refresh tokens
    # auth.get_refresh_token()
    # auth.get_access_token()
    
