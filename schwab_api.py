import requests
import os
from schwab_auth import SchwabAuth
from logging_config import logger
import pandas as pd
import datetime
import sqlalchemy as sa
from typing import List

class SchwabApi:
    def __init__(self, access_token=None, account_number:int = 0):
        self.access_token = access_token or SchwabAuth().get_token()
        self.account_number_hash = '' ## Got this from database
        self.server_mapping = {
        'trader' : 'https://api.schwabapi.com/trader/v1',
        'market' : 'https://api.schwabapi.com/marketdata/v1'
        }
        self.url_mapping = {
            'account_numbers': f"{self.server_mapping['trader']}/accounts/accountNumbers",
            'accounts': f"{self.server_mapping['trader']}/accounts/{self.account_number_hash}",
            'orders': f"{self.server_mapping['trader']}/accounts/{self.account_number_hash}/orders",
            'transactions': f"{self.server_mapping['trader']}/accounts/{self.account_number_hash}/transactions",
        }
        self.url_keys = self.url_mapping.keys()
        
        self.default_params = {
            'accounts': {'fields': 'positions'},
            'orders': {'fromEnteredTime': (datetime.datetime.now() - datetime.timedelta(days=1)).isoformat(),
                       'toEnteredTime': datetime.datetime.now().isoformat()},
        }

    def get_api_data(self, endpoint:str='', params:dict = {}, url:str='') -> dict:
        if endpoint not in self.url_keys and url == '':
            logger.error(f"Invalid endpoint: {endpoint}. Available endpoints: {', '.join(self.url_keys)}")
            return {"error": "Invalid endpoint", "status_code": 400, "message": "Endpoint not found"}
        elif endpoint not in self.url_keys and url != '':
            logger.info(f"Using custom URL: {url}")
            url = url
        else:
            url = self.url_mapping[endpoint]
            
        headers = {"Authorization": f"Bearer {self.access_token}"}
        if params == {} and endpoint in self.default_params:
            params = self.default_params[endpoint]
            
        r = requests.get(url, headers=headers, params=params)
        try:
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error occurred: {e.response.status_code} - {e.response.text}")
            return {"error": str(e), "status_code": e.response.status_code, "message": e.response.text}
        

        
if __name__ == '__main__':
    api = SchwabApi()
    data = api.get_api_data('accounts')
    print(data)
