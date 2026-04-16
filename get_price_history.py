import requests
import os
from schwab_auth import SchwabAuth
from logging_config import logger
import pandas as pd  # Ensure pandas is imported and 'pd' is not redefined elsewhere
import datetime
import sqlalchemy as sa
from database_connect import connector
from get_market_data import get_price_history_frequency_type_id


class schwab_api_market:
    def __init__(self):
        self.db_ods = connector(schema='ods')
        self.access_token = SchwabAuth().get_token()
        self.server_link = 'https://api.schwabapi.com/marketdata/v1'

    def get_price_history(
            self, 
            symbol: str, 
            period_type: str, 
            period: str, 
            frequency_type: str, 
            frequency: str,
            start_date: str,
            end_date: str,
            need_extended_hours_data: bool=True,
            need_previous_close: bool=True
            ):
        valid_periods = {
            'day': ['1', '2', '3', '4', '5', '10'],
            'month': ['1', '2', '3', '6'],
            'year': ['1', '2', '3', '5', '10', '15', '20'],
            'ytd': ['1']
        }
        valid_frequencies = {
            'minute': ['1', '5', '10', '15', '30'],
            'daily': ['1'],
            'weekly': ['1'],
            'monthly': ['1']
        }
        frequency_type_id = get_price_history_frequency_type_id(frequency_type)
        if period_type not in ['day', 'month', 'year', 'ytd'] or period not in valid_periods.get(period_type, []):
            raise ValueError("period_type must be one of 'day', 'month', 'year', 'ytd' with valid period values")
        if frequency_type not in ['minute', 'daily', 'weekly', 'monthly'] or frequency not in valid_frequencies.get(frequency_type, []):
            raise ValueError("frequency_type must be one of 'minute', 'daily', 'weekly', 'monthly' with valid frequency values")

        start_timestamp = int(datetime.datetime.strptime(start_date, '%Y-%m-%d').timestamp() * 1000)
        end_timestamp = int(datetime.datetime.strptime(end_date, '%Y-%m-%d').timestamp() * 1000)
        response = requests.get(f'{self.server_link}/pricehistory',
                                headers={'Authorization': f'Bearer {self.access_token}'},
                                params={'symbol': symbol, 
                                        'periodType': period_type, 
                                        'period': period, 
                                        'frequencyType': frequency_type, 
                                        'frequency': frequency,
                                        'startDate': start_timestamp,
                                        'endDate': end_timestamp,
                                        'needExtendedHoursData': need_extended_hours_data,
                                        'needPreviousClose': need_previous_close
                                        })
        if response.status_code != 200:
            logger.error(f"Error fetching price history: {response.status_code} - {response.text}")
            return pd.DataFrame()  # Make sure 'pd' is pandas and not a local variable
        df = pd.json_normalize(response.json()['candles'])
        df['symbol'] = response.json().get('symbol', symbol)
        df['frequency_type'] = frequency_type_id
        df['frequency'] = int(frequency)
        return df

if __name__ == "__main__":
    api = schwab_api_market()
    # df = api.get_instrument_fundamental(symbol=['VOO'])
    # df = api.get_instruments(symbols=['VOO'])
    df = api.get_price_history(symbol='AAPL', period_type='month', period='1', frequency_type='daily', frequency='1', start_date='2025-01-01', end_date='2025-01-31', need_extended_hours_data=True, need_previous_close=True)

    print(df)
