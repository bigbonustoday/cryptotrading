from datetime import datetime

import pandas as pd

import cryptotrading.poloneix_api as polo_api

# load polo account
from polo_account_info import POLO_KEY, POLO_SECRET
polo = polo_api.poloniex(APIKey=POLO_KEY, Secret=POLO_SECRET)


# all traded pairs
ALL_PAIRS = list(polo.returnTicker().keys())
ALL_CURRENCIES = list({x.split('_')[0] for x in ALL_PAIRS} | \
                      {x.split('_')[1] for x in ALL_PAIRS})

# default start, end dates in UNIX time, default sampling frequency
START = 0
END = 9999999999
FREQ = 7200  # freq = 2hr

def convert_to_df(input, time_key='date'):
    df = pd.DataFrame(input)

    if time_key in df.columns:
        df.index = [datetime.fromtimestamp(x) for x in df[time_key]]
        df = df.drop(time_key, 1)
    else:
        raise ValueError('time key not found!')

    return df

class dataBot():
    def __init__(self, region, home, freq=FREQ):
        self.home = home
        self.freq = freq
        self.region = region

        self.intraday_ti = None

        # caching intraday and daily returns
        self.get_intraday_data()

    def get_intraday_data(self):
        if self.intraday_ti is None:
            data_panel = {}
            for currency in self.region:
                data_panel[currency] = self._get_bars(currency)
            data_panel = pd.Panel(data_panel)
            self.intraday_ti = data_panel
        return self.intraday_ti


    def get_current_prices(self):
        prices = pd.Series()
        pair_info = polo.returnTicker()
        for currency in self.region:
            currencyPair = self.home + '_' + currency
            if currencyPair in ALL_PAIRS:
                currencyPair = self.home + '_' + currency
                price = float(pair_info[currencyPair]['last'])
            elif currency == self.home:
                price = 1
            else:
                raise ValueError(currencyPair + ' does not exist!')
            prices[currency] = price
        return prices


    def _get_bars(self, currency):
        currencyPair = self.home + '_' + currency
        if currencyPair in ALL_PAIRS:
            bars = self.get_pair_bars(currencyPair)
        elif currency == self.home:
            default_df = self.get_pair_bars('BTC_LTC')
            bars = pd.DataFrame(1, columns=default_df.columns, index=default_df.index)
        else:
            raise ValueError(currencyPair + ' does not exist!')
        return bars


    # volume = volume in BTC; close = close price
    def get_pair_bars(self, currencyPair, start=START, end=END):
        bars = convert_to_df(polo.returnChartData(currencyPair, start, end, self.freq),
                             time_key='date')

        return bars


    def get_current_positions(self):
        balances_dict = polo.returnBalances()
        balances_in_local_currency_units = pd.Series({label: float(balances_dict[label])
                                                      for label in balances_dict.keys()})
        prices = self.get_current_prices()
        balances_in_home_currency_units = balances_in_local_currency_units.multiply(prices).dropna()
        return {
            'local currency': balances_in_local_currency_units,
            'home currency': balances_in_home_currency_units,
            'prices': prices
        }


