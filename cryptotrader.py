import cryptotrading.data as data
import pandas as pd
import datetime
import numpy as np
from pandas.tseries.offsets import BDay

GLOBAL_START_DATE = '2015-9-1'

GLOBAL_SNAP_TIME = datetime.time(17, 0)

liquid_region = ['BTC', 'ETH', 'XRP', 'LTC', 'DASH', 'DGB', 'USDT']


class CryptoTrader():
    def __init__(self, region, data_frequency_in_seconds=7200, cov_window_in_days=120):
        # initialize settings
        self.region = region
        self.period = data_frequency_in_seconds
        self.cov_window = cov_window_in_days

        # initialize data members
        self.data = None
        self.asset_returns = None

        # load raw data, asset returns
        self.load_data()

        # load factors

        # viewgen

        # backtest

    def load_data(self):
        #TODO: robust treatment for data failures
        self.data = data.get_intraday_data(region=self.region, period=self.period)
        self.asset_returns = self.compute_daily_returns()

    def compute_daily_returns(self):
        intraday_ti = self.get_intraday_ti()
        daily_ti = intraday_ti[intraday_ti.index.time <= GLOBAL_SNAP_TIME].resample('B', how='last')
        daily_returns = daily_ti.fillna(method='pad', limit=3).pct_change()
        return daily_returns

    def get_intraday_ti(self):
        return self.data.loc[:, :, 'close'].copy(deep=True)

    def get_risk_model_one_date(self, date, window=None):
        window = window or self.cov_window
        start_date = pd.Timestamp(date) - BDay(window)
        sliced_ti = self.get_intraday_ti().ix[start_date: date]
        annualizer = 260.0 / (float(window) / len(sliced_ti))
        cov_matrix = sliced_ti.pct_change().cov() * annualizer
        return cov_matrix

