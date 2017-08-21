import cryptotrading.data as data
import pandas as pd
import datetime
import numpy as np
from pandas.tseries.offsets import BDay

GLOBAL_START_DATE = '2015-9-1'

GLOBAL_SNAP_TIME = datetime.time(17, 0)

liquid_region = ['BTC', 'ETH', 'XRP', 'LTC', 'DASH', 'DGB', 'USDT']

HOME = data.HOME


def _print_date_every_year(date):
    if date.month == 12 and date.day in [30, 31]:
        print('...', date)
    return


class CryptoTrader():
    def __init__(self, region, data_frequency_in_seconds=7200, cov_window_in_days=120,
                 trading_lag=1, no_naked_short=True):
        # initialize settings
        self.region = region
        self.period = data_frequency_in_seconds
        self.cov_window = cov_window_in_days
        self.lag = trading_lag
        self.no_naked_short = no_naked_short

        # initialize data members
        self.data = None
        self.asset_returns = None
        self.start_date = datetime.date(2015, 9, 1)
        self.end_date = datetime.date.today()
        self.dates = pd.date_range(start=self.start_date, end=self.end_date, freq='B')
        self.factor_weights = {
            'mom 1m': 1
        }
        self.factors = {}
        self.cov = None

        # run portfolio
        self.load_data()
        self.load_factors()
        self.covgen()
        self.viewgen()

    def load_data(self):
        #TODO: robust treatment for data failures
        self.data = data.get_intraday_data(region=self.region, period=self.period)
        self.asset_returns = self.compute_daily_asset_returns()
        return

    def compute_daily_asset_returns(self):
        intraday_ti = self.get_intraday_ti()
        daily_ti = intraday_ti[intraday_ti.index.time <= GLOBAL_SNAP_TIME].resample('B').last()
        daily_returns = daily_ti.fillna(method='pad', limit=5).pct_change()
        return daily_returns

    def get_intraday_ti(self):
        ti = self.data.loc[:, :, 'close'].copy(deep=True)
        ti[HOME] = 1
        return ti

    def get_risk_model_one_date(self, date, window=None):
        window = window or self.cov_window
        start_date = pd.Timestamp(date) - BDay(window)
        sliced_ti = self.get_intraday_ti().ix[start_date: date]
        annualizer = 260.0 / (float(window) / len(sliced_ti))
        cov_matrix = sliced_ti.pct_change().cov() * annualizer
        return cov_matrix

    def load_factors(self):
        # mom 1m
        self.factors['mom 1m'] = self.compute_mom_factor(window=20)

        if set(self.factors.keys()) != set(self.factor_weights.keys()):
            raise ValueError('Factor list mismatch!')
        return

    def compute_mom_factor(self, window):
        return self.asset_returns.rolling(window=window, min_periods=window - 5, center=False).mean()

    def covgen(self):
        print('Running covgen')
        cov_matrix_panel = {}
        for date in self.dates:
            _print_date_every_year(date)
            cov_matrix_panel[date] = self.get_risk_model_one_date(date=date)
        self.cov = pd.Panel(cov_matrix_panel)

    def viewgen(self):
        view_panel = {}

        print('Running factor viewgen')
        for factor_name in self.factors.keys():
            print('...', factor_name)
            factor_values = self.factors[factor_name]
            views = factor_values
            vols = self.compute_portfolio_vol(views)
            views = views.divide(vols, axis=0).fillna(0) * 0.1
            view_panel[factor_name] = views.reindex(self.dates)

        print ('Running portfolio viewgen')
        view = 0
        for factor_name in view_panel.keys():
            view += view_panel[factor_name] * self.factor_weights[factor_name]
        if self.no_naked_short:
            view[view < 0] = 0
        view = view.divide(self.compute_portfolio_vol(view), axis=0) * 0.1
        view_panel['PORT'] = view.reindex(self.dates)
        self.views = pd.Panel(view_panel)
        return

    def compute_portfolio_vol(self, views):
        vols = pd.Series()
        for date in self.dates:
            vols[date] = views.loc[date, :].dot(self.cov[date]).dot(views.loc[date, :])
        vols = vols ** 0.5
        return vols

    def compute_portfolio_returns(self, port='PORT', rebal_rule='W'):
        views = self.views[port].resample(rebal_rule).last()
        intraday_ti = self.get_intraday_ti()
        ti = intraday_ti.resample(rebal_rule).last()
        asset_returns = ti.pct_change()
        gross_returns = asset_returns.multiply(views.shift(self.lag)).sum(1)
        tcost = views.diff().abs().sum(1)/2 * 0.003
        net_returns = gross_returns - tcost
        return net_returns


