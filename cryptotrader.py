import cryptotrading.data as data
import pandas as pd
import datetime
import numpy as np
from pandas.tseries.offsets import BDay

# global backtest start date
GLOBAL_START_DATE = datetime.date(2015, 9, 1)

# global daily price snap time
GLOBAL_SNAP_TIME = datetime.time(17, 0)

# trading cross section
liquid_region = ['BTC', 'ETH', 'XRP', 'LTC', 'DASH', 'DGB']

# default home currency=USDT
HOME = data.HOME

EPSILON = 10 ** -6

def _print_date_every_year(date):
    if date.month == 12 and date.day in [30, 31]:
        print('...', date)
    return


# main tradebot class
class CryptoTrader():
    def __init__(self, region=liquid_region, risk_target=0.1, data_frequency_in_seconds=7200, cov_window_in_days=120,
                 trading_lag=1, no_naked_short=True, max_out_cash=True):

        # initialize settings
        self.region = region
        self.period = data_frequency_in_seconds
        self.cov_window = cov_window_in_days
        self.lag = trading_lag
        self.no_naked_short = no_naked_short
        self.risk_target = risk_target
        self.max_out_cash = max_out_cash # override risk target; always max out cash usage

        # initialize data members
        self.data = None
        self.asset_returns = None
        self.start_date = GLOBAL_START_DATE
        self.end_date = datetime.date.today()
        self.dates = pd.date_range(start=self.start_date, end=self.end_date, freq='B')
        self.factor_weights = pd.Series({
            'mom 1w': 1,
            'mom 2w': 1,
            'mom 1m': 1,
            'mom 3m': 1,
            'mom 6m': 1
        })
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

    # daily asset returns series for backtest
    def compute_daily_asset_returns(self):
        intraday_ti = self.get_intraday_ti()
        daily_ti = intraday_ti[intraday_ti.index.time <= GLOBAL_SNAP_TIME].resample('B').last()
        daily_returns = daily_ti.fillna(method='pad', limit=5).pct_change()
        return daily_returns

    # public method to get raw intraday total return index
    def get_intraday_ti(self):
        ti = self.data.loc[:, :, 'close'].copy(deep=True)
        return ti

    # compute variance covariance matrix for one date
    def get_risk_model_one_date(self, date, window=None):
        window = window or self.cov_window
        start_date = pd.Timestamp(date) - BDay(window)
        # TODO: robust treatment for small number of returns
        sliced_ti = self.get_intraday_ti().ix[start_date: date]
        annualizer = 260.0 / (float(window) / len(sliced_ti))
        cov_matrix = sliced_ti.pct_change().cov() * annualizer
        return cov_matrix

    # load factor values
    def load_factors(self):
        # mom 1/2/3/6 m
        self.factors['mom 1w'] = self.compute_mom_factor(window=5)
        self.factors['mom 2w'] = self.compute_mom_factor(window=10)
        self.factors['mom 1m'] = self.compute_mom_factor(window=20)
        self.factors['mom 3m'] = self.compute_mom_factor(window=60)
        self.factors['mom 6m'] = self.compute_mom_factor(window=120)

        # inv vol
        self.factors['invvol 1m'] = self.compute_inv_vol_factor(window=20)
        self.factors['invvol 6m'] = self.compute_inv_vol_factor(window=120)

        # skew
        self.factors['skew 1m'] = self.compute_skew_factor(window=20)
        self.factors['skew 6m'] = self.compute_skew_factor(window=120)

        if not set(self.factor_weights.keys()).issubset(set(self.factors.keys())):
            raise ValueError('Undefined factor(s) found!')
        return

    # price mom
    def compute_mom_factor(self, window):
        return self.asset_returns.rolling(window=window, min_periods=window - 5, center=False).mean()

    # inverse vol
    def compute_inv_vol_factor(self, window):
        return -self.asset_returns.rolling(window=window, min_periods=window - 5, center=False).std()

    # skew
    def compute_skew_factor(self, window):
        return -self.asset_returns.rolling(window=window, min_periods=window - 5).skew()

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
            views = views.divide(vols, axis=0).fillna(0) * self.risk_target
            view_panel[factor_name] = views.reindex(self.dates)

        print ('Running portfolio viewgen')
        view = 0
        for factor_name in self.factor_weights.keys():
            view += view_panel[factor_name] * self.factor_weights[factor_name]
        view = view.divide(self.factor_weights.sum())
        if self.no_naked_short:
            view[view < 0] = 0
        vols = self.compute_portfolio_vol(view)
        view = view.divide(vols, axis=0).multiply(self.risk_target)
        view = view.fillna(0)

        # cap leverage at 1.00
        leverage = view.sum(1)
        leverage[leverage < 1.00] = 1.00
        view = view.divide(leverage, axis=0).multiply(1.00).fillna(0)

        # home currency view
        if self.max_out_cash:
            view = view.divide(view.sum(1), axis=0).multiply(1.00).fillna(0)
        view[HOME] = 1 - view.sum(1)

        view_panel['PORT'] = view.reindex(self.dates)
        self.views = pd.Panel(view_panel)
        return

    # compute ex ante portfolio vol using covs
    def compute_portfolio_vol(self, views):
        vols = pd.Series()
        for date in self.dates:
            vols[date] = views.loc[date, :].dot(self.cov[date]).dot(views.loc[date, :])
        vols = vols ** 0.5
        return vols

    # compute gross/net portfolio returns, assuming 0.3% tcost
    def compute_portfolio_returns(self, rebal_rule='W'):
        net_returns = pd.DataFrame()
        for port in self.views.keys():
            views = self.views[port].resample(rebal_rule).last()
            intraday_ti = self.get_intraday_ti()
            ti = intraday_ti.resample(rebal_rule).last()
            asset_returns = ti.pct_change()
            gross_returns = asset_returns.multiply(views.shift(self.lag)).sum(1)
            tcost = views.diff().abs().sum(1)/2 * 0.003
            net_returns[port] = gross_returns - tcost
        return net_returns

    # generate portfolio trades from current holdings
    def tradegen(self):
        current_view = self.views['PORT'].ix[-1]
        position_dict = data.get_current_positions()
        current_positions = pd.Series(
            {label: float(position_dict[label])
             for label in position_dict.keys()
             })
        trade = (current_view - current_positions).dropna()
        return trade


