from cryptotrading.dataBot import dataBot
from cryptotrading.executionBot import executionBot
import pandas as pd
import datetime
import numpy as np
import logging
from pandas.tseries.offsets import BDay

# global backtest start date
GLOBAL_START_DATE = datetime.date(2015, 9, 1)

# global daily price snap time
GLOBAL_SNAP_TIME = datetime.time(10, 0)

# trading cross section
POLO_CROSS_SECTION = ['BTC', 'ETH', 'XRP', 'LTC', 'DASH', 'DGB', 'BTS']

# home currency and hub currencies
HOME = 'BTC'

SECONDS_IN_A_YEAR = 365.0 * 24.0 * 3600.0
EPSILON = 10 ** -6

MIN_NUM_OF_RETURNS_FOR_COV = 500.0


# main tradebot class
class traderBot():
    def __init__(self, region=POLO_CROSS_SECTION, home=HOME, risk_target=1.00, data_frequency_in_seconds=7200.0,
                 cov_window_in_days=260.0,
                 trading_lag=1, no_naked_short=True, max_out_cash=False, warn=True):

        # initialize settings
        self.region = region
        self.home = home
        self.freq = data_frequency_in_seconds
        self.cov_window = cov_window_in_days
        self.lag = trading_lag
        self.no_naked_short = no_naked_short
        self.risk_target = risk_target
        self.max_out_cash = max_out_cash  # override risk target; always max out cash usage
        self.warn = warn # whether to set a y/n manual input before rebalancing

        # initialiaze logger
        self.initialize_logging()


        # initialize data members
        self.data = dataBot(region=self.region, home=self.home)
        self.start_date = GLOBAL_START_DATE
        self.end_date = datetime.date.today()
        self.dates = pd.date_range(start=self.start_date, end=self.end_date, freq='B')
        self.factor_weights = pd.Series({
            'mom 1w': 0.67,
            'mom 1m': 0.33
        })
        self.factors = {}
        self.cov = None

        # run portfolio
        self.load_factors()
        self.covgen()
        self.viewgen()

    # initialize logging
    def initialize_logging(self):
        logger = logging.getLogger('tradeBot logger')
        logger.setLevel(logging.DEBUG)
        file_path = 'h:\\traderBot_log\\' + datetime.date.today().strftime('%Y-%m-%d.log')
        fh = logging.FileHandler(file_path)
        fh.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        logger.addHandler(fh)
        logger.addHandler(ch)
        self.logger = logger

    # master function for rebalancing
    def rebalance(self):
        trade_df = self.tradegen()
        print('Rebalancing from ... to ...')
        print(trade_df)

        if self.warn:
            keyboard_input = input('Proceed with rebalancing? (y/n): ')
            if keyboard_input.upper() == 'N':
                return
            elif keyboard_input.upper() not in ['Y', 'N']:
                raise ValueError('Input not y/n!')

        portfolio_diff = trade_df['trade']
        portfolio_diff = portfolio_diff.sort_values(inplace=False)

        # generate executable orders in format
        # (currency to buy, currency to sell, amount in buy currency, amount in sell currency)
        self.logger.info ('Breaking trade down into executable orders...')
        orders = []
        for currency in [x for x in portfolio_diff.index if x != self.home]:
            buy_currency = None
            sell_currency = None
            buy_amount = None
            sell_amount = None
            if portfolio_diff[currency] > 0:
                buy_currency = currency
                sell_currency = self.home
                buy_amount = portfolio_diff[currency]
            elif portfolio_diff[currency] < 0:
                buy_currency = self.home
                sell_currency = currency
                sell_amount = -portfolio_diff[currency]
            else:
                continue
            orders.append((buy_currency, sell_currency, buy_amount, sell_amount))

        # execute all trades
        eb = executionBot(orders=orders, logger=self.logger)

        return


    # daily asset returns series for backtest
    def get_daily_asset_returns(self):
        intraday_ti = self.get_intraday_ti()
        daily_ti = intraday_ti[intraday_ti.index.time <= GLOBAL_SNAP_TIME].resample('B').last()
        daily_returns = daily_ti.fillna(method='pad', limit=5).pct_change()
        return daily_returns

    # public method to get raw intraday total return index
    def get_intraday_ti(self):
        return self.data.get_intraday_data()

    # compute variance covariance matrix for one date
    def get_risk_model_one_date(self, date, window=None):
        window = window or self.cov_window
        start_date = pd.Timestamp(date) - BDay(window)
        sliced_ti = self.get_intraday_ti().ix[start_date: date]

        # drop columns that don't have enough return nobs
        sliced_ti = sliced_ti.T[sliced_ti.count(0) > MIN_NUM_OF_RETURNS_FOR_COV].T

        # annualize
        annualizer = SECONDS_IN_A_YEAR / self.freq
        cov_matrix = sliced_ti.pct_change().cov() * annualizer
        return cov_matrix

    # load factor values
    def load_factors(self):
        # mom
        self.factors['mom 1w'] = self.compute_ewma_mom_factor(com=5)
        self.factors['mom 1m'] = self.compute_ewma_mom_factor(com=20)

        if not set(self.factor_weights.keys()).issubset(set(self.factors.keys())):
            raise ValueError('Undefined factor(s) found!')
        return

    # price mom
    def compute_ewma_mom_factor(self, com):
        return self.get_daily_asset_returns().ewm(com=com, min_periods=com, adjust=True, ignore_na=False).mean()

    # inverse vol
    def compute_inv_vol_factor(self, window):
        return -self.get_daily_asset_returns().rolling(window=window, min_periods=window - 5, center=False).std()

    # skew
    def compute_skew_factor(self, window):
        return -self.get_daily_asset_returns().rolling(window=window, min_periods=window - 5).skew()

    def covgen(self):
        self.logger.info('Running covgen')
        cov_matrix_panel = {}
        for date in self.dates:
            cov_matrix_panel[date] = self.get_risk_model_one_date(date=date)
        self.cov = pd.Panel(cov_matrix_panel)

    def viewgen(self):
        view_panel = {}

        self.logger.info('Running factor viewgen')
        for factor_name in self.factors.keys():
            self.logger.info('...' + factor_name)
            factor_values = self.factors[factor_name]
            views = factor_values
            vols = self.compute_portfolio_vol(views)
            views = views.divide(vols, axis=0).fillna(0) * self.risk_target
            view_panel[factor_name] = views.reindex(self.dates)

        self.logger.info('Running portfolio viewgen')
        view = 0
        for factor_name in self.factor_weights.keys():
            view += view_panel[factor_name] * self.factor_weights[factor_name]
        view = view.divide(self.factor_weights.sum())

        # remove negative positions if needed
        if self.no_naked_short:
            view[view < 0] = 0

        # vol targeting
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

        view[self.home] = 1 - view.sum(1)

        view_panel['PORT'] = view.reindex(self.dates)
        self.views = pd.Panel(view_panel)
        return

    # compute ex ante portfolio vol using covs
    def compute_portfolio_vol(self, views):
        vols = pd.Series()
        for date in self.dates:
            cov_cols = set(self.cov[date].dropna(axis=1, how='all').columns)
            view_cols = set(views.loc[date, :].dropna().index)
            available_cols = list(cov_cols.intersection(view_cols))
            vols[date] = views.loc[date, available_cols].dot(self.cov[date].loc[available_cols, available_cols]). \
                dot(views.loc[date, available_cols])
        vols = vols ** 0.5
        return vols

    # compute gross/net portfolio returns, assuming 1% tcost
    def compute_portfolio_returns(self, rebal_rule='W', unit_tcost=0.01):
        net_returns = pd.DataFrame()
        for port in self.views.keys():
            views = self.views[port].resample(rebal_rule).last()
            intraday_ti = self.get_intraday_ti()
            ti = intraday_ti.resample(rebal_rule).last()
            asset_returns = ti.pct_change()
            gross_returns = asset_returns.multiply(views.shift(self.lag)).sum(1)
            tcost = views.diff().abs().sum(1) / 2 * unit_tcost
            net_returns[port] = gross_returns - tcost
        return net_returns

    # generate portfolio trades from current holdings
    def tradegen(self, date=datetime.date.today()):
        current_view = self.views['PORT'].ix[date]
        position_dict = self.data.get_current_positions()

        nav_in_home_currency = position_dict['home currency'].sum()
        prices = position_dict['prices']
        current_positions_in_local_currency = position_dict['local currency']
        current_view_in_home_currency = current_view.multiply(nav_in_home_currency).dropna()
        current_view_in_local_currency = current_view_in_home_currency.divide(prices).dropna()

        if not set(current_view_in_local_currency.index).issubset(set(current_positions_in_local_currency.index)):
            raise ValueError('Currency found not tradable!!')

        df = pd.DataFrame({
            'current': current_positions_in_local_currency,
            'desired': current_view_in_local_currency,
            'desired in BTC': current_view_in_home_currency
        }).dropna()
        df['trade'] = df['desired'] - df['current']
        return df


if __name__ == "__main__":
    tb = traderBot(warn=False)
    tb.rebalance()

