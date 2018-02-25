from cryptotrading.dataBot import dataBot
from cryptotrading.executionBot import executionBot
from cryptotrading.emailer import send_email
from cryptotrading.logger_builder import logger
import numpy as np
import pandas as pd
import datetime
from pandas.tseries.offsets import BDay

# CONSTANTS - these should almost never be changed!!!
SECONDS_IN_A_YEAR = 365.0 * 24.0 * 3600.0
EPSILON = 10 ** -6
VIEWGEN_FREQ = 'D'
FREQ_DICT = {
    'D': 365,
    'B': 260,
    'W': 52,
    'BM': 12
}

# PARAMETERS
# global backtest start date
GLOBAL_START_DATE = datetime.date(2015, 9, 1)

# global daily price snap time
GLOBAL_SNAP_TIME = datetime.time(9, 1)

# trading cross section
POLO_CROSS_SECTION = ['BTC', 'AMP', 'ARDR', 'BCH', 'BCN', 'BCY', 'BELA', 'BLK', 'BTCD', 'BTM', 'BTS', 'BURST', 'CLAM',
                      'CVC', 'DASH', 'DCR', 'DGB', 'DOGE', 'EMC2', 'ETC', 'ETH', 'EXP', 'FCT', 'FLDC', 'FLO', 'GAME',
                      'GAS', 'GNO', 'GNT', 'GRC', 'HUC', 'LBC', 'LSK', 'LTC', 'MAID', 'NAV', 'NEOS', 'NMC','NXC',
                      'NXT', 'OMG', 'OMNI', 'PASC', 'PINK', 'POT', 'PPC', 'RADS', 'REP', 'RIC', 'SBD', 'SC', 'STEEM',
                      'STORJ', 'STR', 'STRAT', 'SYS', 'VIA', 'VRC', 'VTC', 'XBC', 'XCP', 'XEM', 'XMR', 'XPM', 'XRP',
                      'XVC', 'ZEC', 'ZRX']

# home currency and hub currencies
HOME = 'BTC'

# cov related
MIN_NUM_OF_RETURNS_FOR_COV = 500.0


# factor research helpers

def get_sharpe_ratios(ret, freq):
    return ret.mean() / ret.std() * FREQ_DICT[freq] ** 0.5

def get_factor_return_stats(ret_dict):
    net_returns = ret_dict['net']
    gross_returns_by_pair_dict = ret_dict['gross by pair']

    freq = pd.infer_freq(net_returns.index)

    net_sharpes = get_sharpe_ratios(net_returns, freq)
    factor_stats = pd.DataFrame({
        factor: pd.Series({
            'full net sharpe': net_sharpes[factor],
            'pair gross sharpe mean': get_sharpe_ratios(gross_returns_by_pair_dict[factor], freq).mean(),
            'pair gross sharpe std': get_sharpe_ratios(gross_returns_by_pair_dict[factor], freq).std(),
            'pair gross sharpe bottom 10%': get_sharpe_ratios(gross_returns_by_pair_dict[factor], freq).quantile(0.1),
            'pair gross sharpe min': get_sharpe_ratios(gross_returns_by_pair_dict[factor], freq).min()
                          })
        for factor in gross_returns_by_pair_dict.keys()
    })
    return factor_stats



# main tradebot class
class traderBot():
    def __init__(self, region=POLO_CROSS_SECTION, home=HOME, risk_target=1.00, price_data_frequency_in_seconds=7200.0,
                 cov_window_in_days=260.0, viewgen_freq = VIEWGEN_FREQ,
                 trading_lag=1, no_naked_short=True, force_max_out_cash=False,
                 leverage_cap=0.98):

        # initialize settings
        self.region = region
        self.home = home
        self.viewgen_freq = viewgen_freq
        self.price_data_freq = price_data_frequency_in_seconds
        self.cov_window = cov_window_in_days
        self.lag = trading_lag
        self.no_naked_short = no_naked_short
        self.risk_target = risk_target
        self.force_max_out_cash = force_max_out_cash  # override risk target; always max out cash usage
        self.leverage_cap = leverage_cap

        # initialiaze logger
        self.initialize_logging()


        # initialize data members
        self.data = dataBot(region=self.region, home=self.home)
        self.start_date = GLOBAL_START_DATE
        self.end_date = datetime.date.today()
        self.dates = pd.date_range(start=self.start_date, end=self.end_date, freq=self.viewgen_freq)
        self.factor_weights = pd.Series({
            'mom 1w': 0.00,
            'mom 1m': 1.00
        })
        self.factors = {}
        self.cov = None

        # run portfolio
        self.load_factors()
        self.covgen()
        self.viewgen()

    # initialize logging
    def initialize_logging(self):
        self.logger = logger

    # master function for rebalancing
    def rebalance(self, warn=True):
        trade_df = self.tradegen()
        print('Rebalancing from ... to ...')
        print(trade_df)

        covcorrel_dict = self.get_covcorrel_and_vol(trade_df['current positions'],
                                                    trade_df['desired positions'])
        logger.info('Changing portfolio ex-ante vol from {vol1} to {vol2}'.format(
            vol1=str(round(covcorrel_dict['vol1'], 3)), vol2=str(round(covcorrel_dict['vol2'], 3))))
        logger.info('covcorrel = {covcorrel}'.format(covcorrel=str(round(covcorrel_dict['covcorrel'], 3))))

        # whether to set a y/n manual input before rebalancing
        if warn:
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
        eb = executionBot(orders=orders)

        return


    # daily asset returns series for backtest
    def get_daily_asset_returns(self):
        intraday_ti = self.get_intraday_ti()
        daily_ti = intraday_ti[intraday_ti.index.time <= GLOBAL_SNAP_TIME].resample(self.viewgen_freq).last()
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
        annualizer = SECONDS_IN_A_YEAR / self.price_data_freq

        # TODO: add support for EWMA
        cov_matrix = sliced_ti.pct_change().cov() * annualizer
        return cov_matrix

    # load factor values
    def load_factors(self):
        # mom
        self.factors['mom 1w'] = self.compute_ewma_mom_factor(com=5)
        self.factors['mom 1m'] = self.compute_ewma_mom_factor(com=20)
        self.factors['mom 3m'] = self.compute_ewma_mom_factor(com=60)

        # centered skew
        self.factors['skew 1w'] = self.compute_skew_factor(window=5, skew_type='centered')
        self.factors['skew 1m'] = self.compute_skew_factor(window=20, skew_type='centered')
        self.factors['skew 3m'] = self.compute_skew_factor(window=60, skew_type='centered')

        # adjusted skew
        self.factors['adj skew 1w'] = self.compute_skew_factor(window=5, skew_type='adjusted')
        self.factors['adj skew 1m'] = self.compute_skew_factor(window=20, skew_type='adjusted')
        self.factors['adj skew 3m'] = self.compute_skew_factor(window=60, skew_type='adjusted')

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
    def compute_skew_factor(self, window, skew_type):
        signal = None
        daily_returns = self.get_daily_asset_returns()
        if skew_type == 'centered':
            signal = daily_returns.rolling(window=window, min_periods=window - 5).skew()
        elif skew_type == 'adjusted':
            m4 = (daily_returns ** 4).rolling(window=window, min_periods=window - 5).sum()
            m3 = (daily_returns ** 3).rolling(window=window, min_periods=window - 5).sum()
            m2 = (daily_returns ** 2).rolling(window=window, min_periods=window - 5).sum()
            m1 = (daily_returns ** 1).rolling(window=window, min_periods=window - 5).sum()
            signal = (m3 - m4 / m2 * m1) / m2 ** 1.5
        return signal

    def covgen(self):
        self.logger.info('Running covgen')
        cov_matrix_panel = {}
        for date in self.dates:
            cov_matrix_panel[date] = self.get_risk_model_one_date(date=date)
        self.cov = pd.Panel(cov_matrix_panel)

    def get_asset_vols(self):
        if self.cov is None:
            self.covgen()
        cov = self.cov
        asset_vols = pd.DataFrame({date: pd.Series(np.diag(cov[date]), index=cov[date].columns)
                                  for date in cov.items})
        return asset_vols.T.drop(self.home, axis=1)

    def viewgen(self):
        view_panel = {}

        self.logger.info('Running factor viewgen')
        for factor_name in self.factors.keys():
            self.logger.info('...' + factor_name)
            factor_values = self.factors[factor_name].drop(self.home, axis=1) # drop home currency because view is
            # meaningless

            # grinold
            asset_vols = self.get_asset_vols()
            views = factor_values.divide(asset_vols, axis=1)

            # risk targeting
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

        # cap leverage
        leverage = view.sum(1)
        leverage[leverage > self.leverage_cap] = self.leverage_cap

        # force max out cash
        if self.force_max_out_cash:
            leverage = self.leverage_cap

        leverage_cap_scalar = leverage / view.sum(1)
        view = view.multiply(leverage_cap_scalar, axis=0).fillna(0)

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
    def compute_portfolio_returns(self, rebal_rule='W', unit_tcost=0.0025):
        net_returns = pd.DataFrame()
        gross_returns_by_pair = {}
        for port in self.views.keys():
            views = self.views[port].resample(rebal_rule).last()
            intraday_ti = self.get_intraday_ti()
            ti = intraday_ti.resample(rebal_rule).last()
            asset_returns = ti.pct_change()
            gross_returns_by_pair[port] = asset_returns.multiply(views.shift(self.lag))
            gross_returns = gross_returns_by_pair[port].sum(1)
            tcost = views.diff().abs().sum(1) / 2 * unit_tcost
            net_returns[port] = gross_returns - tcost
        return {
            'net': net_returns,
            'gross by pair': gross_returns_by_pair
        }

    # generate portfolio trades from current holdings
    def tradegen(self, date=datetime.date.today()):
        if date not in self.views['PORT'].index:
            err_msg = 'PORT views not available for ' + date.strftime('%Y-%m-%d')
            self.logger.critical(err_msg)
            raise IndexError(err_msg)
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
            'current positions': position_dict['home currency'] / nav_in_home_currency,
            'desired positions': current_view
        }).dropna()
        df['trade'] = df['desired'] - df['current']

        # check region completeness
        if set(df.index) != set(self.region):
            missed_currency = list(set(self.region) - set(df.index))
            err_msg = 'desired views missing currency(s) - ' + str(missed_currency)
            self.logger.critical(err_msg)
            raise IndexError(err_msg)
        return df

    def log_current_balance(self):
        position_dict = self.data.get_current_positions()
        nav_in_home_currency = position_dict['home currency'].sum()
        self.logger.info('Current balance in BTC = ' + str(round(nav_in_home_currency, 4)))

    def get_covcorrel_and_vol(self, view1, view2, cov_date=datetime.date.today()):
        if self.cov is None:
            self.covgen()
        cov = self.cov[cov_date]

        view1_valid = set(view1.index).issubset(set(self.cov[cov_date].index))
        view2_valid = set(view2.index).issubset(set(self.cov[cov_date].index))
        if not (view1_valid and view2_valid):
            err_msg = 'views contain invalid coins!'
            logger.critical(err_msg)
            raise ValueError(err_msg)

        vol1 = (view1.dot(cov).dot(view1)) ** 0.5
        vol2 = (view2.dot(cov).dot(view2)) ** 0.5
        covar = (view1.dot(cov).dot(view2))
        correl = covar / (vol1 * vol2)

        return {
            'vol1': vol1,
            'vol2': vol2,
            'covcorrel': correl
        }
