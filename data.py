from datetime import datetime

import pandas as pd

import cryptotrading.poloneix_api as polo_api

# polo account key
POLO_KEY = '3KKMO5B5-CTRYWVN8-8MQJW6O0-K84VSH9L'
POLO_SECRET = 'bf58a91a5c669dc014c3320a10fc32349cf4501aa605aafb99f45efd7f52cab8d6113877b7f37584ff96980a3a1c61b6473c7e016a7d001d0fb878e613334494'
polo = polo_api.poloniex(APIKey=POLO_KEY, Secret=POLO_SECRET)

# home currency and hub currencies
HOME = 'USDT'
HUBS = ['BTC', 'ETH', 'XMR']

# all traded pairs
ALL_PAIRS = list(polo.returnTicker().keys())
ALL_CURRENCIES = list({x.split('_')[0] for x in ALL_PAIRS} | \
{x.split('_')[1] for x in ALL_PAIRS} - {HOME})

# default start, end dates in UNIX time, default sampling frequency
START = 0
END = 9999999999
PERIOD = 7200


def get_intraday_data(region=ALL_PAIRS, period=PERIOD):
    print('Loading OHLC, volume data for')
    data_panel = {}
    for currency in [x for x in region if x!= HOME]:
        print('...', currency)
        data_panel[currency] = _get_bars(currency, period=period)
    data_panel = pd.Panel(data_panel)
    return data_panel


def _get_bars(currency, home=HOME, period=PERIOD, start=START, end=END):
    bars = None
    currencyPair = home + '_' + currency
    if currencyPair in ALL_PAIRS:
        bars = get_pair_bars(currencyPair, period=period, start=start, end=end)
    elif currency + '_' + home in ALL_PAIRS:
        currencyPair = currency + '_' + home
        bars_currencyPair = get_pair_bars(currencyPair, period=period, start=start, end=end)
        bars = 1 / bars_currencyPair
        # TODO: volume?

    else:
        for hub in HUBS:
            currencyPair1 = home + '_' + hub
            currencyPair2 = hub + '_' + currency
            if (currencyPair1 in ALL_PAIRS) and (currencyPair2 in ALL_PAIRS):
                bars1 = get_pair_bars(currencyPair1, period=period, start=start, end=end)
                bars2 = get_pair_bars(currencyPair2, period=period, start=start, end=end)

                # TODO: volume?
                bars = bars1 * bars2
                break
    if bars is None:
        raise ValueError('currency or currency pair does not exist!')
    return bars



def get_pair_bars(currencyPair, period=PERIOD, start=START, end=END):
    bars = convert_to_df(polo.returnChartData(currencyPair, start, end ,period),
                         time_key='date')

    return bars


def convert_to_df(input, time_key='date'):
    df = pd.DataFrame(input)

    if time_key in df.columns:
        df.index = [datetime.fromtimestamp(x) for x in df[time_key]]
        df = df.drop(time_key, 1)
    else:
        raise ValueError('time key not found!')

    return df