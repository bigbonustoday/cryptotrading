from cryptotrading.data import *


def get_all_currency_returns_in_USD():
    df = {}
    for currency in [x for x in ALL_CURRENCIES if x!= HOME]:
        print(currency)
        df[currency] = get_bars(currency, period=7200)['close']
    df = pd.DataFrame(df)
