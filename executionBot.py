from datetime import datetime
import pandas as pd
import time

# execution bot
# takes trades and execute them

import cryptotrading.poloneix_api as polo_api

# load polo account
from polo_account_info import POLO_KEY, POLO_SECRET
polo = polo_api.poloniex(APIKey=POLO_KEY, Secret=POLO_SECRET)


class executionBot():
    def __init__(self, orders):
        # trades: list of tuples in the following format
        # (currency to buy, currency to sell, amount in buy currency, amount in sell currency)
        self.orders = orders
        self.check_order_validity()


    def check_order_validity(self):
        for order in self.orders:
            assert len(order) == 4
            assert (order[2] is None) or (order[3] is None)
            assert (order[2] is not None) or (order[3] is not None)
            if order[2] is not None:
                assert order[2] > 0
            if order[3] is not None:
                assert order[3] > 0
        print('All order entries are confirmed valid!')

    def execute_all_orders_on_polo(self):
        order_numbers = []
        for order in self.orders:
            order_number = self.send_single_order_on_polo(order=order)
            if order_number is not None:
                order_numbers.append(order_number)

        print('All orders are placed!')

        all_orders_filled = False
        for attempt in range(1, 60):
            if len(order_numbers) > 0:
                print('Attempt #' + str(attempt) + 'Sleep for 60s for order execution...')
                time.sleep(seconds=60)
                for order_number in order_numbers:
                    status = polo.returnOrderTrades(orderNumber=order_number)
                    # TODO: is order filled?
                    if order_is_filled:
                        order_numbers.remove(order_number)
            else:
                all_orders_filled = True
                print ('All orders are filled!')
                break

        if not all_orders_filled:
            print('Some orders remain unfilled. Timeout after 60 attempts.')

        return all_orders_filled



    def send_single_order_on_polo(self, order, limit_x_spread=0.05):
        ticker_info = polo.returnTicker()
        domestic = None
        foreign = None
        if order[0] + '_' + order[1] in ticker_info.keys():
            domestic = order[0]
            foreign = order[1]
        elif order[1] + '_' + order[0] in ticker_info.keys():
            domestic = order[1]
            foreign = order[0]
        ticker = domestic + '_' + foreign

        if ticker_info[ticker]['isFrozen'] != '0':
            print('Cannot trade ' + ticker + ' due to exchange restriction!')
            return

        order_type = None
        amount = None # in foreign currency
        if order[2] is not None:
            amount = order[2]
            if order[0] == foreign:
                order_type = 'BUY'
            else:
                order_type = 'SELL'
        else:
            amount = order[3]
            if order[1] == foreign:
                order_type = 'SELL'
            else:
                order_type = 'BUY'

        # refresh quotes
        ticker_info = polo.returnTicker()
        bid = ticker_info[ticker]['highestBid']
        ask = ticker_info[ticker]['lowestAsk']
        if order_type == 'BUY':
            limit = bid - (ask - bid) * limit_x_spread
        else:
            limit = ask + (ask - bid) * limit_x_spread

        # place order
        output = None
        if order_type == 'BUY':
            output = polo.buy(currencyPair=ticker, rate=limit, amount=amount)
        else:
            output = polo.sell(currencyPair=ticker, rate=limit, amount=amount)

        if 'error' in output.keys():
            print ('Order error: ' + output['error'])
            return None

        print('...order #' + str(output['orderNumber']))


        return output['orderNumber']


