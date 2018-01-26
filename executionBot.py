from datetime import datetime
import pandas as pd
import time

# execution bot
# takes trades and execute them

import cryptotrading.poloneix_api as polo_api
from cryptotrading.logger_builder import logger

# load polo account
from polo_account_info import POLO_KEY, POLO_SECRET

polo = polo_api.poloniex(APIKey=POLO_KEY, Secret=POLO_SECRET)


class executionBot():
    def __init__(self, orders, debug=False):
        # trades: list of tuples in the following format
        # (currency to buy, currency to sell, amount in buy currency, amount in sell currency)
        self.orders = orders
        self.logger = logger

        self.buy_orders = [order for order in orders if order[3] is None]
        self.sell_orders = [order for order in orders if order[2] is None]

        self.check_order_validity()

        # execute all sell orders
        if not debug:
            self.logger.info('===Executing all sell orders===')
            self.execute_orders_on_polo(order_list=self.sell_orders)
            self.logger.info('===Executing all buy orders===')
            self.execute_orders_on_polo(order_list=self.buy_orders)

    def check_order_validity(self):
        for order in self.orders:
            assert len(order) == 4
            assert (order[2] is None) or (order[3] is None)
            assert (order[2] is not None) or (order[3] is not None)
            if order[2] is not None:
                assert order[2] > 0
            if order[3] is not None:
                assert order[3] > 0

        assert len(self.buy_orders) + len(self.sell_orders) == len(self.orders)

    def execute_orders_on_polo(self, order_list, max_attempts=9, wait_time_in_minutes=20):
        self.logger.info(str(len(order_list)) + ' orders received!')

        unfilled_order_list = order_list
        for attempt in range(1, max_attempts + 1):
            order_numbers_dict = {}
            for order in unfilled_order_list:
                order_number = self.send_single_order_on_polo(order=order)
                if order_number is not None:
                    order_numbers_dict[order_number] = order

            self.logger.info(
                'Attempt #' + str(attempt) + ': ' + str(len(order_numbers_dict.keys())) + ' order(s) sent to exchange')

            # wait for orders to be executed
            time.sleep(60 * wait_time_in_minutes)

            # check order status
            status = polo.returnOpenOrders(currencyPair='all')
            number_of_open_orders = sum([len(status[cp]) for cp in status.keys()])
            if number_of_open_orders == 0:
                break
            self.logger.info(str(number_of_open_orders) + ' order(s) remain unfilled. Cancelling...')

            # cancel unfilled orders
            order_numbers_unfilled = [(cp, status[cp][i]['orderNumber']) for cp in status.keys() for i in range(len(status[cp]))]
            for cp, order_number in order_numbers_unfilled:
                polo.cancel(currencyPair=cp, orderNumber=order_number)
                self.logger.info('...#' + order_number + ' cancelled')

            unfilled_order_list = [order_numbers_dict[order_number] for cp, order_number in order_numbers_unfilled]


        number_of_orders_filled = len(order_list) - len(unfilled_order_list)
        self.logger.info(str(number_of_orders_filled) + ' order(s) filled!')

        return

    def send_single_order_on_polo(self, order, limit_x_spread=0.03):
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
            self.logger.info('Cannot trade ' + ticker + ' due to exchange restriction!')
            return

        order_type = None
        amount = None  # in foreign currency
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
        bid = float(ticker_info[ticker]['highestBid'])
        ask = float(ticker_info[ticker]['lowestAsk'])
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

        order_description = order_type + ' ' + ticker + ' at ' + str(limit) + ', amount = ' + str(amount)

        if 'error' in output.keys():
            self.logger.info('Order error: ' + output['error'])
            self.logger.info('...failed to place order: ' + order_description)
            return None

        self.logger.info('Order placed #' + str(output['orderNumber']) + ': ' + order_description)

        return output['orderNumber']
