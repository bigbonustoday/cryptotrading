from cryptotrading.traderBot import traderBot
from cryptotrading.emailer import send_email
import time
import argparse

def run_portfolio_rebalance():
    tb = traderBot()
    tb.rebalance(warn=False)
    tb.log_current_balance()
    send_email()

def test_portfolio_rebalance():
    tb = traderBot()
    print(tb.tradegen())
    input('Press Enter to exit...')

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--rebalance', help='Run portfolio rebalancing', action='store_true')
    parser.add_argument('--test', help='Test portfolio rebalancing', action='store_true')
    args = parser.parse_args()

    if args.rebalance:
        run_portfolio_rebalance()
    elif args.test:
        test_portfolio_rebalance()
