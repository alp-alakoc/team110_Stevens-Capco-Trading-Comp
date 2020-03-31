import sys
import time
import numpy as np
import pandas as pd
import shift
import datetime

def subscribe():
    trader = shift.Trader("test001")

    # connect and subscribe to all available order books
    try:
        trader.connect("initiator.cfg", "password")
        trader.sub_all_order_book()
    except shift.IncorrectPasswordError as e:
        print(e)
    except shift.ConnectionTimeoutError as e:
        print(e)
    return trader


def market_order(trader: shift.Trader, order_type, symbol, contract_size):

    if order_type == "buy":
        market_buy = shift.Order(shift.Order.Type.MARKET_BUY, symbol, contract_size)
        trader.submit_order(market_buy)

    if order_type == "sell":
        market_sell = shift.Order(shift.Order.Type.MARKET_SELL, symbol, contract_size)
        trader.submit_order(market_sell)
    return


def limit_order(trader: shift.Trader, order_type, symbol, contract_size, limit_price):
    if order_type == "buy":
        limit_buy = shift.Order(shift.Order.Type.LIMIT_BUY, symbol, contract_size, limit_price)
        trader.submit_order(limit_buy)

    if order_type == "sell":
        limit_sell = shift.Order(shift.Order.Type.LIMIT_SELL, symbol, contract_size, limit_price)
        trader.submit_order(limit_sell)
    return



def close_positions(trader: shift.Trader):
    for item in trader.get_portfolio_items().values():
        if item.get_shares() < 0:
            print(f"Closing short position in {item.get_symbol()}...")
            market_order(trader, "buy", item.get_symbol(), contract_size=int(abs(item.get_shares()) / 100))
        if item.get_shares() > 0:
            print(f"Closing long position in {item.get_symbol()}...")
            market_order(trader, "sell", item.get_symbol(), contract_size=int(abs(item.get_shares()) / 100))
        time.sleep(0.05)
    return



def update_prices(ls, prices):
    p = [trader.get_last_price(sym) for sym in ls]
    prices.loc[trader.get_last_trade_time(), :] = p
    return prices


def update_open_interest(ls, open_interest, order_type):
    if order_type == "ask":
        o_i = [shift.BestPrice(sym).get_ask_size() for sym in ls]
        open_interest.loc[trader.get_last_trade_time(), :] = o_i

    if order_type == "bid":
        o_i = [shift.BestPrice(sym).get_bid_size() for sym in ls]
        open_interest.loc[trader.get_last_trade_time(), :] = o_i
    return open_interest


def ma_differntial_variance(df, window=10):
    ma = df.rolling(window=window).mean()
    res = (ma - df).rolling(window=window).var()
    return res


def interest_differential(ask, bid, window=5):
    ask, bid = ask.rolling(window=window).mean(axis=0), bid.rolling(window=window).mean(axis=0)
    diff = ask - bid
    return diff


if __name__ == "__main__":

    trader = subscribe()
    ls = trader.get_stock_list()
    prices, open_ask, open_bid = pd.DataFrame(columns=ls), pd.DataFrame(columns=ls), pd.DataFrame(columns=ls)


    trade_count = 0
    while_count = 0
    while True:
        print(f"Iteration #{while_count} ...")
        if while_count == 0:
            for i in range(60):
                prices = update_prices(ls, prices)
                print("Populating Price Table...{}".format(i))
                open_ask = update_open_interest(ls, open_ask, "ask")
                print("Populating Ask Size Table...{}".format(i))
                open_bid = update_open_interest(ls, open_bid, "bid")
                print("Populating Bid Size Table...{}".format(i))
                time.sleep(10)

        if while_count != 0:
            for i in range(11):
                prices = update_prices(ls, prices)
                print("Populating Price Table...{}".format(i))
                open_ask = update_open_interest(ls, open_ask, "ask")
                print("Populating Ask Size Table...{}".format(i))
                open_bid = update_open_interest(ls, open_bid, "bid")
                print("Populating Bid Size Table...{}".format(i))
                time.sleep(10)

        while_count += 1

        prices = update_prices(ls, prices)
        diff_var = prices.apply(ma_differntial_variance, axis=0)
        latest_var = diff_var[-1:]

        open_ask = update_open_interest(ls, open_ask, "ask")
        open_bid = update_open_interest(ls, open_bid, "bid")
        open_interest_diff = interest_differential(open_ask, open_bid)
        latest_interest_diff = open_interest_diff[-1:]

        print(f"Variance of MA Differentials: {latest_var}", "\n",
              f"Moving Volume Difference in Ask/Bid Samples: {latest_interest_diff}")

        for symbol, diff in latest_var.items():
            print(f"{symbol} and {trader.get_portfolio_item(symbol).get_long_shares()}")
            print(f"{symbol} and {trader.get_portfolio_item(symbol).get_short_shares()}")
            if diff >= 0.03**2: #Lets' decide on what this variance threshold should be....
                if latest_interest_diff[symbol].values() >= 0 and trader.get_portfolio_item(symbol).get_long_shares() != 100:
                    size = int((trader.get_portfolio_item(symbol).get_short_shares() + (100 - trader.get_portfolio_item(symbol).get_long_shares())) / 100)
                    print(f"Buy {symbol}: Contract Size: {size}")
                    limit_order(trader, 'buy', symbol, size, shift.BestPrice(symbol).get_global_ask_price())
                    time.sleep(0.05)

                elif latest_interest_diff[symbol].values() < 0 and trader.get_portfolio_item(symbol).get_short_shares() != 100:
                    size = int((trader.get_portfolio_item(symbol).get_long_shares() + (100 - trader.get_portfolio_item(symbol).get_short_shares())) / 100)
                    print(f"Buy {symbol}: Contract Size: {size}")
                    limit_order(trader, 'sell', symbol, size, shift.BestPrice(symbol).get_global_bid_price())
                    time.sleep(0.05)

                else:
                    pass

            else:
                pass

        # We are going to hold each position for 1.5 mins then close#
        time.sleep(90)
        for order in trader.get_submitted_orders():
            if order.status == shift.Order.Status.FILLED:
                trade_count += 1

        trader.cancel_all_pending_orders()
        close_positions(trader)

        if prices.index[-1].time() >= datetime.time(hour=15,minute=30,second=0):
            if trade_count < 40:
                #We can discuss what we want to do here#...

            close_positions(trader)