import sys
import time
import numpy as np
import pandas as pd
import shift
import datetime

def subscribe():
    trader = shift.Trader("team-110")

    # connect and subscribe to all available order books
    try:
        trader.connect("initiator.cfg", "t7TAFYjhbpN2qSsV")
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


def macd(close,period=(12,26,9)):
    diff = close.ewm(span=period[0]).mean()-close.ewm(span=period[1]).mean()
    dea = diff.ewm(span=period[2]).mean()
    res = diff-dea
    # res.columns = ["macd"]
    return res


def update_open_interest(trader: shift.Trader, ls, open_interest, order_type):
    if order_type == "ask":
        o_i = [trader.get_best_price(sym).get_ask_size() for sym in ls]
        open_interest.loc[trader.get_last_trade_time(), :] = o_i

    if order_type == "bid":
        o_i = [trader.get_best_price(sym).get_bid_size() for sym in ls]
        open_interest.loc[trader.get_last_trade_time(), :] = o_i
    return open_interest



def interest_differential(ask, bid, window=20):
    ask, bid = ask.rolling(window=window).mean(), bid.rolling(window=window).mean()
    diff = ask.subtract(bid)
    return diff



if __name__ == "__main__":

    trader = subscribe()
    ls = trader.get_stock_list()

    trade_count = 0
    while_count = 0
    while True:
        prices, open_ask, open_bid = pd.DataFrame(columns=ls), pd.DataFrame(columns=ls), pd.DataFrame(columns=ls)

        print(f"Iteration #{while_count} ...")
        for i in range(80):
            prices = update_prices(ls, prices)
            print("Populating Price Table...{}".format(i))
            open_ask = update_open_interest(trader, ls, open_ask, "ask")
            print("Populating Ask Size Table...{}".format(i))
            open_bid = update_open_interest(trader, ls, open_bid, "bid")
            print("Populating Bid Size Table...{}".format(i))
            time.sleep(2)

        while_count += 1


        ma = (prices.rolling(window=15).mean())[15:]
        diff_var = ((prices[15:] - ma).rolling(window=15).var())[15:]
        threshold_1 = np.array(diff_var.mean(axis=0))
        latest_var = diff_var.iloc[-1,:]

        open_interest_diff = interest_differential(open_ask, open_bid)
        threshold_2 = np.array(open_interest_diff.mean(axis=0))
        latest_interest_diff = open_interest_diff.iloc[-1, :]

        macd_table = prices.apply(macd, axis=0)
        latest_macd = macd_table.iloc[-1, :]

        print(f"Latest price action to MA variances: {latest_var}", "\n",
              f"Latest open interest differentials: {latest_interest_diff}", "\n",
              f"Latest differences between MACD / MACD signal-line: {latest_macd}")

        idx = 0
        for symbol, var in latest_var.items():
            print(f"{symbol} - # of Long Shares: {trader.get_portfolio_item(symbol).get_long_shares()}")
            print(f"{symbol} - # of Short Shares: {trader.get_portfolio_item(symbol).get_short_shares()}")

            if var >= threshold_1[idx] and latest_interest_diff[symbol] > threshold_2[idx]:
                if latest_macd[symbol] >= 0 and trader.get_portfolio_item(symbol).get_long_shares() != 100:
                    size = int((trader.get_portfolio_item(symbol).get_short_shares() + (100 - trader.get_portfolio_item(symbol).get_long_shares())) / 100)
                    print(f"Buy {symbol}: Contract Size: {size}")
                    market_order(trader, 'buy', symbol, size)
                    time.sleep(0.05)

                elif latest_macd[symbol] < 0 and trader.get_portfolio_item(symbol).get_short_shares() != 100:
                    size = int((trader.get_portfolio_item(symbol).get_long_shares() + (100 - trader.get_portfolio_item(symbol).get_short_shares())) / 100)
                    print(f"Sell {symbol}: Contract Size: {size}")
                    market_order(trader, 'sell', symbol, size)
                    time.sleep(0.05)

                else:
                    pass

            else:
                pass
            idx += 1

        # We are going to hold each position for 1 min then close#
        time.sleep(60)
        for order in trader.get_submitted_orders():
            if order.status == shift.Order.Status.FILLED:
                trade_count += 1

        trader.cancel_all_pending_orders()
        close_positions(trader)

        print(f"PnL: {trader.get_portfolio_summary().get_total_realized_pl()}")

        if prices.index[-1].time() >= datetime.time(hour=15,minute=30,second=0):

            trader.cancel_all_pending_orders()
            close_positions(trader)

            if trade_count < 40:
                for _ in range(40 - trade_count):
                    market_order(trader, "buy", "SPY", 1)
                    time.sleep(5)
                time.sleep(600)
                market_order(trader, "sell", "SPY", 1)

            else:
                market_order(trader, "buy", "SPY", 5)
                time.sleep(600)
                market_order(trader, "sell", "SPY", 5)


            submitted_orders = pd.DataFrame(columns = ["Symbol", "Type", "Price", "Size", "Executed_Size", "ID", "Status"])
            for order in trader.get_submitted_orders():
                if order.status == shift.Order.Status.FILLED:
                    price = order.executed_price
                else:
                    price = order.price
                submission = np.array([order.symbol, order.type, price, order.size, order.executed_price, order.id, order.status])
                submitted_orders.loc[order.timestamp, :] = submission

            print(f"PnL: {trader.get_portfolio_summary().get_total_realized_pl()}")

            trader.disconnect()

            # submitted_orders.to_csv(r'submitted_orders_004.csv')