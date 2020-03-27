import sys
import time
import numpy as np
import pandas as pd
import shift
import datetime

def subscribe():
    trader = shift.Trader("test002")

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


def update_data(ls, prices, window=20):

    p=[trader.get_last_price(sym) for sym in ls ]
    prices.loc[trader.get_last_trade_time(),:] = p
    # if len(prices)>window:
    #     prices = prices.iloc[1:,:]
    return prices

def macd(close,period=(12,26,9)):
    diff = close.rolling(period[0]).mean()-close.rolling(period[1]).mean()
    dea = diff.rolling(period[2]).mean()
    res = diff-dea
    res.columns = ["macd"]
    return res

if __name__== "__main__":

    trader = subscribe()
    ls =  trader.get_stock_list()
    prices = pd.DataFrame(columns=ls)
    #ma_table = pd.DataFrame(columns=ls)
    count = 0
    count_while = 0
    while True:
        print(f"the {count_while} loop")
        if count_while==0:
            for _ in range(30):
                prices = update_data(ls, prices,40)
                print("pre loop")
                time.sleep(20)
        count_while+=1
        prices = update_data(ls, prices, 40)
        macd_table = prices.apply(macd,axis=0)
        latest_diff = macd_table.iloc[-1,:]
        for symbol,diff in latest_diff.items():
            if diff >= 0 and trader.get_portfolio_item(symbol).get_long_shares()!=100:
                size = int((trader.get_portfolio_item(symbol).get_short_shares()+(100-trader.get_portfolio_item(symbol).get_long_shares()))/100)
                print(f"now buy{symbol}, amount{size}")
                market_order(trader, "buy", symbol, contract_size=size)
                count += 1
                time.sleep(0.5)
            elif diff < 0 and trader.get_portfolio_item(symbol).get_short_shares()!=100:
                size = int((trader.get_portfolio_item(symbol).get_long_shares()+(100-trader.get_portfolio_item(symbol).get_short_shares()))/100)
                print(f"now sell{symbol}, amount{size}")
                market_order(trader, "sell", symbol, contract_size=size)
                count += 1
                time.sleep(0.5)
            else:
                pass
        print(f"prices are: {prices.iloc[-1,:]}")
        print("="*100)
        print(f"p&l is {trader.get_portfolio_summary().get_total_realized_pl()}")
        time.sleep(20)
        if prices.index[-1].time()>= datetime.time(hour=15,minute=30,second=0):
            if count<40:
                for _ in range(40-count):
                    print(f"buy VIXY")
                    market_order(trader, "buy", "VIXY", contract_size=5)
                    time.sleep(90)
            for item in trader.get_portfolio_items().values():
                if item.get_shares()<0:
                    print(f"buy {item.get_symbol()}")
                    market_order(trader, "buy", item.get_symbol(), contract_size=int(abs(item.get_shares())/100))
                if item.get_shares()>0:
                    print(f"sell {item.get_symbol()}")
                    market_order(trader, "sell", item.get_symbol(), contract_size=int(abs(item.get_shares())/100))
                time.sleep(1)








