import sys
import time

import numpy as np
import pandas as pd
import shift

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



##Can run this to create a price array for the universe of securities##
def create_data():

    ls = trader.get_stock_list()

    prices = np.zeros((30,len(ls)))

    for i in len(prices):
        p = []
        for sym in ls:
            p_temp = trader.get_last_price(sym)
            p.append(p_temp)
        prices[i] = p
        time.sleep(20)

    moving_av = np.mean(prices, axis=0, keepdims=True)

    return ls, prices, moving_av


##Can run this every "x" seconds to update our moving averages##
def update_data(ls, prices):

    p=[]
    for sym in ls:
        p_temp = trader.get_last_price(sym)
        p.append(p_temp)

    prices = np.append(prices, np.matrix(p), axis=0)[1:]
    moving_av = np.mean(prices, axis=0, keepdims=True)

    return prices, moving_av


if __name__== "__main__":

    trader = subscribe()
    ls, prices, moving_av = create_data()
    trader.disconnect()

    while True:
        #test momentum condition#
        test = np.where(abs((moving_av-prices[-1])/prices[-1])>=0.02, True, False)
        #retreive indices of condition#
        if np.any(test) == True:
            indices = list(np.where(test==True)[1])

            securities = [ls[i] for i in indices]

            test_prices = prices[-1][indices]
            test_ma = moving_av[indices]

            signals = list(test_prices - test_ma)
            signals = ["BUY" if x>=0 else "SELL" for x in signals]

            #check#
            if len(signals)%len(securities) == 0:
                #if this holds then place orders#
                trader=subscribe()
                for idx, i in enumerate(signals):
                    market_order(order_type=i,symbol=securities[idx],contract_size=10)
                trader.disconnect()
            else: print("Error in data retrieval.")

            time.sleep(600)
            signals = ["SELL" if x=="BUY" else "BUY" for x in signals]

            trader = subscribe()
            for idx, i in enumerate(signals):
                market_order(order_type=i, symbol=securities[idx], contract_size=10)
            trader.disconnect()

        else:
            time.sleep(30)
            trader = subscribe()
            prices, moving_av = update_data(ls,prices)






























if __name__ == "__main__":

    trader = shift.Trader("test001")

    # connect and subscribe to all available order books
    try:
        trader.connect("initiator.cfg", "password")
        trader.sub_all_order_book()
    except shift.IncorrectPasswordError as e:
        print(e)
    except shift.ConnectionTimeoutError as e:
        print(e)

    p = trader.get_sample_prices_size("AAPL")
    ls = trader.get_stock_list()

    print(p)

    trader.disconnect()