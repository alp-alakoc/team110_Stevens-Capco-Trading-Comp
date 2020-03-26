import shift
import numpy as np
import pandas as pd

trader = shift.Trader("test001")

# connect and subscribe to all available order books
try:
    trader.connect("initiator.cfg", "password")
    trader.sub_all_order_book()
except shift.IncorrectPasswordError as e:
    print(e)
except shift.ConnectionTimeoutError as e:
    print(e)

trader.









