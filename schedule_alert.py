from datetime import datetime as dt
from datetime import timedelta
from dateutil.relativedelta import relativedelta, TH
import dateutil.relativedelta as REL
import schedule
import datetime as DT
import time
import os
import pandas as pd
import schedule
import time
from algo_trading import algo_imp
from csv import DictWriter
import requests
import json
import math
from datetime import datetime as dt
from datetime import timedelta
from dateutil.relativedelta import relativedelta, TH
import os
from zerodha_auth import zerodha_connect


def nearest_strike_nf(x):
    return round_nearest(x, 50)


def tick():
    # today is market day , dont go into the loop
    while True:
        try:
            kite = zerodha_connect()
            ltp = ((kite.ltp('NSE:NIFTY 50'))["NSE:NIFTY 50"])["last_price"]
            nf_nearest = nearest_strike_nf(ltp)
            break
        except:
            print("unable to fetch nse data")

    files = ["buy_INTRADAY", "sell_INTRADAY", "buy_MARGIN", "sell_MARGIN"]

    for file in files:
        import os
        if os.stat("{}.csv".format(file)).st_size == 0:
            print('empty')
            continue
        existing_alert = pd.read_csv("{}.csv".format(file), header=None)
        existing_alert.columns = ['strike', 'quantity', 'strategy', 'option', 'entry', 'pyra']

        if False in set(existing_alert["entry"]):
            f = open("{}.csv".format(file), "w+")
            f.close()

        else:
            nearest = (existing_alert).iloc[-1, 0]
            quantity = (existing_alert).iloc[-1, 1]
            strategy = (existing_alert).iloc[-1, 2]
            option = (existing_alert).iloc[-1, 3]

            if strategy == "INTRADAY":
                if option == 'buy':
                    if ltp >= nearest + 50:
                        today = DT.date.today()
                        rd = REL.relativedelta(days=1, weekday=REL.TH)
                        next_thursday = today + rd
                        next_thursday = next_thursday + timedelta(days=7)
                        expiry = str(next_thursday.strftime('%d-%b-%Y'))
                        algo_imp(option, strategy, expiry, nf_nearest, quantity, True, True)
                else:
                    if ltp >= nearest - 50:
                        today = DT.date.today()
                        rd = REL.relativedelta(days=1, weekday=REL.TH)
                        next_thursday = today + rd
                        next_thursday = next_thursday + timedelta(days=7)
                        expiry = str(next_thursday.strftime('%d-%b-%Y'))
                        algo_imp(option, strategy, expiry, nf_nearest, quantity, True, True)

            else:

                if (list(existing_alert['pyra'])).count("True") >= 2:
                    pass
                else:
                    if option == 'buy':

                        if ltp >= nearest + 100:
                            today = DT.date.today()
                            rd = REL.relativedelta(days=1, weekday=REL.TH)
                            next_thursday = today + rd
                            next_thursday = next_thursday + timedelta(days=14)
                            if (list(existing_alert['pyra'])).count("True") >= 1:
                                next_thursday = next_thursday + timedelta(days=7)
                            else:
                                pass
                            expiry = str(next_thursday.strftime('%d-%b-%Y'))
                            algo_imp(option, strategy, expiry, nf_nearest, quantity, True, True)
                    else:
                        if ltp >= nearest - 100:
                            today = DT.date.today()
                            rd = REL.relativedelta(days=1, weekday=REL.TH)
                            next_thursday = today + rd
                            next_thursday = next_thursday + timedelta(days=14)
                            if (list(existing_alert['pyra'])).count("True") >= 1:
                                next_thursday = next_thursday + timedelta(days=7)
                            else:
                                pass
                            expiry = str(next_thursday.strftime('%d-%b-%Y'))
                            algo_imp(option, strategy, expiry, nf_nearest, quantity, True, True)


def start_schedule():
    schedule.every(1).minutes.do(tick)

    while True:
        schedule.run_pending()
        time.sleep(1)


#
if __name__ == '__main__':
    tick()
    # start_schedule()