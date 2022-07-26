from csv import DictWriter
import pandas as pd
import requests
import json
import math
from place_order_demat import place_order_zerodha as place_order
from datetime import datetime as dt
from datetime import timedelta
from dateutil.relativedelta import relativedelta, TH
import os
from zerodha_auth import zerodha_connect
import datetime as DT
import dateutil.relativedelta as REL
from datetime import date


def nearest_strike_nf(x):
    return round_nearest(x, 50)


def custom_algo(date_format_final, nearest, quantity, strategy, option, normal_order):
    if not normal_order:
        if strategy == "MARGIN":
            if option == "buy":
                place_order("NIFTY{}{}PE".format(date_format_final, nearest), quantity, 1, strategy)
                place_order("NIFTY{}{}PE".format(date_format_final, nearest - 200), quantity, -1, strategy)
            else:
                place_order("NIFTY{}{}CE".format(date_format_final, nearest), quantity, 1, strategy)
                place_order("NIFTY{}{}CE".format(date_format_final, nearest + 200), quantity, -1, strategy)
        else:
            if option == "buy":
                place_order("NIFTY{}{}PE".format(date_format_final, nearest), quantity, 1, strategy)
            else:
                place_order("NIFTY{}{}CE".format(date_format_final, nearest), quantity, 1, strategy)
    else:
        if strategy == "MARGIN":
            if option == "buy":
                place_order("NIFTY{}{}PE".format(date_format_final, nearest - 200), quantity, 1, strategy)
                place_order("NIFTY{}{}PE".format(date_format_final, nearest), quantity, -1, strategy)
            else:
                place_order("NIFTY{}{}CE".format(date_format_final, nearest + 200), quantity, 1, strategy)
                place_order("NIFTY{}{}CE".format(date_format_final, nearest), quantity, -1, strategy)
        else:
            if option == "buy":
                place_order("NIFTY{}{}PE".format(date_format_final, nearest), quantity, -1, strategy)
            else:
                place_order("NIFTY{}{}CE".format(date_format_final, nearest), quantity, -1, strategy)


def algo_imp(option, strategy, expiry, nearest=0, quantity=0, normal_order=True, pyramidin=False):
    expiry = dt.strptime(expiry, "%d-%b-%Y")
    end_of_month = dt.today() + relativedelta(day=31)
    last_thursday = end_of_month + relativedelta(weekday=TH(-1))
    last_before_thursday = last_thursday - timedelta(days=7)

    def monthly_expiry(expiry):
        date_formateed = str(expiry)[:10]
        year = date_formateed[2:4]
        month = (expiry.strftime("%B"))
        month = (month.upper())[:3]
        date_format_final = year + month
        return date_format_final

    def weekly_expiry(expiry):
        date_formateed = str(expiry)[:10]
        year = date_formateed[2:4]
        month = str(int(date_formateed[5:7]))
        if month == '10':
            month = "O"
        if month == '11':
            month = "N"
        if month == '12':
            month = "D"

        day = str(int(date_formateed[8:10]))
        date_format_final = year + month + day
        return date_format_final

    if last_before_thursday <= dt.today() <= end_of_month:
        date_format_final = monthly_expiry(expiry)
    #    elif strategy == "MARGIN":
    #        if last_thursday <= expiry <= end_of_month:
    #            date_format_final = monthly_expiry(last_thursday +  timedelta(days=7))
    #        else:
    #            date_format_final = monthly_expiry(expiry)
    else:
        if expiry == dt.now():
            expiry = expiry + timedelta(days=7)
            if last_before_thursday <= expiry <= end_of_month:
                date_format_final = monthly_expiry(expiry)
            else:
                date_format_final = weekly_expiry(expiry)
        else:
            date_format_final = weekly_expiry(expiry)

    def insert_nearest(nearest, quantity, strategy, option, entry, pyramidin):

        field_names = ['nearest', 'quantity',
                       'strategy', 'option', "entry", "pyra"]

        dict = {'nearest': nearest, 'quantity': quantity, 'strategy': strategy,
                'option': option, "entry": entry, "pyra": pyramidin}

        with open('{}_{}.csv'.format(option, strategy), 'a') as f_object:
            dictwriter_object = DictWriter(f_object, fieldnames=field_names)
            dictwriter_object.writerow(dict)
            f_object.close()

    def multi_close(date_format_final, nearest, quantity, strategy, option, normal_order):
        file = option + "_" + strategy
        if os.stat("{}.csv".format(file)).st_size == 0:
            print('empty')
        existing_alert = pd.read_csv("{}.csv".format(file), header=None)
        existing_alert.columns = ['strike', 'quantity', 'strategy', 'option', 'entry', 'pyra']
        nearest_list = list(existing_alert['strike'])
        for nearest in nearest_list:
            custom_algo(date_format_final, nearest, quantity, strategy, option, normal_order)

        return nearest

    if not normal_order:
        multi_close(date_format_final, nearest, quantity, strategy, option, normal_order)
        delete_file(option+"_"+strategy)
    else:
        custom_algo(date_format_final, nearest, quantity, strategy, option, normal_order)
    insert_nearest(nearest, quantity, strategy, option, normal_order, pyramidin)


def delete_file(file):
    f = open("{}.csv".format(file), "w+")
    f.close()


def get_next_thursday():
    today = DT.date.today()
    rd = REL.relativedelta(days=1, weekday=REL.TH)
    next_thursday = today + rd
    next_thursday = str(next_thursday.strftime('%d-%b-%Y'))
    return next_thursday


def algo(option, strategy, quantity, normal_order=True, pyramidin=False):
    while True:
        try:
            kite = zerodha_connect()
            ltp = ((kite.ltp('NSE:NIFTY 50'))["NSE:NIFTY 50"])["last_price"]
            nf_nearest = nearest_strike_nf(ltp)
            break
        except:
            print("unable to fetch nse data")

    next_thursday = get_next_thursday()

    algo_imp(option, strategy, next_thursday, nf_nearest, quantity, normal_order, pyramidin)


if __name__ == '__main__':
    algo("sell", "INTRADAY", 50)
