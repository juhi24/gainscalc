#!/usr/bin/env python3
import datetime

import pandas as pd

from gainscalc.tools.bstamp import read_bitstamp
from gainscalc.fifo import FIFODeque


def select_year(df, year, datecol='date'):
    cond1 = df[datecol] > datetime.datetime(year-1, 12, 31)
    cond2 = df[datecol] < datetime.datetime(year+1, 1, 1)
    return df[cond1 & cond2]


def find_pair(pairs, row):
    for pair in pairs:
        currency_cond1 = pair.currency == row.currency
        currency_cond2 = row.currency is None
        currency_cond = currency_cond1 or currency_cond2
        if (pair.asset == row.asset) and currency_cond:
            return pair


class Pair:
    def __init__(self, asset, currency):
        self.xc = FIFODeque()
        self.stash = FIFODeque()
        self.asset = asset
        self.currency = currency
        self.gains = []

    def gains_df(self, year=2020):
        return select_year(pd.DataFrame(self.gains), year)


def main(df, pairs):
    for i, row in df.iterrows():
        pair = find_pair(pairs, row)
        if pair is None:
            continue
        xc = pair.xc
        stash = pair.stash
        if row.type == 'buy':
            xc.buy(row.date, row.amount, row.unitvalue)
        if row.type == 'sell':
            try:
                gain = xc.sell(row.date, row.amount, row.unitvalue)
                pair.gains.append(dict(date=row.date, value=gain))
            except IndexError as e:
                print(pair.asset)
                print(e)
                continue
        if row.type == 'stash':
            xc.send(stash, row.amount)
        if row.type == 'unstash':
            stash.send(xc, row.amount)


if __name__ == '__main__':
    df = read_bitstamp(csv)
    btcusd = Pair('BTC', 'USD')
    ethusd = Pair('ETH', 'USD')
    main(df, [btcusd, ethusd])
    btcfee20 = select_year(df, 2020, datecol='Datetime').fee
