#!/usr/bin/env python3
import os
import datetime

import pandas as pd

from gainscalc.tools.bstamp import read_bitstamp
from gainscalc.fifo import FIFODeque
from gainscalc.rates import convertbook


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
        if asset=='ETH':
            self.xc.buy(df.iloc[0].Datetime, 8, 1) # prevent emptying
        else:
            self.xc.buy(df.iloc[0].Datetime, 0.0001, 1) # prevent emptying
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
            xc.buy(row.Datetime, row.amount, row.unitvalue)
        if row.type == 'sell':
            try:
                gain = xc.sell(row.Datetime, row.amount, row.unitvalue)
                pair.gains.append(dict(date=row.Datetime, value=gain))
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
    outdir = '/tmp/report'
    os.makedirs(outdir, exist_ok=True)
    btcusd = Pair('BTC', 'USD')
    ethusd = Pair('ETH', 'USD')
    main(df, [btcusd, ethusd])
    dateformatter = lambda t: t.strftime('%Y-%d-%m %H:%M')
    header = ['ostohetki', 'myyntihetki', 'määrä',
              'ostohinta ($)', 'myyntihinta ($)',
              'ostohinta (€)', 'myyntihinta (€)']
    formatters = dict(buydate=dateformatter, selldate=dateformatter)
    for key, pair in dict(btc=btcusd, eth=ethusd).items():
        report20 = select_year(pair.xc.book, 2020, datecol='selldate')
        report20 = convertbook(report20, 'EUR=X')
        tabfile = os.path.join(outdir, '{}.tex'.format(key))
        report20.to_latex(tabfile, index=False, formatters=formatters,
                          header=header, label='tab:'+key,
                          caption=key.upper()+' osto ja myyntihinnat. Hinnat ovat kokonaishintoja.')
    btcfee20 = select_year(df, 2020, datecol='Datetime').fee
