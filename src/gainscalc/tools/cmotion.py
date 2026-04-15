
import math

import pandas as pd

from gainscalc.tools import parse_float

EUR = 'EUR'
TX_TYPES = {'Pikaosto': 'buy',
            'Pikamyynti': 'sell',
            'Myyntitarjous': 'sell',
            'Holvi-talletus': 'stash',
            'Holvi-nosto': 'unstash',
            'Palkkio': 'receive',
            'Lähetetty maksu': 'spend',
            'Vastaanotettu maksu': 'receive'}


def read_coinmotion(csv, year=0, until=None, asset='BTC'):
    df = pd.read_csv(csv, parse_dates=['Date'], dayfirst=True)
    #df = df[df.Account.apply(lambda x: x in [asset, 'EUR'])]
    df = df[df.Status=='Valmis']
    df['currency'] = 'EUR'
    df['amount'] = df.Amount.apply(parse_float).apply(abs)
    df.rename(columns={'Account': 'asset', 'Date': 'date'}, inplace=True)
    df['type'] = df.Type.apply(txtype)
    df['unitvalue'] = df.Rate.apply(rate)
    df = df[df.type != 'irrelevant']
    if year:
        df = df[df.date.apply(lambda t: t.year==year)]
    if until:
        df = df[df.date<=until]
    return df[::-1]


def drop_asset(df, asset):
    txs = df[df.asset == asset]
    for i, tx in txs.iterrows():
        selection = (df['asset']=='EUR') & (df['date']==tx['date']) & (df['Type']==tx['Type'])
        df = df[-selection]
    return df[df.asset != asset]


def txtype(typein):
    try:
        return TX_TYPES[typein]
    except KeyError:
        return 'irrelevant'


def rate(ratestr):
    try:
        ratestr = ratestr.replace('(', '').replace(')','')
    except AttributeError:
        return math.nan
    return float(ratestr.split(' €')[0])


