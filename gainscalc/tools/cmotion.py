#!/usr/bin/env python3

import math

import pandas as pd

EUR = 'EUR'
TX_TYPES = {'Pikaosto': 'buy',
            'Pikamyynti': 'sell',
            'Myyntitarjous': 'sell',
            'Holvi-talletus': 'stash',
            'Holvi-nosto': 'unstash',
            'Palkkio': 'receive',
            'Lähetetty maksu': 'spend'}


def read_coinmotion(csv, year=0, asset='BTC'):
    df = pd.read_csv(csv, parse_dates=['Date'])
    #df = df[df.Account.apply(lambda x: x in [asset, 'EUR'])]
    df = df[df.Status=='Valmis']
    if year:
        df = df[df.Date.apply(lambda t: t.year==year)]
    return df


def drop_asset(df, asset):
    txs = df[df.Account == asset]
    for i, tx in txs.iterrows():
        selection = (df['Account']=='EUR') & (df['Date']==tx['Date']) & (df['Type']==tx['Type'])
        df = df[-selection]
    return df[df.Account != asset]


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


if __name__ == '__main__':
    cm_csv = '/home/jussi24/Asiakirjat/talous/coinmotion_balances-20210424-182437.csv'
    df = read_coinmotion(cm_csv, year=2020)
    df['txtype'] = df.Type.apply(txtype)
    df['unitvalue'] = df.Rate.apply(rate)
    df = df[df.txtype != 'irrelevant']
    df_btc = drop_asset(df, 'ETH')
    df_btc = drop_asset(df_btc, 'LTC')
