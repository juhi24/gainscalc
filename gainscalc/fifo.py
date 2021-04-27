#!/usr/bin/env python3

# builtin
from collections import deque
import datetime

# pypi
import pandas as pd


CM_TX_TYPES = {'Pikaosto': 'buy',
               'Pikamyynti': 'sell',
               'Myyntitarjous': 'sell',
               'Holvi-talletus': 'stash',
               'Holvi-nosto': 'unstash',
               'Palkkio': 'new'}
SAT_PER_BTC = 100000000

# acquisition cost assumption
ACA_COEF_LOW = 0.2
ACA_COEF_HIGH = 0.4
ACA_TIME_THRESHOLD = datetime.timedelta(days=3652) # 10 years


def read_coinmotion(csv, asset='BTC'):
    df = pd.read_csv(csv, parse_dates=['Date'])
    df = df[df.Account.apply(lambda x: x in [asset, 'EUR'])]
    df = df[df.Status=='Valmis']
    
def aca_coef(buydate, selldate):
    """acquisition cost assumption coefficient based on hodl time"""
    if selldate-buydate > ACA_TIME_THRESHOLD:
        return ACA_COEF_HIGH
    return ACA_COEF_LOW


class FIFODeque:
    def __init__(self):
        self._wallet = deque()
    
    def buy(self, date, amount, unitvalue):
        tx = dict(date=date, amount=amount, unitvalue=unitvalue)
        self._wallet.append(tx)

    def sell(self, date, amount, value):
        tmp_amount = 0
        gain = 0
        while True:
            fi = self._wallet.popleft()
            tmp_amount += fi['amount']
            if tmp_amount >= amount:
                break
        back_amount = tmp_amount-amount
        if back_amount < 0:
            raise Exception('back_amount = {} < 0'.format(back_amount))
        elif back_amount > 0:
            fi.update(amount=back_amount)
            self._wallet.appendleft(fi)
        
        return gain

    def to_dataframe(self):
        df = pd.DataFrame(self._wallet)
        return df.set_index('date')


class FIFOtxs:
    def __init__(self, init_balance=0, asset='BTC'):
        self._init_balance = init_balance
        self._asset = asset
        