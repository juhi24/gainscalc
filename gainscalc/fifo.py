#!/usr/bin/env python3

# builtin
from collections import deque
import datetime

# pypi
import pandas as pd


# acquisition cost assumption
ACA_COEF_LOW = 0.2
ACA_COEF_HIGH = 0.4
ACA_TIME_THRESHOLD = datetime.timedelta(days=3652) # 10 years


def aca_coef(buydate, selldate):
    """acquisition cost assumption coefficient based on hodl time"""
    if selldate-buydate > ACA_TIME_THRESHOLD:
        return ACA_COEF_HIGH
    return ACA_COEF_LOW


def aca_buyvalue(buyprice, sellprice, buydate, selldate):
    """buyprice taking into account acquisition cost assumption where useful"""
    c_aca = aca_coef(buydate, selldate)
    buyprice_aca = c_aca*sellprice
    return max(buyprice, buyprice_aca)


class FIFODeque:
    """a wallet/exchange model/calculator using the FIFO principle"""

    def __init__(self):
        self._wallet = deque()

    def _chronology_check(self, date):
        try:
            last_date = self._wallet[-1]['date']
        except IndexError:
            return
        if date < last_date:
            efmt = 'Transaction {} is older than the latest {}'
            raise ValueError(efmt.format(date, last_date))

    def buy(self, date, amount, unitvalue):
        self._chronology_check(date)
        tx = dict(date=date, amount=amount, unitvalue=unitvalue)
        self._wallet.append(tx)

    def sell(self, date, amount, unitvalue):
        self._chronology_check(date)
        tmp_amount = 0
        buyprice = 0
        while True:
            fi = self._wallet.popleft()
            tmp_amount += fi['amount']
            buyunitvalue = aca_buyvalue(fi['unitvalue'], unitvalue, fi['date'], date)
            if tmp_amount >= amount:
                break
            buyprice += fi['amount']*buyunitvalue
        back_amount = tmp_amount-amount
        fi_spent = self._put_residual_back(back_amount, fi)
        buyprice += buyunitvalue*fi_spent['amount']
        return amount*unitvalue - buyprice

    def _put_residual_back(self, back_amount, fi):
        fi_spent = fi.copy()
        fi_residual = fi.copy()
        if back_amount < 0:
            raise Exception('back_amount = {} < 0'.format(back_amount))
        elif back_amount > 0:
            fi_residual.update(amount=back_amount)
            fi_spent.update(amount=fi_spent['amount']-back_amount)
            self._wallet.appendleft(fi_residual)
        return fi_spent

    def to_dataframe(self):
        df = pd.DataFrame(self._wallet)
        return df.set_index('date')

    def receive(self, tx_in):
        for i, tx in enumerate(self._wallet):
            if tx['date'] > tx_in['date']:
                self._wallet.insert(i, tx_in)
                return
        self._wallet.append(tx_in)

    def extract(self, amount):
        tmp_amount = 0
        while True:
            fi = self._wallet.popleft()
            tmp_amount += fi['amount']
            if tmp_amount >= amount:
                break
            yield fi
        back_amount = tmp_amount-amount
        yield self._put_residual_back(back_amount, fi)

    def send(self, other, amount):
        for tx in self.extract(amount):
            other.receive(tx)

