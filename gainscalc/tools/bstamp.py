import pandas as pd

from gainscalc.tools import parse_float

TX_TYPES = {'Withdrawal': 'spend',
            'Deposit': 'receive'}
STASH_TYPES = {'Withdrawal': 'stash',
               'Deposit': 'unstash'}


def parse_unit(x):
    try:
        return x.split(' ')[1]
    except AttributeError:
        return


def parse_type(row):
    subtype = row['Sub Type']
    try:
        if row.asset not in ('USD', 'EUR'):
            return STASH_TYPES[row.Type]
        return TX_TYPES[row.Type]
    except KeyError:
        pass
    return subtype.lower()


def read_bitstamp(csv):
    df = pd.read_csv(csv, parse_dates=['Datetime'])
    df['amount'] = df.Amount.apply(parse_float)
    df['fee'] = df.Fee.apply(parse_float)
    df['unitvalue'] = df.Rate.apply(parse_float)
    df['asset'] = df.Amount.apply(parse_unit)
    df['currency'] = df.Value.apply(parse_unit)
    df['type'] = df.apply(parse_type, axis=1)
    return df



