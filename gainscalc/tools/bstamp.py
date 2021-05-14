import pandas as pd

TX_TYPES = {'Withdrawal': 'spend',
            'Deposit': 'receive'}
STASH_TYPES = {'Withdrawal': 'stash',
               'Deposit': 'unstash'}


def parse_float(x):
    try:
        return float(x.split(' ')[0])
    except AttributeError: # it was nan
        return 0


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
    df['asset'] = df.Amount.apply(lambda x: x.split(' ')[1])
    df['type'] = df.apply(parse_type, axis=1)
    return df


if __name__ == '__main__':
    df = read_bitstamp(csv)

