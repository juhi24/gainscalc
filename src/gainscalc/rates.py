# coding: utf-8
"""
yahoo finance data
"""
import os

import pandas as pd


def dfrowconvert(row, target):
    if row.currency is None or row.currency==target:
        return row
    out = row.copy()
    pair = '{}-{}'.format(row.asset, target)
    feepair = '{}-{}'.format(row.currency, target)
    out.unitvalue = rate(row.date, pair)
    out.fee = rate(row.date, feepair)*row.fee
    out.currency = target
    return out


def yearpath(year, pair, datadir='/tmp'):
    fmt = '{pair}{year}.pkl'
    pr = pair.lower().split('=')[0]
    return os.path.join(datadir, fmt.format(pair=pr, year=year))


def _flatten_columns(data):
    """Flatten MultiIndex columns returned by newer yfinance versions."""
    if isinstance(data.columns, pd.MultiIndex):
        data = data.copy()
        data.columns = data.columns.get_level_values(0)
    return data


def download(year, pair='ETH-EUR', **kws):
    import yfinance as yf
    outfile = yearpath(year, pair, **kws)
    data = yf.download(pair,
                       start='{}-01-01'.format(year),
                       end='{}-01-01'.format(year+1), interval='1d')
    data = _flatten_columns(data)
    data = data.asfreq('1d').interpolate()
    data.to_pickle(outfile)


def load(year, pair='ETH-EUR', **kws):
    filepath = yearpath(year, pair, **kws)
    if not os.path.isfile(filepath):
        download(year, pair, **kws)
    data = _flatten_columns(pd.read_pickle(filepath))
    if data.empty:
        # Stale or failed download — delete and retry once.
        os.remove(filepath)
        download(year, pair, **kws)
        data = _flatten_columns(pd.read_pickle(filepath))
    return data


def rate(timestamp, pair, field='Open'):
    data = load(timestamp.year, pair=pair)
    col = data[field]
    result = col.loc[timestamp.date():timestamp.date()]
    if result.empty:
        raise ValueError(
            f"No rate data for {pair} on {timestamp.date()} "
            f"(year {timestamp.year} unavailable from Yahoo Finance)"
        )
    return result.iloc[0]


def _rowconvert(row, prefix='sell', **kws):
    rt = rate(row['{}date'.format(prefix)], **kws)
    return rt*row['{}value'.format(prefix)]


def convertbook(book, pair):
    out = book.copy()
    out['buyvalueconv'] = book.apply(_rowconvert, axis=1, prefix='buy', pair=pair)
    out['sellvalueconv'] = book.apply(_rowconvert, axis=1, prefix='sell', pair=pair)
    return out
