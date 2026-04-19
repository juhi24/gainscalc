# coding: utf-8
"""Exchange rate fetching and caching.

Sources
-------
EUR/USD  — Frankfurter API (ECB-backed, https://www.frankfurter.app).
           ``pair='EUR=X'`` returns USD per 1 EUR, matching the old Yahoo
           Finance convention so callers don't need to change.
Crypto   — CryptoCompare free API (https://min-api.cryptocompare.com).
           ``pair='BTC-EUR'`` etc. returns EUR per 1 unit of the asset.

Rates are cached as pickled DataFrames in /tmp, keyed by (pair, year).
Weekend/holiday gaps are forward-filled from the previous available day.
"""

import datetime
import os

import pandas as pd
import requests as _http

_SESSION = _http.Session()


def yearpath(year, pair, datadir='/tmp'):
    fmt = '{pair}{year}.pkl'
    pr = pair.lower().split('=')[0]
    return os.path.join(datadir, fmt.format(pair=pr, year=year))


# ---------------------------------------------------------------------------
# Per-source downloaders
# ---------------------------------------------------------------------------

def _download_eur_usd(year):
    """Return a DataFrame (Open, Close) of USD per 1 EUR for *year*.

    Uses Frankfurter (ECB data).  Frankfurter returns only business days;
    weekends/holidays are forward-filled so every calendar day has a value.
    """
    r = _SESSION.get(
        f'https://api.frankfurter.app/{year}-01-01..{year}-12-31',
        params={'from': 'EUR', 'to': 'USD'},
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    if 'rates' not in data:
        raise ValueError(f"Frankfurter returned no data for EUR=X {year}: {data}")
    records = {
        pd.Timestamp(date_str): v['USD']
        for date_str, v in data['rates'].items()
    }
    s = pd.Series(records, name='Open')
    df = s.to_frame()
    df['Close'] = df['Open']
    full = pd.date_range(f'{year}-01-01', f'{year}-12-31', freq='D')
    return df.reindex(full).ffill().bfill()


def _download_crypto_eur(asset, year):
    """Return a DataFrame (Open, Close) of EUR per 1 unit of *asset* for *year*.

    Uses CryptoCompare free API.  *asset* is the symbol without '-EUR'
    (e.g. 'BTC', 'ETH').
    """
    to_ts = int(datetime.datetime(year + 1, 1, 2).timestamp())
    r = _SESSION.get(
        'https://min-api.cryptocompare.com/data/v2/histoday',
        params={'fsym': asset, 'tsym': 'EUR', 'limit': 366, 'toTs': to_ts},
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    if data.get('Response') != 'Success':
        raise ValueError(
            f"CryptoCompare error for {asset}/EUR {year}: {data.get('Message', data)}"
        )
    rows = [
        row for row in data['Data']['Data']
        if datetime.datetime.fromtimestamp(row['time']).year == year
        and row['open'] > 0
    ]
    if not rows:
        raise ValueError(f"No CryptoCompare data for {asset}/EUR in {year}")
    df = pd.DataFrame(
        {
            'Open': [row['open'] for row in rows],
            'Close': [row['close'] for row in rows],
        },
        index=pd.to_datetime([
            datetime.datetime.fromtimestamp(row['time']).date() for row in rows
        ]),
    )
    full = pd.date_range(f'{year}-01-01', f'{year}-12-31', freq='D')
    return df.reindex(full).ffill().bfill()


# ---------------------------------------------------------------------------
# Public API (same interface as before)
# ---------------------------------------------------------------------------

def download(year, pair='ETH-EUR', **kws):
    """Download and pickle a full year of daily rates for *pair*."""
    outfile = yearpath(year, pair, **kws)
    if pair == 'EUR=X':
        df = _download_eur_usd(year)
    else:
        asset = pair.split('-')[0]
        df = _download_crypto_eur(asset, year)
    df.to_pickle(outfile)


def load(year, pair='ETH-EUR', **kws):
    """Return cached DataFrame for *pair*/*year*, downloading if needed."""
    path = yearpath(year, pair, **kws)
    if os.path.isfile(path):
        df = pd.read_pickle(path)
        if not df.empty:
            return df
        os.remove(path)
    download(year, pair, **kws)
    return pd.read_pickle(path)


def rate(timestamp, pair, field='Open'):
    """Return the daily rate for *pair* on *timestamp*.

    EUR=X  : USD per 1 EUR (ECB via Frankfurter).
    BTC-EUR: EUR per 1 BTC (CryptoCompare).
    """
    data = load(timestamp.year, pair=pair)
    col = data[field]
    result = col.loc[timestamp.date():timestamp.date()]
    if result.empty:
        raise ValueError(
            f"No rate data for {pair} on {timestamp.date()} "
            f"(year {timestamp.year} unavailable)"
        )
    return result.iloc[0]


def dfrowconvert(row, target):
    if row.currency is None or row.currency == target:
        return row
    out = row.copy()
    pair = '{}-{}'.format(row.asset, target)
    feepair = '{}-{}'.format(row.currency, target)
    out.unitvalue = rate(row.date, pair)
    out.fee = rate(row.date, feepair) * row.fee
    out.currency = target
    return out


def _rowconvert(row, prefix='sell', **kws):
    rt = rate(row['{}date'.format(prefix)], **kws)
    return rt * row['{}value'.format(prefix)]


def convertbook(book, pair):
    out = book.copy()
    out['buyvalueconv'] = book.apply(_rowconvert, axis=1, prefix='buy', pair=pair)
    out['sellvalueconv'] = book.apply(_rowconvert, axis=1, prefix='sell', pair=pair)
    return out
