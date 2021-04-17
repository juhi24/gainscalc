#!/usr/bin/env python3
import os
import pandas as pd


def yearpath(year, datadir='/tmp'):
    return os.path.join(datadir, '{}.pkl'.format(year))


def download(year, **kws):
    import yfinance as yf
    outfile = yearpath(year, **kws)
    data = yf.download('ETH-EUR', 
                       start='{}-01-01'.format(year), 
                       end='{}-01-01'.format(year+1), interval='1h')
    data.to_pickle(outfile)
    
    
def load(year, **kws):
    filepath = yearpath(year, **kws)
    if not os.path.isfile(filepath):
        download(year)
    return pd.read_pickle(filepath)


if __name__ == "__main__":
    data = load(2020)
    data = data.apply(lambda row: (row.Open+row.Close)/2, axis=1)
    trades = pd.read_csv('/tmp/export-address-token-0x7.csv', index_col='Txhash', 
                         usecols=('Txhash', 'DateTime', 'Value'), parse_dates=['DateTime'])
    trades['ETHEUR'] = trades.apply(lambda t: data.iloc[data.index.get_loc(t.DateTime, method='pad')], axis=1)
    trades['EURValue'] = trades.apply(lambda row: row.Value*row.ETHEUR, axis=1)
    trades.to_csv('/tmp/2020eur.csv')