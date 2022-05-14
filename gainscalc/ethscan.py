#!/usr/bin/env python3
import os

import click
import pandas as pd

from gainscalc.rates import load


@click.command()
@click.argument('infile')
@click.argument('outfile')
def main(infile, outfile):
    os.makedirs(os.path.dirname(outfile), exist_ok=True)
    trades = pd.read_csv(infile, index_col='Txhash',
                         usecols=('Txhash', 'DateTime', 'Value'), parse_dates=['DateTime'])
    year = pd.Timestamp(trades.DateTime.values[0]).year
    data = load(year)
    data = data.apply(lambda row: (row.Open+row.Close)/2, axis=1)
    trades['ETHEUR'] = trades.apply(lambda t: data.iloc[data.index.get_loc(t.DateTime, method='pad')], axis=1)
    trades['EURValue'] = trades.apply(lambda row: row.Value*row.ETHEUR, axis=1)
    trades.to_csv(outfile)



if __name__ == "__main__":
    main()