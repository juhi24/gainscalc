"""Multi-source capital gains CLI.

Usage
-----
    gains --coinmotion reports/cm.csv \\
          --bitstamp   reports/bs.csv \\
          --supplement reports/supplement.csv \\
          --year 2025  \\
          [--output reports/gains2025.csv]

The full transaction history from all sources is processed (FIFO requires
all lots from day one).  Gains are reported for the requested calendar year.
"""

import click
import pandas as pd

from gainscalc.tools import supplement as supp_mod
from gainscalc.tools.gains import Pair, main as gains_main
from gainscalc.tools.multi import merge_sources, detect_transfers


def _pairs_from_df(df):
    combos = df[["asset", "currency"]].drop_duplicates()
    return [Pair(row.asset, row.currency) for row in combos.itertuples()]


def _print_report(pairs, year):
    grand_total = 0.0
    print(f"\n=== Capital gains {year} ===\n")
    for pair in sorted(pairs, key=lambda p: p.asset):
        year_gains = pair.gains_df(year=year)
        if year_gains.empty:
            continue
        print(f"{pair.asset}/{pair.currency}")
        total = 0.0
        for row in year_gains.itertuples():
            date_str = row.date.strftime("%Y-%m-%d")
            print(f"  {date_str}  gain: {row.value:>12.2f} EUR")
            total += row.value
        print(f"  {'Total ' + pair.asset + ':':<30} {total:>12.2f} EUR\n")
        grand_total += total
    print(f"{'GRAND TOTAL ' + str(year) + ':':<36} {grand_total:>12.2f} EUR\n")
    return grand_total


def _write_book(pairs, year, output_path):
    frames = []
    for pair in pairs:
        book = pair.xc.book.copy()
        if book.empty:
            continue
        year_book = book[book["selldate"].dt.year == year]
        if year_book.empty:
            continue
        year_book = year_book.copy()
        year_book.insert(0, "asset", pair.asset)
        frames.append(year_book)
    if frames:
        combined = pd.concat(frames, ignore_index=True)
        combined.to_csv(output_path, index=False)
        click.echo(f"Wrote lot detail ({len(combined)} rows) to {output_path}", err=True)
    else:
        click.echo(f"No sells in {year} — nothing written to {output_path}", err=True)


@click.command()
@click.option("--coinmotion", "cm_csv", default=None, metavar="CSV",
              help="Coinmotion transaction statement CSV.")
@click.option("--bitstamp", "bs_csv", default=None, metavar="CSV",
              help="Bitstamp transaction export CSV.")
@click.option("--supplement", "-s", "supplement_path", default=None, metavar="SUPPLEMENT",
              help="Unified supplement CSV (from transfer-form or deposit-form).")
@click.option("--year", "-y", default=2025, show_default=True,
              help="Calendar year to report gains for.")
@click.option("--output", "-o", default=None, metavar="OUTPUT",
              help="Write lot-level detail to this CSV file.")
def main(cm_csv, bs_csv, supplement_path, year, output):
    """Compute capital gains from one or more exchange transaction CSVs.

    Processes the full transaction history for correct FIFO cost basis and
    reports gains for the requested YEAR.
    """
    from gainscalc.tools.cmotion import read_coinmotion
    from gainscalc.tools.bstamp import read_bitstamp

    dfs = []
    if cm_csv:
        dfs.append(read_coinmotion(cm_csv))
    if bs_csv:
        click.echo("Reading Bitstamp CSV (may fetch exchange rates)…", err=True)
        dfs.append(read_bitstamp(bs_csv))

    if not dfs:
        raise click.UsageError("Provide at least one of --coinmotion or --bitstamp.")

    df = merge_sources(*dfs)

    if supplement_path:
        loaded = supp_mod.load_supplement(supplement_path)
        df = supp_mod.apply_supplement(df, loaded)

    pairs = _pairs_from_df(df)
    gains_main(df, pairs)

    _print_report(pairs, year)

    if output:
        _write_book(pairs, year, output)
