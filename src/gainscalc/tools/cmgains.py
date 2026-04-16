"""Compute and report capital gains from a Coinmotion transaction statement.

Usage
-----
    # Basic — uses market rate at deposit time as cost basis for deposits:
    cmgains reports/TransactionStatement-20260415-214021.csv --year 2025

    # With deposit supplement (actual acquisition costs):
    cmgains reports/TransactionStatement-20260415-214021.csv --year 2025 \\
        --supplement reports/deposits.csv

    # Also write lot-level detail to a CSV:
    cmgains reports/TransactionStatement-20260415-214021.csv --year 2025 \\
        --supplement reports/deposits.csv \\
        --output reports/gains2025.csv
"""

import click
import pandas as pd

from gainscalc.tools.cmotion import read_coinmotion
from gainscalc.tools.gains import Pair, main as gains_main


def _pairs_from_df(df):
    """Return one Pair per (asset, currency) combination found in *df*."""
    combos = df[["asset", "currency"]].drop_duplicates()
    return [Pair(row.asset, row.currency) for row in combos.itertuples()]


def _print_report(pairs, year):
    """Print a human-readable gains summary for *year* to stdout."""
    grand_total = 0.0
    print(f"\n=== Capital gains {year} ===\n")
    for pair in pairs:
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
    """Write lot-level detail from each pair's cost-basis book to a CSV."""
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
@click.argument("csv_path", metavar="CSV")
@click.option("--year", "-y", default=2025, show_default=True,
              help="Calendar year to report gains for.")
@click.option("--supplement", "-s", default=None, metavar="SUPPLEMENT",
              help="Filled-in deposit cost-basis CSV (from deposit-form).")
@click.option("--output", "-o", default=None, metavar="OUTPUT",
              help="Write lot-level detail to this CSV file.")
def main(csv_path, year, supplement, output):
    """Compute capital gains from a Coinmotion transaction statement CSV.

    Processes the full transaction history (required for correct FIFO) and
    reports gains for the requested YEAR.
    """
    # Load full history — no year filter here; FIFO must consume all lots.
    df = read_coinmotion(csv_path, supplement=supplement)

    pairs = _pairs_from_df(df)
    gains_main(df, pairs)

    _print_report(pairs, year)

    if output:
        _write_book(pairs, year, output)
