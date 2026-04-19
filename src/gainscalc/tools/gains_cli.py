"""gains — cryptocurrency capital gains CLI (Finnish tax rules).

Subcommands
-----------
  gains report      Compute capital gains from exchange transaction CSVs.
  gains supplement  Generate a deposit cost-basis supplement template.
  gains eth-export  Convert an Etherscan ETH transaction CSV to EUR values.
"""

import sys

import click
import pandas as pd


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _pairs_from_df(df):
    from gainscalc.tools.gains import Pair
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


# ---------------------------------------------------------------------------
# gains group
# ---------------------------------------------------------------------------

@click.group()
def gains():
    """gainscalc — cryptocurrency capital gains calculator (Finnish tax rules).

    \b
    Workflow:
      1. gains supplement --coinmotion cm.csv --bitstamp bs.csv -o supp.csv
      2. Fill in actual_buy_date / actual_unitvalue in supp.csv
      3. gains report --coinmotion cm.csv --bitstamp bs.csv -s supp.csv
    """


# ---------------------------------------------------------------------------
# gains report
# ---------------------------------------------------------------------------

@gains.command("report")
@click.option("--coinmotion", "cm_csv", default=None, metavar="CSV",
              help="Coinmotion transaction statement CSV.")
@click.option("--bitstamp", "bs_csv", default=None, metavar="CSV",
              help="Bitstamp transaction export CSV.")
@click.option("--supplement", "-s", "supplement_path", default=None, metavar="SUPPLEMENT",
              help="Deposit cost-basis supplement CSV (from 'gains supplement').")
@click.option("--year", "-y", default=2025, show_default=True,
              help="Calendar year to report gains for.")
@click.option("--output", "-o", default=None, metavar="OUTPUT",
              help="Write lot-level detail to this CSV file.")
def report(cm_csv, bs_csv, supplement_path, year, output):
    """Compute capital gains from one or more exchange transaction CSVs.

    Processes the full transaction history for correct FIFO cost basis and
    reports gains for the requested YEAR.
    """
    from gainscalc.tools import supplement as supp_mod
    from gainscalc.tools.gains import main as gains_main
    from gainscalc.tools.multi import detect_transfers, merge_sources, route_default_wallet
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
    transfers = detect_transfers(df)
    df = route_default_wallet(df, transfers)

    if supplement_path:
        loaded = supp_mod.load_supplement(supplement_path)
        df = supp_mod.apply_supplement(df, loaded)

    pairs = _pairs_from_df(df)
    gains_main(df, pairs)

    _print_report(pairs, year)

    if output:
        _write_book(pairs, year, output)


# ---------------------------------------------------------------------------
# gains supplement
# ---------------------------------------------------------------------------

@gains.command("supplement")
@click.option("--coinmotion", "cm_csv", default=None, metavar="CSV",
              help="Coinmotion transaction statement CSV.")
@click.option("--bitstamp", "bs_csv", default=None, metavar="CSV",
              help="Bitstamp transaction export CSV.")
@click.option("--max-hours", default=72, show_default=True,
              help="Maximum hours between spend and buy to call it a transfer.")
@click.option("--output", "-o", default="-", help="Output file (default: stdout).")
def supplement(cm_csv, bs_csv, max_hours, output):
    """Generate a deposit cost-basis supplement template.

    Reads all provided exchange CSVs, merges them, auto-detects likely
    cross-exchange transfers, and emits a supplement template CSV.  Fill in
    actual_buy_date and/or actual_unitvalue for any deposit whose cost basis
    differs from the exchange rate at deposit time.  Detected transfers are
    annotated in the transfer_note column.
    """
    from gainscalc.tools.cmotion import read_coinmotion
    from gainscalc.tools.bstamp import read_bitstamp
    from gainscalc.tools.deposit_form import generate_combined_form
    from gainscalc.tools.multi import detect_transfers, merge_sources, route_default_wallet

    dfs = []
    if cm_csv:
        dfs.append(read_coinmotion(cm_csv))
    if bs_csv:
        click.echo("Reading Bitstamp CSV (may fetch exchange rates)…", err=True)
        dfs.append(read_bitstamp(bs_csv))

    if not dfs:
        raise click.UsageError("Provide at least one of --coinmotion or --bitstamp.")

    merged = merge_sources(*dfs)
    transfers = detect_transfers(merged, max_hours=max_hours)
    routed = route_default_wallet(merged, transfers)

    if transfers:
        click.echo(f"Detected {len(transfers)} likely cross-exchange transfer(s).", err=True)
    else:
        click.echo("No cross-exchange transfers detected.", err=True)

    form = generate_combined_form(routed, transfers=transfers)

    if output == "-":
        form.to_csv(sys.stdout, index=False)
    else:
        form.to_csv(output, index=False)
        click.echo(f"Wrote {len(form)} rows to {output}", err=True)


# ---------------------------------------------------------------------------
# gains eth-export
# ---------------------------------------------------------------------------

@gains.command("eth-export")
@click.argument("infile")
@click.argument("outfile")
def eth_export(infile, outfile):
    """Convert an Etherscan ETH transaction CSV to EUR values.

    Annotates each transaction with the ETH/EUR rate on that day and computes
    the EUR value.  Writes the result to OUTFILE.
    """
    import os
    from gainscalc.rates import load

    os.makedirs(os.path.dirname(outfile), exist_ok=True)
    trades = pd.read_csv(infile, index_col="Txhash",
                         usecols=("Txhash", "DateTime", "Value"),
                         parse_dates=["DateTime"])
    year = pd.Timestamp(trades.DateTime.values[0]).year
    data = load(year)
    data = data.apply(lambda row: (row.Open + row.Close) / 2, axis=1)
    trades["ETHEUR"] = trades.apply(
        lambda t: data.iloc[data.index.get_loc(t.DateTime, method="pad")], axis=1
    )
    trades["EURValue"] = trades.apply(lambda row: row.Value * row.ETHEUR, axis=1)
    trades.to_csv(outfile)
