"""Generate a deposit cost-basis supplement template from a Coinmotion CSV.

Usage
-----
    deposit-form reports/TransactionStatement-20260415-214021.csv > reports/deposits.csv

The output CSV lists every crypto deposit and account_transfer_in row.
Fill in ``actual_buy_date`` and ``actual_unitvalue`` for entries where the
crypto was originally acquired elsewhere (different exchange or P2P) at a
different price than the CM market rate shown in ``cm_rate``.

Rules
-----
- Leave ``actual_buy_date`` blank  → deposit date is used
- Leave ``actual_unitvalue`` blank → ``cm_rate`` (market rate at CM deposit) is used
- Fill both in                     → FIFO will use your real acquisition date and cost
"""

import sys

import click
import pandas as pd


# Transaction types that represent an inflow of crypto whose cost basis
# may have originated outside Coinmotion.
_INFLOW_TYPES = {"deposit", "account_transfer_in"}


def generate_deposit_form(csv_path) -> pd.DataFrame:
    """Return a DataFrame of inflow rows that may need a cost-basis override.

    Parameters
    ----------
    csv_path : str or path-like
        Path to the Coinmotion transaction statement CSV.

    Returns
    -------
    pd.DataFrame with columns:
        deposit_date, asset, amount, cm_rate, actual_buy_date, actual_unitvalue
    """
    raw = pd.read_csv(csv_path)
    raw["time"] = pd.to_datetime(raw["time"], utc=True).dt.tz_localize(None)

    rows = []
    for _, row in raw.iterrows():
        t = raw_type = row["type"]
        if t not in _INFLOW_TYPES:
            continue
        fc = row["fromCurrency"]
        if fc == "EUR":
            continue  # EUR-only rows are irrelevant
        crypto_raw = row["cryptoAmount"]
        rate_raw = row["rate"]
        crypto = float(crypto_raw) if pd.notna(crypto_raw) else 0.0
        rate = float(rate_raw) if pd.notna(rate_raw) else 0.0
        rows.append(
            {
                "deposit_date": row["time"],
                "asset": fc,
                "amount": crypto,
                "cm_rate": rate,
                "actual_buy_date": "",
                "actual_unitvalue": "",
            }
        )

    cols = ["deposit_date", "asset", "amount", "cm_rate", "actual_buy_date", "actual_unitvalue"]
    return pd.DataFrame(rows, columns=cols)


@click.command()
@click.argument("csv_path", metavar="CSV")
@click.option(
    "--output", "-o", default="-", show_default=True,
    help="Output file path. Defaults to stdout (-).",
)
def main(csv_path, output):
    """Generate a deposit cost-basis supplement template from a Coinmotion CSV.

    Writes a CSV with one row per crypto deposit / account_transfer_in.
    Fill in actual_buy_date and actual_unitvalue for entries where the
    original acquisition happened on a different exchange.
    """
    df = generate_deposit_form(csv_path)
    if output == "-":
        df.to_csv(sys.stdout, index=False)
    else:
        df.to_csv(output, index=False)
        click.echo(f"Wrote {len(df)} rows to {output}", err=True)
