"""Generate deposit cost-basis supplement templates.

Two commands
------------
deposit-form CSV
    Single-source Coinmotion form (backwards-compatible).

transfer-form --coinmotion CM_CSV [--bitstamp BS_CSV] [...]
    Multi-source unified form.  Reads all provided exchange CSVs, merges
    them, and emits a supplement template covering all external-deposit
    ``buy`` rows.  Auto-detected cross-exchange transfers are annotated in
    the ``transfer_note`` column so the user can verify the cost basis.

Supplement CSV format
---------------------
  source, deposit_date, asset, amount, source_rate,
  actual_buy_date, actual_unitvalue[, transfer_note]

Rules for filling in
--------------------
- Leave actual_buy_date blank  → deposit date is used
- Leave actual_unitvalue blank → source_rate (market/trade rate) is used
- Fill both in                 → FIFO uses the real acquisition date and cost
"""

import sys

import click
import pandas as pd

from gainscalc.tools.multi import detect_transfers, merge_sources


# CM-specific inflow types (for single-source deposit-form command).
_CM_INFLOW_TYPES = {"deposit", "account_transfer_in"}


# ---------------------------------------------------------------------------
# Single-source Coinmotion form (backwards-compatible)
# ---------------------------------------------------------------------------

def generate_deposit_form(csv_path) -> pd.DataFrame:
    """Return a supplement template DataFrame from a Coinmotion CSV.

    Covers every crypto ``deposit`` and ``account_transfer_in`` row.
    """
    raw = pd.read_csv(csv_path)
    raw["time"] = pd.to_datetime(raw["time"], utc=True).dt.tz_localize(None)

    rows = []
    for _, row in raw.iterrows():
        if row["type"] not in _CM_INFLOW_TYPES:
            continue
        fc = row["fromCurrency"]
        if fc == "EUR":
            continue
        crypto = float(row["cryptoAmount"]) if pd.notna(row["cryptoAmount"]) else 0.0
        rate = float(row["rate"]) if pd.notna(row["rate"]) else 0.0
        rows.append(
            {
                "source": "coinmotion",
                "deposit_date": row["time"],
                "asset": fc,
                "amount": crypto,
                "source_rate": rate,
                "actual_buy_date": "",
                "actual_unitvalue": "",
            }
        )

    cols = ["source", "deposit_date", "asset", "amount", "source_rate",
            "actual_buy_date", "actual_unitvalue"]
    return pd.DataFrame(rows, columns=cols)


# ---------------------------------------------------------------------------
# Multi-source unified form
# ---------------------------------------------------------------------------

def generate_combined_form(df: pd.DataFrame, transfers=None) -> pd.DataFrame:
    """Return a supplement template from a merged normalized DataFrame.

    Parameters
    ----------
    df        : merged normalized DataFrame (with ``source`` column)
    transfers : list of transfer dicts from ``detect_transfers()``; when
                provided, matching buy rows receive an annotation in the
                ``transfer_note`` column.

    Only ``buy`` rows from external deposits are included — market-trade
    purchases (where the exchange already provides the correct cost basis)
    are excluded.  Specifically: rows from ``coinmotion`` whose original
    CM type was a market trade are already correctly priced, but since the
    normalized DataFrame doesn't carry CM row type, we include ALL buy rows
    and let the user skip lines that don't need overrides.
    """
    transfer_notes = {}
    transfer_prefill = {}
    if transfers:
        for t in transfers:
            transfer_notes[t["buy_idx"]] = (
                f"likely transfer from {t['source_spend']} "
                f"({t['spend_date'].date()} {t['spend_amount']:.8g} {t['asset']})"
            )
            # Prefill with the spend-side date and market rate so the user has a
            # reasonable starting point without manual look-up.
            spend_row = df.loc[t["spend_idx"]]
            transfer_prefill[t["buy_idx"]] = {
                "actual_buy_date": t["spend_date"],
                "actual_unitvalue": spend_row["unitvalue"],
            }

    mask = df["type"] == "buy"
    if "subtype" in df.columns:
        mask = mask & (df["subtype"] == "deposit")
    rows = []
    for idx, row in df[mask].iterrows():
        prefill = transfer_prefill.get(idx, {})
        rows.append(
            {
                "source": row.get("source", ""),
                "deposit_date": row["date"],
                "asset": row["asset"],
                "amount": row["amount"],
                "source_rate": row["unitvalue"],
                "actual_buy_date": prefill.get("actual_buy_date", ""),
                "actual_unitvalue": prefill.get("actual_unitvalue", ""),
                "transfer_note": transfer_notes.get(idx, ""),
            }
        )

    cols = ["source", "deposit_date", "asset", "amount", "source_rate",
            "actual_buy_date", "actual_unitvalue", "transfer_note"]
    return pd.DataFrame(rows, columns=cols)


# ---------------------------------------------------------------------------
# CLI commands
# ---------------------------------------------------------------------------

@click.command("deposit-form")
@click.argument("csv_path", metavar="CSV")
@click.option("--output", "-o", default="-", help="Output file (default: stdout).")
def main(csv_path, output):
    """Generate a deposit cost-basis supplement template from a Coinmotion CSV."""
    df = generate_deposit_form(csv_path)
    if output == "-":
        df.to_csv(sys.stdout, index=False)
    else:
        df.to_csv(output, index=False)
        click.echo(f"Wrote {len(df)} rows to {output}", err=True)


@click.command("transfer-form")
@click.option("--coinmotion", "cm_csv", default=None, metavar="CSV",
              help="Coinmotion transaction statement CSV.")
@click.option("--bitstamp", "bs_csv", default=None, metavar="CSV",
              help="Bitstamp transaction export CSV.")
@click.option("--max-hours", default=72, show_default=True,
              help="Maximum hours between spend and buy to call it a transfer.")
@click.option("--output", "-o", default="-", help="Output file (default: stdout).")
def transfer_main(cm_csv, bs_csv, max_hours, output):
    """Generate a unified supplement template with transfer detection.

    Reads all provided exchange CSVs, merges them, auto-detects likely
    cross-exchange transfers, and emits a supplement template CSV.
    Transfers are annotated in the transfer_note column.
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

    merged = merge_sources(*dfs)
    transfers = detect_transfers(merged, max_hours=max_hours)

    if transfers:
        click.echo(f"Detected {len(transfers)} likely cross-exchange transfer(s).", err=True)
    else:
        click.echo("No cross-exchange transfers detected.", err=True)

    form = generate_combined_form(merged, transfers=transfers)

    if output == "-":
        form.to_csv(sys.stdout, index=False)
    else:
        form.to_csv(output, index=False)
        click.echo(f"Wrote {len(form)} rows to {output}", err=True)
