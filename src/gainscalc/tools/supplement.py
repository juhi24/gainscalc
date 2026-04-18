"""Shared supplement loading and application utilities.

A supplement CSV overrides the acquisition date and/or cost basis for
specific ``buy`` rows in a normalized transaction DataFrame.  This is used
to correct the cost basis for crypto deposits whose original acquisition
happened on a different exchange or outside any tracked exchange.

Supplement CSV format
---------------------
Required columns:
  source        — exchange name ('coinmotion', 'bitstamp', …).
                  Leave blank to match any source.
  deposit_date  — UTC-naive datetime matching the buy row's date exactly.
  asset         — crypto symbol (BTC, ETH, …).

Optional override columns:
  actual_buy_date   — replace the buy row's date with this value.
  actual_unitvalue  — replace the buy row's unitvalue with this value.

Other columns (e.g. amount, source_rate, transfer_note) are informational
and ignored during application.
"""

import pandas as pd


def load_supplement(path) -> dict:
    """Load a supplement CSV into an override lookup dict.

    Returns a dict keyed by ``(deposit_date, asset, source)`` where
    *source* is the lowercase exchange name, or ``''`` for source-agnostic
    entries.  Values are ``(actual_buy_date, actual_unitvalue)`` — either
    may be ``None`` when the column is blank.
    """
    df = pd.read_csv(path)
    df["deposit_date"] = pd.to_datetime(df["deposit_date"], utc=False)

    lookup = {}
    for _, row in df.iterrows():
        source = str(row.get("source", "")).strip().lower()
        key = (row["deposit_date"], str(row["asset"]).strip(), source)

        buy_date = None
        raw_date = row.get("actual_buy_date", "")
        if pd.notna(raw_date) and str(raw_date).strip():
            buy_date = pd.to_datetime(raw_date)

        unitvalue = None
        raw_uv = row.get("actual_unitvalue", "")
        if pd.notna(raw_uv) and str(raw_uv).strip():
            unitvalue = float(raw_uv)

        lookup[key] = (buy_date, unitvalue)
    return lookup


def apply_supplement(df: pd.DataFrame, supp: dict) -> pd.DataFrame:
    """Apply supplement overrides to a normalized transaction DataFrame.

    Only ``buy`` rows are candidates for override.  Each buy row is looked
    up by ``(date, asset, source)``.  A source-agnostic entry (key has
    ``source=''``) matches any source value.

    Returns a new DataFrame (copy) sorted by date after any date overrides.
    """
    if not supp:
        return df

    df = df.copy()

    for idx, row in df[df["type"] == "buy"].iterrows():
        source = str(row.get("source", "")).strip().lower()
        date = row["date"]
        asset = str(row["asset"])

        override = supp.get((date, asset, source)) or supp.get((date, asset, ""))
        if override is None:
            continue

        actual_date, actual_uv = override
        if actual_date is not None:
            df.at[idx, "date"] = actual_date
        if actual_uv is not None:
            df.at[idx, "unitvalue"] = actual_uv

    return df.sort_values("date", kind="stable").reset_index(drop=True)
