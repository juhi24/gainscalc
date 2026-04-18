"""Utilities for combining normalized transaction DataFrames from multiple sources.

merge_sources   -- concatenate and sort
detect_transfers -- find spend→buy pairs that are likely cross-exchange transfers
"""

from datetime import timedelta

import pandas as pd


def merge_sources(*dfs) -> pd.DataFrame:
    """Concatenate normalized DataFrames from multiple sources and sort by date.

    Each input DataFrame must have the standard columns:
      date, asset, currency, type, amount, unitvalue, source
    """
    combined = pd.concat(dfs, ignore_index=True)
    return combined.sort_values("date", kind="stable").reset_index(drop=True)


def detect_transfers(
    df: pd.DataFrame,
    max_hours: float = 72,
    amount_rtol: float = 0.01,
) -> list:
    """Identify likely cross-exchange transfers in a merged DataFrame.

    A transfer is a ``spend`` on one source followed by a ``buy`` on a
    *different* source, for the same asset, within ``max_hours``, where
    the amounts match within a relative tolerance of ``amount_rtol``
    (accounts for network fees consuming a small fraction of the amount).

    Returns a list of dicts, each describing one detected transfer:
      spend_idx, buy_idx       -- row indices in *df*
      asset                    -- e.g. 'BTC'
      spend_amount, buy_amount -- amounts (may differ by network fee)
      spend_date, buy_date     -- datetimes
      source_spend, source_buy -- exchange names
    """
    if "source" not in df.columns:
        return []

    window = timedelta(hours=max_hours)
    spends = df[df["type"] == "spend"].copy()
    buys = df[df["type"] == "buy"].copy()

    results = []
    matched_buy_idxs = set()

    for s_idx, spend in spends.iterrows():
        asset = spend["asset"]
        s_date = spend["date"]
        s_src = spend["source"]
        s_amt = spend["amount"]

        candidates = buys[
            (buys["asset"] == asset)
            & (buys["source"] != s_src)
            & (buys["date"] >= s_date)
            & (buys["date"] <= s_date + window)
            & (~buys.index.isin(matched_buy_idxs))
        ]

        for b_idx, buy in candidates.iterrows():
            b_amt = buy["amount"]
            if s_amt > 0 and abs(b_amt - s_amt) / s_amt <= amount_rtol:
                results.append(
                    {
                        "spend_idx": s_idx,
                        "buy_idx": b_idx,
                        "asset": asset,
                        "spend_amount": s_amt,
                        "buy_amount": b_amt,
                        "spend_date": s_date,
                        "buy_date": buy["date"],
                        "source_spend": s_src,
                        "source_buy": buy["source"],
                    }
                )
                matched_buy_idxs.add(b_idx)
                break  # each spend matches at most one buy

    return results
