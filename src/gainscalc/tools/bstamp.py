"""Bitstamp RFC 4180 transaction export parser.

Column layout (new format):
  ID, Account, Type, Subtype, Datetime,
  Amount, Amount currency,
  Value, Value currency,
  Rate, Rate currency,
  Fee, Fee currency,
  Order ID

Note: Bitstamp exports include a trailing comma on data rows, creating an
      extra empty field.  Read with index_col=False to handle this cleanly.

Transaction type mapping
------------------------
Market / Buy      : buy  (cost basis = (Value+Fee)/Amount, in EUR)
Market / Sell     : sell (proceeds = (Value-Fee)/Amount, in EUR)
Deposit (fiat)    : irrelevant
Deposit (crypto)  : buy  (unitvalue from rates.py)
Withdrawal (fiat) : irrelevant
Withdrawal (crypto): spend
Sub Account Transfer: buy or spend depending on direction in Value field
Staking reward    : irrelevant (deferred)
Staked assets     : irrelevant (deferred)
Started generating rewards: irrelevant (deferred)
Converted assets  : irrelevant (crypto→crypto, not yet modelled)

All monetary values are converted from USD to EUR using daily EUR/USD rates
from rates.py.  Sub Account Transfers and crypto Deposits/Withdrawals have no
transaction price; the asset's EUR market rate on that day is used instead.
"""

import pandas as pd

from gainscalc.rates import rate as fetch_rate

_FIAT = {"USD", "EUR"}

_SKIP_TYPES = {
    "Staking reward",
    "Staked assets",
    "Started generating rewards",
    "Converted assets",
}

_SKIP_ASSETS = {"ETH2", "ETH2R", "STRK"}


def _is_fiat(currency):
    return currency in _FIAT


def _safe_float(x):
    """Convert *x* to float, returning 0.0 for NaN or non-numeric values."""
    try:
        if pd.isna(x):
            return 0.0
        return float(x)
    except (ValueError, TypeError):
        return 0.0


def _parse_row_bs(row):
    """Return (type, asset, amount_crypto, total_value_usd, _reserved) or ('irrelevant', ...).

    ``total_value_usd`` is the total USD cost/proceeds (fee already
    added/deducted per Finnish tax rules) for market trades.  For deposits,
    withdrawals, and sub-account transfers it is ``None`` — the caller looks
    up the market rate via rates.py.
    """
    tx_type = str(row["Type"]).strip()
    subtype = str(row["Subtype"]).strip() if pd.notna(row["Subtype"]) else ""
    asset = str(row["Amount currency"]).strip()

    if tx_type in _SKIP_TYPES or asset in _SKIP_ASSETS:
        return "irrelevant", None, None, None, None

    amount = _safe_float(row["Amount"])

    if tx_type == "Market":
        if _is_fiat(asset):
            return "irrelevant", None, None, None, None
        value = _safe_float(row["Value"])
        fee = _safe_float(row["Fee"])
        if subtype == "Buy":
            # Finnish tax: fee adds to acquisition cost.
            return "buy", asset, amount, value + fee, None
        if subtype == "Sell":
            # Finnish tax: fee deducted from proceeds.
            return "sell", asset, amount, value - fee, None
        return "irrelevant", None, None, None, None

    if tx_type == "Deposit":
        if _is_fiat(asset):
            return "irrelevant", None, None, None, None
        # Crypto deposit from external wallet — price looked up via rates.py.
        return "buy", asset, amount, None, None

    if tx_type == "Withdrawal":
        if _is_fiat(asset):
            return "irrelevant", None, None, None, None
        return "spend", asset, amount, None, None

    if tx_type == "Sub Account Transfer":
        # Direction encoded in Value field: "Main Account -> Faija" = spend,
        # "Faija -> Main Account" = buy on the main account.
        value_str = str(row["Value"]).strip() if pd.notna(row["Value"]) else ""
        if "Main Account ->" in value_str:
            return "spend", asset, amount, None, None
        if "-> Main Account" in value_str:
            return "buy", asset, amount, None, None
        return "irrelevant", None, None, None, None

    return "irrelevant", None, None, None, None


def _eur_per_usd(date):
    """Return EUR per 1 USD using the daily EUR/USD rate from Yahoo Finance."""
    usd_per_eur = fetch_rate(date, "EUR=X")  # how many USD buys 1 EUR
    return 1.0 / usd_per_eur


def _asset_eur_rate(asset, date):
    """Return daily EUR price per unit of *asset* from Yahoo Finance."""
    return fetch_rate(date, f"{asset}-EUR")


def read_bitstamp(csv, year=0, until=None):
    """Read a Bitstamp RFC 4180 transaction export CSV.

    Returns a DataFrame with columns:
      date, asset, currency, type, amount, unitvalue, source
    sorted chronologically (oldest first).  All monetary values are in EUR.

    Parameters
    ----------
    csv  : path or file-like
    year : int -- if non-zero, keep only rows from that calendar year
    until: datetime -- if given, keep only rows on or before this date
    """
    # index_col=False handles the trailing comma Bitstamp adds to data rows.
    df = pd.read_csv(csv, index_col=False)
    df["Datetime"] = pd.to_datetime(df["Datetime"], utc=True).dt.tz_localize(None)

    rows = []
    for _, row in df.iterrows():
        tx_type, asset, amount, total_usd, _ = _parse_row_bs(row)
        if tx_type == "irrelevant":
            continue

        tx_date = row["Datetime"]

        if total_usd is not None:
            # Market trade: convert USD total to EUR unitvalue.
            eur_uv = (total_usd * _eur_per_usd(tx_date)) / amount if amount > 0 else 0.0
        else:
            # Deposit / withdrawal / sub-account transfer: look up market rate.
            try:
                eur_uv = _asset_eur_rate(asset, tx_date)
            except Exception:
                eur_uv = 0.0

        rows.append(
            {
                "date": tx_date,
                "asset": asset,
                "currency": "EUR",
                "type": tx_type,
                "amount": amount,
                "unitvalue": eur_uv,
                "source": "bitstamp",
            }
        )

    cols = ["date", "asset", "currency", "type", "amount", "unitvalue", "source"]
    result = pd.DataFrame(rows, columns=cols).sort_values("date", kind="stable")

    if year and not result.empty:
        result = result[result["date"].dt.year == year]
    if until is not None and not result.empty:
        result = result[result["date"] <= until]

    return result.reset_index(drop=True)
