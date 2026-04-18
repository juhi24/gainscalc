"""Coinmotion transaction statement CSV parser.

Column layout (new format):
  fromCurrency, toCurrency, type, eurAmount, cryptoAmount,
  rate, fee, feeCurrency, time

Transaction type mapping
------------------------
market_trade / market_trade_limit:
  EUR->crypto  : buy  (eurAmount = total cost incl. fee)
  crypto->EUR  : sell (eurAmount = net proceeds excl. fee)
deposit (crypto)         : buy  (cost basis = rate)
withdrawal (crypto)      : spend (net = cryptoAmount - fee)
referral_reward (crypto) : buy  (cost basis = rate)
account_transfer_in      : buy  (cost basis = rate)
account_transfer_out     : spend (net = cryptoAmount - fee)
vault_deposit            : stash (net = cryptoAmount - fee)
vault_withdrawal         : unstash
vault_fee                : sell (taxable disposal at rate)
EUR-only rows            : ignored
"""

import pandas as pd

from gainscalc.tools import supplement as supp_mod


def _is_crypto(currency):
    return currency != "EUR"


def _parse_row(row):
    """Return (type, asset, amount, unitvalue) or ('irrelevant', ...)."""
    t = row["type"]
    fc = row["fromCurrency"]
    tc = row["toCurrency"]
    crypto_raw = row["cryptoAmount"]
    eur_raw = row["eurAmount"]
    fee_raw = row["fee"]
    rate_raw = row["rate"]
    crypto = float(crypto_raw) if pd.notna(crypto_raw) else 0.0
    eur = float(eur_raw) if pd.notna(eur_raw) else 0.0
    fee = float(fee_raw) if pd.notna(fee_raw) else 0.0
    rate = float(rate_raw) if pd.notna(rate_raw) else 0.0

    if t in ("market_trade", "market_trade_limit"):
        if fc == "EUR" and _is_crypto(tc):
            # EUR->crypto buy.  eurAmount already includes the exchange fee,
            # so unitvalue = total cost per unit (Finnish tax: fee added to basis).
            uv = eur / crypto if crypto > 0 else rate
            return "buy", tc, crypto, uv
        if _is_crypto(fc) and tc == "EUR":
            # crypto->EUR sell.  eurAmount is net proceeds after the fee,
            # so unitvalue = net proceeds per unit (fee already deducted).
            uv = eur / crypto if crypto > 0 else rate
            return "sell", fc, crypto, uv
        # crypto->crypto swap: not yet modelled
        return "irrelevant", None, None, None

    if t == "deposit":
        if _is_crypto(fc):
            return "buy", fc, crypto, rate
        return "irrelevant", None, None, None

    if t == "withdrawal":
        if _is_crypto(fc):
            return "spend", fc, crypto - fee, rate
        return "irrelevant", None, None, None

    if t == "referral_reward":
        if _is_crypto(tc):
            return "buy", tc, crypto, rate
        return "irrelevant", None, None, None

    if t == "account_transfer_in":
        return "buy", fc, crypto, rate

    if t == "account_transfer_out":
        return "spend", fc, crypto - fee, rate

    if t == "vault_deposit":
        return "stash", fc, crypto - fee, rate

    if t == "vault_withdrawal":
        return "unstash", fc, crypto, rate

    if t == "vault_fee":
        # Monthly vault fee: disposal of crypto at market rate -- taxable event.
        return "sell", fc, crypto, rate

    return "irrelevant", None, None, None


def read_coinmotion(csv, year=0, until=None, supplement=None):
    """Read a Coinmotion transaction statement CSV.

    Returns a DataFrame with columns:
      date, asset, currency, type, amount, unitvalue, source
    sorted chronologically (oldest first).

    Parameters
    ----------
    csv        : path or file-like
    year       : int -- if non-zero, keep only rows from that calendar year
    until      : datetime -- if given, keep only rows on or before this date
    supplement : path or file-like -- optional filled-in deposit cost-basis CSV
                 (produced by ``deposit-form`` or ``transfer-form``).
                 Applied via the shared supplement module.
    """
    df = pd.read_csv(csv)
    # Parse ISO-8601 timestamps with tz offset, convert to UTC-naive datetime
    df["time"] = pd.to_datetime(df["time"], utc=True).dt.tz_localize(None)

    rows = []
    for _, row in df.iterrows():
        tx_type, asset, amount, unitvalue = _parse_row(row)
        if tx_type == "irrelevant":
            continue
        rows.append(
            {
                "date": row["time"],
                "asset": asset,
                "currency": "EUR",
                "type": tx_type,
                "amount": amount,
                "unitvalue": unitvalue,
                "source": "coinmotion",
            }
        )

    cols = ["date", "asset", "currency", "type", "amount", "unitvalue", "source"]
    result = pd.DataFrame(rows, columns=cols).sort_values("date", kind="stable")

    if supplement is not None:
        loaded = supp_mod.load_supplement(supplement)
        result = supp_mod.apply_supplement(result, loaded)

    if year and not result.empty:
        result = result[result["date"].dt.year == year]
    if until is not None and not result.empty:
        result = result[result["date"] <= until]

    return result.reset_index(drop=True)
