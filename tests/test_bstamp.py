#!/usr/bin/env python3
"""Tests for the Bitstamp RFC 4180 CSV parser.

Covers _parse_row_bs() with synthetic rows.  Integration tests against the
real CSV are skipped here because read_bitstamp() fetches live exchange rates
from Yahoo Finance; those belong in a manual smoke-test script.
"""

import unittest
from unittest.mock import patch

import pandas as pd

from gainscalc.tools.bstamp import _parse_row_bs, read_bitstamp


def _row(tx_type, subtype="", amount=None, amount_cur="BTC",
         value=None, value_cur="USD", rate=None, rate_cur="USD",
         fee=None, fee_cur="USD", account="Main Account", value_field=None):
    """Build a minimal dict that _parse_row_bs() can consume."""
    return {
        "Type": tx_type,
        "Subtype": subtype,
        "Amount": amount,
        "Amount currency": amount_cur,
        "Value": value_field if value_field is not None else value,
        "Value currency": value_cur,
        "Rate": rate,
        "Rate currency": rate_cur,
        "Fee": fee,
        "Fee currency": fee_cur,
        "Account": account,
    }


class TestParseRowBs(unittest.TestCase):

    # -- Market Buy ----------------------------------------------------------

    def test_market_buy_type_and_asset(self):
        r = _row("Market", "Buy", amount=1.0, amount_cur="BTC",
                 value=42.85, fee=0.22)
        tx, asset, amount, value_usd, _ = _parse_row_bs(r)
        self.assertEqual(tx, "buy")
        self.assertEqual(asset, "BTC")
        self.assertAlmostEqual(amount, 1.0)

    def test_market_buy_value_includes_fee(self):
        """Cost basis = Value + Fee (Finnish tax: fee added to acquisition cost)."""
        r = _row("Market", "Buy", amount=4.0, amount_cur="BTC",
                 value=171.40, fee=0.86)
        _, _, amount, value_usd, _ = _parse_row_bs(r)
        self.assertAlmostEqual(value_usd, 171.40 + 0.86)
        self.assertAlmostEqual(amount, 4.0)

    def test_market_buy_eth(self):
        r = _row("Market", "Buy", amount=10.0, amount_cur="ETH",
                 value=200.0, fee=1.0)
        tx, asset, _, _, _ = _parse_row_bs(r)
        self.assertEqual(tx, "buy")
        self.assertEqual(asset, "ETH")

    def test_market_buy_fiat_amount_is_irrelevant(self):
        """A Market Buy where Amount currency is USD (fiat) should be ignored."""
        r = _row("Market", "Buy", amount=100.0, amount_cur="USD")
        tx, *_ = _parse_row_bs(r)
        self.assertEqual(tx, "irrelevant")

    # -- Market Sell ---------------------------------------------------------

    def test_market_sell_type_and_asset(self):
        r = _row("Market", "Sell", amount=1.1, amount_cur="BTC",
                 value=50.51, fee=0.26)
        tx, asset, amount, value_usd, _ = _parse_row_bs(r)
        self.assertEqual(tx, "sell")
        self.assertEqual(asset, "BTC")
        self.assertAlmostEqual(amount, 1.1)

    def test_market_sell_value_minus_fee(self):
        """Net proceeds = Value - Fee (Finnish tax: fee deducted from proceeds)."""
        r = _row("Market", "Sell", amount=1.1, amount_cur="BTC",
                 value=50.51, fee=0.26)
        _, _, _, value_usd, _ = _parse_row_bs(r)
        self.assertAlmostEqual(value_usd, 50.51 - 0.26)

    # -- Deposit / Withdrawal ------------------------------------------------

    def test_deposit_fiat_is_irrelevant(self):
        r = _row("Deposit", amount=291.46, amount_cur="USD")
        tx, *_ = _parse_row_bs(r)
        self.assertEqual(tx, "irrelevant")

    def test_deposit_crypto_is_buy(self):
        r = _row("Deposit", amount=2.0, amount_cur="BTC")
        tx, asset, amount, value_usd, _ = _parse_row_bs(r)
        self.assertEqual(tx, "buy")
        self.assertEqual(asset, "BTC")
        self.assertAlmostEqual(amount, 2.0)
        self.assertIsNone(value_usd)  # rate looked up separately

    def test_withdrawal_fiat_is_irrelevant(self):
        r = _row("Withdrawal", amount=100.0, amount_cur="USD")
        tx, *_ = _parse_row_bs(r)
        self.assertEqual(tx, "irrelevant")

    def test_withdrawal_crypto_is_spend(self):
        r = _row("Withdrawal", amount=0.1, amount_cur="BTC", fee=0.0)
        tx, asset, amount, _, _ = _parse_row_bs(r)
        self.assertEqual(tx, "spend")
        self.assertEqual(asset, "BTC")
        self.assertAlmostEqual(amount, 0.1)

    # -- Sub Account Transfer ------------------------------------------------

    def test_sub_account_transfer_main_to_sub_is_spend(self):
        r = _row("Sub Account Transfer", amount=1.22646670, amount_cur="BTC",
                 value_field="Main Account -> Faija")
        tx, asset, amount, _, _ = _parse_row_bs(r)
        self.assertEqual(tx, "spend")
        self.assertEqual(asset, "BTC")

    def test_sub_account_transfer_sub_to_main_is_buy(self):
        r = _row("Sub Account Transfer", amount=0.771, amount_cur="BTC",
                 value_field="Faija -> Main Account")
        tx, asset, amount, _, _ = _parse_row_bs(r)
        self.assertEqual(tx, "buy")
        self.assertEqual(asset, "BTC")
        self.assertAlmostEqual(amount, 0.771)

    def test_sub_account_transfer_unknown_direction_is_irrelevant(self):
        r = _row("Sub Account Transfer", amount=1.0, amount_cur="BTC",
                 value_field="SomeOther -> AnotherAccount")
        tx, *_ = _parse_row_bs(r)
        self.assertEqual(tx, "irrelevant")

    # -- Staking / skipped types ---------------------------------------------

    def test_staking_reward_is_irrelevant(self):
        r = _row("Staking reward", amount=0.001, amount_cur="ETH2R")
        tx, *_ = _parse_row_bs(r)
        self.assertEqual(tx, "irrelevant")

    def test_staked_assets_is_irrelevant(self):
        r = _row("Staked assets", amount=4.0, amount_cur="ETH")
        tx, *_ = _parse_row_bs(r)
        self.assertEqual(tx, "irrelevant")

    def test_started_generating_rewards_is_irrelevant(self):
        r = _row("Started generating rewards", amount=4.0, amount_cur="ETH2")
        tx, *_ = _parse_row_bs(r)
        self.assertEqual(tx, "irrelevant")

    def test_converted_assets_is_irrelevant(self):
        r = _row("Converted assets", amount=1.0, amount_cur="BTC")
        tx, *_ = _parse_row_bs(r)
        self.assertEqual(tx, "irrelevant")

    def test_eth2_asset_is_irrelevant(self):
        r = _row("Market", "Buy", amount=1.0, amount_cur="ETH2")
        tx, *_ = _parse_row_bs(r)
        self.assertEqual(tx, "irrelevant")

    def test_strk_asset_is_irrelevant(self):
        r = _row("Market", "Buy", amount=100.0, amount_cur="STRK")
        tx, *_ = _parse_row_bs(r)
        self.assertEqual(tx, "irrelevant")


# ---------------------------------------------------------------------------
# Integration test with mocked rate fetching
# ---------------------------------------------------------------------------

_MINIMAL_BS_CSV = (
    "ID,Account,Type,Subtype,Datetime,Amount,Amount currency,"
    "Value,Value currency,Rate,Rate currency,Fee,Fee currency,Order ID\n"
    "1,Main Account,Market,Buy,2020-01-15T12:00:00Z,1.0,BTC,8000.0,USD,8000.0,USD,40.0,USD,\n"
    "2,Main Account,Market,Sell,2020-06-01T12:00:00Z,0.5,BTC,5000.0,USD,10000.0,USD,25.0,USD,\n"
    "3,Main Account,Deposit,,2019-07-01T00:00:00Z,0.3,BTC,,,,,,,\n"
    "4,Main Account,Withdrawal,,2020-09-01T00:00:00Z,0.1,BTC,,,,,0.0,BTC,\n"
    "5,Main Account,Deposit,,2020-01-01T00:00:00Z,500.0,USD,,,,,,,\n"
)

_EUR_USD_RATE = 1.12  # 1 EUR = 1.12 USD  ⟹  EUR/USD = 1/1.12


class TestReadBitstampMocked(unittest.TestCase):
    """read_bitstamp() with Yahoo Finance rate lookups mocked out."""

    def _mock_rate(self, date, pair):
        if pair == "EUR=X":
            return _EUR_USD_RATE
        # For deposits without a price, return a plausible BTC-EUR rate.
        return 7500.0

    def _read(self, extra_rows=""):
        import io
        csv_text = _MINIMAL_BS_CSV + extra_rows
        with patch("gainscalc.tools.bstamp.fetch_rate", side_effect=self._mock_rate):
            return read_bitstamp(io.StringIO(csv_text))

    def test_returns_dataframe(self):
        df = self._read()
        self.assertIsInstance(df, pd.DataFrame)

    def test_required_columns(self):
        df = self._read()
        for col in ("date", "asset", "currency", "type", "amount", "unitvalue", "source"):
            self.assertIn(col, df.columns)

    def test_source_is_bitstamp(self):
        df = self._read()
        self.assertTrue((df["source"] == "bitstamp").all())

    def test_currency_is_eur(self):
        df = self._read()
        self.assertTrue((df["currency"] == "EUR").all())

    def test_fiat_deposit_excluded(self):
        """USD Deposit rows must not appear in output."""
        df = self._read()
        self.assertFalse(any((df["asset"] == "USD")))

    def test_market_buy_unitvalue_eur(self):
        """Buy 1 BTC for $8040 total → EUR unitvalue = 8040 / 1.12."""
        df = self._read()
        buy = df[(df["type"] == "buy") & (df["amount"].apply(lambda x: abs(x - 1.0) < 1e-6))]
        buy = buy[buy["asset"] == "BTC"]
        # Only the market buy row has amount=1.0
        market_buy = buy.iloc[0]
        expected_uv = (8000.0 + 40.0) / _EUR_USD_RATE  # (Value+Fee)/EUR_USD
        self.assertAlmostEqual(market_buy["unitvalue"], expected_uv, places=2)

    def test_market_sell_unitvalue_eur(self):
        """Sell 0.5 BTC for $4975 net → EUR unitvalue = 4975 / 0.5 / 1.12."""
        df = self._read()
        sell = df[(df["type"] == "sell") & (df["asset"] == "BTC")]
        self.assertEqual(len(sell), 1)
        expected_uv = (5000.0 - 25.0) / 0.5 / _EUR_USD_RATE
        self.assertAlmostEqual(sell.iloc[0]["unitvalue"], expected_uv, places=2)

    def test_crypto_deposit_is_buy(self):
        """BTC deposit from external wallet → buy type."""
        df = self._read()
        dep = df[(df["type"] == "buy") & (df["asset"] == "BTC") &
                 (df["amount"].apply(lambda x: abs(x - 0.3) < 1e-6))]
        self.assertGreater(len(dep), 0)

    def test_withdrawal_is_spend(self):
        df = self._read()
        spends = df[(df["type"] == "spend") & (df["asset"] == "BTC")]
        self.assertGreater(len(spends), 0)

    def test_chronological_order(self):
        df = self._read()
        dates = df["date"].tolist()
        self.assertEqual(dates, sorted(dates))

    def test_year_filter(self):
        with patch("gainscalc.tools.bstamp.fetch_rate", side_effect=self._mock_rate):
            import io
            df = read_bitstamp(io.StringIO(_MINIMAL_BS_CSV), year=2020)
        self.assertTrue((df["date"].dt.year == 2020).all())
        self.assertGreater(len(df), 0)


if __name__ == "__main__":
    unittest.main()
