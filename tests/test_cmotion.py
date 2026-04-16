#!/usr/bin/env python3
"""Tests for the Coinmotion CSV parser and the gains pipeline.

Covers:
  * _parse_row()  -- unit tests with synthetic rows
  * read_coinmotion() -- integration tests against the real CSV
  * gains.main()  -- end-to-end pipeline test
"""

import math
import os
import unittest

import pandas as pd

from gainscalc.tools.cmotion import _parse_row, read_coinmotion
from gainscalc.tools.gains import Pair, main as gains_main

CSV_PATH = os.path.join(
    os.path.dirname(__file__), "..", "reports",
    "TransactionStatement-20260415-214021.csv",
)


def _row(type_, from_cur, to_cur, eur=None, crypto=None,
         rate=0.0, fee=0.0, fee_cur="EUR"):
    """Build a minimal dict that _parse_row() can consume."""
    return {
        "type": type_,
        "fromCurrency": from_cur,
        "toCurrency": to_cur,
        "eurAmount": eur,
        "cryptoAmount": crypto,
        "rate": rate,
        "fee": fee,
        "feeCurrency": fee_cur,
        "time": "2020-01-01T12:00:00+02:00",
    }


# ---------------------------------------------------------------------------
# Unit tests for _parse_row()
# ---------------------------------------------------------------------------

class TestParseRow(unittest.TestCase):

    # -- market_trade / market_trade_limit -----------------------------------

    def test_buy_eur_to_btc(self):
        """EUR->BTC: type=buy, asset=BTC, unitvalue=eurAmount/crypto."""
        row = _row("market_trade", "EUR", "BTC",
                   eur=100.80, crypto=0.01, rate=10000.0, fee=0.80)
        tx, asset, amount, uv = _parse_row(row)
        self.assertEqual(tx, "buy")
        self.assertEqual(asset, "BTC")
        self.assertAlmostEqual(amount, 0.01)
        # eurAmount (100.80) already includes the 0.80 EUR fee
        self.assertAlmostEqual(uv, 100.80 / 0.01)

    def test_sell_btc_to_eur(self):
        """BTC->EUR: type=sell, unitvalue=eurAmount/crypto (net proceeds)."""
        row = _row("market_trade", "BTC", "EUR",
                   eur=9920.0, crypto=1.0, rate=10000.0, fee=80.0)
        tx, asset, amount, uv = _parse_row(row)
        self.assertEqual(tx, "sell")
        self.assertEqual(asset, "BTC")
        self.assertAlmostEqual(amount, 1.0)
        # eurAmount (9920.0) is net proceeds after the 80.0 EUR fee
        self.assertAlmostEqual(uv, 9920.0)

    def test_buy_eur_to_eth_limit(self):
        row = _row("market_trade_limit", "EUR", "ETH",
                   eur=201.5, crypto=1.0, rate=200.0, fee=1.5)
        tx, asset, amount, uv = _parse_row(row)
        self.assertEqual(tx, "buy")
        self.assertEqual(asset, "ETH")
        self.assertAlmostEqual(uv, 201.5)

    def test_crypto_to_crypto_ignored(self):
        """crypto->crypto swaps are not yet modelled."""
        row = _row("market_trade", "BTC", "ETH",
                   crypto=0.1, rate=2000.0)
        tx, *_ = _parse_row(row)
        self.assertEqual(tx, "irrelevant")

    # -- deposit / withdrawal ------------------------------------------------

    def test_deposit_crypto_is_buy(self):
        row = _row("deposit", "BTC", "BTC", crypto=1.0, rate=5000.0)
        tx, asset, amount, uv = _parse_row(row)
        self.assertEqual(tx, "buy")
        self.assertEqual(asset, "BTC")
        self.assertAlmostEqual(amount, 1.0)
        self.assertAlmostEqual(uv, 5000.0)

    def test_deposit_eur_is_irrelevant(self):
        row = _row("deposit", "EUR", "EUR", eur=1000.0)
        tx, *_ = _parse_row(row)
        self.assertEqual(tx, "irrelevant")

    def test_withdrawal_crypto_net_amount(self):
        """Net sent = cryptoAmount - fee (network fee consumed separately)."""
        row = _row("withdrawal", "BTC", "BTC",
                   crypto=1.0005, rate=50000.0, fee=0.0005, fee_cur="BTC")
        tx, asset, amount, uv = _parse_row(row)
        self.assertEqual(tx, "spend")
        self.assertEqual(asset, "BTC")
        self.assertAlmostEqual(amount, 1.0)

    def test_withdrawal_eur_is_irrelevant(self):
        row = _row("withdrawal", "EUR", "EUR", eur=500.0, fee=0.90)
        tx, *_ = _parse_row(row)
        self.assertEqual(tx, "irrelevant")

    # -- referral_reward -----------------------------------------------------

    def test_referral_reward_crypto_is_buy(self):
        """Crypto rewards add to FIFO with market-rate cost basis."""
        row = _row("referral_reward", "BTC", "BTC", crypto=0.01, rate=8000.0)
        tx, asset, amount, uv = _parse_row(row)
        self.assertEqual(tx, "buy")
        self.assertEqual(asset, "BTC")
        self.assertAlmostEqual(amount, 0.01)
        self.assertAlmostEqual(uv, 8000.0)

    def test_referral_reward_eur_is_irrelevant(self):
        row = _row("referral_reward", "EUR", "EUR", eur=2.50)
        tx, *_ = _parse_row(row)
        self.assertEqual(tx, "irrelevant")

    # -- account transfers ---------------------------------------------------

    def test_account_transfer_in_is_buy(self):
        row = _row("account_transfer_in", "BTC", "BTC",
                   crypto=0.5, rate=30000.0)
        tx, asset, amount, uv = _parse_row(row)
        self.assertEqual(tx, "buy")
        self.assertEqual(asset, "BTC")
        self.assertAlmostEqual(amount, 0.5)

    def test_account_transfer_out_net_amount(self):
        row = _row("account_transfer_out", "BTC", "BTC",
                   crypto=2.0001, rate=40000.0, fee=0.0001, fee_cur="BTC")
        tx, asset, amount, uv = _parse_row(row)
        self.assertEqual(tx, "spend")
        self.assertAlmostEqual(amount, 2.0)

    # -- vault operations ----------------------------------------------------

    def test_vault_deposit_stash_net_amount(self):
        row = _row("vault_deposit", "BTC", "BTC",
                   crypto=10.0002, rate=5000.0, fee=0.0002, fee_cur="BTC")
        tx, asset, amount, uv = _parse_row(row)
        self.assertEqual(tx, "stash")
        self.assertAlmostEqual(amount, 10.0)

    def test_vault_withdrawal_is_unstash(self):
        row = _row("vault_withdrawal", "BTC", "BTC", crypto=10.0, rate=8000.0)
        tx, asset, amount, uv = _parse_row(row)
        self.assertEqual(tx, "unstash")
        self.assertAlmostEqual(amount, 10.0)

    def test_vault_fee_is_taxable_sell(self):
        """Vault fee = disposal of crypto at market rate (Finnish tax rules)."""
        row = _row("vault_fee", "BTC", "BTC", crypto=0.0001, rate=50000.0)
        tx, asset, amount, uv = _parse_row(row)
        self.assertEqual(tx, "sell")
        self.assertEqual(asset, "BTC")
        self.assertAlmostEqual(amount, 0.0001)
        self.assertAlmostEqual(uv, 50000.0)

    # -- unknown / EUR-only --------------------------------------------------

    def test_bank_payment_is_irrelevant(self):
        row = _row("bank_payment", "EUR", "EUR", eur=500.0)
        tx, *_ = _parse_row(row)
        self.assertEqual(tx, "irrelevant")

    def test_unknown_type_is_irrelevant(self):
        row = _row("some_future_type", "EUR", "EUR")
        tx, *_ = _parse_row(row)
        self.assertEqual(tx, "irrelevant")


# ---------------------------------------------------------------------------
# Integration tests -- read_coinmotion() against the real CSV
# ---------------------------------------------------------------------------

class TestReadCoinmotion(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.df = read_coinmotion(CSV_PATH)

    def test_returns_nonempty_dataframe(self):
        self.assertIsInstance(self.df, pd.DataFrame)
        self.assertGreater(len(self.df), 0)

    def test_has_required_columns(self):
        for col in ("date", "asset", "currency", "type", "amount", "unitvalue"):
            self.assertIn(col, self.df.columns)

    def test_currency_always_eur(self):
        self.assertTrue((self.df["currency"] == "EUR").all())

    def test_chronological_order(self):
        dates = self.df["date"].tolist()
        self.assertEqual(dates, sorted(dates))

    def test_no_eur_asset(self):
        """EUR must never appear as the asset column (only crypto)."""
        self.assertNotIn("EUR", self.df["asset"].unique())

    def test_only_known_types(self):
        valid = {"buy", "sell", "spend", "stash", "unstash"}
        self.assertTrue(set(self.df["type"].unique()).issubset(valid))

    def test_amounts_non_negative(self):
        self.assertTrue((self.df["amount"] >= 0).all())

    def test_unitvalues_non_negative(self):
        self.assertTrue((self.df["unitvalue"] >= 0).all())

    def test_year_filter(self):
        df_2020 = read_coinmotion(CSV_PATH, year=2020)
        self.assertTrue((df_2020["date"].dt.year == 2020).all())
        self.assertGreater(len(df_2020), 0)

    def test_until_filter(self):
        cutoff = pd.Timestamp("2019-12-31")
        df_until = read_coinmotion(CSV_PATH, until=cutoff)
        self.assertTrue((df_until["date"] <= cutoff).all())
        self.assertGreater(len(df_until), 0)

    def test_first_btc_deposit(self):
        """CSV row 2: deposit 1 BTC at 385.28 EUR/BTC (2016-03-04)."""
        first_btc_buy = self.df[self.df["asset"] == "BTC"].iloc[0]
        self.assertEqual(first_btc_buy["type"], "buy")
        self.assertAlmostEqual(first_btc_buy["amount"], 1.0)
        self.assertAlmostEqual(first_btc_buy["unitvalue"], 385.28)

    def test_market_trade_buy_fee_in_unitvalue(self):
        """EUR->BTC buy 0.07820802 BTC for 450.00 EUR (fee 3.58 already included).
        unitvalue = 450.00 / 0.07820802."""
        btc_buys = self.df[
            (self.df["asset"] == "BTC") & (self.df["type"] == "buy")
        ]
        match = btc_buys[
            btc_buys["amount"].apply(lambda x: abs(x - 0.07820802) < 1e-7)
        ]
        self.assertGreater(len(match), 0)
        expected = 450.00 / 0.07820802
        self.assertAlmostEqual(match.iloc[0]["unitvalue"], expected, places=2)

    def test_market_trade_sell_net_proceeds_in_unitvalue(self):
        """BTC->EUR sell 1.0 BTC, eurAmount=3799.05 (fee 19.10 deducted).
        unitvalue = 3799.05 / 1.0."""
        btc_sells = self.df[
            (self.df["asset"] == "BTC") & (self.df["type"] == "sell")
        ]
        match = btc_sells[
            btc_sells["amount"].apply(lambda x: abs(x - 1.0) < 1e-7)
        ]
        self.assertGreater(len(match), 0)
        self.assertAlmostEqual(match.iloc[0]["unitvalue"], 3799.05, places=2)

    def test_vault_fee_rows_parsed_as_sell(self):
        """vault_fee rows must appear as small 'sell' entries for the asset."""
        tiny_sells = self.df[
            (self.df["type"] == "sell") & (self.df["amount"] < 0.001)
        ]
        self.assertGreater(len(tiny_sells), 0)

    def test_withdrawal_net_amount(self):
        """Withdrawal of 3.0005 BTC with 0.0005 fee -> spend 3.0 BTC."""
        btc_spends = self.df[
            (self.df["asset"] == "BTC") & (self.df["type"] == "spend")
        ]
        match = btc_spends[
            btc_spends["amount"].apply(lambda x: abs(x - 3.0) < 1e-7)
        ]
        self.assertGreater(len(match), 0)


# ---------------------------------------------------------------------------
# End-to-end test -- gains pipeline on the real CSV
# ---------------------------------------------------------------------------

class TestGainsPipeline(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        df = read_coinmotion(CSV_PATH)
        cls.btc = Pair("BTC", "EUR")
        cls.eth = Pair("ETH", "EUR")
        cls.ltc = Pair("LTC", "EUR")
        gains_main(df, [cls.btc, cls.eth, cls.ltc])

    def test_btc_gains_populated(self):
        self.assertGreater(len(self.btc.gains), 0)

    def test_eth_gains_populated(self):
        self.assertGreater(len(self.eth.gains), 0)

    def test_all_gains_are_finite(self):
        for pair in (self.btc, self.eth, self.ltc):
            for g in pair.gains:
                self.assertTrue(
                    math.isfinite(g["value"]),
                    f"Non-finite gain {g['value']} in {pair.asset}",
                )

    def test_btc_book_columns(self):
        """After sells, the cost-basis book must have the expected schema."""
        book = self.btc.xc.book
        self.assertGreater(len(book), 0)
        for col in ("buydate", "selldate", "buyvalue", "sellvalue", "amount"):
            self.assertIn(col, book.columns)

    def test_btc_2018_gain_is_positive(self):
        """The 2018-11-21 BTC->EUR sell was at ~3799 EUR, well above any
        reasonable cost basis -> gain must be positive."""
        gains_2018 = self.btc.gains_df(year=2018)
        self.assertGreater(len(gains_2018), 0)
        self.assertGreater(gains_2018.iloc[0]["value"], 0)

    def test_btc_2018_gain_below_gross_proceeds(self):
        """Gain must be less than gross proceeds (3799.05 EUR)."""
        gains_2018 = self.btc.gains_df(year=2018)
        self.assertLess(gains_2018.iloc[0]["value"], 3799.05)

    def test_eth_2020_gains_exist(self):
        """Multiple ETH sells in 2020 (April and July)."""
        gains_2020 = self.eth.gains_df(year=2020)
        self.assertGreater(len(gains_2020), 0)

    def test_btc_sell_gain_aca_applied(self):
        """The 2018 BTC sell held <10 years.  With ACA coef 0.20 and sell
        price 3799.05, the ACA basis is 0.20*3799.05=759.81 per unit.
        For the first lot (1 BTC at 385.28), ACA beats actual cost ->
        overall gain < 3799.05 - 385.28 (confirming ACA is used)."""
        gains_2018 = self.btc.gains_df(year=2018)
        gain = gains_2018.iloc[0]["value"]
        raw_gain = 3799.05 - 385.28   # if actual cost were used for full lot
        self.assertLess(gain, raw_gain)


if __name__ == "__main__":
    unittest.main()
