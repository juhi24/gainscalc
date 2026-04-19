"""Tests for multi.py: merge_sources, detect_transfers, route_default_wallet."""

import datetime
import unittest

import pandas as pd

from gainscalc.tools.multi import detect_transfers, merge_sources, route_default_wallet


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ts(year, month, day, hour=0):
    return pd.Timestamp(datetime.datetime(year, month, day, hour))


def _row(type_, subtype, asset, amount, date, source="xA", unitvalue=1.0):
    return {
        "date": date,
        "asset": asset,
        "currency": "EUR",
        "type": type_,
        "subtype": subtype,
        "amount": amount,
        "unitvalue": unitvalue,
        "source": source,
    }


def _df(*rows):
    return pd.DataFrame(list(rows)).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Tests for route_default_wallet
# ---------------------------------------------------------------------------

class TestRouteDefaultWallet(unittest.TestCase):

    def test_unmatched_spend_becomes_stash(self):
        df = _df(_row("spend", "deposit", "BTC", 1.0, _ts(2020, 1, 1)))
        result = route_default_wallet(df, [])
        self.assertEqual(result.loc[0, "type"], "stash")

    def test_matched_spend_stays_spend(self):
        df = _df(
            _row("spend", "deposit", "BTC", 1.0, _ts(2020, 1, 1), source="xA"),
            _row("buy",   "deposit", "BTC", 1.0, _ts(2020, 1, 2), source="xB"),
        )
        transfers = [{"spend_idx": 0, "buy_idx": 1}]
        result = route_default_wallet(df, transfers)
        self.assertEqual(result.loc[0, "type"], "spend")

    def test_deposit_covered_by_wallet_becomes_unstash(self):
        df = _df(
            _row("spend", "deposit", "BTC", 1.0, _ts(2020, 1, 1)),
            _row("buy",   "deposit", "BTC", 1.0, _ts(2020, 1, 5)),
        )
        result = route_default_wallet(df, [])
        self.assertEqual(result.loc[0, "type"], "stash")
        self.assertEqual(result.loc[1, "type"], "unstash")

    def test_deposit_insufficient_wallet_stays_buy(self):
        df = _df(
            _row("spend", "deposit", "BTC", 0.5, _ts(2020, 1, 1)),
            _row("buy",   "deposit", "BTC", 1.0, _ts(2020, 1, 5)),
        )
        result = route_default_wallet(df, [])
        self.assertEqual(result.loc[0, "type"], "stash")
        self.assertEqual(result.loc[1, "type"], "buy")

    def test_tolerance_covered(self):
        """Wallet 0.995 BTC, deposit 1.0 BTC — within 1% tolerance → unstash."""
        df = _df(
            _row("spend", "deposit", "BTC", 0.995, _ts(2020, 1, 1)),
            _row("buy",   "deposit", "BTC", 1.0,   _ts(2020, 1, 5)),
        )
        result = route_default_wallet(df, [], amount_rtol=0.01)
        self.assertEqual(result.loc[1, "type"], "unstash")

    def test_tolerance_just_below(self):
        """Wallet 0.98 BTC, deposit 1.0 BTC — below tolerance → stays buy."""
        df = _df(
            _row("spend", "deposit", "BTC", 0.98, _ts(2020, 1, 1)),
            _row("buy",   "deposit", "BTC", 1.0,  _ts(2020, 1, 5)),
        )
        result = route_default_wallet(df, [], amount_rtol=0.01)
        self.assertEqual(result.loc[1, "type"], "buy")

    def test_trade_subtype_never_touched(self):
        """Market trade buys and sells are untouched regardless of wallet state."""
        df = _df(
            _row("spend", "deposit", "BTC", 1.0, _ts(2020, 1, 1)),
            _row("buy",   "trade",   "BTC", 1.0, _ts(2020, 1, 5)),
            _row("sell",  "trade",   "BTC", 0.5, _ts(2020, 1, 6)),
        )
        result = route_default_wallet(df, [])
        self.assertEqual(result.loc[1, "type"], "buy")   # trade buy unchanged
        self.assertEqual(result.loc[2, "type"], "sell")  # sell unchanged

    def test_multi_asset_independent(self):
        """BTC and ETH wallet balances are tracked independently."""
        df = _df(
            _row("spend", "deposit", "BTC", 1.0, _ts(2020, 1, 1)),
            _row("spend", "deposit", "ETH", 2.0, _ts(2020, 1, 1)),
            _row("buy",   "deposit", "BTC", 1.0, _ts(2020, 1, 5)),
            _row("buy",   "deposit", "ETH", 2.0, _ts(2020, 1, 5)),
        )
        result = route_default_wallet(df, [])
        self.assertEqual(result.loc[2, "type"], "unstash")
        self.assertEqual(result.loc[3, "type"], "unstash")

    def test_wallet_balance_decremented(self):
        """After first unstash the balance drops; the next deposit may not be covered."""
        df = _df(
            _row("spend", "deposit", "BTC", 1.0, _ts(2020, 1, 1)),
            _row("buy",   "deposit", "BTC", 0.6, _ts(2020, 1, 5)),
            _row("buy",   "deposit", "BTC", 0.6, _ts(2020, 1, 6)),
        )
        result = route_default_wallet(df, [])
        self.assertEqual(result.loc[1, "type"], "unstash")  # 1.0 ≥ 0.6 → covered
        self.assertEqual(result.loc[2, "type"], "buy")      # 0.4 < 0.594 → not covered

    def test_matched_buy_stays_buy(self):
        """A buy that is a matched cross-exchange transfer stays buy."""
        df = _df(
            _row("spend", "deposit", "BTC", 1.0, _ts(2020, 1, 1), source="xA"),
            _row("buy",   "deposit", "BTC", 1.0, _ts(2020, 1, 2), source="xB"),
        )
        transfers = [{"spend_idx": 0, "buy_idx": 1}]
        result = route_default_wallet(df, transfers)
        self.assertEqual(result.loc[1, "type"], "buy")

    def test_no_subtype_column(self):
        """DataFrame without subtype column is returned unchanged (copy)."""
        df = pd.DataFrame([{
            "date": _ts(2020, 1, 1), "asset": "BTC", "currency": "EUR",
            "type": "spend", "amount": 1.0, "unitvalue": 1.0, "source": "xA",
        }])
        result = route_default_wallet(df, [])
        self.assertEqual(result.loc[0, "type"], "spend")

    def test_original_df_not_mutated(self):
        """route_default_wallet must not modify its input."""
        df = _df(_row("spend", "deposit", "BTC", 1.0, _ts(2020, 1, 1)))
        _ = route_default_wallet(df, [])
        self.assertEqual(df.loc[0, "type"], "spend")

    def test_same_index_after_routing(self):
        """Result index is identical to input index."""
        df = _df(
            _row("spend", "deposit", "BTC", 1.0, _ts(2020, 1, 1)),
            _row("buy",   "deposit", "BTC", 1.0, _ts(2020, 1, 5)),
        )
        result = route_default_wallet(df, [])
        self.assertEqual(result.index.tolist(), df.index.tolist())

    def test_wallet_label_accepted(self):
        """wallet_label kwarg is accepted without error."""
        df = _df(_row("spend", "deposit", "BTC", 1.0, _ts(2020, 1, 1)))
        result = route_default_wallet(df, [], wallet_label="hardware")
        self.assertEqual(result.loc[0, "type"], "stash")


# ---------------------------------------------------------------------------
# Integration test: cost basis preserved through wallet
# ---------------------------------------------------------------------------

class TestCostBasisPreservedThroughWallet(unittest.TestCase):
    """Full pipeline: buy → withdraw (stash) → deposit (unstash) → sell.

    The gain must use the original purchase cost, not the market rate at
    the time of the deposit (which is what happens if the deposit stays as buy).
    """

    def test_cost_basis_preserved_through_wallet(self):
        from gainscalc.tools.gains import Pair, main as gains_main

        t1 = _ts(2020, 1, 1)
        t2 = _ts(2020, 6, 1)
        t3 = _ts(2020, 6, 5)
        t4 = _ts(2020, 12, 1)

        df = _df(
            # 1. Buy 1 BTC at €10 000 on exchange A (market trade — cost known)
            {**_row("buy",   "trade",   "BTC", 1.0, t1, source="xA"), "unitvalue": 10_000.0},
            # 2. Withdraw 1 BTC from exchange A (unmatched — goes to default wallet)
            {**_row("spend", "deposit", "BTC", 1.0, t2, source="xA"), "unitvalue": 12_000.0},
            # 3. Deposit 1 BTC to exchange B (wallet covers it → unstash)
            {**_row("buy",   "deposit", "BTC", 1.0, t3, source="xB"), "unitvalue": 12_500.0},
            # 4. Sell 1 BTC at €20 000 on exchange B
            {**_row("sell",  "trade",   "BTC", 1.0, t4, source="xB"), "unitvalue": 20_000.0},
        )

        transfers = detect_transfers(df)    # no cross-exchange transfer detected
        routed = route_default_wallet(df, transfers)

        self.assertEqual(routed.loc[1, "type"], "stash",   "withdraw → stash")
        self.assertEqual(routed.loc[2, "type"], "unstash", "deposit → unstash")

        pair = Pair("BTC", "EUR")
        gains_main(routed, [pair])

        year_gains = pair.gains_df(year=2020)
        self.assertEqual(len(year_gains), 1)
        # Gain = proceeds (20 000) − cost (10 000) = 10 000
        self.assertAlmostEqual(year_gains.iloc[0]["value"], 10_000.0, places=2)

    def test_uncovered_deposit_keeps_market_rate(self):
        """Deposit with no wallet backing stays as buy at market rate."""
        from gainscalc.tools.gains import Pair, main as gains_main

        t1 = _ts(2020, 1, 1)
        t2 = _ts(2020, 6, 1)

        df = _df(
            # No prior withdrawal — wallet is empty
            {**_row("buy",  "deposit", "BTC", 1.0, t1, source="xA"), "unitvalue": 9_000.0},
            {**_row("sell", "trade",   "BTC", 1.0, t2, source="xA"), "unitvalue": 20_000.0},
        )

        routed = route_default_wallet(df, [])
        self.assertEqual(routed.loc[0, "type"], "buy")  # stays buy — needs supplement

        pair = Pair("BTC", "EUR")
        gains_main(routed, [pair])

        year_gains = pair.gains_df(year=2020)
        self.assertAlmostEqual(year_gains.iloc[0]["value"], 20_000.0 - 9_000.0, places=2)


if __name__ == "__main__":
    unittest.main()
