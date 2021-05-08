#!/usr/bin/env python3

import unittest
import datetime

from gainscalc import fifo

class TestFIFODeque(unittest.TestCase):
    def setUp(self):
        self.deque = fifo.FIFODeque()
        self.deque.buy(datetime.datetime(2020, 1, 1), 2, 200)
        self.deque.buy(datetime.datetime(2020, 2, 1), 5, 220)
        self.deque.buy(datetime.datetime(2020, 4, 2), 3, 311)

    def test_sell_more_than_first_buy(self):
        self.deque.sell(datetime.datetime(2020, 12, 1), 3, 814)
        self.assertEqual(self.deque._wallet[0]['amount'], 4)
        self.assertEqual(len(self.deque._wallet), 2)

    def test_sell_more_than_2_first_buys(self):
        self.deque.sell(datetime.datetime(2020, 12, 1), 8, 814)
        self.assertEqual(self.deque._wallet[0]['amount'], 2)
        self.assertEqual(len(self.deque._wallet), 1)

    def test_enforce_chronology(self):
        with self.assertRaises(ValueError):
            self.deque.sell(datetime.datetime(2020, 1, 2), 8, 814)

    def test_gain(self):
        gain1 = self.deque.sell(datetime.datetime(2020, 12, 1), 1, 300)
        gain2 = self.deque.sell(datetime.datetime(2020, 12, 2), 2, 330)
        self.assertEqual(gain1, 100)
        self.assertEqual(gain2, 240)

    def test_aca(self):
        gain1 = self.deque.sell(datetime.datetime(2020, 12, 1), 1, 2000)
        gain2 = self.deque.sell(datetime.datetime(2030, 12, 1), 1, 2000)
        gain3 = self.deque.sell(datetime.datetime(2031, 12, 1), 6, 2000)
        self.assertAlmostEqual(gain1, 1600)
        self.assertAlmostEqual(gain2, 1200)
        self.assertAlmostEqual(gain3, 7200)


if __name__ == '__main__':
    unittest.main()
