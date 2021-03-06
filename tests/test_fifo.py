#!/usr/bin/env python3
# coding: utf-8

import unittest
import datetime

from gainscalc import fifo

class TestFIFODeque(unittest.TestCase):
    def setUp(self):
        self.deque = fifo.FIFODeque()
        self.other = fifo.FIFODeque()
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

    def test_extract(self):
        out1 = list(self.deque.extract(2))
        self.assertEqual(len(out1), 1)
        self.assertEqual(out1[0]['unitvalue'], 200)
        out2 = list(self.deque.extract(6))
        self.assertEqual(len(out2), 2)
        self.assertEqual(out2[1]['unitvalue'], 311)
        self.assertEqual(out2[1]['amount'], 1)

    def test_send_receive_full_tx(self):
        self.deque.send(self.other, 2)
        self.assertEqual(len(self.other._wallet), 1)
        self.assertEqual(self.deque._wallet[0]['unitvalue'], 220)
        self.assertEqual(self.other._wallet[0]['unitvalue'], 200)
        self.deque.send(self.other, 5)
        self.assertEqual(self.other._wallet[1]['unitvalue'], 220)
        # send 1st tx back
        self.other.send(self.deque, 2)
        self.assertEqual(self.deque._wallet[0]['unitvalue'], 200)

    def test_send_receive_partial_tx(self):
        self.deque.send(self.other, 1)
        self.assertEqual(self.deque._wallet[0]['amount'], 1)
        self.assertEqual(self.other._wallet[0]['amount'], 1)
        self.assertEqual(self.deque._wallet[0]['unitvalue'], 200)
        self.assertEqual(self.other._wallet[0]['unitvalue'], 200)
        self.deque.send(self.other, 4)
        self.assertEqual(self.other._wallet[-1]['amount'], 3)


if __name__ == '__main__':
    unittest.main()
