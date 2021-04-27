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
        

if __name__ == '__main__':
    unittest.main()