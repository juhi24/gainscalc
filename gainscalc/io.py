import pandas as pd


CM_TX_TYPES = {'Pikaosto': 'buy',
               'Pikamyynti': 'sell',
               'Myyntitarjous': 'sell',
               'Holvi-talletus': 'stash',
               'Holvi-nosto': 'unstash',
               'Palkkio': 'new'}
SAT_PER_BTC = 100000000


def read_coinmotion(csv, asset='BTC'):
    df = pd.read_csv(csv, parse_dates=['Date'])
    df = df[df.Account.apply(lambda x: x in [asset, 'EUR'])]
    df = df[df.Status=='Valmis']


class FIFOtxs: # ??
    def __init__(self, init_balance=0, asset='BTC'):
        self._init_balance = init_balance
        self._asset = asset
