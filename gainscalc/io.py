
SAT_PER_BTC = 100000000


class FIFOtxs: # ??
    def __init__(self, init_balance=0, asset='BTC'):
        self._init_balance = init_balance
        self._asset = asset
