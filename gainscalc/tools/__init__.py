# https://www.exchangerates.org.uk/USD-EUR-spot-exchange-rates-history-2021.html
EURUSD21 = 0.8458 # yearly mean


COL_NAMES_FI = {'buydate': 'ostohetki',
                'selldate': 'myyntihetki',
                'amount': 'määrä',
                'buyvalue': 'ostohinta ($)',
                'sellvalue': 'myyntihinta ($)',
                'eurbuyval': 'ostohinta (€)',
                'eursellval': 'myyntihinta (€)'}


def parse_float(x):
    try:
        return float(x.split(' ')[0])
    except AttributeError: # it was nan
        return 0


def prettify(style):
    dateformatter = lambda t: t.strftime('%Y-%d-%m %H:%M')
    fiatformat = '{:0.2f}'
    fmts = dict(eurbuyval=fiatformat, eursellval=fiatformat,
                sellvalue=fiatformat, buyvalue=fiatformat,
                buydate=dateformatter, selldate=dateformatter)
    style.format(fmts)
    style.hide() # index
    return style

