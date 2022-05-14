
def parse_float(x):
    try:
        return float(x.split(' ')[0])
    except AttributeError: # it was nan
        return 0
