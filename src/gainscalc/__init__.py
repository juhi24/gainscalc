try:
    from gainscalc._version import __version__
except ImportError:
    try:
        from importlib.metadata import version
        __version__ = version("gainscalc")
    except Exception:
        __version__ = "unknown"
