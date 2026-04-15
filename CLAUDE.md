# instructions

This file provides guidance to LLM agents when working with code in this repository.

## Project Overview

gainscalc is a FIFO engine and tax reporting tool for calculating capital gains on cryptocurrency (Bitcoin, Ethereum). It targets Finnish tax guidance, including the Acquisition Cost Assumption (ACA) rules.

## Commands

**Install for development:**
```bash
pip install -e .
```

**Run all tests:**
```bash
python -m unittest tests.test_fifo -v
```

**Run a single test:**
```bash
python -m unittest tests.test_fifo.TestFIFODeque.test_gain -v
```

**CLI tool (after install):**
```bash
ethexport <infile> <outfile>
```

## Code Style

Use **Black** formatting for all Python code.

## Architecture

The core is `FIFODeque` in [src/gainscalc/fifo.py](src/gainscalc/fifo.py): a deque-backed wallet that processes buy/sell/transfer transactions using the FIFO principle and computes capital gains. Finnish ACA rules are applied per transaction: 20% acquisition cost assumption for holdings under 10 years, 40% for over 10 years.

[src/gainscalc/tools/gains.py](src/gainscalc/tools/gains.py) is the main workflow. It wraps `FIFODeque` in a `Pair` class (one per asset/currency pair, e.g. BTC/EUR), processes a normalized transaction DataFrame, and generates LaTeX tax reports via [src/gainscalc/templates/breakdown.tpl](src/gainscalc/templates/breakdown.tpl).

[src/gainscalc/rates.py](src/gainscalc/rates.py) fetches and caches daily price data from Yahoo Finance (cached as pickles in `/tmp`), providing `rate(timestamp, pair)` and `convertbook(book, pair)` for currency conversion.

Exchange-specific parsers normalize CSV exports into DataFrames consumed by the gains workflow:
- [src/gainscalc/tools/bstamp.py](src/gainscalc/tools/bstamp.py) — Bitstamp
- [src/gainscalc/tools/cmotion.py](src/gainscalc/tools/cmotion.py) — Coinmotion (Finnish exchange)
- [src/gainscalc/ethscan.py](src/gainscalc/ethscan.py) — Etherscan CSV (also the `ethexport` CLI entry point)

[src/gainscalc/io.py](src/gainscalc/io.py) is an incomplete stub.
