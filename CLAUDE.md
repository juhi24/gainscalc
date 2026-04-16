# instructions

This file provides guidance to LLM agents when working with code in this repository.

## Project Overview

gainscalc is a FIFO engine and tax reporting tool for calculating capital gains on cryptocurrency (Bitcoin, Ethereum). It targets Finnish tax guidance, including the Acquisition Cost Assumption (ACA) rules.

## Commands

Use the virtualenv in `~/.virtualenvs/gainscalc/`.

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

The core is `FIFODeque` in [src/gainscalc/fifo.py](src/gainscalc/fifo.py): a deque-backed wallet that processes buy/sell/transfer transactions using the FIFO principle and computes capital gains. Finnish ACA rules are applied per transaction.

[src/gainscalc/tools/gains.py](src/gainscalc/tools/gains.py) is the main workflow. It wraps `FIFODeque` in a `Pair` class (one per asset/currency pair, e.g. BTC/EUR), processes a normalized transaction DataFrame, and generates LaTeX tax reports via [src/gainscalc/templates/breakdown.tpl](src/gainscalc/templates/breakdown.tpl).

[src/gainscalc/rates.py](src/gainscalc/rates.py) fetches and caches daily price data from Yahoo Finance (cached as pickles in `/tmp`), providing `rate(timestamp, pair)` and `convertbook(book, pair)` for currency conversion.

Exchange-specific parsers normalize CSV exports into DataFrames consumed by the gains workflow:
- [src/gainscalc/tools/bstamp.py](src/gainscalc/tools/bstamp.py) — Bitstamp
- [src/gainscalc/tools/cmotion.py](src/gainscalc/tools/cmotion.py) — Coinmotion (Finnish exchange)
- [src/gainscalc/ethscan.py](src/gainscalc/ethscan.py) — Etherscan CSV (also the `ethexport` CLI entry point)

[src/gainscalc/io.py](src/gainscalc/io.py) is an incomplete stub.

## Finnish Tax Rules (Verohallinto guidance)

Source: https://www.vero.fi/syventavat-vero-ohjeet/ohje-hakusivu/48411/kryptovarojen-verotus/

### Taxable events
Every disposal of crypto is a separate realization event: selling for fiat, exchanging one crypto for another, and using crypto to pay for goods/services. Staking and lending are taxable if the counterparty gains use rights over the assets.

### FIFO and cost basis
FIFO is the mandatory default (KHO 2024:123): assets are treated as sold in acquisition order unless the taxpayer can demonstrate otherwise. FIFO tracking may be per-wallet or per-account — global FIFO across all wallets is not required, but the chosen method must be consistently documented.

Instead of actual acquisition cost, the taxpayer may use the **acquisition cost assumption (hankintameno-olettama)**:
- Holdings held **< 10 years**: deduct **20%** of sale price
- Holdings held **≥ 10 years**: deduct **40%** of sale price

The higher of actual cost or ACA is used (taxpayer's choice per transaction).

### Fees
- Trading/exchange fees paid when buying: added to acquisition cost.
- Trading/exchange fees paid when selling: deducted from proceeds.
- Network/transfer fees paid in crypto: constitute a taxable disposal at fair market value on the date paid, but are deductible as an expense.

### Transfers between own wallets
The guidance does not exempt own-wallet transfers from taxation. Network fees paid in crypto for such transfers are deductible expenses and themselves trigger a small taxable disposal.

### Small sales exemption
Total crypto disposals of **€1,000 or less** in a calendar year are tax-exempt (TVL 48 § 6).

### Reporting and record-keeping
- Gains reported as "other property sales" in OmaVero; losses as capital losses.
- Records must be kept for **6 years**: transaction dates, amounts, EUR fair value at transaction time, and cost basis calculations.
