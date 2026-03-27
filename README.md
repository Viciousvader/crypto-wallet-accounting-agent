# Crypto Wallet Accounting Agent - Ethereum MVP

This project is an **Ethereum-first crypto wallet accounting pipeline** built in phases to keep the blast radius small and make testing easier.

Current MVP pipeline:

`wallet scan -> raw ingestion -> normalization -> classification -> pricing -> xlsx export`

## What it does right now

- validates a public Ethereum wallet address
- fetches normal transactions from Etherscan V2
- fetches ERC-20 token transfer history
- saves raw JSON files to disk
- normalizes wallet history into a cleaner transaction record set
- classifies transactions into accounting-relevant event types
- enriches many ETH and mainstream stablecoin rows with historical USD pricing
- exports an accountant-style Excel workbook

## What is working well in the current MVP

- Ethereum mainnet ingestion
- raw / normalized / classified / priced JSON saves
- Excel workbook export
- Google Sheets import of the workbook
- workbook tabs:
  - `All Transactions`
  - `Needs Review`
  - `Summary`
- mainstream stablecoin handling is mostly in good shape for USDC and USDT
- many ETH rows price correctly
- weird long-tail / meme / spam tokens are intentionally left as review items when needed

## Current limitations

This is an MVP, not a full tax engine.

Current limitations include:

- Ethereum only
- not all long-tail tokens price cleanly
- some contract-interaction noise may still appear in review on messy wallets
- cost basis and tax-lot accounting are not implemented
- tax forms are not generated
- output is designed for review, not blind one-click filing

## Setup

1. Install Python 3.11+ if needed.
2. Open a terminal in this folder.
3. Install dependencies:

```bash
pip install -r requirements.txt
```

4. Copy `.env.example` to `.env`
5. Put your Etherscan API key into `.env`

## Run

Basic run:

```bash
python main.py --wallet 0xYourWalletAddressHere
```

Optional chain argument is included for future-proofing, but the MVP currently supports Ethereum only:

```bash
python main.py --wallet 0xYourWalletAddressHere --chain ethereum
```

## Output files

### Raw data
Saved to `data/raw/`:
- `normal_transactions_<wallet>.json`
- `token_transfers_<wallet>.json`

### Processed data
Saved to `data/processed/`:
- `normalized_transactions_<wallet>.json`
- `classified_transactions_<wallet>.json`
- `priced_transactions_<wallet>.json`

### Workbook export
Saved to `data/output/`:
- `accountant_export_<wallet>.xlsx`

## Workbook tabs

### All Transactions
Main export sheet with normalized, classified, and priced transaction history.

### Needs Review
Rows that still need human review because they are uncertain, unsupported, or intentionally conservative.

### Summary
High-level workbook summary for quick review.

## Example use case

This tool is currently best suited for:
- reading Ethereum wallet history
- making ETH and mainstream stablecoin activity more understandable
- giving an accountant or reviewer a structured workbook instead of raw explorer data

## Design approach

The project was built in phases to reduce regressions:
- Phase 1: ingestion
- Phase 2: normalization
- Phase 3: classification
- Phase 4: spreadsheet export
- Phase 5: workbook polish
- Phase 6: pricing hardening and messy-wallet review cleanup
- Phase 7: repo cleanup and portfolio-ready polish

## Success criteria for the current MVP

A valid Ethereum wallet should:
- complete the full pipeline without crashing
- save raw, normalized, classified, and priced JSON outputs
- generate an Excel workbook
- make ETH and mainstream stablecoin activity readable enough for human review

## Notes

This repo is intentionally honest about scope. The goal is to produce a useful accounting-oriented wallet export for Ethereum activity, not to pretend every token or tax edge case is already solved.
