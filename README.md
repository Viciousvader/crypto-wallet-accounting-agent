# Crypto Wallet Accounting Agent - Ethereum MVP

Ethereum-first crypto wallet accounting pipeline that scans wallet activity, normalizes transactions, classifies activity, enriches rows with pricing, and exports an accountant-friendly `.xlsx` workbook.

This project was built in phases to keep the blast radius small and make testing easier.

## Current MVP Pipeline

wallet scan -> raw ingestion -> normalization -> classification -> pricing -> xlsx export

## What It Does

- connects to an Ethereum wallet address
- fetches wallet activity using Etherscan V2
- saves raw transaction data
- normalizes transaction records into a cleaner internal structure
- classifies rows into accounting-relevant categories
- enriches rows with pricing where possible
- exports an accountant-friendly Excel workbook
- flags uncertain / weird / spam / long-tail token activity for human review instead of pretending to know

## Current Scope

This is an **Ethereum-first MVP**.

Current priorities:
- ETH activity
- mainstream stablecoins like USDT / USDC
- accountant-friendly spreadsheet export
- readable review workflow for ambiguous rows

This is **not** pretending to be a full tax filing engine yet.

Planned future chain expansion:
- Base
- BNB Chain
- Avalanche

## Output

The workbook currently exports tabs such as:

- `All Transactions`
- `Needs Review`
- `Summary`

It is designed to work well in Excel and can also be imported into Google Sheets.

## Web App

A thin FastAPI web wrapper is included so a user can:

1. paste a wallet address
2. click **Analyze Wallet**
3. download the generated `.xlsx`

### Public App
Add your Railway link here:

`PASTE_PUBLIC_URL_HERE`

## Local Run - Web Version

Install dependencies:

```bash
python -m pip install -r requirements.txt
```

Run the web app locally:

```bash
python -m uvicorn app:app --reload
```

Open in browser:

```text
http://127.0.0.1:8000
```

## Local Run - CLI Version

Install dependencies:

```bash
python -m pip install -r requirements.txt
```

Run from command line:

```bash
python main.py --wallet 0xYOURWALLET
```

## Environment Variables

Create a `.env` file and add:

```env
ETHERSCAN_API_KEY=your_key_here
DEFAULT_CHAIN=ethereum
RAW_DATA_DIR=data/raw
REQUEST_TIMEOUT_SECONDS=60
```

## Tech Stack

- Python
- FastAPI
- OpenPyXL
- Requests
- Etherscan API

## Repository Structure

```text
.
├── app.py
├── main.py
├── config.py
├── requirements.txt
└── src
    ├── export
    │   └── xlsx_exporter.py
    ├── ingest
    │   └── etherscan_client.py
    ├── process
    │   ├── classifier.py
    │   ├── normalizer.py
    │   └── pricing.py
    └── utils
        ├── file_io.py
        └── validators.py
```

## MVP Notes

- ETH and mainstream stablecoin handling are the main priority
- weird / meme / spam tokens are intentionally pushed toward review when confidence is low
- failed price lookups are handled conservatively
- Google Sheets import works, though some column widths may need manual adjustment after import

## Why This Project Exists

This project was built as a practical accounting-oriented crypto wallet pipeline:
- structured ingestion
- staged processing
- conservative classification
- pricing enrichment
- usable spreadsheet output

The goal was to build something real, testable, and resume-worthy without overengineering the first version.

## Next Likely Improvements

- add more chains
- improve public UI polish
- improve Google Sheets formatting compatibility
- add direct hosted demo polish
- expand pricing coverage for more assets

## License

MIT
