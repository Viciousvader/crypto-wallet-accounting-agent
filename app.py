from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, Form, HTTPException
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse

from main import run_wallet_pipeline
from src.ingest.etherscan_client import EtherscanClientError

app = FastAPI(title="Crypto Wallet Accounting Agent")


@app.get("/health")
def health() -> JSONResponse:
    return JSONResponse({"status": "ok"})


@app.get("/", response_class=HTMLResponse)
def home() -> HTMLResponse:
    return HTMLResponse(
        """
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Crypto Wallet Accounting Agent</title>
            <style>
                body { font-family: Arial, sans-serif; max-width: 720px; margin: 40px auto; padding: 0 16px; }
                h1 { margin-bottom: 8px; }
                p { color: #444; }
                form { margin-top: 24px; display: grid; gap: 12px; }
                input, button { padding: 12px; font-size: 16px; }
                button { cursor: pointer; }
            </style>
        </head>
        <body>
            <h1>Crypto Wallet Accounting Agent</h1>
            <p>Paste an Ethereum wallet address and download the accountant workbook.</p>
            <form method="post" action="/analyze">
                <input
                    type="text"
                    name="wallet"
                    placeholder="0x..."
                    required
                />
                <button type="submit">Analyze Wallet</button>
            </form>
        </body>
        </html>
        """
    )


@app.post("/analyze")
def analyze(wallet: str = Form(...)) -> FileResponse:
    try:
        result = run_wallet_pipeline(wallet_address=wallet, chain="ethereum")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except EtherscanClientError as exc:
        raise HTTPException(status_code=502, detail=f"Ingestion failed: {exc}") from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Pipeline failed: {exc}") from exc

    workbook_path = Path(result["paths"]["workbook"])
    if not workbook_path.exists():
        raise HTTPException(status_code=500, detail="Workbook was not created.")

    return FileResponse(
        path=workbook_path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=workbook_path.name,
    )
