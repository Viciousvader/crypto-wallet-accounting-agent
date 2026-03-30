from __future__ import annotations

import json
import os
import secrets
import shutil
import traceback
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Lock

from fastapi import BackgroundTasks, FastAPI, Form, HTTPException, Query
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse

from main import run_wallet_pipeline
from src.ingest.etherscan_client import EtherscanClientError
from src.utils.validators import validate_ethereum_wallet

app = FastAPI(title="Crypto Wallet Accounting Agent")

JOB_ROOT_DIR = Path(os.getenv("JOB_ROOT_DIR", "data/jobs"))
JOB_TTL_SECONDS = int(os.getenv("JOB_TTL_SECONDS", "3600"))
JOB_FILE_NAME = "job.json"
JOB_LOCK = Lock()


def _utc_now() -> datetime:
    return datetime.now(tz=UTC)


def _iso_now() -> str:
    return _utc_now().isoformat()


def _job_dir(job_id: str) -> Path:
    return JOB_ROOT_DIR / job_id


def _job_file(job_id: str) -> Path:
    return _job_dir(job_id) / JOB_FILE_NAME


def _write_json_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    with temp_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
    temp_path.replace(path)


def _read_job(job_id: str) -> dict | None:
    job_path = _job_file(job_id)
    if not job_path.exists():
        return None
    try:
        with job_path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _write_job(job_id: str, payload: dict) -> None:
    with JOB_LOCK:
        _write_json_atomic(_job_file(job_id), payload)


def _update_job(job_id: str, **updates: object) -> dict:
    with JOB_LOCK:
        payload = _read_job(job_id)
        if payload is None:
            raise RuntimeError("Job state is missing.")
        payload.update(updates)
        payload["updated_at"] = _iso_now()
        _write_json_atomic(_job_file(job_id), payload)
        return payload


def _require_job(job_id: str) -> dict:
    payload = _read_job(job_id)
    if payload is None:
        raise HTTPException(status_code=404, detail="Job not found or expired.")
    return payload


def _require_token(job: dict, token: str) -> None:
    expected_token = str(job.get("token", "") or "")
    if not token or not expected_token or not secrets.compare_digest(expected_token, token):
        raise HTTPException(status_code=403, detail="Invalid job token.")


def _cleanup_expired_jobs() -> None:
    now = _utc_now()
    if not JOB_ROOT_DIR.exists():
        return

    for child in JOB_ROOT_DIR.iterdir():
        if not child.is_dir():
            continue

        job_path = child / JOB_FILE_NAME
        expires_at: datetime | None = None

        if job_path.exists():
            try:
                with job_path.open("r", encoding="utf-8") as handle:
                    payload = json.load(handle)
                expires_at_raw = str(payload.get("expires_at", "") or "")
                if expires_at_raw:
                    expires_at = datetime.fromisoformat(expires_at_raw)
            except Exception:
                expires_at = None

        if expires_at is None:
            modified_at = datetime.fromtimestamp(child.stat().st_mtime, tz=UTC)
            expires_at = modified_at + timedelta(seconds=JOB_TTL_SECONDS)

        if expires_at <= now:
            shutil.rmtree(child, ignore_errors=True)


def _create_job(wallet_address: str) -> dict:
    job_id = secrets.token_urlsafe(16)
    token = secrets.token_urlsafe(32)
    created_at = _utc_now()
    expires_at = created_at + timedelta(seconds=JOB_TTL_SECONDS)

    payload = {
        "job_id": job_id,
        "token": token,
        "wallet_address": wallet_address,
        "chain": "ethereum",
        "status": "queued",
        "current_step": "Queued",
        "error": "",
        "result": None,
        "workbook_path": "",
        "workbook_filename": "",
        "created_at": created_at.isoformat(),
        "updated_at": created_at.isoformat(),
        "expires_at": expires_at.isoformat(),
    }
    _write_job(job_id, payload)
    return payload


def _run_job(job_id: str, wallet_address: str) -> None:
    job_base_dir = _job_dir(job_id)
    raw_dir = job_base_dir / "raw"
    processed_dir = job_base_dir / "processed"
    output_dir = job_base_dir / "output"
    cache_path = job_base_dir / "cache" / "price_cache.json"

    def report(message: str) -> None:
        _update_job(job_id, status="running", current_step=message)

    try:
        _update_job(job_id, status="running", current_step="Starting pipeline", error="")
        result = run_wallet_pipeline(
            wallet_address=wallet_address,
            chain="ethereum",
            raw_data_dir=raw_dir,
            processed_data_dir=processed_dir,
            output_data_dir=output_dir,
            price_cache_path=cache_path,
            progress_callback=report,
        )
        workbook_path = Path(result["paths"]["workbook"])
        if not workbook_path.exists():
            raise RuntimeError("Workbook was not created.")

        _update_job(
            job_id,
            status="complete",
            current_step="Workbook ready",
            result=result,
            workbook_path=str(workbook_path),
            workbook_filename=workbook_path.name,
        )
    except Exception as exc:
        detail = str(exc).strip() or exc.__class__.__name__
        if isinstance(exc, EtherscanClientError):
            detail = f"Ingestion failed: {detail}"
        _update_job(
            job_id,
            status="failed",
            current_step="Failed",
            error=detail,
            traceback=traceback.format_exc(limit=5),
        )


@app.get("/health")
def health() -> JSONResponse:
    _cleanup_expired_jobs()
    return JSONResponse({"status": "ok"})


@app.get("/", response_class=HTMLResponse)
def home() -> HTMLResponse:
    _cleanup_expired_jobs()
    return HTMLResponse(
        """
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8" />
            <meta name="viewport" content="width=device-width, initial-scale=1.0" />
            <title>Crypto Wallet Accounting Agent</title>
            <style>
                :root {
                    color-scheme: light;
                    font-family: Arial, sans-serif;
                }
                body {
                    max-width: 760px;
                    margin: 40px auto;
                    padding: 0 16px;
                }
                h1 {
                    margin-bottom: 8px;
                }
                p {
                    color: #444;
                    line-height: 1.5;
                }
                form {
                    margin-top: 24px;
                    display: grid;
                    gap: 12px;
                }
                input, button {
                    padding: 12px;
                    font-size: 16px;
                }
                button {
                    cursor: pointer;
                }
                button:disabled {
                    cursor: wait;
                    opacity: 0.7;
                }
                .panel {
                    margin-top: 20px;
                    padding: 16px;
                    border: 1px solid #d9d9d9;
                    border-radius: 8px;
                    background: #fafafa;
                }
                .muted {
                    color: #666;
                    font-size: 14px;
                }
                .hidden {
                    display: none;
                }
                .error {
                    color: #9c0006;
                }
                .success {
                    color: #1d5e20;
                }
                a.download-link {
                    display: inline-block;
                    margin-top: 10px;
                    font-weight: bold;
                }
                code {
                    background: #f1f1f1;
                    padding: 2px 4px;
                    border-radius: 4px;
                }
            </style>
        </head>
        <body>
            <h1>Crypto Wallet Accounting Agent</h1>
            <p>Paste an Ethereum wallet address and generate an accountant workbook.</p>
            <p class="muted">
                Jobs are processed asynchronously for better reliability on larger wallets.
                Finished files are stored temporarily and auto-deleted after about one hour.
            </p>

            <form id="analyze-form">
                <input
                    id="wallet"
                    type="text"
                    name="wallet"
                    placeholder="0x..."
                    autocomplete="off"
                    required
                />
                <button id="submit-button" type="submit">Analyze Wallet</button>
            </form>

            <div id="status-panel" class="panel hidden">
                <div><strong>Status:</strong> <span id="job-status">Waiting</span></div>
                <div style="margin-top: 8px;"><strong>Current step:</strong> <span id="job-step">-</span></div>
                <div id="job-error" class="error hidden" style="margin-top: 10px;"></div>
                <a id="download-link" class="download-link hidden" href="#">Download workbook</a>
                <div id="job-meta" class="muted" style="margin-top: 10px;"></div>
            </div>

            <script>
                const form = document.getElementById("analyze-form");
                const walletInput = document.getElementById("wallet");
                const submitButton = document.getElementById("submit-button");
                const statusPanel = document.getElementById("status-panel");
                const jobStatus = document.getElementById("job-status");
                const jobStep = document.getElementById("job-step");
                const jobError = document.getElementById("job-error");
                const downloadLink = document.getElementById("download-link");
                const jobMeta = document.getElementById("job-meta");

                let activePoll = null;

                function resetStatus() {
                    statusPanel.classList.remove("hidden");
                    jobStatus.textContent = "Queued";
                    jobStep.textContent = "-";
                    jobError.textContent = "";
                    jobError.classList.add("hidden");
                    downloadLink.classList.add("hidden");
                    downloadLink.removeAttribute("href");
                    jobMeta.textContent = "";
                }

                function stopPolling() {
                    if (activePoll !== null) {
                        clearInterval(activePoll);
                        activePoll = null;
                    }
                }

                async function pollJob(jobId, token) {
                    const response = await fetch(`/status/${jobId}?token=${encodeURIComponent(token)}`);
                    const payload = await response.json();

                    if (!response.ok) {
                        throw new Error(payload.detail || "Unable to fetch job status.");
                    }

                    jobStatus.textContent = payload.status;
                    jobStep.textContent = payload.current_step || "-";

                    if (payload.created_at) {
                        jobMeta.textContent = `Wallet: ${payload.wallet_address} | Expires: ${payload.expires_at}`;
                    }

                    if (payload.status === "complete") {
                        stopPolling();
                        downloadLink.href = payload.download_url;
                        downloadLink.textContent = `Download workbook (${payload.workbook_filename})`;
                        downloadLink.classList.remove("hidden");
                        submitButton.disabled = false;
                        submitButton.textContent = "Analyze Wallet";
                    } else if (payload.status === "failed") {
                        stopPolling();
                        jobError.textContent = payload.error || "Pipeline failed.";
                        jobError.classList.remove("hidden");
                        submitButton.disabled = false;
                        submitButton.textContent = "Analyze Wallet";
                    }
                }

                form.addEventListener("submit", async (event) => {
                    event.preventDefault();
                    stopPolling();
                    resetStatus();
                    submitButton.disabled = true;
                    submitButton.textContent = "Starting...";

                    const formData = new FormData();
                    formData.append("wallet", walletInput.value);

                    try {
                        const response = await fetch("/analyze", {
                            method: "POST",
                            body: formData,
                        });
                        const payload = await response.json();

                        if (!response.ok) {
                            throw new Error(payload.detail || "Unable to start analysis.");
                        }

                        jobStatus.textContent = payload.status;
                        jobStep.textContent = payload.current_step || "Queued";
                        jobMeta.textContent = `Wallet: ${payload.wallet_address} | Expires: ${payload.expires_at}`;

                        activePoll = setInterval(() => {
                            pollJob(payload.job_id, payload.token).catch((error) => {
                                stopPolling();
                                jobError.textContent = error.message;
                                jobError.classList.remove("hidden");
                                submitButton.disabled = false;
                                submitButton.textContent = "Analyze Wallet";
                            });
                        }, 2500);

                        pollJob(payload.job_id, payload.token).catch((error) => {
                            stopPolling();
                            jobError.textContent = error.message;
                            jobError.classList.remove("hidden");
                            submitButton.disabled = false;
                            submitButton.textContent = "Analyze Wallet";
                        });
                    } catch (error) {
                        jobError.textContent = error.message || "Unable to start analysis.";
                        jobError.classList.remove("hidden");
                        submitButton.disabled = false;
                        submitButton.textContent = "Analyze Wallet";
                    }
                });
            </script>
        </body>
        </html>
        """
    )


@app.post("/analyze")
def analyze(background_tasks: BackgroundTasks, wallet: str = Form(...)) -> JSONResponse:
    _cleanup_expired_jobs()

    try:
        normalized_wallet = validate_ethereum_wallet(wallet)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    job = _create_job(normalized_wallet)
    background_tasks.add_task(_run_job, job["job_id"], normalized_wallet)

    return JSONResponse(
        {
            "job_id": job["job_id"],
            "token": job["token"],
            "wallet_address": job["wallet_address"],
            "status": job["status"],
            "current_step": job["current_step"],
            "created_at": job["created_at"],
            "expires_at": job["expires_at"],
        }
    )


@app.get("/status/{job_id}")
def job_status(job_id: str, token: str = Query(...)) -> JSONResponse:
    _cleanup_expired_jobs()
    job = _require_job(job_id)
    _require_token(job, token)

    download_url = None
    if str(job.get("status", "")) == "complete":
        download_url = f"/download/{job_id}?token={token}"

    return JSONResponse(
        {
            "job_id": job["job_id"],
            "wallet_address": job.get("wallet_address", ""),
            "status": job.get("status", "unknown"),
            "current_step": job.get("current_step", ""),
            "error": job.get("error", ""),
            "created_at": job.get("created_at", ""),
            "updated_at": job.get("updated_at", ""),
            "expires_at": job.get("expires_at", ""),
            "workbook_filename": job.get("workbook_filename", ""),
            "download_url": download_url,
            "counts": (job.get("result") or {}).get("counts", {}),
            "summaries": (job.get("result") or {}).get("summaries", {}),
        }
    )


@app.get("/download/{job_id}")
def download(job_id: str, token: str = Query(...)) -> FileResponse:
    _cleanup_expired_jobs()
    job = _require_job(job_id)
    _require_token(job, token)

    if str(job.get("status", "")) != "complete":
        raise HTTPException(status_code=409, detail="Workbook is not ready yet.")

    workbook_path = Path(str(job.get("workbook_path", "") or ""))
    if not workbook_path.exists():
        raise HTTPException(status_code=404, detail="Workbook has expired or is missing.")

    filename = str(job.get("workbook_filename", "") or workbook_path.name)
    return FileResponse(
        path=workbook_path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=filename,
    )
