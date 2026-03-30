from __future__ import annotations

import argparse
from pathlib import Path
from typing import Callable

from config import get_settings
from src.export.xlsx_exporter import export_classified_history_to_workbook
from src.ingest.etherscan_client import EtherscanClient, EtherscanClientError
from src.process.classifier import classify_wallet_history
from src.process.normalizer import normalize_wallet_history
from src.process.pricing import enrich_wallet_history_with_prices
from src.utils.file_io import save_json
from src.utils.validators import validate_ethereum_wallet


def build_raw_output_paths(raw_dir: Path, wallet_address: str) -> tuple[Path, Path]:
    wallet_slug = wallet_address.lower()
    normal_path = raw_dir / f"normal_transactions_{wallet_slug}.json"
    token_path = raw_dir / f"token_transfers_{wallet_slug}.json"
    return normal_path, token_path


def build_processed_output_paths(processed_dir: Path, wallet_address: str) -> tuple[Path, Path, Path]:
    wallet_slug = wallet_address.lower()
    normalized_path = processed_dir / f"normalized_transactions_{wallet_slug}.json"
    classified_path = processed_dir / f"classified_transactions_{wallet_slug}.json"
    priced_path = processed_dir / f"priced_transactions_{wallet_slug}.json"
    return normalized_path, classified_path, priced_path


def build_export_output_path(output_dir: Path, wallet_address: str) -> Path:
    wallet_slug = wallet_address.lower()
    return output_dir / f"accountant_export_{wallet_slug}.xlsx"


def count_results(payload: dict) -> int:
    result = payload.get("result", [])
    return len(result) if isinstance(result, list) else 0


def run_wallet_pipeline(
    wallet_address: str,
    chain: str = "ethereum",
    *,
    raw_data_dir: Path | None = None,
    processed_data_dir: Path | None = None,
    output_data_dir: Path | None = None,
    price_cache_path: Path | None = None,
    progress_callback: Callable[[str], None] | None = None,
) -> dict:
    settings = get_settings()

    def report(message: str) -> None:
        if progress_callback is not None:
            progress_callback(message)

    requested_chain = (chain or settings.default_chain).strip().lower()
    if requested_chain != "ethereum":
        raise ValueError("This MVP currently supports Ethereum mainnet only.")

    wallet_address = validate_ethereum_wallet(wallet_address)

    resolved_raw_data_dir = raw_data_dir or settings.raw_data_dir
    resolved_processed_data_dir = processed_data_dir or settings.processed_data_dir
    resolved_output_data_dir = output_data_dir or settings.output_data_dir
    resolved_price_cache_path = price_cache_path or settings.price_cache_path

    client = EtherscanClient(
        api_key=settings.etherscan_api_key,
        timeout_seconds=settings.request_timeout_seconds,
    )

    report("Fetching normal transactions")
    normal_payload = client.fetch_normal_transactions(wallet_address)

    report("Fetching ERC-20 token transfers")
    token_payload = client.fetch_erc20_token_transfers(wallet_address)

    normal_path, token_path = build_raw_output_paths(resolved_raw_data_dir, wallet_address)
    save_json(normal_path, normal_payload)
    save_json(token_path, token_payload)

    report("Normalizing wallet history")
    normalized_payload = normalize_wallet_history(
        wallet_address=wallet_address,
        normal_payload=normal_payload,
        token_payload=token_payload,
        chain=requested_chain,
    )
    normalized_path, classified_path, priced_path = build_processed_output_paths(
        resolved_processed_data_dir, wallet_address
    )
    save_json(normalized_path, normalized_payload)

    report("Classifying wallet history")
    classified_payload = classify_wallet_history(normalized_payload)
    save_json(classified_path, classified_payload)

    report("Enriching wallet history with historical USD pricing")
    priced_payload = enrich_wallet_history_with_prices(
        classified_payload=classified_payload,
        cache_path=resolved_price_cache_path,
        timeout_seconds=settings.request_timeout_seconds,
    )
    save_json(priced_path, priced_payload)

    report("Exporting accountant workbook")
    workbook_path = build_export_output_path(resolved_output_data_dir, wallet_address)
    workbook_result = export_classified_history_to_workbook(priced_payload, workbook_path)

    normalized_summary = normalized_payload.get("summary", {})
    classified_summary = classified_payload.get("summary", {})
    priced_summary = priced_payload.get("summary", {})

    report("Workbook ready")

    return {
        "wallet_address": wallet_address,
        "chain": requested_chain,
        "paths": {
            "normal": str(normal_path),
            "token": str(token_path),
            "normalized": str(normalized_path),
            "classified": str(classified_path),
            "priced": str(priced_path),
            "workbook": str(workbook_path),
        },
        "counts": {
            "normal_transactions": count_results(normal_payload),
            "token_transfers": count_results(token_payload),
            "normalized_records": normalized_summary.get("record_count", 0),
            "classified_records": classified_summary.get("record_count", 0),
        },
        "summaries": {
            "event_type_counts": classified_summary.get("event_type_counts", {}),
            "classification_confidence_counts": classified_summary.get("classification_confidence_counts", {}),
            "review_flag_counts": classified_summary.get("review_flag_counts", {}),
            "review_reason_counts": classified_summary.get("review_reason_counts", {}),
            "pricing_status_counts": priced_summary.get("pricing_status_counts", {}),
            "pricing_review_reason_counts": priced_summary.get("pricing_review_reason_counts", {}),
            "workbook_sheet_counts": workbook_result.get("sheet_counts", {}),
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Crypto Wallet Accounting Agent MVP for Ethereum wallets: "
            "ingestion + normalization + classification + pricing + spreadsheet export."
        )
    )
    parser.add_argument(
        "--wallet",
        required=True,
        help="Public Ethereum wallet address to scan.",
    )
    parser.add_argument(
        "--chain",
        default="ethereum",
        help="Chain to scan. MVP currently supports ethereum only.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    try:
        print(f"Scanning wallet: {args.wallet}")
        print("Fetching normal transactions...")
        print("Fetching ERC-20 token transfers...")
        print("Normalizing wallet history...")
        print("Classifying wallet history...")
        print("Enriching wallet history with historical USD pricing...")
        print("Exporting accountant workbook...")

        result = run_wallet_pipeline(wallet_address=args.wallet, chain=args.chain)
    except ValueError as exc:
        raise SystemExit(f"Wallet validation failed: {exc}") from exc
    except EtherscanClientError as exc:
        raise SystemExit(f"Ingestion failed: {exc}") from exc

    print("Done.")
    print(f"Saved normal transactions to: {result['paths']['normal']}")
    print(f"Saved token transfers to: {result['paths']['token']}")
    print(f"Saved normalized history to: {result['paths']['normalized']}")
    print(f"Saved classified history to: {result['paths']['classified']}")
    print(f"Saved priced history to: {result['paths']['priced']}")
    print(f"Saved accountant workbook to: {result['paths']['workbook']}")
    print(f"Normal transaction count: {result['counts']['normal_transactions']}")
    print(f"Token transfer count: {result['counts']['token_transfers']}")
    print(f"Normalized record count: {result['counts']['normalized_records']}")
    print(f"Classified record count: {result['counts']['classified_records']}")
    print(f"Event counts: {result['summaries']['event_type_counts']}")
    print(f"Confidence counts: {result['summaries']['classification_confidence_counts']}")
    print(f"Review counts: {result['summaries']['review_flag_counts']}")
    print(f"Review reason counts: {result['summaries']['review_reason_counts']}")
    print(f"Pricing counts: {result['summaries']['pricing_status_counts']}")
    print(f"Pricing review reason counts: {result['summaries']['pricing_review_reason_counts']}")
    print(f"Workbook sheet counts: {result['summaries']['workbook_sheet_counts']}")


if __name__ == "__main__":
    main()
