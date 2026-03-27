from __future__ import annotations

import argparse
from pathlib import Path

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
    settings = get_settings()

    requested_chain = (args.chain or settings.default_chain).strip().lower()
    if requested_chain != "ethereum":
        raise SystemExit("This MVP currently supports Ethereum mainnet only.")

    try:
        wallet_address = validate_ethereum_wallet(args.wallet)
    except ValueError as exc:
        raise SystemExit(f"Wallet validation failed: {exc}") from exc

    try:
        client = EtherscanClient(
            api_key=settings.etherscan_api_key,
            timeout_seconds=settings.request_timeout_seconds,
        )

        print(f"Scanning wallet: {wallet_address}")
        print("Fetching normal transactions...")
        normal_payload = client.fetch_normal_transactions(wallet_address)

        print("Fetching ERC-20 token transfers...")
        token_payload = client.fetch_erc20_token_transfers(wallet_address)
    except EtherscanClientError as exc:
        raise SystemExit(f"Ingestion failed: {exc}") from exc

    normal_path, token_path = build_raw_output_paths(settings.raw_data_dir, wallet_address)
    save_json(normal_path, normal_payload)
    save_json(token_path, token_payload)

    print("Normalizing wallet history...")
    normalized_payload = normalize_wallet_history(
        wallet_address=wallet_address,
        normal_payload=normal_payload,
        token_payload=token_payload,
        chain=requested_chain,
    )
    normalized_path, classified_path, priced_path = build_processed_output_paths(
        settings.processed_data_dir, wallet_address
    )
    save_json(normalized_path, normalized_payload)

    print("Classifying wallet history...")
    classified_payload = classify_wallet_history(normalized_payload)
    save_json(classified_path, classified_payload)

    print("Enriching wallet history with historical USD pricing...")
    priced_payload = enrich_wallet_history_with_prices(
        classified_payload=classified_payload,
        cache_path=settings.price_cache_path,
        timeout_seconds=settings.request_timeout_seconds,
    )
    save_json(priced_path, priced_payload)

    print("Exporting accountant workbook...")
    workbook_path = build_export_output_path(settings.output_data_dir, wallet_address)
    workbook_result = export_classified_history_to_workbook(priced_payload, workbook_path)

    normalized_summary = normalized_payload.get("summary", {})
    classified_summary = classified_payload.get("summary", {})
    priced_summary = priced_payload.get("summary", {})

    print("Done.")
    print(f"Saved normal transactions to: {normal_path}")
    print(f"Saved token transfers to: {token_path}")
    print(f"Saved normalized history to: {normalized_path}")
    print(f"Saved classified history to: {classified_path}")
    print(f"Saved priced history to: {priced_path}")
    print(f"Saved accountant workbook to: {workbook_path}")
    print(f"Normal transaction count: {count_results(normal_payload)}")
    print(f"Token transfer count: {count_results(token_payload)}")
    print(f"Normalized record count: {normalized_summary.get('record_count', 0)}")
    print(f"Classified record count: {classified_summary.get('record_count', 0)}")
    print(f"Event counts: {classified_summary.get('event_type_counts', {})}")
    print(f"Confidence counts: {classified_summary.get('classification_confidence_counts', {})}")
    print(f"Review counts: {classified_summary.get('review_flag_counts', {})}")
    print(f"Review reason counts: {classified_summary.get('review_reason_counts', {})}")
    print(f"Pricing counts: {priced_summary.get('pricing_status_counts', {})}")
    print(f"Pricing review reason counts: {priced_summary.get('pricing_review_reason_counts', {})}")
    print(f"Workbook sheet counts: {workbook_result.get('sheet_counts', {})}")


if __name__ == "__main__":
    main()
