from __future__ import annotations

import json
from collections import Counter
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from time import sleep
from typing import Any

import requests

ZERO = Decimal("0")
COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3"
ETHEREUM_COIN_ID = "ethereum"
ETHEREUM_PLATFORM_ID = "ethereum"
NATIVE_PRICE_KEY = "native:ethereum"
STABLECOIN_SYMBOLS = {"USDT", "USDC", "DAI", "TUSD"}
CACHEABLE_SUCCESS_STATUSES = {"priced"}
NATIVE_ETH_LOOKUP_ATTEMPTS = 3
NATIVE_ETH_RETRY_SLEEP_SECONDS = 1


@dataclass(frozen=True)
class PriceLookupResult:
    price_usd: str | None
    source: str
    status: str


class PriceLookupClient:
    def __init__(self, timeout_seconds: int = 30) -> None:
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()

    def lookup_price_usd(
        self,
        *,
        asset_key: str,
        asset_symbol: str,
        contract_address: str | None,
        date_key: str,
    ) -> PriceLookupResult:
        try:
            if asset_key == NATIVE_PRICE_KEY:
                return self._fetch_native_eth_price(date_key)

            if contract_address:
                contract_result = self._fetch_contract_price(contract_address, date_key)
                if contract_result.price_usd is not None:
                    return contract_result

            stablecoin_result = _stablecoin_fallback(asset_symbol)
            if stablecoin_result is not None:
                return stablecoin_result
        except requests.RequestException:
            stablecoin_result = _stablecoin_fallback(asset_symbol)
            if stablecoin_result is not None:
                return stablecoin_result
            return PriceLookupResult(price_usd=None, source="coingecko", status="lookup_error")
        except ValueError:
            stablecoin_result = _stablecoin_fallback(asset_symbol)
            if stablecoin_result is not None:
                return stablecoin_result
            return PriceLookupResult(price_usd=None, source="coingecko", status="lookup_error")

        return PriceLookupResult(price_usd=None, source="coingecko", status="unsupported_asset")

    def _fetch_native_eth_price(self, date_key: str) -> PriceLookupResult:
        last_error: requests.RequestException | None = None
        for attempt in range(1, NATIVE_ETH_LOOKUP_ATTEMPTS + 1):
            try:
                payload = self._get_json(
                    f"{COINGECKO_BASE_URL}/coins/{ETHEREUM_COIN_ID}/history",
                    params={"date": _to_coingecko_date(date_key), "localization": "false"},
                )
                return _extract_price_lookup_result(payload)
            except requests.RequestException as exc:
                last_error = exc
                if attempt < NATIVE_ETH_LOOKUP_ATTEMPTS:
                    sleep(NATIVE_ETH_RETRY_SLEEP_SECONDS)
                    continue
                raise

        if last_error is not None:
            raise last_error
        return PriceLookupResult(price_usd=None, source="coingecko", status="lookup_error")

    def _fetch_contract_price(self, contract_address: str, date_key: str) -> PriceLookupResult:
        payload = self._get_json(
            f"{COINGECKO_BASE_URL}/coins/{ETHEREUM_PLATFORM_ID}/contract/{contract_address.lower()}/history",
            params={"date": _to_coingecko_date(date_key), "localization": "false"},
        )
        return _extract_price_lookup_result(payload)

    def _get_json(self, url: str, params: dict[str, Any]) -> dict[str, Any]:
        response = self.session.get(url, params=params, timeout=self.timeout_seconds)
        response.raise_for_status()
        return response.json()


def enrich_wallet_history_with_prices(
    classified_payload: dict[str, Any],
    cache_path: Path,
    timeout_seconds: int = 30,
) -> dict[str, Any]:
    wallet_address = str(classified_payload.get("wallet_address", "") or "")
    chain = str(classified_payload.get("chain", "") or "")
    original_records = classified_payload.get("records", [])
    records = [deepcopy(record) for record in original_records if isinstance(record, dict)]

    client = PriceLookupClient(timeout_seconds=timeout_seconds)
    cache = _load_price_cache(cache_path)

    price_status_counts: Counter[str] = Counter()
    pricing_review_reason_counts: Counter[str] = Counter()

    for record in records:
        asset_key = _build_asset_key(record)
        asset_symbol = str(record.get("asset_symbol", "") or "").upper().strip()
        date_key = _extract_date_key(record)
        cache_key = f"{asset_key}|{date_key}"
        lookup: PriceLookupResult

        if not date_key:
            lookup = PriceLookupResult(price_usd=None, source="coingecko", status="missing_timestamp")
        elif cache_key in cache:
            cached_entry = cache[cache_key]
            lookup = PriceLookupResult(
                price_usd=cached_entry.get("price_usd"),
                source=str(cached_entry.get("source", "cache") or "cache"),
                status=str(cached_entry.get("status", "cached") or "cached"),
            )
        else:
            lookup = client.lookup_price_usd(
                asset_key=asset_key,
                asset_symbol=asset_symbol,
                contract_address=_contract_address_or_none(record),
                date_key=date_key,
            )
            if lookup.status in CACHEABLE_SUCCESS_STATUSES and lookup.price_usd is not None:
                cache[cache_key] = {
                    "price_usd": lookup.price_usd,
                    "source": lookup.source,
                    "status": lookup.status,
                }

        _apply_price_enrichment(record, lookup)
        price_status_counts[str(record.get("pricing_status", "unpriced") or "unpriced")] += 1

        review_reason = str(record.get("review_reason", "") or "")
        if review_reason:
            for reason_code in review_reason.split("|"):
                pricing_review_reason_counts[reason_code] += 1

    _save_price_cache(cache_path, cache)

    summary = deepcopy(classified_payload.get("summary", {})) if isinstance(classified_payload, dict) else {}
    summary["record_count"] = len(records)
    summary["pricing_status_counts"] = dict(price_status_counts)
    summary["pricing_review_reason_counts"] = dict(pricing_review_reason_counts)
    summary["priced_record_count"] = price_status_counts.get("priced", 0)
    summary["priced_usd_value_total"] = _sum_usd_values(records)

    return {
        "wallet_address": wallet_address,
        "chain": chain,
        "summary": summary,
        "records": records,
    }


def _apply_price_enrichment(record: dict[str, Any], lookup: PriceLookupResult) -> None:
    event_type = str(record.get("event_type", "") or "")
    amount = _to_decimal(record.get("amount"))
    fee_amount = _to_decimal(record.get("fee_amount"))
    basis_amount = fee_amount if event_type == "fee" and fee_amount > ZERO else amount

    record["price_source"] = lookup.source
    record["pricing_status"] = lookup.status

    if lookup.price_usd is None:
        record["price_usd"] = None
        record["usd_value"] = None
        record["review_flag"] = True
        record["review_reason"] = _merge_review_reason(record.get("review_reason"), "price_missing")
        return

    price_decimal = _to_decimal(lookup.price_usd)
    usd_value = basis_amount * price_decimal

    record["price_usd"] = _decimal_to_string(price_decimal)
    record["usd_value"] = _decimal_to_string(usd_value)


def _extract_price_lookup_result(payload: dict[str, Any]) -> PriceLookupResult:
    market_data = payload.get("market_data", {}) if isinstance(payload, dict) else {}
    current_price = market_data.get("current_price", {}) if isinstance(market_data, dict) else {}
    price = current_price.get("usd") if isinstance(current_price, dict) else None

    if price is None:
        return PriceLookupResult(price_usd=None, source="coingecko", status="missing_price")

    return PriceLookupResult(price_usd=_decimal_to_string(_to_decimal(price)), source="coingecko", status="priced")


def _extract_date_key(record: dict[str, Any]) -> str:
    timestamp_utc = str(record.get("timestamp_utc", "") or "")
    if not timestamp_utc:
        return ""
    return timestamp_utc[:10]


def _build_asset_key(record: dict[str, Any]) -> str:
    asset_symbol = str(record.get("asset_symbol", "") or "").upper()
    contract_address = _contract_address_or_none(record)

    if asset_symbol == "ETH" and not contract_address:
        return NATIVE_PRICE_KEY
    if contract_address:
        return f"contract:{contract_address.lower()}"
    return f"symbol:{asset_symbol}"


def _contract_address_or_none(record: dict[str, Any]) -> str | None:
    contract_address = str(record.get("contract_address", "") or "").strip()
    return contract_address or None


def _stablecoin_fallback(asset_symbol: str) -> PriceLookupResult | None:
    normalized_symbol = str(asset_symbol or "").upper().strip()
    if normalized_symbol in STABLECOIN_SYMBOLS:
        return PriceLookupResult(price_usd="1", source="stablecoin_fallback", status="priced")
    return None


def _merge_review_reason(existing: Any, new_reason: str) -> str:
    reasons = {part for part in str(existing or "").split("|") if part}
    reasons.add(new_reason)
    return "|".join(sorted(reasons))


def _sum_usd_values(records: list[dict[str, Any]]) -> str:
    total = ZERO
    for record in records:
        total += _to_decimal(record.get("usd_value"))
    return _decimal_to_string(total)


def _load_price_cache(cache_path: Path) -> dict[str, dict[str, Any]]:
    if not cache_path.exists():
        return {}

    try:
        with cache_path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (json.JSONDecodeError, OSError):
        return {}

    if not isinstance(payload, dict):
        return {}
    return {
        str(key): value
        for key, value in payload.items()
        if isinstance(key, str) and isinstance(value, dict)
    }


def _save_price_cache(cache_path: Path, cache: dict[str, dict[str, Any]]) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with cache_path.open("w", encoding="utf-8") as handle:
        json.dump(cache, handle, indent=2, sort_keys=True)


def _to_coingecko_date(date_key: str) -> str:
    return datetime.strptime(date_key, "%Y-%m-%d").strftime("%d-%m-%Y")


def _to_decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value or "0"))
    except (InvalidOperation, ValueError):
        return ZERO


def _decimal_to_string(value: Decimal) -> str:
    normalized = value.normalize()
    text = format(normalized, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"
