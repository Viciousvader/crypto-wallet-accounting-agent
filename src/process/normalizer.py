from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation, getcontext
from typing import Any

getcontext().prec = 50

ETH_DECIMALS = 18
ZERO = Decimal("0")


def normalize_wallet_history(
    wallet_address: str,
    normal_payload: dict[str, Any],
    token_payload: dict[str, Any],
    chain: str = "ethereum",
) -> dict[str, Any]:
    wallet_lower = wallet_address.lower()
    normal_rows = normal_payload.get("result", []) if isinstance(normal_payload, dict) else []
    token_rows = token_payload.get("result", []) if isinstance(token_payload, dict) else []

    records: list[dict[str, Any]] = []

    if isinstance(normal_rows, list):
        for row in normal_rows:
            if isinstance(row, dict):
                records.append(_normalize_normal_transaction(row, wallet_address, wallet_lower, chain))

    if isinstance(token_rows, list):
        for row in token_rows:
            if isinstance(row, dict):
                records.append(_normalize_token_transfer(row, wallet_address, wallet_lower, chain))

    records.sort(
        key=lambda item: (
            item.get("timestamp") or 0,
            _safe_int(item.get("block_number")),
            item.get("transaction_key") or "",
            item.get("source_type") or "",
        )
    )

    summary = _build_summary(wallet_address, chain, records)

    return {
        "wallet_address": wallet_address,
        "chain": chain,
        "summary": summary,
        "records": records,
    }


def _normalize_normal_transaction(
    row: dict[str, Any], wallet_address: str, wallet_lower: str, chain: str
) -> dict[str, Any]:
    from_address = str(row.get("from", "") or "")
    to_address = str(row.get("to", "") or "")
    value_raw = str(row.get("value", "0") or "0")
    gas_used_raw = str(row.get("gasUsed", row.get("gas", "0")) or "0")
    gas_price_raw = str(row.get("gasPrice", "0") or "0")
    timestamp = _safe_int(row.get("timeStamp"))
    amount = _decimal_to_string(_scale_amount(value_raw, ETH_DECIMALS))
    fee_amount = _decimal_to_string(_scale_amount(_multiply_strings(gas_used_raw, gas_price_raw), ETH_DECIMALS))

    return {
        "transaction_key": str(row.get("hash", "") or ""),
        "source_type": "normal_transaction",
        "chain": chain,
        "wallet_address": wallet_address,
        "direction": _determine_direction(wallet_lower, from_address, to_address),
        "tx_hash": str(row.get("hash", "") or ""),
        "block_number": str(row.get("blockNumber", "") or ""),
        "timestamp": timestamp,
        "datetime_utc": _timestamp_to_iso(timestamp),
        "status": "failed" if str(row.get("isError", "0")) == "1" else "success",
        "from_address": from_address,
        "to_address": to_address,
        "asset_type": "native",
        "asset_symbol": "ETH",
        "asset_name": "Ether",
        "raw_value": value_raw,
        "amount": amount,
        "fee_symbol": "ETH",
        "fee_amount": fee_amount,
        "token_contract": None,
        "token_decimals": ETH_DECIMALS,
        "log_index": None,
        "nonce": str(row.get("nonce", "") or ""),
        "method_id": str(row.get("methodId", "") or ""),
        "function_name": str(row.get("functionName", "") or ""),
    }


def _normalize_token_transfer(
    row: dict[str, Any], wallet_address: str, wallet_lower: str, chain: str
) -> dict[str, Any]:
    from_address = str(row.get("from", "") or "")
    to_address = str(row.get("to", "") or "")
    decimals = _safe_int(row.get("tokenDecimal"), default=0)
    value_raw = str(row.get("value", "0") or "0")
    timestamp = _safe_int(row.get("timeStamp"))
    gas_used_raw = str(row.get("gasUsed", row.get("gas", "0")) or "0")
    gas_price_raw = str(row.get("gasPrice", "0") or "0")

    return {
        "transaction_key": f"{str(row.get('hash', '') or '')}:{str(row.get('logIndex', '') or '')}",
        "source_type": "erc20_transfer",
        "chain": chain,
        "wallet_address": wallet_address,
        "direction": _determine_direction(wallet_lower, from_address, to_address),
        "tx_hash": str(row.get("hash", "") or ""),
        "block_number": str(row.get("blockNumber", "") or ""),
        "timestamp": timestamp,
        "datetime_utc": _timestamp_to_iso(timestamp),
        "status": "success",
        "from_address": from_address,
        "to_address": to_address,
        "asset_type": "erc20",
        "asset_symbol": str(row.get("tokenSymbol", "") or ""),
        "asset_name": str(row.get("tokenName", "") or ""),
        "raw_value": value_raw,
        "amount": _decimal_to_string(_scale_amount(value_raw, decimals)),
        "fee_symbol": "ETH",
        "fee_amount": _decimal_to_string(_scale_amount(_multiply_strings(gas_used_raw, gas_price_raw), ETH_DECIMALS)),
        "token_contract": str(row.get("contractAddress", "") or ""),
        "token_decimals": decimals,
        "log_index": str(row.get("logIndex", "") or ""),
        "nonce": str(row.get("nonce", "") or ""),
        "method_id": str(row.get("methodId", "") or ""),
        "function_name": str(row.get("functionName", "") or ""),
    }


def _build_summary(wallet_address: str, chain: str, records: list[dict[str, Any]]) -> dict[str, Any]:
    direction_counts = Counter(record.get("direction", "unknown") for record in records)
    source_counts = Counter(record.get("source_type", "unknown") for record in records)
    asset_counts = Counter(record.get("asset_symbol", "") for record in records if record.get("asset_symbol"))

    timestamps = [record["timestamp"] for record in records if isinstance(record.get("timestamp"), int) and record.get("timestamp") > 0]
    first_ts = min(timestamps) if timestamps else None
    last_ts = max(timestamps) if timestamps else None

    return {
        "wallet_address": wallet_address,
        "chain": chain,
        "record_count": len(records),
        "source_counts": dict(source_counts),
        "direction_counts": dict(direction_counts),
        "asset_symbol_counts": dict(asset_counts),
        "first_activity_timestamp": first_ts,
        "first_activity_datetime_utc": _timestamp_to_iso(first_ts) if first_ts else None,
        "last_activity_timestamp": last_ts,
        "last_activity_datetime_utc": _timestamp_to_iso(last_ts) if last_ts else None,
    }


def _determine_direction(wallet_lower: str, from_address: str, to_address: str) -> str:
    from_lower = from_address.lower()
    to_lower = to_address.lower()

    if from_lower == wallet_lower and to_lower == wallet_lower:
        return "self"
    if from_lower == wallet_lower:
        return "out"
    if to_lower == wallet_lower:
        return "in"
    return "other"


def _scale_amount(value_raw: str, decimals: int) -> Decimal:
    value = _safe_decimal(value_raw)
    if decimals <= 0:
        return value
    return value / (Decimal(10) ** decimals)


def _multiply_strings(left: str, right: str) -> str:
    return str(_safe_decimal(left) * _safe_decimal(right))


def _safe_decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value or "0"))
    except (InvalidOperation, ValueError):
        return ZERO


def _decimal_to_string(value: Decimal) -> str:
    normalized = value.normalize()
    if normalized == normalized.to_integral():
        return format(normalized.quantize(Decimal("1")), "f")
    return format(normalized, "f")


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return default


def _timestamp_to_iso(timestamp: int | None) -> str | None:
    if not timestamp:
        return None
    return datetime.fromtimestamp(timestamp, tz=UTC).isoformat()
