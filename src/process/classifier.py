from __future__ import annotations

from collections import Counter, defaultdict
from copy import deepcopy
from decimal import Decimal, InvalidOperation
from typing import Any

ZERO = Decimal("0")


def classify_wallet_history(normalized_payload: dict[str, Any]) -> dict[str, Any]:
    wallet_address = str(normalized_payload.get("wallet_address", "") or "")
    chain = str(normalized_payload.get("chain", "") or "")
    original_records = normalized_payload.get("records", [])
    records = [deepcopy(record) for record in original_records if isinstance(record, dict)]

    by_tx_hash: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        tx_hash = str(record.get("tx_hash", "") or "")
        by_tx_hash[tx_hash].append(record)

    for tx_hash, group in by_tx_hash.items():
        if _looks_like_simple_swap(group):
            for record in group:
                _apply_classification(
                    record,
                    event_type="swap",
                    confidence="LOW",
                    review_flag=True,
                    review_reason="swap_heuristic",
                    counterparty=_pick_counterparty(record),
                )
            continue

        handled_row_ids = _classify_companion_normal_rows(group)
        for record in group:
            if id(record) in handled_row_ids:
                continue
            _classify_single_record(record)

    summary = _build_summary(wallet_address, chain, records)
    return {
        "wallet_address": wallet_address,
        "chain": chain,
        "summary": summary,
        "records": records,
    }



def _classify_companion_normal_rows(group: list[dict[str, Any]]) -> set[int]:
    handled_row_ids: set[int] = set()
    has_positive_erc20_transfer = any(
        str(record.get("source_type", "") or "") == "erc20_transfer"
        and _to_decimal(record.get("amount")) > ZERO
        and str(record.get("direction", "") or "") in {"in", "out"}
        for record in group
    )

    if not has_positive_erc20_transfer:
        return handled_row_ids

    for record in group:
        source_type = str(record.get("source_type", "") or "")
        amount = _to_decimal(record.get("amount"))
        status = str(record.get("status", "") or "").lower()

        if source_type != "normal_transaction":
            continue
        if status == "failed":
            continue
        if amount > ZERO:
            continue

        _apply_classification(
            record,
            event_type="contract_interaction",
            confidence="MEDIUM",
            review_flag=False,
            review_reason="",
            counterparty=_pick_counterparty(record),
        )
        handled_row_ids.add(id(record))

    return handled_row_ids

def _classify_single_record(record: dict[str, Any]) -> None:
    direction = str(record.get("direction", "") or "")
    source_type = str(record.get("source_type", "") or "")
    amount = _to_decimal(record.get("amount"))
    fee_amount = _to_decimal(record.get("fee_amount"))
    counterparty = _pick_counterparty(record)

    if source_type == "normal_transaction" and amount <= ZERO and fee_amount > ZERO and direction == "out":
        _apply_classification(
            record,
            event_type="fee",
            confidence="HIGH",
            review_flag=False,
            review_reason="",
            counterparty=counterparty,
        )
        return

    if direction == "in" and amount > ZERO:
        _apply_classification(
            record,
            event_type="transfer_in",
            confidence="HIGH",
            review_flag=False,
            review_reason="",
            counterparty=counterparty,
        )
        return

    if direction == "out" and amount > ZERO:
        _apply_classification(
            record,
            event_type="transfer_out",
            confidence="HIGH",
            review_flag=False,
            review_reason="",
            counterparty=counterparty,
        )
        return

    if fee_amount > ZERO and direction == "out":
        _apply_classification(
            record,
            event_type="fee",
            confidence="HIGH",
            review_flag=False,
            review_reason="",
            counterparty=counterparty,
        )
        return

    review_reason = _build_uncertain_review_reason(
        source_type=source_type,
        amount=amount,
        fee_amount=fee_amount,
        direction=direction,
    )

    _apply_classification(
        record,
        event_type="unknown",
        confidence="UNCLASSIFIED",
        review_flag=True,
        review_reason=review_reason,
        counterparty=counterparty,
    )


def _looks_like_simple_swap(group: list[dict[str, Any]]) -> bool:
    incoming_assets: set[tuple[str, str]] = set()
    outgoing_assets: set[tuple[str, str]] = set()

    for record in group:
        amount = _to_decimal(record.get("amount"))
        if amount <= ZERO:
            continue

        asset_symbol = str(record.get("asset_symbol", "") or "")
        token_contract = str(record.get("token_contract", "") or "")
        asset_key = (asset_symbol, token_contract)
        direction = str(record.get("direction", "") or "")

        if direction == "in":
            incoming_assets.add(asset_key)
        elif direction == "out":
            outgoing_assets.add(asset_key)

    return bool(incoming_assets and outgoing_assets)



def _build_uncertain_review_reason(*, source_type: str, amount: Decimal, fee_amount: Decimal, direction: str) -> str:
    if source_type == "normal_transaction" and amount <= ZERO and fee_amount <= ZERO:
        return "non_value_normal_transaction"
    if source_type == "erc20_transfer" and amount <= ZERO:
        return "zero_amount_token_transfer"
    if source_type == "normal_transaction" and amount <= ZERO and fee_amount > ZERO and direction != "out":
        return "fee_direction_uncertain"
    return "classification_uncertain"

def _apply_classification(
    record: dict[str, Any],
    *,
    event_type: str,
    confidence: str,
    review_flag: bool,
    review_reason: str,
    counterparty: str,
) -> None:
    method_label = str(record.get("function_name", "") or record.get("method_id", "") or "")
    raw_data = {
        "tx_hash": record.get("tx_hash"),
        "source_type": record.get("source_type"),
        "asset_symbol": record.get("asset_symbol"),
        "amount": record.get("amount"),
        "fee_amount": record.get("fee_amount"),
        "direction": record.get("direction"),
    }

    record["row_id"] = str(record.get("transaction_key", "") or record.get("tx_hash", "") or "")
    record["timestamp_utc"] = record.get("datetime_utc")
    record["event_type"] = event_type
    record["contract_address"] = record.get("token_contract")
    record["fee_asset"] = record.get("fee_symbol")
    record["price_usd"] = None
    record["usd_value"] = None
    record["counterparty"] = counterparty
    record["method_label"] = method_label
    record["classification_confidence"] = confidence
    record["review_flag"] = review_flag
    record["review_reason"] = review_reason
    record["source_provider"] = "etherscan"
    record["raw_data"] = raw_data


def _pick_counterparty(record: dict[str, Any]) -> str:
    direction = str(record.get("direction", "") or "")
    if direction == "in":
        return str(record.get("from_address", "") or "")
    if direction == "out":
        return str(record.get("to_address", "") or "")
    return ""


def _build_summary(wallet_address: str, chain: str, records: list[dict[str, Any]]) -> dict[str, Any]:
    event_type_counts = Counter(str(record.get("event_type", "unknown") or "unknown") for record in records)
    confidence_counts = Counter(
        str(record.get("classification_confidence", "UNCLASSIFIED") or "UNCLASSIFIED")
        for record in records
    )
    review_flag_counts = Counter("true" if record.get("review_flag") else "false" for record in records)
    review_reason_counts = Counter(
        str(record.get("review_reason", "") or "") for record in records if record.get("review_reason")
    )

    return {
        "wallet_address": wallet_address,
        "chain": chain,
        "record_count": len(records),
        "event_type_counts": dict(event_type_counts),
        "classification_confidence_counts": dict(confidence_counts),
        "review_flag_counts": dict(review_flag_counts),
        "review_reason_counts": dict(review_reason_counts),
    }


def _to_decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value or "0"))
    except (InvalidOperation, ValueError):
        return ZERO
