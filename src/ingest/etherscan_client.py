from __future__ import annotations

from typing import Any, Dict

import requests


class EtherscanClientError(Exception):
    """Raised when the Etherscan client fails."""


class EtherscanClient:
    BASE_URL = "https://api.etherscan.io/v2/api"
    ETHEREUM_MAINNET_CHAIN_ID = "1"

    def __init__(self, api_key: str, timeout_seconds: int = 30) -> None:
        self.api_key = api_key
        self.timeout_seconds = timeout_seconds

        if not self.api_key:
            raise EtherscanClientError(
                "Missing ETHERSCAN_API_KEY. Add it to your .env file before running."
            )

    def _get(self, params: Dict[str, Any]) -> Dict[str, Any]:
        merged_params = {
            "chainid": self.ETHEREUM_MAINNET_CHAIN_ID,
            **params,
            "apikey": self.api_key,
        }

        try:
            response = requests.get(
                self.BASE_URL,
                params=merged_params,
                timeout=self.timeout_seconds,
            )
            response.raise_for_status()
            payload = response.json()
        except requests.RequestException as exc:
            raise EtherscanClientError(f"HTTP request failed: {exc}") from exc
        except ValueError as exc:
            raise EtherscanClientError(
                "Received a non-JSON response from Etherscan."
            ) from exc

        status = str(payload.get("status", ""))
        message = str(payload.get("message", ""))
        result = payload.get("result")

        # Etherscan returns status=0 for empty histories; that is not a hard failure.
        if status == "0" and "No transactions found" not in message:
            raise EtherscanClientError(
                f"Etherscan API error. Message: {message}. Result: {result}"
            )

        return payload

    def fetch_normal_transactions(self, wallet_address: str) -> Dict[str, Any]:
        return self._get(
            {
                "module": "account",
                "action": "txlist",
                "address": wallet_address,
                "startblock": 0,
                "endblock": 99999999,
                "sort": "asc",
            }
        )

    def fetch_erc20_token_transfers(self, wallet_address: str) -> Dict[str, Any]:
        return self._get(
            {
                "module": "account",
                "action": "tokentx",
                "address": wallet_address,
                "startblock": 0,
                "endblock": 99999999,
                "sort": "asc",
            }
        )
