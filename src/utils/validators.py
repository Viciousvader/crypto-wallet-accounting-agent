from __future__ import annotations

import string


HEX_CHARS = set(string.hexdigits)


def validate_ethereum_wallet(wallet_address: str) -> str:
    """
    Validate a public Ethereum wallet address using basic MVP rules.

    Rules:
    - must be a string
    - must start with 0x
    - must be 42 characters long
    - remaining characters must be valid hex

    Returns the normalized lowercase wallet address.
    Raises ValueError if invalid.
    """
    if not isinstance(wallet_address, str):
        raise ValueError("Wallet address must be a string.")

    cleaned = wallet_address.strip()

    if not cleaned:
        raise ValueError("Wallet address cannot be empty.")

    if not cleaned.startswith("0x"):
        raise ValueError("Ethereum wallet address must start with 0x.")

    if len(cleaned) != 42:
        raise ValueError("Ethereum wallet address must be 42 characters long.")

    hex_part = cleaned[2:]
    if any(char not in HEX_CHARS for char in hex_part):
        raise ValueError("Ethereum wallet address contains non-hex characters.")

    return cleaned.lower()
