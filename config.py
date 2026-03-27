from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    etherscan_api_key: str
    default_chain: str
    raw_data_dir: Path
    processed_data_dir: Path
    output_data_dir: Path
    price_cache_path: Path
    request_timeout_seconds: int


def get_settings() -> Settings:
    raw_dir = Path(os.getenv("RAW_DATA_DIR", "data/raw"))
    processed_dir = Path(os.getenv("PROCESSED_DATA_DIR", "data/processed"))
    output_dir = Path(os.getenv("OUTPUT_DATA_DIR", "data/output"))
    price_cache_path = Path(os.getenv("PRICE_CACHE_PATH", "cache/price_cache.json"))

    return Settings(
        etherscan_api_key=os.getenv("ETHERSCAN_API_KEY", "").strip(),
        default_chain=os.getenv("DEFAULT_CHAIN", "ethereum").strip().lower(),
        raw_data_dir=raw_dir,
        processed_data_dir=processed_dir,
        output_data_dir=output_dir,
        price_cache_path=price_cache_path,
        request_timeout_seconds=int(os.getenv("REQUEST_TIMEOUT_SECONDS", "30")),
    )
