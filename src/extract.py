from dataclasses import dataclass
from io import StringIO
from typing import Optional, Tuple

import pandas as pd
import requests
import yfinance as yf

from .logger import get_logger

logger = get_logger(__name__)


@dataclass
class CompanyMeta:
    """Basic company metadata extracted from Yahoo Finance."""

    ticker: str
    name: Optional[str]
    country: Optional[str]
    industry: Optional[str]
    currency: Optional[str]


def get_sp500_tickers(max_companies: int = 300) -> list[str]:
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    tables = pd.read_html(StringIO(response.text))
    df = tables[1]

    tickers = df["Symbol"].dropna().astype(str).str.strip().tolist()
    return tickers[:max_companies]


def fetch_all_from_ticker(
    ticker: str,
) -> Tuple[CompanyMeta, Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    """
    Fetch metadata and annual financial statements for a single ticker.

    This function creates a single yfinance.Ticker instance and reuses it
    for all data pulls to avoid unnecessary repeated instantiation.
    """
    # Single Ticker object to avoid overhead of multiple instantiations
    t = yf.Ticker(ticker)

    # info can occasionally be missing or incomplete; use defensive access
    try:
        info = t.info or {}
    except Exception as exc:  # yfinance sometimes throws on .info
        logger.warning("Failed to fetch metadata for %s: %s", ticker, exc)
        info = {}

    meta = CompanyMeta(
        ticker=ticker,
        name=info.get("longName") or info.get("shortName"),
        country=info.get("country"),
        industry=info.get("industry"),
        currency=info.get("financialCurrency") or info.get("currency"),
    )

    # Annual income statement and balance sheet
    income_df: Optional[pd.DataFrame]
    balance_df: Optional[pd.DataFrame]

    try:
        income_df = t.financials
    except Exception as exc:
        logger.warning("Failed to fetch income statement for %s: %s", ticker, exc)
        income_df = None

    try:
        balance_df = t.balance_sheet
    except Exception as exc:
        logger.warning("Failed to fetch balance sheet for %s: %s", ticker, exc)
        balance_df = None

    # Normalize empty DataFrames to None for easier downstream handling
    if isinstance(income_df, pd.DataFrame) and income_df.empty:
        logger.info("Empty income statement for %s", ticker)
        income_df = None

    if isinstance(balance_df, pd.DataFrame) and balance_df.empty:
        logger.info("Empty balance sheet for %s", ticker)
        balance_df = None

    return meta, income_df, balance_df
