from typing import List, Optional

import pandas as pd

from .logger import get_logger
from .models import CompanyFinancialRecord, CompanyMeta

logger = get_logger(__name__)


def _extract_year(col) -> Optional[int]:
    """Extract year from a column (Timestamp or string)."""
    try:
        if hasattr(col, 'year'):
            return col.year
        return int(str(col)[:4])
    except (ValueError, AttributeError):
        return None


def _get_value(df: Optional[pd.DataFrame], row_name: str, col) -> Optional[float]:
    """Safely pull a numeric value from a statement dataframe."""
    if df is None or df.empty or col is None:
        return None

    if col not in df.columns or row_name not in df.index:
        return None

    value = df.at[row_name, col]
    if pd.isna(value):
        return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def build_company_records(
    meta: CompanyMeta,
    income_df: Optional[pd.DataFrame],
    balance_df: Optional[pd.DataFrame],
    latest_n_years: int = 3,
    ) -> List[CompanyFinancialRecord]:
    """
    Transform raw Yahoo Finance statements into a list of CompanyFinancialRecord.
    """
    if income_df is None or income_df.empty:
        logger.info("Skipping %s due to missing income statement.", meta.ticker)
        return []

    col_year_pairs = []
    for col in income_df.columns:
        year = _extract_year(col)
        if year is not None:
            col_year_pairs.append((col, year))

    if not col_year_pairs:
        logger.info("No valid year columns found for %s.", meta.ticker)
        return []

    col_year_pairs.sort(key=lambda x: x[1], reverse=True)
    col_year_pairs = col_year_pairs[:latest_n_years]

    records: List[CompanyFinancialRecord] = []

    for col, year in col_year_pairs:
        revenue = _get_value(income_df, "Total Revenue", col)
        gross_profit = _get_value(income_df, "Gross Profit", col)
        operating_income = _get_value(income_df, "Operating Income", col)
        net_income = _get_value(income_df, "Net Income", col)

        total_assets = _get_value(balance_df, "Total Assets", col)
        total_liabilities = _get_value(balance_df, "Total Liabilities Net Minority Interest", col)

        record = CompanyFinancialRecord(
            ticker=meta.ticker,
            company_name=meta.name,
            country=meta.country,
            industry=meta.industry,
            year=year,
            revenue=revenue,
            currency_unit=meta.currency,
            gross_profit=gross_profit,
            operating_income=operating_income,
            net_income=net_income,
            total_assets=total_assets,
            total_liabilities=total_liabilities,
        )
        records.append(record)

    return records
