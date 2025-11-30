from typing import List

import pandas as pd
from tqdm import tqdm

from src.logger import get_logger
from src.extract import fetch_all_from_ticker, get_sp500_tickers
from src.transform import build_company_records
from src.models import CompanyFinancialRecord

from dotenv import load_dotenv
import os

logger = get_logger(__name__)

load_dotenv()

MAX_COMPANIES = int(os.getenv("MAX_COMPANIES"))
LATEST_N_YEARS = int(os.getenv("LATEST_N_YEARS"))


def main(max_companies: int = MAX_COMPANIES, latest_n_years: int = LATEST_N_YEARS) -> pd.DataFrame:
    """
    Run the ETL pipeline and return a pandas DataFrame.
    """
    logger.info("Fetching S&P 500 tickers (max %d)", max_companies)

    # 1. get the tickers
    tickers = get_sp500_tickers(max_companies)
    logger.info("Loaded %d tickers", len(tickers))

    all_records: List[CompanyFinancialRecord] = []

    for ticker in (pbar := tqdm(tickers, desc="Processing")):
        pbar.set_postfix_str(ticker)
        try:
            # 2. extract
            meta, income_df, balance_df = fetch_all_from_ticker(ticker)

            # 3. transform
            records = build_company_records(
                meta=meta,
                income_df=income_df,
                balance_df=balance_df,
                latest_n_years=latest_n_years,
            )
            all_records.extend(records)
        except Exception as exc:
            logger.exception("Failed to process ticker %s: %s", ticker, exc)

    # Filter out rows without revenue
    all_records = [r for r in all_records if r.revenue is not None]

    # 4. Load (into df and file)
    df = pd.DataFrame([r.model_dump() for r in all_records])
    logger.info("Pipeline finished. DataFrame shape: %s", df.shape)
    df.to_csv("company_financials.csv", encoding='utf-8')
    return df


if __name__ == "__main__":
    df = main()
