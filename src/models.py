from __future__ import annotations

from typing import Optional
from pydantic import BaseModel, Field


class CompanyFinancialRecord(BaseModel):
    ticker: str = Field(..., description="Exchange ticker symbol")
    company_name: Optional[str] = Field(None, description="Company name")
    country: Optional[str] = Field(None, description="Headquarters country")
    industry: Optional[str] = Field(None, description="Industry classification")

    year: int = Field(..., description="Fiscal year of the data")

    revenue: Optional[float] = Field(None, description="Total revenue")
    currency_unit: Optional[str] = Field(None, description="Currency or unit, e.g. USD")
    gross_profit: Optional[float] = Field(None, description="Gross profit")
    operating_income: Optional[float] = Field(None, description="Operating income")
    net_income: Optional[float] = Field(None, description="Net income")
    total_assets: Optional[float] = Field(None, description="Total assets")
    total_liabilities: Optional[float] = Field(None, description="Total liabilities")
