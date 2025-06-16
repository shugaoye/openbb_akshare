"""AKShare Historical Dividends Model."""

# pylint: disable=unused-argument
from datetime import (
    date as dateType,
    datetime,
)
from typing import Any, Dict, List, Optional
from pydantic import Field, field_validator

from openbb_core.app.model.abstract.error import OpenBBError
from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.standard_models.historical_dividends import (
    HistoricalDividendsData,
    HistoricalDividendsQueryParams,
)

import logging
logger = logging.getLogger(__name__)

class AKShareHistoricalDividendsQueryParams(HistoricalDividendsQueryParams):
    """AKShare Historical Dividends Query."""


class AKShareHistoricalDividendsData(HistoricalDividendsData):
    """AKShare Historical Dividends Data. All data is split-adjusted."""
    reported_date: Optional[dateType] = Field(
        default=None,
        description="Earnings Announcement Date.",
    )
    description: Optional[str] = Field(
        default=None,
        description="Record date of the historical dividends.",
    )
    record_date: Optional[dateType] = Field(
        default=None,
        description="Record date of the historical dividends.",
    )
    declaration_date: Optional[dateType] = Field(
        default=None,
        description="Declaration date of the historical dividends.",
    )

    @field_validator(
        "declaration_date",
        "record_date",
        "reported_date",
        "ex_dividend_date",
        mode="before",
        check_fields=False,
    )
    @classmethod
    def date_validate(cls, v: str):  # pylint: disable=E0213
        """Validate dates."""
        if not isinstance(v, str):
            return v
        return dateType.fromisoformat(v) if v else None
    
class AKShareHistoricalDividendsFetcher(
    Fetcher[
        AKShareHistoricalDividendsQueryParams, List[AKShareHistoricalDividendsData]
    ]
):
    """AKShare Historical Dividends Fetcher."""

    @staticmethod
    def transform_query(
        params: Dict[str, Any],
    ) -> AKShareHistoricalDividendsQueryParams:
        """Transform the query."""
        return AKShareHistoricalDividendsQueryParams(**params)

    @staticmethod
    def extract_data(
        query: AKShareHistoricalDividendsQueryParams,
        credentials: Optional[Dict[str, str]],
        **kwargs: Any,
    ) -> List[Dict]:
        """Extract the raw data from AKShare."""
        # pylint: disable=import-outside-toplevel
        from openbb_akshare.utils.helpers import get_post_tax_dividend_per_share
        import akshare as ak

        div_df = ak.stock_fhps_detail_em(symbol=query.symbol)
        #ticker = div_df[['报告期', '业绩披露日期', '现金分红-现金分红比例描述', '现金分红-股息率', '预案公告日', '股权登记日', '除权除息日']]
        ticker = div_df[['现金分红-现金分红比例描述',
                         '除权除息日']]
        ticker['amount'] = div_df['现金分红-现金分红比例描述'].apply(
            lambda x: get_post_tax_dividend_per_share(x) if isinstance(x, str) else None
        )
        #ticker.rename(columns={"报告期": "period_ending", "业绩披露日期": "reported_date", "现金分红-现金分红比例描述": "description", "现金分红-股息率": "div_rate", "预案公告日": "declaration_date", "股权登记日": "record_date", "除权除息日": "ex_dividend_date"}, inplace=True)
        ticker.rename(columns={"现金分红-现金分红比例描述": "description", 
                               "除权除息日": "ex_dividend_date"}, inplace=True)
        dividends = ticker.to_dict("records")  # type: ignore

        logger.info(
            "Fetched historical dividends:\n%s",
            dividends
        )
        return dividends

    @staticmethod
    def transform_data(
        query: AKShareHistoricalDividendsQueryParams,
        data: List[Dict],
        **kwargs: Any,
    ) -> List[AKShareHistoricalDividendsData]:
        """Transform the data."""
        #return [AKShareHistoricalDividendsData.model_validate(d) for d in data]
        result: List[AKShareHistoricalDividendsData] = []
        for d in data:
            result.append(AKShareHistoricalDividendsData(**d))
        logger.info(
            "Transformed historical dividends completed.\n"
        )        
        return result
