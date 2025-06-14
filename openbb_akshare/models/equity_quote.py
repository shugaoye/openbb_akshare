"""AKShare Equity Quote Model."""

# pylint: disable=unused-argument

from typing import Any, Dict, List, Optional
from warnings import warn

from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.standard_models.equity_quote import (
    EquityQuoteData,
    EquityQuoteQueryParams,
)
from pydantic import Field


class AKShareEquityQuoteQueryParams(EquityQuoteQueryParams):
    """AKShare Equity Quote Query."""

    __json_schema_extra__ = {"symbol": {"multiple_items_allowed": True}}


class AKShareEquityQuoteData(EquityQuoteData):
    """AKShare Equity Quote Data."""

    __alias_dict__ = {
        "name": "longName",
        "asset_type": "quoteType",
        "last_price": "currentPrice",
        "high": "dayHigh",
        "low": "dayLow",
        "prev_close": "previousClose",
        "year_high": "fiftyTwoWeekHigh",
        "year_low": "fiftyTwoWeekLow",
        "ma_50d": "fiftyDayAverage",
        "ma_200d": "twoHundredDayAverage",
        "volume_average": "averageVolume",
        "volume_average_10d": "averageDailyVolume10Day",
    }

    ma_50d: Optional[float] = Field(
        default=None,
        description="50-day moving average price.",
    )
    ma_200d: Optional[float] = Field(
        default=None,
        description="200-day moving average price.",
    )
    volume_average: Optional[float] = Field(
        default=None,
        description="Average daily trading volume.",
    )
    volume_average_10d: Optional[float] = Field(
        default=None,
        description="Average daily trading volume in the last 10 days.",
    )
    currency: Optional[str] = Field(
        default=None,
        description="Currency of the price.",
    )


class AKShareEquityQuoteFetcher(
    Fetcher[AKShareEquityQuoteQueryParams, List[AKShareEquityQuoteData]]
):
    """AKShare Equity Quote Fetcher."""

    @staticmethod
    def transform_query(params: Dict[str, Any]) -> AKShareEquityQuoteQueryParams:
        """Transform the query."""
        return AKShareEquityQuoteQueryParams(**params)

    @staticmethod
    async def aextract_data(
        query: AKShareEquityQuoteQueryParams,
        credentials: Optional[Dict[str, str]],
        **kwargs: Any,
    ) -> List[Dict]:
        """Extract the raw data from AKShare."""
        # pylint: disable=import-outside-toplevel
        import asyncio  # noqa
        from curl_adapter import CurlCffiAdapter
        from openbb_core.provider.utils.helpers import get_requests_session
        import akshare as ak

        symbols = query.symbol.split(",")
        results = []
        fields = ['代码', '52周最高', '流通股', '跌停', '最高', '流通值', '最小交易单位', '涨跌', '每股收益', '昨收', '成交量', '周转率', '52周最低', '名称', '交易所', '市盈率(动)', '基金份额/总股本', '净资产中的商誉', '均价', '涨幅', '振幅', '现价', '今年以来涨幅', '发行日期', '最低', '资产净值/总市值', '股息(TTM)', '股息率(TTM)', '货币', '每股净资产', '市盈率(静)', '成交额', '市净率', '涨停', '市盈率(TTM)', '时间', '今开']

        async def get_one(symbol):
            """Get the data for one ticker symbol."""
            result: dict = {}
            ticker: dict = {}
            try:
                ticker_df = ak.stock_individual_spot_xq(symbol)
            except Exception as e:
                warn(f"Error getting data for {symbol}: {e}")
            if ticker_df.empty is False:
                for field in fields:
                    if field in ticker:
                        result[field] = ticker.get(field, None)
                if result:
                    results.append(result)

        tasks = [get_one(symbol) for symbol in symbols]

        await asyncio.gather(*tasks)

        return results

    @staticmethod
    def transform_data(
        query: AKShareEquityQuoteQueryParams,
        data: List[Dict],
        **kwargs: Any,
    ) -> List[AKShareEquityQuoteData]:
        """Transform the data."""
        return [AKShareEquityQuoteData.model_validate(d) for d in data]
