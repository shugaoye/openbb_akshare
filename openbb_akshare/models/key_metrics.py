"""AKShare Key Metrics Model."""

# pylint: disable=unused-argument

import asyncio
from datetime import (
    date as dateType,
    datetime,
)
from typing import Any, Dict, List, Literal, Optional
from warnings import warn

from openbb_core.provider.abstract.data import ForceInt
from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.standard_models.key_metrics import (
    KeyMetricsData,
    KeyMetricsQueryParams,
)
from openbb_core.provider.utils.descriptions import QUERY_DESCRIPTIONS
from openbb_core.provider.utils.errors import EmptyDataError
from pydantic import Field


class AKShareKeyMetricsQueryParams(KeyMetricsQueryParams):
    """AKShare Key Metrics Query.

    Source: https://emweb.securities.eastmoney.com/pc_hsf10/pages/index.html?type=web&code=SH600028
    """

    __json_schema_extra__ = {
        "symbol": {"multiple_items_allowed": True},
        "period": {
            "choices": ["annual", "quarter"],
        },
    }

    period: Literal["annual", "quarter"] = Field(
        default="annual",
        description=QUERY_DESCRIPTIONS.get("period", ""),
    )

class AKShareKeyMetricsData(KeyMetricsData):
    """AKShare Key Metrics Data."""

    __alias_dict__ = {
        "market_cap": "流通值",
        "pe_ratio": "市盈率(动)",
    }


class AKShareKeyMetricsFetcher(
    Fetcher[
        AKShareKeyMetricsQueryParams,
        List[AKShareKeyMetricsData],
    ]
):
    """AKShare Key Metrics Fetcher."""

    @staticmethod
    def transform_query(params: Dict[str, Any]) -> AKShareKeyMetricsQueryParams:
        """Transform the query params."""
        return AKShareKeyMetricsQueryParams(**params)

    @staticmethod
    async def aextract_data(
        query: AKShareKeyMetricsQueryParams,
        credentials: Optional[Dict[str, str]],
        **kwargs: Any,
    ) -> List[Dict]:
        """Return the raw data from the AKShare endpoint."""
        api_key = credentials.get("akshare_api_key") if credentials else ""

        base_url = "https://financialmodelingprep.com/api/v3"
        symbols = query.symbol.split(",")
        results: List = []

        async def get_one(symbol, api_key):
            """Get data for one symbol."""
            import akshare as ak
            from mysharelib.em.get_a_info_em import get_a_info_em
            from mysharelib.tools import normalize_symbol

            symbol_b, symbol_f, market = normalize_symbol(symbol)
            df_base, df_comparison = get_a_info_em(symbol_f)

            ak.stock.cons.xq_a_token=api_key
            stock_individual_spot_xq_df = ak.stock_individual_spot_xq(symbol=f"SH{symbol_b}")
            pe_dynamic = stock_individual_spot_xq_df.loc[stock_individual_spot_xq_df['item'] == '市盈率(动)', 'value'].iloc[0]
            market_cap = stock_individual_spot_xq_df.loc[stock_individual_spot_xq_df['item'] == '流通值', 'value'].iloc[0]
            df_base.loc['市盈率(动)'] = pe_dynamic
            df_base.loc['流通值'] = market_cap

            results.append(df_base['数值'].to_dict())

        await asyncio.gather(*[get_one(symbol, api_key) for symbol in symbols])

        if not results:
            raise EmptyDataError(f"No data found for given symbols -> {query.symbol}.")

        return results

    @staticmethod
    def transform_data(
        query: AKShareKeyMetricsQueryParams, data: List[Dict], **kwargs: Any
    ) -> List[AKShareKeyMetricsData]:
        """Return the transformed data."""
        return [AKShareKeyMetricsData.model_validate(d) for d in data]
