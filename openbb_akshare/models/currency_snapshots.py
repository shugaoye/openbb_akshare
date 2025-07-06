"""AKShare Currency Snapshots Model."""

# pylint: disable=unused-argument

from datetime import datetime
from typing import Any, Dict, List, Optional

from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.standard_models.currency_snapshots import (
    CurrencySnapshotsData,
    CurrencySnapshotsQueryParams,
)
from openbb_core.provider.utils.errors import EmptyDataError
from pydantic import Field, field_validator


class AKShareCurrencySnapshotsQueryParams(CurrencySnapshotsQueryParams):
    """AKShare Currency Snapshots Query.

    Source: https://site.financialmodelingprep.com/developer/docs#exchange-prices-quote
    """

    __json_schema_extra__ = {"base": {"multiple_items_allowed": True}}


class AKShareCurrencySnapshotsData(CurrencySnapshotsData):
    """AKShare Currency Snapshots Data."""

    __alias_dict__ = {
        "last_rate": "最新价",
        "open": "今开",
        "high": "最高",
        "low": "最低",
        "symbol": "代码",
        "name": "名称",
        "prev_close": "昨收",
        "change": "涨跌额",
        "change_percent": "涨跌幅",
    }

    symbol: str = Field(
        description="Can use CURR1-CURR2 or CURR1CURR2 format."
    )
    name: str = Field(
        description="Name of currency pair."
    )
    change: Optional[float] = Field(
        description="The change in the price from the previous close.", default=None
    )
    change_percent: Optional[float] = Field(
        description="The change in the price from the previous close, as a normalized percent.",
        default=None,
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )

    @field_validator("change_percent", mode="before", check_fields=False)
    @classmethod
    def normalize_percent(cls, v):
        """Normalize the percent."""
        return v / 100 if v is not None else None


class AKShareCurrencySnapshotsFetcher(
    Fetcher[AKShareCurrencySnapshotsQueryParams, List[AKShareCurrencySnapshotsData]]
):
    """AKShare Currency Snapshots Fetcher."""

    @staticmethod
    def transform_query(params: Dict[str, Any]) -> AKShareCurrencySnapshotsQueryParams:
        """Transform the query parameters."""
        return AKShareCurrencySnapshotsQueryParams(**params)

    @staticmethod
    async def extract_data(
        query: AKShareCurrencySnapshotsQueryParams,
        credentials: Optional[Dict[str, str]],
        **kwargs: Any,
    ) -> List[Dict]:
        """Extract the data from the AKShare endpoint."""
        # pylint: disable=import-outside-toplevel
        import akshare as ak

        forex_spot_em_df = ak.forex_spot_em()
        return forex_spot_em_df.to_dict(orient="records")

    @staticmethod
    def transform_data(
        query: AKShareCurrencySnapshotsQueryParams,
        data: List[Dict],
        **kwargs: Any,
    ) -> List[AKShareCurrencySnapshotsData]:
        """Filter by the query parameters and validate the model."""
        # pylint: disable=import-outside-toplevel
        from datetime import timezone  # noqa
        from pandas import DataFrame, concat  # noqa
        from openbb_core.provider.utils.helpers import safe_fromtimestamp  # noqa

        if not data:
            raise EmptyDataError("No data was returned from the AKShare endpoint.")

        # Drop all the zombie columns AKShare returns.
        df = (
            DataFrame(data)
            .dropna(how="all", axis=1)
            .drop(columns=["exchange", "avgVolume"])
        )

        new_df = DataFrame()

        # Filter for the base currencies requested and the quote_type.
        for symbol in query.base.split(","):
            temp = (
                df.query("`symbol`.str.startswith(@symbol)")
                if query.quote_type == "indirect"
                else df.query("`symbol`.str.endswith(@symbol)")
            ).rename(columns={"symbol": "base_currency", "name": "counter_currency"})
            temp["base_currency"] = symbol
            temp["counter_currency"] = (
                [d.split("/")[1] for d in temp["counter_currency"]]
                if query.quote_type == "indirect"
                else [d.split("/")[0] for d in temp["counter_currency"]]
            )
            # Filter for the counter currencies, if requested.
            if query.counter_currencies is not None:
                counter_currencies = (  # noqa: F841  # pylint: disable=unused-variable
                    query.counter_currencies
                    if isinstance(query.counter_currencies, list)
                    else query.counter_currencies.split(",")
                )
                temp = (
                    temp.query("`counter_currency`.isin(@counter_currencies)")
                    .set_index("counter_currency")
                    # Sets the counter currencies in the order they were requested.
                    .filter(items=counter_currencies, axis=0)
                    .reset_index()
                ).rename(columns={"index": "counter_currency"})
            # If there are no records, don't concatenate.
            if len(temp) > 0:
                # Convert the Unix timestamp to a datetime.
                temp.timestamp = temp.timestamp.apply(
                    lambda x: safe_fromtimestamp(x, tz=timezone.utc)
                )
                new_df = concat([new_df, temp])
            if len(new_df) == 0:
                raise EmptyDataError(
                    "No data was found using the applied filters. Check the parameters."
                )
            # Fill and replace any NaN values with NoneType.
            new_df = new_df.fillna("N/A").replace("N/A", None)
        return [
            AKShareCurrencySnapshotsData.model_validate(d)
            for d in new_df.reset_index(drop=True).to_dict(orient="records")
        ]
