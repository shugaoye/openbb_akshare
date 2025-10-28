"""AKShare ETF Search Model."""

# pylint: disable=unused-argument

from typing import Any, Dict, List, Optional
from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.utils.errors import EmptyDataError
from pydantic import BaseModel, Field


class AKShareEtfSearchQueryParams(BaseModel):
    """AKShare ETF Search Query."""

    query: Optional[str] = Field(
        default="",
        description="Search query string to filter ETFs by name or symbol.",
    )
    use_cache: bool = Field(
        default=False,
        description="Whether to use a cached request. Not used in this implementation.",
    )
    limit: Optional[int] = Field(
        default=100, description="Limit the number of results to return."
    )


class AKShareEtfSearchData(BaseModel):
    """AKShare ETF Search Data."""

    symbol: str = Field(description="ETF symbol/code.")
    name: str = Field(description="ETF name.")


class AKShareEtfSearchFetcher(
    Fetcher[
        AKShareEtfSearchQueryParams,
        List[AKShareEtfSearchData],
    ]
):
    """Transform the query, extract and transform the data from the AKShare endpoints."""

    @staticmethod
    def transform_query(params: Dict[str, Any]) -> AKShareEtfSearchQueryParams:
        """Transform the query."""
        return AKShareEtfSearchQueryParams(**params)

    @staticmethod
    async def aextract_data(
        query: AKShareEtfSearchQueryParams,
        credentials: Optional[Dict[str, str]],
        **kwargs: Any,
    ) -> List[Dict]:
        """Return the raw data from the AKShare endpoint."""
        # pylint: disable=import-outside-toplevel
        import akshare as ak

        def get_etf_list():
            """Fetch ETF list from akshare."""
            try:
                # akshare function to get ETF list
                df = ak.fund_etf_list_em()
                return df
            except Exception as e:
                # Try alternative function if available
                try:
                    df = ak.fund_etf_spot_em()
                    return df
                except Exception:
                    raise EmptyDataError(
                        f"Failed to fetch ETF list from akshare: {str(e)}"
                    )

        # Always fetch fresh data (no cache)
        df = get_etf_list()
        
        # Validate DataFrame structure
        if df.empty:
            raise EmptyDataError("ETF list from akshare is empty.")
        
        # Check for required columns and determine column names
        # fund_etf_list_em() returns "代码" and "名称"
        # fund_etf_spot_em() might return different column names
        if "代码" in df.columns and "名称" in df.columns:
            code_col = "代码"
            name_col = "名称"
        elif "基金代码" in df.columns and "基金简称" in df.columns:
            code_col = "基金代码"
            name_col = "基金简称"
        else:
            raise EmptyDataError(
                f"ETF list from akshare has unexpected columns. "
                f"Expected '代码'/'名称' or '基金代码'/'基金简称', "
                f"but got: {list(df.columns)}"
            )

        # Filter by query if provided
        if query.query:
            query_lower = query.query.lower()
            try:
                mask = (
                    df[code_col].astype(str).str.lower().str.contains(query_lower, na=False)
                    | df[name_col].astype(str).str.lower().str.contains(query_lower, na=False)
                )
                df = df[mask]
            except Exception as e:
                raise EmptyDataError(f"Error filtering ETFs: {str(e)}")

        # Limit results
        if query.limit is not None and query.limit > 0:
            df = df.head(query.limit)

        if df.empty:
            raise EmptyDataError("No ETFs found matching the search criteria.")

        # Rename columns to standard format
        try:
            df = df.rename(columns={code_col: "symbol", name_col: "name"})
        except Exception as e:
            raise EmptyDataError(f"Error renaming columns: {str(e)}")

        # Select only needed columns
        try:
            result_df = df[["symbol", "name"]].copy()
            return result_df.to_dict(orient="records")
        except KeyError as e:
            raise EmptyDataError(f"Error selecting columns: {str(e)}")

    @staticmethod
    def transform_data(
        query: AKShareEtfSearchQueryParams, data: List[Dict], **kwargs: Any
    ) -> List[AKShareEtfSearchData]:
        """Return the transformed data."""
        return [AKShareEtfSearchData.model_validate(d) for d in data]

