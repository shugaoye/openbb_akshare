"""AKShare helpers module."""

# pylint: disable=unused-argument,too-many-arguments,too-many-branches,too-many-locals,too-many-statements
from typing import Dict, Any, List, Optional, Union
import pandas as pd
from openbb_core.provider.utils.errors import EmptyDataError
from pandas import DataFrame
from datetime import (
    date as dateType,
    datetime,
    timedelta
)
import akshare as ak
from mysharelib.table_cache import TableCache
import logging
from mysharelib.tools import setup_logger, normalize_symbol
from openbb_akshare import project_name

setup_logger(project_name)
logger = logging.getLogger(__name__)

EQUITY_HISTORY_SCHEMA = {
    "date": "TEXT PRIMARY KEY",
    "open": "REAL",
    "high": "REAL",
    "low": "REAL",
    "close": "REAL",
    "volume": "REAL",
    "vwap": "REAL",
    "change": "REAL",
    "change_percent": "REAL",
    "amount": "REAL"
}

def get_list_date(symbol: str, api_key : Optional[str] = "") -> dateType:
    """
    Retrieves the listing date for a given stock symbol.

    Args:
        symbol (str): The stock symbol to fetch the listing date for.

    Returns:
        Optional[str]: The listing date in 'YYYY-MM-DD' format, or None if not found.
    """
    from openbb_akshare.utils.fetch_equity_info import fetch_equity_info

    equity_info = fetch_equity_info(symbol, api_key=api_key)
    listed_date = equity_info.get("listed_date")
   
    if listed_date is not None:
        logger.info(f"Listing date for {symbol} is {listed_date}.")
        return pd.to_datetime(listed_date, unit='ms').iloc[0].date()
    
    return (datetime.now() - timedelta(days=365)).date()

def check_cache(symbol: str, 
        cache: TableCache,
        api_key : Optional[str] = "",
        period: Optional[str] = "daily"
        ) -> bool:
    """
    Check if the cache contains the latest data for the given symbol.
    """
    from mysharelib.tools import last_closing_day
    from mysharelib.em.orginfo import get_listing_date
    
    start = get_listing_date(symbol)
    end = last_closing_day()
    # Please note that the format of the date string must be "YYYY-MM-DD" in database.
    cache_df = cache.fetch_date_range(start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
    cache_df = cache_df.set_index('date')
    cache_df.index = pd.to_datetime(cache_df.index)
    is_cache_valid = cache_df.index.max().date() == last_closing_day()
    if not is_cache_valid:
        logger.warning(f"Cache for {symbol} is not up-to-date. Last date in cache: {cache_df.index.max().date()}, expected: {last_closing_day()}.")
        data_util_today_df = ak_download_without_cache(symbol, period=period, api_key="", start_date=start.strftime("%Y%m%d"), end_date=end.strftime("%Y%m%d"))
        cache.write_dataframe(data_util_today_df)
    return is_cache_valid

def ak_download_without_cache(
        symbol: str,
        start_date: str,
        end_date: str,
        period: Optional[str] = "daily",
        use_cache: Optional[bool] = True, 
        api_key : Optional[str] = "",
        adjust: Optional[str] = "",
    ) -> DataFrame:
    """
    Downloads historical equity data without using cache.
    Parameters:
    symbol: str
        Stock symbol to fetch data for.
    start_date (str): Start date for fetching data in 'YYYYMMDD' format.
    end_date (str): End date for fetching data in 'YYYYMMDD' format.
    period: str
        Data frequency, e.g., "daily", "weekly", "monthly".
    use_cache: bool
        Whether to use cache for fetching data.
    api_key: str
        API key for authentication, if required.
    adjust: str
        Adjustment type
    """

    symbol_b, symbol_f, market = normalize_symbol(symbol)
    if market == "HK":
        hist_df = ak.stock_hk_hist(symbol_b, period, start_date, end_date, adjust=adjust)
        hist_df.rename(columns={"日期": "date", "开盘": "open", "收盘": "close", "最高": "high", "最低": "low", "成交量": "volume", "成交额": "amount", "涨跌幅":"change_percent", "涨跌额": "change"}, inplace=True)
        hist_df = hist_df.drop(columns=["振幅"])
        hist_df = hist_df.drop(columns=["换手率"])
    else:
        hist_df = ak.stock_zh_a_hist(symbol_b, period, start_date, end_date, adjust=adjust)
    
        hist_df.rename(columns={"日期": "date", "开盘": "open", "收盘": "close", "最高": "high", "最低": "low", "成交量": "volume", "成交额": "amount", "涨跌幅":"change_percent", "涨跌额": "change"}, inplace=True)
        hist_df = hist_df.drop(columns=["股票代码"])
        hist_df = hist_df.drop(columns=["振幅"])
        hist_df = hist_df.drop(columns=["换手率"])

    return hist_df

def ak_download(
        symbol: str,
        start_date: Optional[dateType] = None,
        end_date: Optional[dateType] = None,
        period: Optional[str] = "daily",
        use_cache: Optional[bool] = True,
        api_key: Optional[str] = "",
        adjust: Optional[str] = "",
    ) -> DataFrame:

    from mysharelib.tools import get_valid_date

    if start_date is None:
        start_date = (datetime.now() - timedelta(days=365)).date()
    if end_date is None: end_date = datetime.now().date()
    start_dt = get_valid_date(start_date)
    end_dt = get_valid_date(end_date)

    # Retrieve data from cache first
    symbol_b, symbol_f, market = normalize_symbol(symbol)
    cache = TableCache(EQUITY_HISTORY_SCHEMA, project=project_name, table_name=f"{market}{symbol_b}", primary_key="date")
    
    if use_cache:
        check_cache(symbol=symbol_b, cache=cache, api_key=api_key, period=period)
        data_from_cache = cache.fetch_date_range(start_dt.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d"))
        if not data_from_cache.empty:
            logger.info(f"Getting equity {symbol} historical data from cache...")
            return data_from_cache

    # If not in cache, download data
    # Download data using AKShare
    data_util_today_df = ak_download_without_cache(symbol_b, period=period, api_key=api_key, start_date=start_dt.strftime("%Y%m%d"), end_date=end_dt.strftime("%Y%m%d"))
    cache.write_dataframe(data_util_today_df)
    
    return cache.fetch_date_range(start_dt.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d"))

def get_post_tax_dividend_per_share(dividend_str: str) -> float:
    """
    Parses Chinese dividend descriptions and returns post-tax dividend per share.
    
    Handles:
        - Non-dividend cases (return 0)
        - Per-share direct values (e.g., "每股0.38港元")
        - Base-share values (e.g., "10派1元")
        - Complex combinations (e.g., "每股派发现金股利0.088332港元,每10股派送股票股利3股")

    Parameters:
        dividend_str (str): Dividend description string

    Returns:
        float: Post-tax dividend amount per share, rounded to 4 decimal places
    """
    import re

    # Case 1: Non-dividend cases
    if re.search(r'不分红|不分配不转增|转增.*不分配', dividend_str):
        return 0.0
    
    # If A股 is present, extract only that part
    a_share_match = re.search(r'(A股[^,]*)', dividend_str)
    if a_share_match:
        dividend_str = a_share_match.group(1)
    # Remove 'A股' prefix if present
    dividend_str = dividend_str.replace('A股', '')

     # Extract base shares
    base_match = re.match(r'(\d+(?:\.\d+)?)', dividend_str)
    base = float(base_match.group(1)) if base_match else 0.0

    # Extract bonus shares (送)
    bonus_match = re.search(r'送(\d+(?:\.\d+)?)股', dividend_str)
    bonus = float(bonus_match.group(1)) if bonus_match else 0.0

    # Extract conversion shares (转)
    conversion_match = re.search(r'转(\d+(?:\.\d+)?)股', dividend_str)
    conversion = float(conversion_match.group(1)) if conversion_match else 0.0

    # Extract cash dividend (派)
    cash_match = re.search(r'派(\d+(?:\.\d+)?)元', dividend_str)
    cash = float(cash_match.group(1)) if cash_match else 0.0

    if base != 0 and cash != 0:
        return round(cash / base, 4)
       
    # Case 2: Direct per-share values (e.g., "每股0.38港元", "每股人民币0.25元")
    direct_match = re.search(r'每股[\u4e00-\u9fa5]*([\d\.]+)[^\d]*(?:港元|人民币|元)', dividend_str)
    if direct_match:
        return round(float(direct_match.group(1)), 4)
    
    # Case 3: Base-share values (e.g., "10派1元", "10转10股派1元")
    # Match "10转10股派1元" or similar
    match = re.match(r'(\d+)转(\d+)股派([\d\.]+)元', dividend_str)
    if match:
        base = int(match.group(1))
        bonus = int(match.group(2))
        cash = float(match.group(3))
        total_shares = base
        return round(cash / total_shares, 4)
    # Handle "10派1元" or "10.00派2.00元"
    match = re.match(r'(\d+(?:\.\d+)?)派([\d\.]+)元', dividend_str)
    if match:
        base = int(float(match.group(1)))
        cash = float(match.group(2))
        return round(cash / base, 4)

    base_match = re.search(r'(\d+)(?:[转股]+[\d\.]+)*(?:派|现金股利)([\d\.]+)', dividend_str)
    if base_match:
        base_shares = int(float(base_match.group(1)))  # Handle '10.00' cases
        dividend_amount = float(base_match.group(2))
        return round(dividend_amount / base_shares, 4)
    
    # Case 4: Complex mixed formats (e.g., "每股派发现金股利0.088332港元,每10股派送股票股利3股")
    complex_match = re.search(r'每股[\u4e00-\u9fa5]*([\d\.]+)[^\d]*(?:港元|人民币|元)', dividend_str)
    if complex_match:
        return round(float(complex_match.group(1)), 4)
    
    # Default: Return 0 for unrecognized formats
    return 0.0
def get_a_dividends(
    symbol: str,
    start_date: Optional[Union[str, "date"]] = None,
    end_date: Optional[Union[str, "date"]] = None,
) -> List[Dict]:
    """
    Fetches historical dividends for a Shanghai/Shenzhen/Beijing stock symbol.

    Parameters:
        symbol (str): Stock symbol to fetch dividends for.
        start_date (Optional[Union[str, date]]): Start date for fetching dividends.
        end_date (Optional[Union[str, date]]): End date for fetching dividends.

    Returns:
        DataFrame: DataFrame containing dividend information.
    """
    import akshare as ak

    if not symbol:
        raise EmptyDataError("Symbol cannot be empty.")

    div_df = ak.stock_fhps_detail_ths(symbol)
    div_df.dropna(inplace=True)
    ticker = div_df[['实施公告日',
                        '分红方案说明',
                        'A股股权登记日',
                        'A股除权除息日']]
    ticker['amount'] = div_df['分红方案说明'].apply(
        lambda x: get_post_tax_dividend_per_share(x) if isinstance(x, str) else None
    )
    ticker.rename(columns={'实施公告日': "report_date",
                            '分红方案说明': "description", 
                            'A股股权登记日': "record_date",
                            'A股除权除息日': "ex_dividend_date"}, inplace=True)
    dividends = ticker.to_dict("records")  # type: ignore
    
    if not dividends:
        raise EmptyDataError(f"No dividend data found for {symbol}.")

    return dividends

def get_hk_dividends(
    symbol: str,
    start_date: Optional[Union[str, "date"]] = None,
    end_date: Optional[Union[str, "date"]] = None,
) -> List[Dict]:
    """
    Fetches historical dividends for a Hong Kong stock symbol.

    Parameters:
        symbol (str): Stock symbol to fetch dividends for.
        start_date (Optional[Union[str, date]]): Start date for fetching dividends.
        end_date (Optional[Union[str, date]]): End date for fetching dividends.

    Returns:
        DataFrame: DataFrame containing dividend information.
    """
    import akshare as ak

    if not symbol:
        raise EmptyDataError("Symbol cannot be empty.")

    div_df = ak.stock_hk_fhpx_detail_ths(symbol[1:])
    div_df.dropna(inplace=True)
    ticker = div_df[['公告日期',
                        '方案',
                        '除净日',
                        '派息日']]
    ticker['amount'] = div_df['方案'].apply(
        lambda x: get_post_tax_dividend_per_share(x) if isinstance(x, str) else None
    )
    ticker.rename(columns={'公告日期': "report_date",
                            '方案': "description", 
                            '除净日': "record_date",
                            '派息日': "ex_dividend_date"}, inplace=True)
    dividends = ticker.to_dict("records")  # type: ignore
    
    if not dividends:
        raise EmptyDataError(f"No dividend data found for {symbol}.")

    return dividends