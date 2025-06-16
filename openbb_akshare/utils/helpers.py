"""AKShare helpers module."""

# pylint: disable=unused-argument,too-many-arguments,too-many-branches,too-many-locals,too-many-statements
import re
from typing import TYPE_CHECKING, Any, Literal, Optional, Union

from openbb_core.provider.utils.errors import EmptyDataError
from openbb_yfinance.utils.references import INTERVALS, MONTHS, PERIODS
from pandas import DataFrame
from datetime import datetime, timedelta
from .tools import normalize_date, normalize_symbol

import logging
from openbb_akshare.utils.tools import setup_logger, normalize_symbol

#setup_logger()
logger = logging.getLogger(__name__)

def ak_download(  # pylint: disable=too-many-positional-arguments
    symbol: str,
    start_date: Optional[Union[str, "date"]] = None,
    end_date: Optional[Union[str, "date"]] = None,
    interval: INTERVALS = "1d",
    period: Optional[PERIODS] = None,
    **kwargs: Any,
) -> DataFrame:
    import akshare as ak

    start = start_date.strftime("%Y%m%d")
    end = end_date.strftime("%Y%m%d")

    symbol_b, symbol_f, market = normalize_symbol(symbol)
    if market == "HK":
        stock_zh_a_hist_df = ak.stock_hk_hist(symbol_b, period, start, end, adjust="")
    else:
        stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol_b, period, start, end, adjust="")
    
    stock_zh_a_hist_df.rename(columns={"日期": "date", "股票代码": "symbol", "开盘": "open", "收盘": "close", "最高": "high", "最低": "low", "成交量": "volume", "成交额": "amount", "涨跌幅":"change_percent", "涨跌额": "change"}, inplace=True)
    stock_zh_a_hist_df = stock_zh_a_hist_df.drop(columns=["振幅"])
    stock_zh_a_hist_df = stock_zh_a_hist_df.drop(columns=["换手率"])
    return stock_zh_a_hist_df


def get_post_tax_dividend_per_share(dividend_str: str) -> float:
    """
    Parses Chinese dividend description and returns post-tax cash dividend per share.
    
    Handles formats like:
        - "10转10.00派1.00元(含税,扣税后0.90元)"
        - "10派1.04元(含税,扣税后0.936元)"

    Parameters:
        dividend_str (str): Dividend description string

    Returns:
        float: Post-tax dividend amount per share, rounded to 4 decimal places
    """
    # Extract base shares and pre-tax dividend part
    base_match = re.search(r'([\d\.]+)[转股]*派([\d\.]+)', dividend_str)
    after_tax_match = re.search(r'扣税后([\d\.]+)', dividend_str)

    if not base_match:
        logger.error("Could not parse base shares and pre-tax dividend from the string: %s", dividend_str)
        #raise ValueError("Could not parse base shares and pre-tax dividend.")
        return 0
    if not after_tax_match:
        logger.error("Could not parse post-tax dividend value from the string: %s", dividend_str)
        #raise ValueError("Could not parse post-tax dividend value.")
        return 0

    # Convert extracted values to appropriate types
    base_shares = int(float(base_match.group(1)))
    post_tax_total = float(after_tax_match.group(1))

    # Calculate per-share post-tax dividend
    if not base_shares:
        logger.error("Base shares cannot be zero or empty from the string: %s", dividend_str)
        raise ValueError("Base shares cannot be zero or empty.")
    
    post_tax_per_share = post_tax_total / base_shares

    return round(post_tax_per_share, 4)