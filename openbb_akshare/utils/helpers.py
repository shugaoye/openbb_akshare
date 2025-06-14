"""AKShare helpers module."""

# pylint: disable=unused-argument,too-many-arguments,too-many-branches,too-many-locals,too-many-statements

from typing import TYPE_CHECKING, Any, Literal, Optional, Union

from openbb_core.provider.utils.errors import EmptyDataError
from openbb_yfinance.utils.references import INTERVALS, MONTHS, PERIODS
from pandas import DataFrame
from datetime import datetime, timedelta
from .tools import normalize_date, normalize_symbol
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