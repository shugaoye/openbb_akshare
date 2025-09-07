import pytest
from openbb import obb
import pandas as pd

@pytest.mark.parametrize("symbol", ["001979","600177","000333","601857","600050","600941","601728","601319",
                                    "600704","600886","601880","601998","600999","000001","601318",
                                    "601288","601988","601939","601398"])
def test_equity_historical(symbol, logger):
    df = obb.equity.price.historical(symbol=symbol, provider="akshare").to_dataframe()
    assert isinstance(df, pd.DataFrame)


@pytest.mark.parametrize("symbol", ["001979","600177","000333","601857","600050","600941","601728","601319",
                                    "600704","600886","601880","601998","600999","000001","601318",
                                    "601288","601988","601939","601398"])
def test_equity_info(symbol, akshare_api_key, logger):
    from openbb_akshare.utils.fetch_equity_info import fetch_equity_info
    df = fetch_equity_info(symbol, api_key=akshare_api_key)
    listed_date = pd.to_datetime(df.get("listed_date"), unit='ms').iloc[0].date()
    logger.info(f"Listed date of {symbol} is {listed_date}.")
    assert isinstance(df, pd.DataFrame)