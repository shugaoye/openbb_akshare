import pytest
from openbb import obb
import pandas as pd

@pytest.mark.parametrize("symbol", ["001979","600177","000333","601857","600050","600941","601728","601319",
                                    "600704","600886","601880","601998","600999","000001","601318",
                                    "601288","601988","601939","601398"])
def test_equity_historical(symbol, default_provider):
    df = obb.equity.price.historical(symbol=symbol, provider=default_provider).to_dataframe()
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

def test_equity_historical_with_date_format1(logger, default_provider):
    df = obb.equity.price.historical(symbol="600325", start_date="2024-01-01", end_date="2025-09-08", provider=default_provider).to_dataframe()
    assert isinstance(df, pd.DataFrame)