import pytest
import pandas as pd
from openbb import obb

symbols = ["600325", "600028","600036","600048","01088"]

@pytest.mark.parametrize("symbol", symbols)
@pytest.mark.parametrize("period_t", ["annual", "quarter"])
@pytest.mark.parametrize("limit", [3,5,7,10])
def test_balance_sheet(symbol, period_t, limit, default_provider):
    balance_df = obb.equity.fundamental.balance(symbol=symbol, period=period_t, limit=limit, provider=default_provider).to_dataframe()
    assert not balance_df.empty
    assert len(balance_df) == limit

@pytest.mark.parametrize("symbol", symbols)
@pytest.mark.parametrize("period_t", ["annual", "quarter"])
@pytest.mark.parametrize("limit", [3,5,7,10])
def test_income(symbol, period_t, limit, default_provider):
    income_df = obb.equity.fundamental.income(symbol=symbol, period=period_t, limit=limit, provider=default_provider).to_dataframe()
    assert not income_df.empty
    assert len(income_df) == limit

@pytest.mark.parametrize("symbol", symbols)
@pytest.mark.parametrize("period_t", ["annual", "quarter"])
@pytest.mark.parametrize("limit", [3,5,7,10])
def test_cash_flow(symbol, period_t, limit, default_provider):
    cash_flow_df = obb.equity.fundamental.cash(symbol=symbol, period=period_t, limit=limit, provider=default_provider).to_dataframe()
    assert not cash_flow_df.empty
    assert len(cash_flow_df) == limit
