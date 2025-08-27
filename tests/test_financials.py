import pytest
import pandas as pd
from openbb import obb

symbols = ["600325", "600028","600036","600048"]

@pytest.mark.parametrize("symbol", symbols)
@pytest.mark.parametrize("period_t", ["annual", "quarter"])
def test_balance_sheet(symbol, period_t, default_provider):
    balance_df = obb.equity.fundamental.balance(symbol=symbol, period=period_t, limit=7, provider=default_provider).to_dataframe()
    assert not balance_df.empty
    assert len(balance_df) == 7

@pytest.mark.parametrize("symbol", symbols)
@pytest.mark.parametrize("period_t", ["annual", "quarter"])
def test_income(symbol, period_t, default_provider):
    income_df = obb.equity.fundamental.income(symbol=symbol, period=period_t, limit=3, provider=default_provider).to_dataframe()
    assert not income_df.empty
    assert len(income_df) == 3

@pytest.mark.parametrize("symbol", symbols)
@pytest.mark.parametrize("period_t", ["annual", "quarter"])
def test_cash_flow(symbol, period_t, default_provider):
    cash_flow_df = obb.equity.fundamental.cash(symbol=symbol, period=period_t, limit=3, provider=default_provider).to_dataframe()
    assert not cash_flow_df.empty
    assert len(cash_flow_df) == 3


