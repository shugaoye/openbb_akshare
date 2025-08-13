import pandas as pd
from openbb import obb

def test_balance_sheet(default_provider):
    balance_df = obb.equity.fundamental.balance(symbol="600325", period="annual", limit=7, provider=default_provider).to_dataframe()
    assert not balance_df.empty
    assert len(balance_df) == 7

def test_income(default_provider):
    income_df = obb.equity.fundamental.income(symbol="600325", limit=3, provider=default_provider).to_dataframe()
    assert not income_df.empty
    assert len(income_df) == 3

def test_cash_flow(default_provider):
    cash_flow_df = obb.equity.fundamental.cash(symbol="600325", limit=3, provider=default_provider).to_dataframe()
    assert not cash_flow_df.empty
    assert len(cash_flow_df) == 3
