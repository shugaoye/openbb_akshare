import pytest
import pandas as pd
from openbb_akshare.utils.ak_key_metrics import fetch_key_metrics

def test_fetch_key_metrics_invalid_symbol():
    """Test fetch_key_metrics with invalid symbol returns empty DataFrame"""
    result = fetch_key_metrics("INVALID")
    assert isinstance(result, pd.DataFrame)
    assert result.empty

def test_fetch_key_metrics_periods(akshare_api_key):
    """Test fetch_key_metrics with different periods"""
    # Test quarter period
    df_quarter = fetch_key_metrics("601127.SH", period="quarter", api_key=akshare_api_key)
    assert isinstance(df_quarter, pd.DataFrame)
    
    # Test annual period
    df_annual = fetch_key_metrics("601127.SH", period="annual", api_key=akshare_api_key) 
    assert isinstance(df_annual, pd.DataFrame)

def test_fetch_key_metrics_cache(akshare_api_key):
    """Test fetch_key_metrics caching behavior"""
    # Test with cache
    df_cached = fetch_key_metrics("601127.SH", use_cache=True, api_key=akshare_api_key)
    assert isinstance(df_cached, pd.DataFrame)
    
    # Test without cache
    df_no_cache = fetch_key_metrics("601127.SH", use_cache=False, api_key=akshare_api_key)
    assert isinstance(df_no_cache, pd.DataFrame)

def test_fetch_key_metrics_api_key(akshare_api_key):
    """Test fetch_key_metrics with API key"""
    df = fetch_key_metrics("00300", api_key=akshare_api_key)
    assert isinstance(df, pd.DataFrame)

def test_fetch_key_metrics_symbol_formats(akshare_api_key):
    """Test fetch_key_metrics with different symbol formats"""
    symbols = ["601127.SH", "601127"]
    for symbol in symbols:
        df = fetch_key_metrics(symbol, api_key=akshare_api_key)
        assert isinstance(df, pd.DataFrame)

def test_key_metrics():
    from openbb import obb
    df = obb.equity.fundamental.metrics(symbol="000002,600036,430047", provider="akshare").to_dataframe()
    assert isinstance(df, pd.DataFrame)