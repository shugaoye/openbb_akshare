import os
import pytest
import pandas as pd
import tempfile
from datetime import datetime, timedelta
from openbb_akshare.utils.blob_cache import (
    BlobCache,
    calculate_cache_ttl,
    constant_ttl,
    get_next_quarter_start,
    get_next_year_start
)

def test_calculate_cache_ttl_with_constant_ttl():
    now = datetime(2024, 1, 1, 12, 0, 0)
    ttl = 3600
    result = calculate_cache_ttl(constant_ttl, ttl, now=now) 
    assert result == now + timedelta(seconds=ttl)

def test_calculate_cache_ttl_without_now(monkeypatch):
    fixed_now = datetime(2024, 1, 1, 8, 0, 0)
    class DummyDatetime(datetime):
        @classmethod
        def now(cls):
            return fixed_now

    import openbb_akshare.utils.blob_cache as blob_cache
    monkeypatch.setattr(blob_cache, "datetime", DummyDatetime)

    ttl = 1800
    result = calculate_cache_ttl(constant_ttl, ttl)
    assert result == fixed_now + timedelta(seconds=ttl)

def test_calculate_cache_ttl_with_next_quarter():
    test_cases = [
        (datetime(2024, 1, 15), datetime(2024, 4, 1)),  # Q1 -> Q2
        (datetime(2024, 4, 15), datetime(2024, 7, 1)),  # Q2 -> Q3
        (datetime(2024, 7, 15), datetime(2024, 10, 1)), # Q3 -> Q4
        (datetime(2024, 10, 15), datetime(2025, 1, 1)), # Q4 -> Q1 next year
        (datetime(2024, 12, 31), datetime(2025, 1, 1))  # Year end
    ]
    
    for input_date, expected in test_cases:
        result = calculate_cache_ttl(get_next_quarter_start, now=input_date)
        assert result == expected

def test_calculate_cache_ttl_with_next_year():
    test_cases = [
        (datetime(2024, 1, 1), datetime(2025, 1, 1)),
        (datetime(2024, 6, 15), datetime(2025, 1, 1)),
        (datetime(2024, 12, 31), datetime(2025, 1, 1)),
        (datetime(2025, 1, 1), datetime(2026, 1, 1))
    ]
    
    for input_date, expected in test_cases:
        result = calculate_cache_ttl(get_next_year_start, now=input_date)
        assert result == expected

def test_edge_cases():
    now = datetime(2024, 12, 31, 23, 59, 59)
    
    # Test constant TTL crossing year boundary
    result = calculate_cache_ttl(constant_ttl, 10, now=now)
    assert result == datetime(2025, 1, 1, 0, 0, 9)
    
    # Test quarter transition at year boundary
    result = calculate_cache_ttl(get_next_quarter_start, now=now)
    assert result == datetime(2025, 1, 1)
    
    # Test year transition
    result = calculate_cache_ttl(get_next_year_start, now=now)
    assert result == datetime(2025, 1, 1)

def test_invalid_quarter_inputs():
    # Test various months to ensure correct quarter transitions
    for month in range(1, 13):
        now = datetime(2024, month, 15)
        result = calculate_cache_ttl(get_next_quarter_start, now=now)
        expected_month = ((month - 1) // 3 + 1) * 3 + 1
        expected_year = 2024 if expected_month <= 12 else 2025
        if expected_month > 12:
            expected_month = 1
        assert result == datetime(expected_year, expected_month, 1)

def test_blob_cache_initialization():
    # Test initialization without table name
    with pytest.raises(ValueError):
        BlobCache(table_name=None)

    # Test default db_path
    cache = BlobCache(table_name="test_table")
    assert cache.db_path is not None
    assert os.path.exists(cache.db_path)
    
    # Test initialization with table name
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = BlobCache(table_name="test_table", db_path=tmpdir)
            assert cache.table_name == "test_table"
            assert cache.db_path == f"{tmpdir}/equity.db"
    except PermissionError:
        # Ignore the PermissionError
        pass        

def test_blob_cache_load_data():
    
    # Mock data function
    def mock_get_data(symbol, report_type, *args, **kwargs):
        print(f"Mock get_data called with symbol={symbol}, report_type={report_type}")
        return pd.DataFrame({'col1': [1,2,3]})
    
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = BlobCache(table_name="test_table", db_path=tmpdir)
        
            # Test first load - should call get_data
            df1 = cache.load_cached_data("000001.SZ", "daily", mock_get_data)
            assert isinstance(df1, pd.DataFrame)
            assert len(df1) == 3
            
            # Test cached load - should return cached data
            df2 = cache.load_cached_data("000001.SZ", "daily", mock_get_data) 
            assert df2.equals(df1)
            
            # Test different symbol - should call get_data again
            df3 = cache.load_cached_data("000002.SZ", "annual", mock_get_data)
            assert isinstance(df3, pd.DataFrame)
            assert len(df3) == 3

            df4 = cache.load_cached_data("000002.SZ", "quarter", mock_get_data)
            assert isinstance(df4, pd.DataFrame)
            assert len(df4) == 3
    except PermissionError:
        # Ignore the PermissionError
        pass
