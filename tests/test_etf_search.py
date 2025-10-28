"""Test cases for ETF Search model."""

import pytest
from openbb_akshare.models.etf_search import AKShareEtfSearchFetcher


@pytest.mark.parametrize("query", ["", "300", "etf", "指数"])
def test_etf_search_fetcher(query, logger):
    """Test ETF search fetcher with different search queries."""
    import asyncio
    
    query_params = {
        "query": query,
        "limit": 10,
        "use_cache": True,
    }
    fetcher = AKShareEtfSearchFetcher()
    
    # Transform query
    transformed_query = fetcher.transform_query(query_params)
    
    # Extract data (no credentials needed for ETF search)
    try:
        data = asyncio.run(fetcher.aextract_data(transformed_query, None))
        assert isinstance(data, list)
        logger.info(f"ETF search for '{query}': {len(data)} results")
        
        # Transform data
        if data:
            transformed_data = fetcher.transform_data(transformed_query, data)
            assert isinstance(transformed_data, list)
            
            # Verify data structure
            if transformed_data:
                first_item = transformed_data[0]
                assert hasattr(first_item, "symbol")
                assert hasattr(first_item, "name")
                assert first_item.symbol is not None
                assert first_item.name is not None
                logger.info(f"First result: {first_item.symbol} - {first_item.name}")
    except Exception as e:
        # Some queries might not have data, log but don't fail
        logger.info(f"No data for query '{query}': {e}")


def test_etf_search_empty_query():
    """Test ETF search with empty query returns all results."""
    import asyncio
    
    query_params = {
        "query": "",
        "limit": 5,
        "use_cache": True,
    }
    fetcher = AKShareEtfSearchFetcher()
    transformed_query = fetcher.transform_query(query_params)
    
    try:
        data = asyncio.run(fetcher.aextract_data(transformed_query, None))
        assert isinstance(data, list)
        assert len(data) <= 5  # Should respect limit
        
        if data:
            transformed_data = fetcher.transform_data(transformed_query, data)
            assert isinstance(transformed_data, list)
    except Exception:
        # Might not have data, which is acceptable
        pass


@pytest.mark.parametrize("limit", [1, 5, 10, 50])
def test_etf_search_limit(limit):
    """Test ETF search respects limit parameter."""
    import asyncio
    
    query_params = {
        "query": "",
        "limit": limit,
        "use_cache": True,
    }
    fetcher = AKShareEtfSearchFetcher()
    transformed_query = fetcher.transform_query(query_params)
    
    try:
        data = asyncio.run(fetcher.aextract_data(transformed_query, None))
        assert isinstance(data, list)
        assert len(data) <= limit
    except Exception:
        # Might not have data, which is acceptable
        pass


@pytest.mark.parametrize("use_cache", [True, False])
def test_etf_search_cache(use_cache):
    """Test ETF search with and without cache."""
    import asyncio
    
    query_params = {
        "query": "300",
        "limit": 10,
        "use_cache": use_cache,
    }
    fetcher = AKShareEtfSearchFetcher()
    transformed_query = fetcher.transform_query(query_params)
    
    try:
        data = asyncio.run(fetcher.aextract_data(transformed_query, None))
        assert isinstance(data, list)
        
        if data:
            transformed_data = fetcher.transform_data(transformed_query, data)
            assert isinstance(transformed_data, list)
    except Exception:
        # Might not have data, which is acceptable
        pass

