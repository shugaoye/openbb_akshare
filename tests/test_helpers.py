import pytest
from openbb_akshare.utils.helpers import get_post_tax_dividend_per_share

def test_get_post_tax_dividend_per_share_valid():
    # Test with normal valid input
    assert get_post_tax_dividend_per_share("10派1.04元(含税,扣税后0.936元)") == 0.0936
    
    # Test with different numbers
    assert get_post_tax_dividend_per_share("5派2.50元(含税,扣税后2.25元)") == 0.45
    
    # Test with larger numbers
    assert get_post_tax_dividend_per_share("100派15.00元(含税,扣税后13.50元)") == 0.135

def test_get_post_tax_dividend_per_share_invalid():
    # Test with invalid input formats
    assert get_post_tax_dividend_per_share("invalid text") == 0.0
        
    assert get_post_tax_dividend_per_share("10转3.00") == 0.0
        
    assert get_post_tax_dividend_per_share("10派1.04元(含税)") == 0.0
        
    assert get_post_tax_dividend_per_share("") == 0.0

def test_get_post_tax_dividend_per_share_edge_cases():
    # Test with zero values
    assert get_post_tax_dividend_per_share("10派0元(含税,扣税后0元)") == 0.0
    
    # Test with very small numbers
    assert get_post_tax_dividend_per_share("1000派0.001元(含税,扣税后0.0009元)") == 0.0000
    
    # Test with decimal base shares (should still work as it gets converted to int)
    #assert get_post_tax_dividend_per_share("10.0派1.04元(含税,扣税后0.936元)") == 0.0936