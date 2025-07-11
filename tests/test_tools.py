import pytest
import time
from openbb_akshare.utils.tools import get_timestamp

@pytest.mark.parametrize(
    "any_timestamp,expected",
    [
        (1077638400000, 1077638400),
        (1279209600000, 1279209600),
        (665078400000, 665078400),
        (1457366400000, 1457366400),
        (9.972e+11, 997200000),
        (1,1),
        ("1992-10-27 16:00:00", 720172800)
    ]
)

def test_get_timestamp(any_timestamp, expected):
    result = get_timestamp(any_timestamp)
    assert result == expected

def test_get_timestamp_none():
    assert get_timestamp(None) is None

def test_get_timestamp_milliseconds():
    # Current time in ms + 10_000 ms (10 seconds in the future)
    future_ms = int((time.time() + 10) * 1000)
    result = get_timestamp(future_ms)
    assert abs(result - int(future_ms / 1000)) <= 1  # Allow 1s drift

def test_get_timestamp_seconds():
    # Current time in s + 10 seconds (in the future)
    future_s = int(time.time() + 10)
    result = get_timestamp(future_s)
    assert result == future_s

def test_get_timestamp_now():
    now = int(time.time())
    assert get_timestamp(now) == now
