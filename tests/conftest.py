import logging
import pytest
from mysharelib.tools import setup_logger, normalize_symbol
from openbb_akshare import project_name


@pytest.fixture(scope="session", autouse=True)
def setup_logging():
    setup_logger(project_name)

@pytest.fixture
def logger():
    return logging.getLogger(__name__)

@pytest.fixture
def default_provider():
    return "akshare"
