"""openbb_akshare OpenBB Platform Provider."""

from openbb_core.provider.abstract.provider import Provider
from openbb_akshare.models.company_news import AKShareCompanyNewsFetcher

# mypy: disable-error-code="list-item"

provider = Provider(
    name="openbb_akshare",
    description="Data provider for openbb-akshare.",
    # Only add 'credentials' if they are needed.
    # For multiple login details, list them all here.
    # credentials=["api_key"],
    website="https://akshare.akfamily.xyz/",
    # Here, we list out the fetchers showing what our provider can get.
    # The dictionary key is the fetcher's name, used in the `router.py`.
    fetcher_dict={
        "Example": AKShareCompanyNewsFetcher,
    }
)
