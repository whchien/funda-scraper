from funda_scraper.scrape import FundaScraper
import pytest


@pytest.fixture
def scraper():
    scraper = FundaScraper(area="amsterdam", want_to="rent", find_past=False, n_pages=1)
    return scraper


def test_run(scraper):
    df = scraper.run()
    assert len(scraper.links) > 1
    assert df.shape[1] == 21