from funda_scraper.scrape import FundaScraper
import pytest


class TestFundaScraper(object):

    def test_rent(self):
        scraper = FundaScraper(area="amsterdam", want_to="rent", find_past=False, n_pages=1)
        df = scraper.run()
        assert len(scraper.links) > 1
        assert df.shape[1] == 21

    def test_rent_past(self):
        scraper = FundaScraper(area="amsterdam", want_to="rent", find_past=True, n_pages=1)
        df = scraper.run()
        assert len(scraper.links) > 1
        assert df.shape[1] == 25

    def test_buy(self):
        scraper = FundaScraper(area="amsterdam", want_to="buy", find_past=False, n_pages=1)
        df = scraper.run()
        assert len(scraper.links) > 1
        assert df.shape[1] == 21

    def test_buy_past(self):
        scraper = FundaScraper(area="amsterdam", want_to="buy", find_past=True, n_pages=1)
        df = scraper.run()
        assert len(scraper.links) > 1
        assert df.shape[1] == 25
