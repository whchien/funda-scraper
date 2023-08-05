from funda_scraper.preprocess import preprocess_data
from funda_scraper.scrape import FundaScraper


class TestFundaScraper(object):
    def test_rent(self):
        scraper = FundaScraper(area="amsterdam", want_to="rent", find_past=False, n_pages=1)
        df = scraper.run(raw_data=True)
        assert len(scraper.links) > 1
        assert df.shape[1] == 26

        df = preprocess_data(df, is_past=False)
        assert df.shape[1] == 21

    def test_rent_past(self):
        scraper = FundaScraper(area="amsterdam", want_to="rent", find_past=True, n_pages=1)
        df = scraper.run(raw_data=True)
        assert len(scraper.links) > 1
        assert df.shape[1] == 29

        df = preprocess_data(df, is_past=True)
        assert df.shape[1] == 25

    def test_buy(self):
        scraper = FundaScraper(area="amsterdam", want_to="buy", find_past=False, n_pages=1)
        df = scraper.run(raw_data=True)
        assert len(scraper.links) > 1
        assert df.shape[1] == 26

        df = preprocess_data(df, is_past=False)
        assert df.shape[1] == 21

    def test_buy_past(self):
        scraper = FundaScraper(area="amsterdam", want_to="buy", find_past=True, n_pages=1)
        df = scraper.run(raw_data=True)
        assert len(scraper.links) > 1
        assert df.shape[1] == 29

        df = preprocess_data(df, is_past=True)
        assert df.shape[1] == 25
