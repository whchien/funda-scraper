from funda_scraper.preprocess import preprocess_data
from funda_scraper.scrape import FundaScraper


class TestFundaScraper(object):
    def test_rent(self):
        scraper = FundaScraper(area="amsterdam", want_to="rent", find_past=False, page_start=1, n_pages=1)
        df = scraper.run(raw_data=True)
        assert len(scraper.links) == 15
        assert df.shape == (15, 27)
        assert df['city'].unique()[0] == 'amsterdam'

        df = preprocess_data(df, is_past=False)
        assert df.shape[0] > 12
        assert df.shape[1] == 17

    def test_rent_past(self):
        scraper = FundaScraper(area="amsterdam", want_to="rent", find_past=True, page_start=1, n_pages=1)
        df = scraper.run(raw_data=True)
        assert len(scraper.links) == 15
        assert df.shape == (15, 30)
        assert df['city'].unique()[0] == 'amsterdam'

        df = preprocess_data(df, is_past=True)
        assert df.shape[0] > 12
        assert df.shape[1] == 17

    def test_buy(self):
        scraper = FundaScraper(area="amsterdam", want_to="buy", find_past=False, page_start=1, n_pages=1)
        df = scraper.run(raw_data=True)
        assert len(scraper.links) == 15
        assert df.shape == (15, 27)
        assert df['city'].unique()[0] == 'amsterdam'

        df = preprocess_data(df, is_past=False)
        assert df.shape[0] > 12
        assert df.shape[1] == 17

    def test_buy_past(self):
        scraper = FundaScraper(area="amsterdam", want_to="buy", find_past=True, page_start=1, n_pages=1)
        df = scraper.run(raw_data=True)
        assert len(scraper.links) == 15
        assert df.shape == (15, 30)
        assert df['city'].unique()[0] == 'amsterdam'

        df = preprocess_data(df, is_past=True)
        assert df.shape[0] > 12
        assert df.shape[1] == 17
