from funda_scraper.scrape import FundaScraper


# def test_run():
#     data = ("utrecht", "rent", 1, True)
#     scraper = FundaScraper(data[0], data[1], data[2], data[3])
#     df = scraper.run()


def test_scrape_from_url():
    url = 'https://www.funda.nl/en/huur/verhuurd/den-haag/huis-88273228-schaakplein-20/'
    result = FundaScraper(want_to="rent").scrape_from_url(url)
    assert len(result) == 27
