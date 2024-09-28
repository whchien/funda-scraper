from funda_scraper.scrape import FundaScraper
from funda_scraper.extract import DataExtractor

if __name__ == "__main__":
    print('there u go again')

    scraper = FundaScraper(
        area="amsterdam",
        want_to="buy",
        find_past=False,
        page_start=1,
        n_pages=3,
        # min_price=500,
        # max_price=2000
    )
    # #df = scraper.run(raw_data=True, save=True, filepath="test.csv")
    df = scraper.run(raw_data = True, save = True, filepath = "test.csv")
    df.head()

    # data_extractor = DataExtractor()
    # data_extractor.extract_data(to_buy = True, find_past = False, raw_data = True, save = True, file_path = 'test.csv')



