from funda_scraper.scrape import FundaScraper
from funda_scraper.extract import DataExtractor
from funda_scraper.searchrequest import SearchRequest

if __name__ == "__main__":

    search_params = SearchRequest(
                        area = "Amsterdam",
                        want_to = "buy",
                        find_past = False,
                        page_start = 1,
                        n_pages = 3,
                        # min_price=500,
                        # max_price=2000
                    )

    scraper = FundaScraper(search_params)
    df = scraper.run(clean_data = False)
    df.head()

    #data_extractor = DataExtractor()
    #data_extractor.extract_data(search_params, run_id = "14431b3e-7e59-11ef-a3d4-a0510ba6104e", clean_data = False)



