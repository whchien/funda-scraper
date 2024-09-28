from funda_scraper.scrape import FundaScraper

if __name__ == "__main__":
    print('there u go')

    scraper = FundaScraper(
        area="amsterdam",
        want_to="buy",
        find_past=False,
        page_start=1,
        n_pages=3,
        # min_price=500,
        # max_price=2000
    )
    df = scraper.run(raw_data=True, save=True, filepath="test.csv")
    df.head()



