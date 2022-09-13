import multiprocessing as mp
from multiprocessing import Pool
import pandas as pd
import requests
from bs4 import BeautifulSoup
from typing import List, Dict
import datetime
from funda_scraper.config.core import config
from funda_scraper.utils import logger
from tqdm import tqdm


class FundaScraper:
    def __init__(
        self,
        area: str = "amsterdam",
        want_to: str = "rent",
        n_pages: int = 1,
        find_past: bool = False,
    ):
        self.area = area.lower().replace(" ", "-")
        self.want_to = want_to
        self.find_past = find_past
        assert n_pages >= 1, "The min value for n_page is 1."
        self.n_pages = n_pages
        self.urls_for_respective_houses = []
        self.result_df = None
        self.base_url = config.base_url
        self.selectors = config.css_selector

    def __repr__(self):
        return (
            f"FundaScraper(area={self.area}, "
            f"to_buy={self.to_buy}, "
            f"n_pages={self.n_pages}, "
            f"use_past_data={self.find_past})"
        )

    @property
    def site_url(self) -> Dict[str, str]:
        if self.to_buy:
            return {
                "close": f"{self.base_url}/koop/verkocht/{self.area}/",
                "open": f"{self.base_url}/koop/{self.area}/",
            }
        else:
            return {
                "close": f"{self.base_url}/huur/{self.area}/verhuurd/",
                "open": f"{self.base_url}/huur/{self.area}/",
            }

    @property
    def to_buy(self) -> bool:
        if self.want_to.lower() in ["buy", "koop", "b"]:
            return True
        elif self.want_to.lower() in ["rent", "huur", "r"]:
            return False
        else:
            raise ValueError("'want_to' must be 'either buy' or 'rent'.")

    @staticmethod
    def get_urls_from_one_page(url: str) -> List[str]:
        response = requests.get(url, headers=config.header)
        soup = BeautifulSoup(response.text, "lxml")
        house = soup.find_all(attrs={"data-object-url-tracking": "resultlist"})
        item_list = [h.get("href") for h in house]
        return list(set(item_list))

    def get_urls_from_n_pages(self) -> None:
        logger.info("*** Start to retrieve urls for all pages *** ")

        urls = []
        main_url = (
            self.site_url["close"]
            if self.find_past
            else self.site_url["open"]
        )
        for i in tqdm(range(0, self.n_pages + 1)):
            item_list = self.get_urls_from_one_page(main_url + f"p{i}")
            if len(item_list) == 0:
                self.n_pages = i
                break
            urls += item_list
        urls = list(set(urls))
        logger.info(
            f"*** Got all the urls. {len(urls)} houses found in {self.n_pages} pages. ***"
        )
        self.urls_for_respective_houses = ["https://www.funda.nl" + url for url in urls]

    @staticmethod
    def get_value(soup: BeautifulSoup, selector: str) -> str:
        try:
            return soup.select(selector)[0].text
        except IndexError:
            return "na"

    def scrape_result_from_one_house(self, url: str) -> List[str]:

        # Initialize for each page
        response = requests.get(url, headers=config.header)
        soup = BeautifulSoup(response.text, "lxml")

        # Get the value according to respective CSS selectors
        list_since_selector = (
            self.selectors.listed_since
            if self.to_buy
            else ".fd-align-items-center:nth-child(7) span"
        )
        result = [
            url,
            self.get_value(soup, self.selectors.price),
            self.get_value(soup, self.selectors.address),
            self.get_value(soup, self.selectors.descrip),
            self.get_value(soup, list_since_selector).replace("\n", ""),
            self.get_value(soup, self.selectors.zip_code).replace("\n", "").replace("\r        ", ""),
            self.get_value(soup, self.selectors.size),
            self.get_value(soup, self.selectors.year),
            self.get_value(soup, self.selectors.living_area),
            self.get_value(soup, self.selectors.kind_of_house),
            self.get_value(soup, self.selectors.building_type),
            self.get_value(soup, self.selectors.num_of_rooms).replace("\n", ""),
            self.get_value(soup, self.selectors.num_of_bathrooms).replace("\n", ""),
            self.get_value(soup, self.selectors.layout),
            self.get_value(soup, self.selectors.energy_label).replace(
                "\r\n        ", ""),
            self.get_value(soup, self.selectors.insulation).replace("\n", ""),
            self.get_value(soup, self.selectors.heating).replace("\n", ""),
            self.get_value(soup, self.selectors.ownership).replace("\n", ""),
            self.get_value(soup, self.selectors.exteriors),
            self.get_value(soup, self.selectors.parking),
            self.get_value(soup, self.selectors.neighborhood_name),
            self.get_value(soup, self.selectors.date_list),
            self.get_value(soup, self.selectors.date_sold),
            self.get_value(soup, self.selectors.term),
            self.get_value(soup, self.selectors.price_sold),
            self.get_value(soup, self.selectors.last_ask_price).replace("\n", ""),
            self.get_value(soup, self.selectors.last_ask_price_m2).split("\r")[0],
        ]

        logger.info(f">>> DONE: {url}")

        return result

    def scrape_pages(self) -> None:
        logger.info("*** Start scraping results ***")
        df = pd.DataFrame({key: [] for key in self.selectors.keys()})

        # Scrape pages with multiprocessing to improve efficiency
        pools = mp.cpu_count()
        with Pool(pools) as p:
            content = p.map(
                self.scrape_result_from_one_house, self.urls_for_respective_houses
            )

        for i, c in enumerate(content):
            df.loc[len(df)] = c

        df["city"] = self.area
        df["log_id"] = datetime.datetime.now().strftime("%Y%m-%d%H-%M%S")
        logger.info(f"*** All scraping done: {df.shape[0]} results ***")
        self.result_df = df

    def save_to_csv(self) -> None:
        date = str(datetime.datetime.now().date()).replace("-", "")
        status = "close" if self.find_past else "open"
        want_to = "buy" if self.to_buy else "rent"
        filename = f"./data/raw/{status}/{want_to}/houseprice_{date}_{self.area}_{status}_{want_to}_{len(self.urls_for_respective_houses)}.csv"
        self.result_df.to_csv(filename, index=False)
        logger.info(f"*** File saved: {filename}. ***")

    def run(self) -> None:
        self.get_urls_from_n_pages()
        self.scrape_pages()


if __name__ == "__main__":
    scraper = FundaScraper(area="den-haag", want_to="rent", n_pages=999, find_past=True)
    scraper.run()
    scraper.save_to_csv()
