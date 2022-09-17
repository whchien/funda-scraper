"""Main funda scraper module"""
import multiprocessing as mp
import os
import pandas as pd
import requests
from bs4 import BeautifulSoup
from typing import List, Dict
import datetime
from funda_scraper.config.core import config
from funda_scraper.preprocess import preprocess_data
from funda_scraper.utils import logger
from tqdm import tqdm
from tqdm.contrib.concurrent import process_map


class FundaScraper:
    """
    Handles the main scraping function.
    """

    def __init__(
        self,
        area: str = None,
        want_to: str = "buy",
        n_pages: int = 1,
        find_past: bool = False,
    ):
        self.area = area.lower().replace(" ", "-") if isinstance(area, str) else area
        self.want_to = want_to
        self.find_past = find_past
        self.n_pages = min(max(n_pages, 1), 999)
        self.links: List[str] = []
        self.raw_df = pd.DataFrame()
        self.clean_df = pd.DataFrame()
        self.base_url = config.base_url
        self.selectors = config.css_selector

    def __repr__(self):
        return (
            f"FundaScraper(area={self.area}, "
            f"want_to={self.want_to}, "
            f"n_pages={self.n_pages}, "
            f"use_past_data={self.find_past})"
        )

    @property
    def site_url(self) -> Dict[str, str]:
        """Return the corresponding urls."""
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
        """Whether to buy or not"""
        if self.want_to.lower() in ["buy", "koop", "b"]:
            return True
        elif self.want_to.lower() in ["rent", "huur", "r"]:
            return False
        else:
            raise ValueError("'want_to' must be 'either buy' or 'rent'.")

    @staticmethod
    def _check_dir() -> None:
        """Check whether a temporary directory for data"""
        if not os.path.exists("data"):
            os.makedirs("data")

    @staticmethod
    def _get_links_from_one_page(url: str) -> List[str]:
        """Scrape all the available housing items from one Funda search page."""
        response = requests.get(url, headers=config.header)
        soup = BeautifulSoup(response.text, "lxml")
        house = soup.find_all(attrs={"data-object-url-tracking": "resultlist"})
        item_list = [h.get("href") for h in house]
        return list(set(item_list))

    def init(
        self,
        area: str = None,
        want_to: str = None,
        n_pages: int = None,
        find_past: bool = None,
    ) -> None:
        """Overwrite or initialise the searching scope."""
        if area is not None:
            self.area = area
        if want_to is not None:
            self.want_to = want_to
        if n_pages is not None:
            self.n_pages = n_pages
        if find_past is not None:
            self.find_past = find_past

    def fetch_links(self) -> None:
        """Find all the available links across multiple pages. """
        if self.area is None or self.want_to is None:
            raise ValueError("You haven't set the area and what you're looking for.")

        logger.info("*** Phase 1: Fetch all the available links from all pages *** ")

        urls = []
        main_url = self.site_url["close"] if self.find_past else self.site_url["open"]
        for i in tqdm(range(0, self.n_pages + 1)):
            item_list = self._get_links_from_one_page(main_url + f"p{i}")
            if len(item_list) == 0:
                self.n_pages = i
                break
            urls += item_list
        urls = list(set(urls))
        logger.info(
            f"*** Got all the urls. {len(urls)} houses found in {self.n_pages} pages. ***"
        )
        self.links = ["https://www.funda.nl" + url for url in urls]

    @staticmethod
    def get_value(soup: BeautifulSoup, selector: str) -> str:
        """Use CSS selector to find certain features."""
        try:
            return soup.select(selector)[0].text
        except IndexError:
            return "na"

    def scrape_from_url(self, url: str) -> List[str]:
        """Scrape all the features from one house item given a link. """

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
            self.get_value(soup, self.selectors.zip_code)
            .replace("\n", "")
            .replace("\r        ", ""),
            self.get_value(soup, self.selectors.size),
            self.get_value(soup, self.selectors.year),
            self.get_value(soup, self.selectors.living_area),
            self.get_value(soup, self.selectors.kind_of_house),
            self.get_value(soup, self.selectors.building_type),
            self.get_value(soup, self.selectors.num_of_rooms).replace("\n", ""),
            self.get_value(soup, self.selectors.num_of_bathrooms).replace("\n", ""),
            self.get_value(soup, self.selectors.layout),
            self.get_value(soup, self.selectors.energy_label).replace(
                "\r\n        ", ""
            ),
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

        return result

    def scrape_pages(self) -> None:
        """Scrape all the content acoss multiple pages."""

        logger.info("*** Phase 2: Start scraping results from individual links ***")
        df = pd.DataFrame({key: [] for key in self.selectors.keys()})

        # Scrape pages with multiprocessing to improve efficiency
        pools = mp.cpu_count()
        content = process_map(self.scrape_from_url, self.links, max_workers=pools)

        for i, c in enumerate(content):
            df.loc[len(df)] = c

        df["city"] = self.area
        df["log_id"] = datetime.datetime.now().strftime("%Y%m-%d%H-%M%S")
        logger.info(f"*** All scraping done: {df.shape[0]} results ***")
        self.raw_df = df

    def save_csv(self, df: pd.DataFrame, filepath: str = None) -> None:
        """Save the result to a .csv file."""
        if filepath is None:
            self._check_dir()
            date = str(datetime.datetime.now().date()).replace("-", "")
            if self.find_past:
                if self.to_buy:
                    status = "sold"
                else:
                    status = "rented"
            else:
                if self.to_buy:
                    status = "selling"
                else:
                    status = "renting"
            filepath = (
                f"./data/houseprice_{date}_{self.area}_{status}_{len(self.links)}.csv"
            )
        df.to_csv(filepath, index=False)
        logger.info(f"*** File saved: {filepath}. ***")

    def run(self, raw_data: bool = False, save: bool = False, filepath: str = None) -> pd.DataFrame:
        """Scrape all links and all content."""
        self.fetch_links()
        self.scrape_pages()

        if raw_data:
            if save:
                self.save_csv(self.raw_df, filepath)
            return self.raw_df
        else:
            logger.info("*** Cleaning data ***")
            clean_df = preprocess_data(df=self.raw_df, is_past=self.find_past)
            self.clean_df = clean_df
            if save:
                self.save_csv(self.clean_df, filepath)
            return clean_df
        logger.info("*** Done! ***")


if __name__ == "__main__":
    scraper = FundaScraper(area="amsterdam", want_to="rent", find_past=False, n_pages=1)
    df = scraper.run()
    print(df.head())