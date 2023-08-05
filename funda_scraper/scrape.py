"""Main funda scraper module"""
import datetime
import json
import multiprocessing as mp
import os
from typing import Dict, List

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from tqdm.contrib.concurrent import process_map

from funda_scraper.config.core import config
from funda_scraper.preprocess import preprocess_data
from funda_scraper.utils import logger


class FundaScraper(object):
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
                "close": f'{self.base_url}/zoeken/koop/?selected_area="{self.area}"&availability="unavailable"',
                "open": f'{self.base_url}/zoeken/koop?selected_area="{self.area}"/',
            }
        else:
            return {
                "close": f'{self.base_url}/zoeken/huur?selected_area="{self.area}"&availability="unavailable"',
                "open": f'{self.base_url}/zoeken/huur?selected_area="{self.area}"/',
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
    def _get_links_from_one_parent(url: str) -> List[str]:
        """Scrape all the available housing items from one Funda search page."""
        response = requests.get(url, headers=config.header)
        soup = BeautifulSoup(response.text, "lxml")

        script_tag = soup.find_all("script", {"type": "application/ld+json"})[0]
        json_data = json.loads(script_tag.contents[0])
        urls = [item["url"] for item in json_data["itemListElement"]]
        return list(set(urls))

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

    def fetch_all_links(self) -> None:
        """Find all the available links across multiple pages."""
        if self.area is None or self.want_to is None:
            raise ValueError("You haven't set the area and what you're looking for.")

        logger.info("*** Phase 1: Fetch all the available links from all pages *** ")
        urls = []
        main_url = self.site_url["close"] if self.find_past else self.site_url["open"]
        for i in tqdm(range(0, self.n_pages + 1)):
            item_list = self._get_links_from_one_parent(f"{main_url}&search_result={i}")
            if len(item_list) == 0:
                self.n_pages = i
                break
            urls += item_list
        urls = list(set(urls))
        logger.info(f"*** Got all the urls. {len(urls)} houses found in {self.n_pages} pages. ***")
        self.links = urls

    @staticmethod
    def get_value_from_css(soup: BeautifulSoup, selector: str) -> str:
        """Use CSS selector to find certain features."""
        result = soup.select(selector)
        if len(result) > 0:
            result = result[0].text
        else:
            result = "na"
        return result

    def scrape_one_link(self, link: str) -> List[str]:
        """Scrape all the features from one house item given a link."""

        # Initialize for each page
        response = requests.get(link, headers=config.header)
        soup = BeautifulSoup(response.text, "lxml")

        # Get the value according to respective CSS selectors
        list_since_selector = self.selectors.listed_since if self.to_buy else ".fd-align-items-center:nth-child(7) span"
        result = [
            link,
            self.get_value_from_css(soup, self.selectors.price),
            self.get_value_from_css(soup, self.selectors.address),
            self.get_value_from_css(soup, self.selectors.descrip),
            self.get_value_from_css(soup, list_since_selector).replace("\n", ""),
            self.get_value_from_css(soup, self.selectors.zip_code).replace("\n", "").replace("\r        ", ""),
            self.get_value_from_css(soup, self.selectors.size),
            self.get_value_from_css(soup, self.selectors.year),
            self.get_value_from_css(soup, self.selectors.living_area),
            self.get_value_from_css(soup, self.selectors.kind_of_house),
            self.get_value_from_css(soup, self.selectors.building_type),
            self.get_value_from_css(soup, self.selectors.num_of_rooms).replace("\n", ""),
            self.get_value_from_css(soup, self.selectors.num_of_bathrooms).replace("\n", ""),
            self.get_value_from_css(soup, self.selectors.layout),
            self.get_value_from_css(soup, self.selectors.energy_label).replace("\r\n        ", ""),
            self.get_value_from_css(soup, self.selectors.insulation).replace("\n", ""),
            self.get_value_from_css(soup, self.selectors.heating).replace("\n", ""),
            self.get_value_from_css(soup, self.selectors.ownership).replace("\n", ""),
            self.get_value_from_css(soup, self.selectors.exteriors),
            self.get_value_from_css(soup, self.selectors.parking),
            self.get_value_from_css(soup, self.selectors.neighborhood_name),
            self.get_value_from_css(soup, self.selectors.date_list),
            self.get_value_from_css(soup, self.selectors.date_sold),
            self.get_value_from_css(soup, self.selectors.term),
            self.get_value_from_css(soup, self.selectors.price_sold),
            self.get_value_from_css(soup, self.selectors.last_ask_price).replace("\n", ""),
            self.get_value_from_css(soup, self.selectors.last_ask_price_m2).split("\r")[0],
        ]

        return result

    def scrape_pages(self) -> None:
        """Scrape all the content acoss multiple pages."""

        logger.info("*** Phase 2: Start scraping results from individual links ***")
        df = pd.DataFrame({key: [] for key in self.selectors.keys()})

        # Scrape pages with multiprocessing to improve efficiency
        pools = mp.cpu_count()
        content = process_map(self.scrape_one_link, self.links, max_workers=pools)

        for i, c in enumerate(content):
            df.loc[len(df)] = c

        df["city"] = self.area
        df["log_id"] = datetime.datetime.now().strftime("%Y%m-%d%H-%M%S")
        if not self.find_past:
            df = df.drop(["term", "price_sold", "date_sold"], axis=1)
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
            filepath = f"./data/houseprice_{date}_{self.area}_{status}_{len(self.links)}.csv"
        df.to_csv(filepath, index=False)
        logger.info(f"*** File saved: {filepath}. ***")

    def run(self, raw_data: bool = False, save: bool = False, filepath: str = None) -> pd.DataFrame:
        """
        Scrape all links and all content.

        :param raw_data: if true, the data won't be pre-processed
        :param save: if true, the data will be saved as a csv file
        :param filepath: the name for the file
        :return: the (pre-processed) dataframe from scraping
        """
        self.fetch_all_links()
        self.scrape_pages()

        if raw_data:
            df = self.raw_df
        else:
            logger.info("*** Cleaning data ***")
            df = preprocess_data(df=self.raw_df, is_past=self.find_past)
            self.clean_df = df

        if save:
            self.save_csv(df, filepath)

        logger.info("*** Done! ***")
        return df


if __name__ == "__main__":
    scraper = FundaScraper(area="amsterdam", want_to="buy", find_past=True, n_pages=1)
    df = scraper.run(raw_data=False)
    print(df.head())
