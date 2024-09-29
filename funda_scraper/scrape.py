"""Main funda scraper module"""

import argparse
import datetime
import json
import multiprocessing as mp
import os
import uuid
from collections import OrderedDict
from typing import List, Optional
from urllib.parse import urlparse, urlunparse

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from tqdm.contrib.concurrent import process_map

from funda_scraper.config.core import config
from funda_scraper.preprocess import clean_date_format, preprocess_data
from funda_scraper.utils import logger
from funda_scraper.extract import DataExtractor
from funda_scraper.filerepository import FileRepository
from funda_scraper.searchrequest import SearchRequest


class FundaScraper(object):
    """
    A class used to scrape real estate data from the Funda website.
    """

    def __init__(self, search_request):
        """

        :param search_request: The parameters for the search
        """
        self.search_request = search_request

        self.links: List[str] = []
        self.raw_df = pd.DataFrame()
        self.clean_df = pd.DataFrame()
        self.base_url = config.base_url

        self.run_id = str(uuid.uuid1())

        self.file_repo = FileRepository()
        self.data_extractor = DataExtractor()


    def __repr__(self):
        return str(self.search_request)


    def _get_list_pages(self, page_start: int = None, number_of_pages: int = None) -> None:

        page_start = self.search_request.page_start if page_start is None else page_start
        number_of_pages = self.search_request.number_of_pages if number_of_pages is None else number_of_pages

        main_url = self._build_main_query_url()

        for i in tqdm(range(page_start, page_start + number_of_pages)):
            url = f"{main_url}&search_result={i}"
            response = requests.get(url, headers = config.header)
            self.file_repo.save_list_page(response.text, i, self.run_id)

        return


    def _get_detail_pages(self):
        urls = []

        list_pages = self.file_repo.get_list_pages(self.run_id)

        for page in list_pages:
            soup = BeautifulSoup(page, "lxml")
            script_tag = soup.find_all("script", {"type": "application/ld+json"})[0]
            json_data = json.loads(script_tag.contents[0])
            item_list = [item["url"] for item in json_data["itemListElement"]]
            urls += item_list

        urls = self.remove_duplicates(urls)
        fixed_urls = [self.fix_link(url) for url in urls]

        pools = mp.cpu_count()
        content = process_map(self.scrape_one_link, fixed_urls, max_workers=pools)

        for i, c in enumerate(content):
             self.file_repo.save_detail_page(c, i, self.run_id)


    def scrape_one_link(self, link: str) -> str:
        response = requests.get(link, headers=config.header)
        return response.text


    @staticmethod
    def _get_links_from_one_parent(url: str) -> List[str]:
        """Scrapes all available property links from a single Funda search page."""
        response = requests.get(url, headers=config.header)
        soup = BeautifulSoup(response.text, "lxml")

        script_tag = soup.find_all("script", {"type": "application/ld+json"})[0]
        json_data = json.loads(script_tag.contents[0])
        urls = [item["url"] for item in json_data["itemListElement"]]
        return urls


    @staticmethod
    def remove_duplicates(lst: List[str]) -> List[str]:
        """Removes duplicate links from a list."""
        return list(OrderedDict.fromkeys(lst))


    @staticmethod
    def fix_link(link: str) -> str:
        """Fixes a given property link to ensure proper URL formatting."""
        link_url = urlparse(link)
        link_path = link_url.path.split("/")
        property_id = link_path.pop(5)
        property_address = link_path.pop(4).split("-")
        link_path = link_path[2:4]
        property_address.insert(1, property_id)
        link_path.extend(["-".join(property_address), "?old_ldp=true"])
        fixed_link = urlunparse(
            (link_url.scheme, link_url.netloc, "/".join(link_path), "", "", "")
        )
        return fixed_link


    def _build_main_query_url(self) -> str:
        """Constructs the main query URL for the search."""
        query = "koop" if self.search_request.to_buy else "huur"

        main_url = (
            f"{self.base_url}/zoeken/{query}?selected_area=%5B%22{self.search_request.area}%22%5D"
        )

        if self.search_request.property_type:
            property_types = self.search_request.property_type.split(",")
            formatted_property_types = [
                "%22" + prop_type + "%22" for prop_type in property_types
            ]
            main_url += f"&object_type=%5B{','.join(formatted_property_types)}%5D"

        if self.search_request.find_past:
            main_url = f'{main_url}&availability=%5B"unavailable"%5D'

        if self.search_request.min_price is not None or self.search_request.max_price is not None:
            min_price = "" if self.search_request.min_price is None else self.search_request.min_price
            max_price = "" if self.search_request.max_price is None else self.search_request.max_price
            main_url = f"{main_url}&price=%22{min_price}-{max_price}%22"

        if self.search_request.days_since is not None:
            main_url = f"{main_url}&publication_date={self.search_request.check_days_since}"

        if self.search_request.min_floor_area or self.search_request.max_floor_area:
            min_floor_area = "" if self.search_request.min_floor_area is None else self.search_request.min_floor_area
            max_floor_area = "" if self.search_request.max_floor_area is None else self.search_request.max_floor_area
            main_url = f"{main_url}&floor_area=%22{min_floor_area}-{max_floor_area}%22"

        if self.search_request.sort is not None:
            main_url = f"{main_url}&sort=%22{self.search_request.sort_by}%22"

        logger.info(f"*** Main URL: {main_url} ***")
        return main_url


    def _get_pages(self):
        self._get_list_pages()
        self._get_detail_pages()


    def run(self, clean_data: bool = False) -> pd.DataFrame:
        """
        Runs the full scraping process, saving the results to a CSV file.

        :param clean_data: if true, the data won't be pre-processed
        :return: the (pre-processed) dataframe from scraping
        """
        logger.info(f"Started scraping, run_id: {self.run_id}")

        logger.info("Fetching pages..")
        self._get_pages()

        logger.info("Extracting data from the html pages")
        df = self.data_extractor.extract_data(self.search_request, self.run_id, clean_data)

        logger.info("*** Done! ***")

        return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--area",
        type=str,
        help="Specify which area you are looking for",
        default="amsterdam",
    )
    parser.add_argument(
        "--want_to",
        type=str,
        help="Specify you want to 'rent' or 'buy'",
        default="rent",
        choices=["rent", "buy"],
    )
    parser.add_argument(
        "--find_past",
        action="store_true",
        help="Indicate whether you want to use historical data",
    )
    parser.add_argument(
        "--page_start", type=int, help="Specify which page to start scraping", default=1
    )
    parser.add_argument(
        "--number_of_pages", type=int, help="Specify how many pages to scrape", default=1
    )
    parser.add_argument(
        "--min_price", type=int, help="Specify the min price", default=None
    )
    parser.add_argument(
        "--max_price", type=int, help="Specify the max price", default=None
    )
    parser.add_argument(
        "--days_since",
        type=int,
        help="Specify the days since publication",
        default=None,
    )
    parser.add_argument(
        "--sort",
        type=str,
        help="Specify sorting",
        default=None,
        choices=[
            None,
            "relevancy",
            "date_down",
            "date_up",
            "price_up",
            "price_down",
            "floor_area_down",
            "plot_area_down",
            "city_up" "postal_code_up",
        ],
    )
    parser.add_argument(
        "--raw_data",
        action="store_true",
        help="Indicate whether you want the raw scraping result",
    )
    parser.add_argument(
        "--save",
        action="store_true",
        help="Indicate whether you want to save the data",
    )

    args = parser.parse_args()
    scraper = FundaScraper(
        area=args.area,
        want_to=args.want_to,
        find_past=args.find_past,
        page_start=args.page_start,
        number_of_pages=args.number_of_pages,
        min_price=args.min_price,
        max_price=args.max_price,
        days_since=args.days_since,
        sort=args.sort,
    )
    df = scraper.run(raw_data=args.raw_data, save=args.save)
    print(df.head())
