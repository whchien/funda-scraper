"""Main funda scraper module"""

import argparse
import datetime
import json
import multiprocessing as mp
import os
from collections import OrderedDict
from typing import List, Optional, Dict, Literal
import pickle

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from tqdm.contrib.concurrent import process_map

from funda_scraper.config.core import config
from funda_scraper.preprocess import preprocess_data
from funda_scraper.utils import logger, get_cookies, COOKIE_PATH


class FundaScraper(object):
    """
    A class used to scrape real estate data from the Funda website.
    """

    def __init__(
        self,
        area: str,
        want_to: Literal["buy", "rent"],
        page_start: int = 1,
        n_pages: int = 1,
        find_past: bool = False,
        min_price: Optional[int] = None,
        max_price: Optional[int] = None,
        days_since: Optional[int] = None,
        property_type: Optional[str] = None,
        min_floor_area: Optional[str] = None,
        max_floor_area: Optional[str] = None,
        sort: Optional[str] = None,
    ):
        """

        :param area: The area to search for properties, formatted for URL compatibility.
        :param want_to: Specifies whether the user wants to buy or rent properties.
        :param page_start: The starting page number for the search.
        :param n_pages: The number of pages to scrape.
        :param find_past: Flag to indicate whether to find past listings.
        :param min_price: The minimum price for the property search.
        :param max_price: The maximum price for the property search.
        :param days_since: The maximum number of days since the listing was published.
        :param property_type: The type of property to search for.
        :param min_floor_area: The minimum floor area for the property search.
        :param max_floor_area: The maximum floor area for the property search.
        :param sort: The sorting criterion for the search results.
        """
        # Init attributes
        self.area = area.lower().replace(" ", "-")
        self.property_type = property_type
        self.want_to = want_to
        self.find_past = find_past
        self.page_start = max(page_start, 1)
        self.n_pages = max(n_pages, 1)
        self.page_end = self.page_start + self.n_pages - 1
        self.min_price = min_price
        self.max_price = max_price
        self.days_since = days_since
        self.min_floor_area = min_floor_area
        self.max_floor_area = max_floor_area
        self.sort = sort

        # Instantiate along the way
        self.links: List[str] = []
        self.raw_df = pd.DataFrame()
        self.clean_df = pd.DataFrame()
        self.base_url = config.base_url
        
        # Get css selector
        if self.want_to == "buy": self.selectors = config.buy_css_selector
        elif self.want_to == "rent": self.selectors = config.rent_css_selector 
        else: raise NotImplementedError(
            f"CSS selectors are not implemented for 'want_to' {self.want_to}")

        # Get cookies
        try:
            with open(COOKIE_PATH.joinpath("cookies.pkl").__str__(), "rb") as file:
                self.cookies = pickle.load(file)
        except FileNotFoundError:
            self.cookies = get_cookies()

        self.requests_session = self._get_requests_session(self.cookies)


    def __repr__(self):
        return (
        f"FundaScraper(area={self.area}, want_to={self.want_to}, n_pages={self.n_pages}, "
        f"page_start={self.page_start}, find_past={self.find_past}, min_price={self.min_price}, "
        f"max_price={self.max_price}, days_since={self.days_since}, min_floor_area={self.min_floor_area}, "
        f"max_floor_area={self.max_floor_area}, sort={self.sort})"
    )


    @property
    def to_buy(self) -> bool:
        """Determines if the search is for buying or renting properties."""
        if self.want_to.lower() in ["buy", "koop", "b", "k"]:
            return True
        elif self.want_to.lower() in ["rent", "huur", "r", "h"]:
            return False
        else:
            raise ValueError("'want_to' must be either 'buy' or 'rent'.")


    @property
    def check_days_since(self) -> int:
        """Validates the 'days_since' attribute."""
        if self.find_past:
            raise ValueError("'days_since' can only be specified when find_past=False.")

        if self.days_since in [None, 1, 3, 5, 10, 30]:
            return self.days_since
        else:
            raise ValueError("'days_since' must be either None, 1, 3, 5, 10 or 30.")


    @property
    def check_sort(self) -> Optional[str]:
        """Validates the 'sort' attribute."""
        if self.sort in [
            None,
            "relevancy",
            "date_down",
            "date_up",
            "price_up",
            "price_down",
            "floor_area_down",
            "plot_area_down",
            "city_up" "postal_code_up",
        ]:
            return self.sort
        else:
            raise ValueError(
                "'sort' must be either None, 'relevancy', 'date_down', 'date_up', 'price_up', 'price_down', "
                "'floor_area_down', 'plot_area_down', 'city_up' or 'postal_code_up'. "
            )


    @staticmethod
    def _check_dir() -> None:
        """Ensures the existence of the directory for storing data."""
        if not os.path.exists("data"):
            os.makedirs("data")


    @staticmethod
    def _get_requests_session(cookies : List[Dict]) -> requests.Session:
        """Return a request session instance with given cookies."""
        session = requests.Session()
        for cookie in cookies:
            session.cookies.set(
                cookie["name"], 
                cookie["value"], 
                domain=cookie["domain"], 
                path=cookie["path"])
        return session 


    @staticmethod
    def _get_soup(session : requests.Session, link) -> BeautifulSoup:
        """Fetches the page content and returns a BeautifulSoup object."""
        try:
            response = session.get(link, headers=config.header)
            response.raise_for_status()
            return BeautifulSoup(response.text, "lxml")
        except requests.RequestException as e:
            logger.error(f"Error fetching {link}: {e}")
            return None


    @staticmethod
    def get_value_from_css(
        soup: BeautifulSoup, 
        selector: str, 
        attribute: str = None, 
        property: str = "text"
    ) -> str:
        """Extracts data from HTML using a CSS selector."""
        result = soup.select(selector)
        if len(result) > 0:
            if attribute:
                result = result[0].get(attribute)
            elif property == "text":
                result = result[0].text
            else:
                raise NotImplementedError("Property not implemented")
        else:
            result = "na"
        return result
    

    @staticmethod
    def remove_duplicates(lst: List[str]) -> List[str]:
        """Removes duplicate links from a list."""
        return list(OrderedDict.fromkeys(lst))


    @staticmethod
    def remove_link_overlap(first_url: str, second_url: str, delimiter: str = "/") -> str:
        """Removes the overlapping part in two links and returns their concatenation.

        :example:
        >>> remove_link_overlap("funda/en", "/en/something/something")
        'funda/en/something/something'
        """

        first_parts = first_url.strip(delimiter).split(delimiter)
        second_parts = second_url.strip(delimiter).split(delimiter)

        first_index = 0
        second_index = -1
        while first_parts[-1] == second_parts[first_index]:
            second_index -= 1
            first_index += 1

        return delimiter.join(first_parts + second_parts[first_index:])


    @staticmethod
    def fix_link(link: str) -> str:
        """Fixes a given property link to ensure proper URL formatting."""

        return link + "?old_ldp=true"

    def _get_links_from_one_parent(self, url: str) -> List[str]:
        """Scrapes all available property links from a single Funda search page."""
        try:
            response = self.requests_session.get(url, headers=config.header, timeout=10)
            response.raise_for_status() 
        except requests.exceptions.RequestException as e:
            raise ConnectionError(f"Failed to fetch URL {url}: {e}")

        if "Je bent bijna op de pagina die je zoekt" in response.text:
            raise ConnectionError("Captcha blocking the page, try refreshing cookies")

        soup = BeautifulSoup(response.text, "lxml")

        # Find housing links
        script_tag = soup.find("script", {"type": "application/ld+json"})
        if script_tag is None or not script_tag.contents:
            raise ValueError("No JSON-LD script tag found on the page")

        try:
            json_data = json.loads(script_tag.string)
            urls = [item["url"] for item in json_data.get("itemListElement", [])]
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            raise ValueError(f"Failed to parse JSON data: {e}")

        return urls


    def reset(
        self,
        area: Optional[str] = None,
        property_type: Optional[str] = None,
        want_to: Optional[str] = None,
        page_start: Optional[int] = None,
        n_pages: Optional[int] = None,
        find_past: Optional[bool] = None,
        min_price: Optional[int] = None,
        max_price: Optional[int] = None,
        days_since: Optional[int] = None,
        min_floor_area: Optional[str] = None,
        max_floor_area: Optional[str] = None,
        sort: Optional[str] = None,
    ) -> None:
        """
        Updates search parameters with new values if provided.

        Parameters are only updated if a new value is given (i.e., not `None`).
        
        Ensures `page_start` and `n_pages` are at least 1.
        """

        params = {
            "area": area,
            "property_type": property_type,
            "want_to": want_to,
            "page_start": max(page_start, 1) if page_start is not None else None,
            "n_pages": max(n_pages, 1) if n_pages is not None else None,
            "find_past": find_past,
            "min_price": min_price,
            "max_price": max_price,
            "days_since": days_since,
            "min_floor_area": min_floor_area,
            "max_floor_area": max_floor_area,
            "sort": sort,
        }

        for key, value in params.items():
            if value is not None:
                setattr(self, key, value)


    def fetch_last_available_page(self, link : str) -> int:
        """Fetch the last available paginated link on a webpage.
        
        :example:
        >>> # 49 is last page in the search
        >>> fetch_last_available_page("https/something/something")
        49 
        """

        try:
            response = self.requests_session.get(link, headers=config.header, timeout=10)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise ConnectionError(f"Failed to fetch URL {link}: {e}")

        return 1
    
    
    def fetch_all_links(self, page_start: int = None, n_pages: int = None) -> None:
        """Collects all available property links across multiple pages."""

        logger.info("*** Phase 1: Fetch all the available links from all pages ***")

        urls = []
        main_url = self._build_main_query_url()

        page_start = page_start or self.page_start
        n_pages = n_pages or self.n_pages
        last_page_fetched = page_start  # Track the last valid page

        for i in tqdm(range(page_start, page_start + n_pages)):
            try:
                item_list = self._get_links_from_one_parent(f"{main_url}&search_result={i}")
                urls += item_list
                last_page_fetched = i  # Only update if successful
            except IndexError:
                logger.info(f"*** The last available page is {last_page_fetched} ***")
                break  # Stop fetching when no more pages exist

        self.page_end = last_page_fetched
        self.links = [self.fix_link(url) for url in self.remove_duplicates(urls)]

        logger.info(
            f"*** Got all the urls. {len(self.links)} houses found from {page_start} to {self.page_end} ***"
        )


    def _build_main_query_url(self) -> str:
        """Constructs the main query URL for the search."""
        query = "koop" if self.to_buy else "huur"

        main_url = (
            f"{self.base_url}/zoeken/{query}?selected_area=%5B%22{self.area}%22%5D"
        )

        if self.property_type:
            property_types = self.property_type.split(",")
            formatted_property_types = [
                "%22" + prop_type + "%22" for prop_type in property_types
            ]
            main_url += f"&object_type=%5B{','.join(formatted_property_types)}%5D"

        if self.find_past:
            main_url = f'{main_url}&availability=%5B"unavailable"%5D'

        if self.min_price is not None or self.max_price is not None:
            min_price = "" if self.min_price is None else self.min_price
            max_price = "" if self.max_price is None else self.max_price
            main_url = f"{main_url}&price=%22{min_price}-{max_price}%22"

        if self.days_since is not None:
            main_url = f"{main_url}&publication_date={self.check_days_since}"

        if self.min_floor_area or self.max_floor_area:
            min_floor_area = "" if self.min_floor_area is None else self.min_floor_area
            max_floor_area = "" if self.max_floor_area is None else self.max_floor_area
            main_url = f"{main_url}&floor_area=%22{min_floor_area}-{max_floor_area}%22"

        if self.sort is not None:
            main_url = f"{main_url}&sort=%22{self.check_sort}%22"

        logger.info(f"*** Main URL: {main_url} ***")
        return main_url


    def scrape_one_link(self, link: str) -> List[str]:
        """Scrapes data from a single property link."""

        # Fetch page content safely
        soup = self._get_soup(self.requests_session, link)
        if not soup:
            return [] 

        # Extract values using CSS selectors
        css_selector_dict = {
            key : val 
            for section in self.selectors.values() 
            for key, val in section.items()
        }
        
        result = {key : "na" for key in css_selector_dict.keys()}
        for key, selector in css_selector_dict.items():
            attribute = "href" if "link" in key else None
            value = self.get_value_from_css(soup, selector, attribute=attribute)

            # Fix relative links
            if attribute == "href" and value:
                value = self.remove_link_overlap(self.base_url, value)
            
            result[key] = value
        
        # Clean up the retried result from one page
        result["url"] = link
        result = {key : val.replace("\n", "").replace("\r", "").strip() for key, val in result.items()}
        
        return result


    def scrape_pages(self) -> None:
        """Scrapes data from all collected property links."""

        logger.info("*** Phase 2: Start scraping from individual links ***")
        self.keys = [key for section in self.selectors.values() for key in section.keys()]
        df = pd.DataFrame({key: [] for key in self.keys})

        # TODO: use asyncio instead
        pools = mp.cpu_count()
        content = process_map(self.scrape_one_link, self.links, max_workers=pools)

        for _, c in enumerate(content):
            df.loc[len(df)] = c

        try:
            df["city"] = df["url"].map(lambda x: x.split("/")[6])
        except IndexError:
            df["city"] = self.area    
        df["log_id"] = datetime.datetime.now().strftime("%Y%m-%d%H-%M%S")
        
        logger.info(f"*** All scraping done: {df.shape[0]} results ***")
        self.raw_df = df


    def save_csv(self, df: pd.DataFrame, filepath: str = None) -> None:
        """Saves the scraped data to a CSV file."""
        if filepath is None:
            self._check_dir()
            date = str(datetime.datetime.now().date()).replace("-", "")
            status = "unavailable" if self.find_past else "unavailable"
            want_to = "buy" if self.to_buy else "rent"
            filepath = f"./data/houseprice_{date}_{self.area}_{want_to}_{status}_{len(self.links)}.csv"
        df.to_csv(filepath, index=False)
        logger.info(f"*** File saved: {filepath}. ***")


    def run(
        self, raw_data: bool = False, save: bool = False, filepath: str = None
    ) -> pd.DataFrame:
        """
        Runs the full scraping process, optionally saving the results to a CSV file.

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
        "--n_pages", type=int, help="Specify how many pages to scrape", default=1
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
        n_pages=args.n_pages,
        min_price=args.min_price,
        max_price=args.max_price,
        days_since=args.days_since,
        sort=args.sort,
    )
    df = scraper.run(raw_data=args.raw_data, save=args.save)
    print(df.head())
