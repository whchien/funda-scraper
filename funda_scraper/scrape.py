"""Main funda scraper module"""

import argparse
import datetime
import json
import time
import re
import multiprocessing as mp
import os
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

class SearchRequest(object):
    def __init__(
        self,
        area: str,
        want_to: str,
        page_start: int = 1,
        n_pages: int = 1,
        find_past: bool = False,
        download_photos: bool = False,
        min_price: Optional[int] = None,
        max_price: Optional[int] = None,
        days_since: Optional[int] = None,
        property_type: Optional[str] = None,
        min_floor_area: Optional[str] = None,
        max_floor_area: Optional[str] = None,
        min_plot_area: Optional[str] = None,
        max_plot_area: Optional[str] = None,
        sort: Optional[str] = None,
    ):
        """

        :param area: The area to search for properties, this can be a comma-seperated list, formatted for URL compatibility.
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
        :param min_plot_area: The minimum plot area for the property search.
        :param max_plot_area: The maximum plot area for the property search.
        :param sort: The sorting criterion for the search results.
        """
        # Init attributes
        self.area = area.lower().replace(" ", "-").replace(",","\",\"") #added functionality to add multiple cities, seperated by ', '
        self.property_type = property_type
        self.want_to = want_to
        self.find_past = find_past
        self.download_photos = download_photos
        self.page_start = max(page_start, 1)
        self.n_pages = max(n_pages, 1)
        self.page_end = self.page_start + self.n_pages - 1
        self.min_price = min_price
        self.max_price = max_price
        self.days_since = days_since
        self.min_floor_area = min_floor_area
        self.max_floor_area = max_floor_area
        self.min_plot_area = min_plot_area
        self.max_plot_area = max_plot_area
        self.sort = sort
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
    def check_sort(self) -> str:
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
            "city_up",
            "postal_code_up",
        ]:
            return self.sort
        else:
            raise ValueError(
                "'sort' must be either None, 'relevancy', 'date_down', 'date_up', 'price_up', 'price_down', "
                "'floor_area_down', 'plot_area_down', 'city_up' or 'postal_code_up'. "
            )

    def reset(
        self,
        area: Optional[str] = None,
        property_type: Optional[str] = None,
        want_to: Optional[str] = None,
        page_start: Optional[int] = None,
        n_pages: Optional[int] = None,
        download_photos: Optional[bool] = None,
        find_past: Optional[bool] = None,
        min_price: Optional[int] = None,
        max_price: Optional[int] = None,
        days_since: Optional[int] = None,
        min_floor_area: Optional[str] = None,
        max_floor_area: Optional[str] = None,
        min_plot_area: Optional[str] = None,
        max_plot_area: Optional[str] = None,
        sort: Optional[str] = None,
    ) -> None:
        """Resets or initializes the search parameters."""
        if area is not None:
            self.area = area
        if property_type is not None:
            self.property_type = property_type
        if want_to is not None:
            self.want_to = want_to
        if page_start is not None:
            self.page_start = max(page_start, 1)
        if n_pages is not None:
            self.n_pages = max(n_pages, 1)
        if download_photos is not None:
            self.download_photos = download_photos
        if find_past is not None:
            self.find_past = find_past
        if min_price is not None:
            self.min_price = min_price
        if max_price is not None:
            self.max_price = max_price
        if days_since is not None:
            self.days_since = days_since
        if min_floor_area is not None:
            self.min_floor_area = min_floor_area
        if max_floor_area is not None:
            self.max_floor_area = max_floor_area
        if min_plot_area is not None:
            self.min_plot_area = min_plot_area
        if max_plot_area is not None:
            self.max_plot_area = max_plot_area
        if sort is not None:
            self.sort = sort


class FundaScraper(object):
    """
    A class used to scrape real estate data from the Funda website.
    """

    def __init__(
        self,
        search_request: SearchRequest
    ):
        """

        :param area: The area to search for properties, this can be a comma-seperated list, formatted for URL compatibility.
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
        :param min_plot_area: The minimum plot area for the property search.
        :param max_plot_area: The maximum plot area for the property search.
        :param sort: The sorting criterion for the search results.
        """
        # Init attributes
        self.search_request = search_request

        # Instantiate along the way
        self.links: List[str] = []
        self.raw_df = pd.DataFrame()
        self.clean_df = pd.DataFrame()
        self.base_url = config.base_url
        self.selectors = config.css_selector

    def __repr__(self):
        return str(self.search_request)

    @staticmethod
    def _check_dir() -> None:
        date = str(datetime.datetime.now().date()).replace("-", "")
        """Ensures the existence of the directory for storing data."""
        if not os.path.exists("data"):
            os.makedirs("data")
        if not os.path.exists(f"data/{date}"):
            os.makedirs(f"data/{date}")

    @staticmethod
    def _get_links_from_one_parent(url: str) -> List[str]:
        """Scrapes all available property links from a single Funda search page."""
        response = requests.get(url, headers=config.header)
        soup = BeautifulSoup(response.text, "lxml")
        script_tag = soup.find_all("script", string=lambda t: t and "window.__NUXT__" in t)[0]
        script_content = script_tag.string
        ids = re.findall(r',_id:"(\d+)"', script_content)
        return ids

    @staticmethod
    def remove_duplicates(lst: List[str]) -> List[str]:
        """Removes duplicate links from a list."""
        return list(OrderedDict.fromkeys(lst))

    @staticmethod
    def gen_link(link: str) -> str:
        """Generates a given property link."""
        gen_url = (
            f"https://listing-detail-page.funda.io/api/v1/listing/nl/{link}"
        )
        return gen_url

    def fetch_all_links(self, page_start: int = None, n_pages: int = None) -> None:
        """Collects all available property links across multiple pages."""

        page_start = self.search_request.page_start if page_start is None else page_start
        n_pages = self.search_request.n_pages if n_pages is None else n_pages

        logger.info("*** Phase 1: Fetch all the available links from all pages *** ")
        ids = []
        main_url = self._build_main_query_url()

        for i in tqdm(range(page_start, page_start + n_pages)):
            try:
                item_list = self._get_links_from_one_parent(
                    f"{main_url}&search_result={i}"
                )
                ids += item_list
                time.sleep(.2)
            except IndexError:
                self.page_end = i
                logger.info(f"*** The last available page is {self.page_end} ***")
                break

        ids = self.remove_duplicates(ids)
        gen_urls = [self.gen_link(id) for id in ids]

        logger.info(
            f"*** Got all the urls. {len(gen_urls)} houses found from {self.search_request.page_start} to {self.search_request.page_end} ***"
        )
        self.links = gen_urls

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
        
        if self.search_request.min_plot_area or self.search_request.max_plot_area:
            min_plot_area = "" if self.search_request.min_plot_area is None else self.search_request.min_plot_area
            max_plot_area = "" if self.search_request.max_plot_area is None else self.search_request.max_plot_area
            main_url = f"{main_url}&plot_area=%22{min_plot_area}-{max_plot_area}%22"

        if self.search_request.min_floor_area or self.search_request.max_floor_area:
            min_floor_area = "" if self.search_request.min_floor_area is None else self.search_request.min_floor_area
            max_floor_area = "" if self.search_request.max_floor_area is None else self.search_request.max_floor_area
            main_url = f"{main_url}&floor_area=%22{min_floor_area}-{max_floor_area}%22"

        if self.search_request.sort is not None:
            main_url = f"{main_url}&sort=%22{self.search_request.check_sort}%22"

        logger.info(f"*** Main URL: {main_url} ***")
        return main_url

#    @staticmethod
#    def get_value_from_css(soup: BeautifulSoup, selector: str) -> str:
#        """Extracts data from HTML using a CSS selector."""
#        result = soup.select(selector)
#        if len(result) > 0:
#            result = result[0].text
#        else:
#            result = "na"
#        return result
    
    @staticmethod
    def extract_value(data, target_key, target_values=None):
        if isinstance(data, dict):
            for key, value in data.items():
                if key == target_key and (target_values is None or (isinstance(target_values, list) and any(value == tv for tv in target_values)) or value == target_values):
                    return data.get("Value", value)
                if isinstance(value, (dict, list)):
                    result = FundaScraper.extract_value(value, target_key, target_values)
                    if result != "na":
                        return result
        elif isinstance(data, list):
            for item in data:
                result = FundaScraper.extract_value(item, target_key, target_values)
                if result != "na":
                    return result
        return "na"
    

    def scrape_one_link(self, link: str) -> List[str]:
        """Scrapes data from a single property link."""

        # Initialize for each page
        date = str(datetime.datetime.now().date()).replace("-", "")
        response = requests.get(link, headers=config.header)
        json_data = response.json()
        id = link.rsplit('/', 1)[1]
        os.makedirs(f"data/{date}/{id}",exist_ok=True)
        with open(f"data/{date}/{id}/{id}.json", 'w') as json_file:
            json.dump(json_data, json_file, indent=4)
        

        # Get the value according to respective json selectors
        if self.search_request.to_buy:
            if self.search_request.find_past:
                list_since_selector = self.selectors.date_list
            else:
                list_since_selector = self.selectors.listed_since
        else:
            if self.search_request.find_past:
                list_since_selector = ".fd-align-items-center:nth-child(9) span"
            else:
                list_since_selector = ".fd-align-items-center:nth-child(7) span"

        result = [
            self.extract_value(json_data, self.selectors.url),
            self.extract_value(json_data, *self.selectors.price),
            self.extract_value(json_data, *self.selectors.price_m2),
            self.extract_value(json_data, *self.selectors.status),
            self.extract_value(json_data, *self.selectors.acceptance),
            self.extract_value(json_data, *self.selectors.listed_since),
            self.extract_value(json_data, *self.selectors.house_type),
            self.extract_value(json_data, *self.selectors.building_type),
            self.extract_value(json_data, self.selectors.year_built),
            self.extract_value(json_data, *self.selectors.building_roofing),
            self.extract_value(json_data, *self.selectors.building_details),
            self.extract_value(json_data, *self.selectors.volume),
            self.extract_value(json_data, self.selectors.living_area),
            self.extract_value(json_data, self.selectors.property_area),
            self.extract_value(json_data, *self.selectors.balcony_size),
            self.extract_value(json_data, *self.selectors.other_interior_area),
            self.extract_value(json_data, *self.selectors.other_exterior_area),
            self.extract_value(json_data, *self.selectors.exteriors),
            self.extract_value(json_data, self.selectors.num_of_rooms),
            self.extract_value(json_data, *self.selectors.num_of_bathrooms),
            self.extract_value(json_data, self.selectors.num_of_bedrooms),
            self.extract_value(json_data, *self.selectors.stories),
            self.extract_value(json_data, *self.selectors.layout),
            self.extract_value(json_data, self.selectors.energy_label),
            self.extract_value(json_data, *self.selectors.insulation),
            self.extract_value(json_data, *self.selectors.heating),
            self.extract_value(json_data, *self.selectors.heatedwater),
            self.extract_value(json_data, *self.selectors.heatingCV),
            self.extract_value(json_data, self.selectors.heatingAge),
            self.extract_value(json_data, self.selectors.solarpanels),
            self.extract_value(json_data, self.selectors.heatpump),
            self.extract_value(json_data, self.selectors.lowenergy),
            self.extract_value(json_data, self.selectors.street),
            self.extract_value(json_data, self.selectors.address),
            self.extract_value(json_data, self.selectors.zip_code),
            self.extract_value(json_data, self.selectors.city),
            self.extract_value(json_data, self.selectors.descrip),
            self.extract_value(json_data, *self.selectors.ownership),
            self.extract_value(json_data, *self.selectors.cadastralarea),
            self.extract_value(json_data, *self.selectors.location),
            self.extract_value(json_data, *self.selectors.garden),
            self.extract_value(json_data, *self.selectors.garden_size),
            self.extract_value(json_data, *self.selectors.gardenorientation),
            self.extract_value(json_data, *self.selectors.balcony),
            self.extract_value(json_data, *self.selectors.parking),
            self.extract_value(json_data, self.selectors.parkingownproperty),
            self.extract_value(json_data, self.selectors.enclosedparking),
            self.extract_value(json_data, self.selectors.neighborhood_name),
            self.extract_value(json_data, self.selectors.latitude),
            self.extract_value(json_data, self.selectors.longitude),
            self.extract_value(json_data, self.selectors.monument),
            self.extract_value(json_data, self.selectors.monumentstatus),
            self.extract_value(json_data, self.selectors.DIYhome),
            self.extract_value(json_data, self.selectors.leasehold),
            self.extract_value(json_data, self.selectors.term),
            self.extract_value(json_data, self.selectors.price_sold),
            self.extract_value(json_data, self.selectors.date_sold)
                    ]
        NeighourhoodId = self.extract_value(json_data,self.selectors.NeighourhoodId)
        hood_pricem2_link = f"https://marketinsights.funda.io/v2/LocalInsights/preview/{NeighourhoodId}"
        hood_data = requests.get(hood_pricem2_link, headers=config.header).json()
        hood_pricem2 = hood_data["averageAskingPricePerM2"]
        hood_families = hood_data["familiesWithChildren"]
        with open(f"data/{date}/{id}/{id}_neighbourhood.json", 'w') as json_file:
            json.dump(hood_data, json_file, indent=4)
        
        photos_list = [
            photo["PhotoUrl"] for photo in json_data["Media"]["Photos"]
        ]
        photos_string = ", ".join(photos_list)
        if self.search_request.download_photos:
            for i, url in enumerate(photos_list):
                response = requests.get(url)
                if response.status_code == 200:
                    with open(f'data/{date}/{id}/{id}_photo_{i+1}.jpg', 'wb') as file:
                        file.write(response.content)
                else:
                    print(f'Failed to download {url}')
        else:
            None    
                
        
        # Clean up the retried result from one page
        result = [r.replace("\n", "").replace("\r", "").strip() if r is not None and not isinstance(r, float) else r for r in result]
        result.append(NeighourhoodId)
        result.append(photos_string)
        result.append(hood_pricem2)
        result.append(hood_families)
        
        # Adding surroundings data from open API
        # first get RDY and RDX 
        lat=self.extract_value(json_data, self.selectors.latitude)
        lon=self.extract_value(json_data, self.selectors.longitude)
        rd_link = f"https://api.pdok.nl/bzk/locatieserver/search/v3_1/free?lat={lat}&lon={lon}&fl=id%20identificatie%20bron%20type%20straatnaam%20huisnummer%20huisletter%20huisnummertoevoeging%20postcode%20centroide_ll%20centroide_rd%20score&fq=type%3A%28adres%29&bq=type%3Aadres%5E1&start=0&rows=1&sort=score%20desc&wt=json"
        rd_data = requests.get(rd_link, headers=config.header).json()
        centroid = rd_data["response"]["docs"][0]["centroide_rd"]
        rdy = int(650000 - float(centroid.split()[1].strip(')')))
        rdx = int(float(centroid.split()[0].strip('POINT(')))
        with open(f"data/{date}/{id}/{id}_rd.json", 'w') as json_file:
            json.dump(rd_data, json_file, indent=4)
        
        #now get 'polution', 'sound-levels', 'no2'
        sound_link = f"https://data.rivm.nl/geo/alo/wms?SERVICE=WMS&VERSION=1.3.0&REQUEST=GetFeatureInfo&LAYERS=rivm_20220601_Geluid_lden_allebronnen_2020&QUERY_LAYERS=rivm_20220601_Geluid_lden_allebronnen_2020&BBOX=0,300000,300000,650000&WIDTH=300000&HEIGHT=350000&FEATURE_COUNT=1&INFO_FORMAT=application/json&CRS=EPSG:28992&i={rdx}&j={rdy}"
        ppm25_link = f"https://data.rivm.nl/geo/alo/wms?SERVICE=WMS&VERSION=1.3.0&REQUEST=GetFeatureInfo&LAYERS=rivm_nsl_20240401_gm_PM252022&QUERY_LAYERS=rivm_nsl_20240401_gm_PM252022&BBOX=0,300000,300000,650000&WIDTH=300000&HEIGHT=350000&FEATURE_COUNT=1&INFO_FORMAT=application/json&CRS=EPSG:28992&i={rdx}&j={rdy}"
        no2_link = f"https://data.rivm.nl/geo/alo/wms?SERVICE=WMS&VERSION=1.3.0&REQUEST=GetFeatureInfo&LAYERS=rivm_nsl_20240401_gm_NO22022&QUERY_LAYERS=rivm_nsl_20240401_gm_NO22022&BBOX=0,300000,300000,650000&WIDTH=300000&HEIGHT=350000&FEATURE_COUNT=1&INFO_FORMAT=application/json&CRS=EPSG:28992&i={rdx}&j={rdy}"
        sound_data = requests.get(sound_link, headers=config.header).json()
        ppm25_data = requests.get(ppm25_link, headers=config.header).json()
        no2_data = requests.get(no2_link, headers=config.header).json()
        sound = round(sound_data["features"][0]["properties"]["GRAY_INDEX"],2)
        ppm25 = round(ppm25_data["features"][0]["properties"]["GRAY_INDEX"],2)
        no2 = round(no2_data["features"][0]["properties"]["GRAY_INDEX"],2)
        with open(f"data/{date}/{id}/{id}_ppm25.json", 'w') as json_file:
            json.dump(ppm25_data, json_file, indent=4)
        with open(f"data/{date}/{id}/{id}_no2.json", 'w') as json_file:
            json.dump(no2_data, json_file, indent=4)
        with open(f"data/{date}/{id}/{id}_sound.json", 'w') as json_file:
            json.dump(sound_data, json_file, indent=4)
        result.append(sound)
        result.append(ppm25)
        result.append(no2)
        
        return result

    def scrape_pages(self) -> None:
        """Scrapes data from all collected property links."""

        logger.info("*** Phase 2: Start scraping from individual links ***")
        df = pd.DataFrame({key: [] for key in self.selectors.keys()})

        # Scrape pages with multiprocessing to improve efficiency
        # TODO: use asyncio instead
        pools = mp.cpu_count()
        content = process_map(self.scrape_one_link, self.links, max_workers=3)

        for i, c in enumerate(content):
            df.loc[len(df)] = c

        df["log_id"] = datetime.datetime.now().strftime("%Y%m-%d%H-%M%S")
        if not self.search_request.find_past:
            df = df.drop(["term", "price_sold", "date_sold"], axis=1)
        logger.info(f"*** All scraping done: {df.shape[0]} results ***")
        self.raw_df = df

    def save_csv(self, df: pd.DataFrame, filepath: str = None) -> None:
        """Saves the scraped data to a CSV file."""
        if filepath is None:
            self._check_dir()
            date = str(datetime.datetime.now().date()).replace("-", "")
            status = "unavailable" if self.search_request.find_past else "unavailable"
            area = self.search_request.area.replace("\",\"", "")[:20]
            want_to = "buy" if self.search_request.to_buy else "rent"
            filepath = f"./data/houseprice_{date}_{area}_{want_to}_{status}_{len(self.links)}.csv"
        df.to_csv(filepath, index=False)
        logger.info(f"*** File saved: {filepath}. ***")

    def run(
        self, raw_data: bool = False, save: bool = False, download_photos: bool = False, filepath: str = None
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
            df = preprocess_data(df=self.raw_df, is_past=self.search_request.find_past)
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
        default="buy",
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
        "--max_plot_area", type=int, help="Specify the max plot area", default=None
    )
    parser.add_argument(
        "--max_floor_area", type=int, help="Specify the max floor area", default=None
    )
    parser.add_argument(
        "--min_plot_area", type=int, help="Specify the min plot area", default=None
    )
    parser.add_argument(
        "--min_floor_area", type=int, help="Specify the min floor area", default=None
    )
    parser.add_argument(
        "--days_since",
        type=int,
        help="Specify the days since publication",
        default=None,
    )
    parser.add_argument(
        "--property_type",
        type=str,
        help="Specify the type of property(house, land, appartment)",
        default="house",
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
        default=True,
    )
    
    parser.add_argument(
        "--download_photos",
        action="store_true",
        help="Indicate whether you want to save the data",
    )

    args = parser.parse_args()
    requestsargs = SearchRequest(
        area=args.area,
        want_to=args.want_to,
        find_past=args.find_past,
        page_start=args.page_start,
        n_pages=args.n_pages,
        min_price=args.min_price,
        max_price=args.max_price,
        min_plot_area=args.min_plot_area,
        max_plot_area=args.max_plot_area,
        min_floor_area=args.min_floor_area,
        max_floor_area=args.max_floor_area,
        property_type=args.property_type,
        days_since=args.days_since,
        download_photos=args.download_photos,
        sort=args.sort,
    )
    print(requestsargs.to_buy)
    scraper = FundaScraper(requestsargs)
    df = scraper.run(raw_data=args.raw_data, save=args.save)
    print(df.head())
