
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
from funda_scraper.filerepository import FileRepository
from funda_scraper.searchrequest import SearchRequest
from funda_scraper.property import Property


class DataExtractor(object):

    def __init__(self):
        self.selectors = config.css_selector
        self.raw_df = pd.DataFrame()
        self.clean_df = pd.DataFrame()
        self.file_repo = FileRepository()


    def extract_data(self, search_request: SearchRequest, run_id: str, clean_data: bool) -> pd.DataFrame:

        detail_pages = self.file_repo.get_detail_pages(run_id)

        houses: list[Property] = []

        for page in detail_pages:
            house = self.extract_data_from_detail_page(page, search_request)
            houses.append(house)

        #df["log_id"] = datetime.datetime.now().strftime("%Y%m-%d%H-%M%S")

        # if not search_request.find_past:
        #     df = df.drop(["term", "price_sold", "date_sold"], axis=1)

        logger.info(f"*** All scraping done: {len(houses)} results ***")

        df = pd.DataFrame([vars(house) for house in houses])

        self.raw_df = df

        if not clean_data:
            df = self.raw_df
        else:
            logger.info("*** Cleaning data ***")
            df = preprocess_data(df = self.raw_df, is_past = search_request.find_past)
            self.clean_df = df

        self.file_repo.save_result_file(df, run_id)

        return df


    def extract_data_from_detail_page(self, page: str, search_request: SearchRequest) -> Property:
        soup = BeautifulSoup(page, "lxml")

        script_tag = soup.find_all("script", {"type": "application/ld+json"})[0]
        json_data = json.loads(script_tag.contents[0])

        link = json_data["url"]
        description = json_data["description"]
        address = f"{json_data["address"]["streetAddress"]}"
        city = json_data["address"]["addressLocality"]
        price = f"{json_data["offers"]["priceCurrency"]} {json_data["offers"]["price"]}"

        # Get the value according to respective CSS selectors
        if search_request.to_buy:
            if search_request.find_past:
                list_since_selector = self.selectors.date_list
            else:
                list_since_selector = self.selectors.listed_since
        else:
            if search_request.find_past:
                list_since_selector = ".fd-align-items-center:nth-child(9) span"
            else:
                list_since_selector = ".fd-align-items-center:nth-child(7) span"

        house = Property()
        house.link = link
        house.price = price
        house.address = address
        house.city = city
        house.description = description
        house.zip_code = self.get_value_from_css(soup, self.selectors.zip_code)
        house.size = self.get_value_from_css(soup, self.selectors.size)
        house.year_of_construction = self.get_value_from_css(soup, self.selectors.year)
        house.living_area = self.get_value_from_css(soup, self.selectors.living_area)
        house.kind_of_house = self.get_value_from_css(soup, self.selectors.kind_of_house)
        house.building_type = self.get_value_from_css(soup, self.selectors.building_type)
        house.number_of_rooms = self.get_value_from_css(soup, self.selectors.num_of_rooms)
        house.number_of_bathrooms = self.get_value_from_css(soup, self.selectors.num_of_bathrooms)
        house.layout = self.get_value_from_css(soup, self.selectors.layout),
        house.energy_label = self.get_value_from_css(soup, self.selectors.energy_label)
        house.insulation = self.get_value_from_css(soup, self.selectors.insulation)
        house.heating = self.get_value_from_css(soup, self.selectors.heating)
        house.ownership = self.get_value_from_css(soup, self.selectors.ownership)
        house.exteriors = self.get_value_from_css(soup, self.selectors.exteriors)
        house.parking = self.get_value_from_css(soup, self.selectors.parking)
        house.neighborhood_name = self.get_value_from_css(soup, self.selectors.neighborhood_name)
        house.date_list = self.get_value_from_css(soup, self.selectors.date_list)
        house.date_sold = self.get_value_from_css(soup, self.selectors.date_sold)
        house.term = self.get_value_from_css(soup, self.selectors.term)
        house.price_sold = self.get_value_from_css(soup, self.selectors.price_sold)
        house.last_ask_price = self.get_value_from_css(soup, self.selectors.last_ask_price)
        house.last_ask_price_m2 = self.get_value_from_css(soup, self.selectors.last_ask_price_m2).split("\r")[0]
        house.photos = [p.get("data-lazy-srcset") for p in soup.select(self.selectors.photo)]

        # Deal with list_since_selector especially, since its CSS varies sometimes
        # if clean_date_format(result[4]) == "na":
        #     for i in range(6, 16):
        #         selector = f".fd-align-items-center:nth-child({i}) span"
        #         update_list_since = self.get_value_from_css(soup, selector)
        #         if clean_date_format(update_list_since) == "na":
        #             pass
        #         else:
        #             result[4] = update_list_since

        for key, value in house.__dict__.items():
            formatted_value = self.format_string(value)
            setattr(house, key, formatted_value)

        return house

    @staticmethod
    def get_value_from_css(soup: BeautifulSoup, selector: str) -> str:
        """Extracts data from HTML using a CSS selector."""
        result = soup.select(selector)
        if len(result) > 0:
            result = result[0].text
        else:
            result = "na"
        return result


    def format_string(self, value):
        if type(value) == "str":
            return value.replace("\n", "").replace("\r", "").strip()
        else:
            return value


