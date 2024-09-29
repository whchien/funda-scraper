
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


class DataExtractor(object):

    def __init__(self):
        self.selectors = config.css_selector
        self.raw_df = pd.DataFrame()
        self.clean_df = pd.DataFrame()
        self.file_repo = FileRepository()

    def extract_data(self, to_buy: bool, find_past: bool, raw_data: bool, save: bool, file_path: str, run_id: str) -> pd.DataFrame:

        df = pd.DataFrame({key: [] for key in self.selectors.keys()})
        detail_pages = self.file_repo.get_detail_pages(run_id)

        for page in detail_pages:
            page_data = self.extract_data_from_page(page, to_buy, find_past)
            df.loc[len(df)] = page_data

        df["city"] = df["url"].map(lambda x: x.split("/")[4])
        df["log_id"] = datetime.datetime.now().strftime("%Y%m-%d%H-%M%S")
        if not find_past:
            df = df.drop(["term", "price_sold", "date_sold"], axis=1)
        logger.info(f"*** All scraping done: {df.shape[0]} results ***")
        self.raw_df = df

        if raw_data:
            df = self.raw_df
        else:
            logger.info("*** Cleaning data ***")
            df = preprocess_data(df=self.raw_df, is_past=self.find_past)
            self.clean_df = df

        print(df)

        if save:
            self.save_csv(df, file_path)

        return df


    def extract_data_from_page(self, page: str, to_buy: bool, find_past: bool):
        soup = BeautifulSoup(page, "lxml")

        script_tag = soup.find_all("script", {"type": "application/ld+json"})[0]
        json_data = json.loads(script_tag.contents[0])

        link = json_data["url"]

        # Get the value according to respective CSS selectors
        if to_buy:
            if find_past:
                list_since_selector = self.selectors.date_list
            else:
                list_since_selector = self.selectors.listed_since
        else:
            if find_past:
                list_since_selector = ".fd-align-items-center:nth-child(9) span"
            else:
                list_since_selector = ".fd-align-items-center:nth-child(7) span"

        result = [
            link,
            self.get_value_from_css(soup, self.selectors.price),
            self.get_value_from_css(soup, self.selectors.address),
            self.get_value_from_css(soup, self.selectors.descrip),
            self.get_value_from_css(soup, list_since_selector),
            self.get_value_from_css(soup, self.selectors.zip_code),
            self.get_value_from_css(soup, self.selectors.size),
            self.get_value_from_css(soup, self.selectors.year),
            self.get_value_from_css(soup, self.selectors.living_area),
            self.get_value_from_css(soup, self.selectors.kind_of_house),
            self.get_value_from_css(soup, self.selectors.building_type),
            self.get_value_from_css(soup, self.selectors.num_of_rooms),
            self.get_value_from_css(soup, self.selectors.num_of_bathrooms),
            self.get_value_from_css(soup, self.selectors.layout),
            self.get_value_from_css(soup, self.selectors.energy_label),
            self.get_value_from_css(soup, self.selectors.insulation),
            self.get_value_from_css(soup, self.selectors.heating),
            self.get_value_from_css(soup, self.selectors.ownership),
            self.get_value_from_css(soup, self.selectors.exteriors),
            self.get_value_from_css(soup, self.selectors.parking),
            self.get_value_from_css(soup, self.selectors.neighborhood_name),
            self.get_value_from_css(soup, self.selectors.date_list),
            self.get_value_from_css(soup, self.selectors.date_sold),
            self.get_value_from_css(soup, self.selectors.term),
            self.get_value_from_css(soup, self.selectors.price_sold),
            self.get_value_from_css(soup, self.selectors.last_ask_price),
            self.get_value_from_css(soup, self.selectors.last_ask_price_m2).split("\r")[
                0
            ],
        ]

        # Deal with list_since_selector especially, since its CSS varies sometimes
        if clean_date_format(result[4]) == "na":
            for i in range(6, 16):
                selector = f".fd-align-items-center:nth-child({i}) span"
                update_list_since = self.get_value_from_css(soup, selector)
                if clean_date_format(update_list_since) == "na":
                    pass
                else:
                    result[4] = update_list_since

        photos_list = [
            p.get("data-lazy-srcset") for p in soup.select(self.selectors.photo)
        ]
        photos_string = ", ".join(photos_list)

        # Clean up the retried result from one page
        result = [r.replace("\n", "").replace("\r", "").strip() for r in result]
        result.append(photos_string)
        return result


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


    @staticmethod
    def get_value_from_css(soup: BeautifulSoup, selector: str) -> str:
        """Extracts data from HTML using a CSS selector."""
        result = soup.select(selector)
        if len(result) > 0:
            result = result[0].text
        else:
            result = "na"
        return result

