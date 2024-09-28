import argparse
import datetime
import json
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


class FileRepository(object):
    LISTPAGES_DIR = 'data/listpages'
    DETAILPAGES_DIR = 'data/detailpages'

    def __init__(self) -> None:
        self._ensure_dir(self.LISTPAGES_DIR)
        self._ensure_dir(self.DETAILPAGES_DIR)

    def _ensure_dir(self, dir_name: str):
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)

    def get_list_pages(self) -> List[str]:
        pages = []

        for f in os.listdir(self.LISTPAGES_DIR):
            file_path = os.path.join(self.LISTPAGES_DIR, f)

            if os.path.isfile(file_path):
                with open(file_path, 'r') as file:
                    content = file.read()
                    pages.append(content)

        return pages

    def get_detail_pages(self) -> List[str]:
        pages = []

        for f in os.listdir(self.DETAILPAGES_DIR):
            file_path = os.path.join(self.DETAILPAGES_DIR, f)

            if os.path.isfile(file_path):
                with open(file_path, 'r') as file:
                    content = file.read()
                    pages.append(content)

        return pages

    def save_list_page(self, content: str, index: int):
        with open(f'./data/listpages/listpage_{index}.html', 'w') as file:
                file.write(content)

    def save_detail_page(self, content: str, index: int):
        with open(f'./data/detailpages/detailpage_{index}.html', 'w') as file:
            file.write(content)





