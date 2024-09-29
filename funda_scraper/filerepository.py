import os
from urllib.parse import urlparse, urlunparse
from tqdm.contrib.concurrent import process_map

from typing import List, Optional

from funda_scraper.config.core import config
from funda_scraper.preprocess import clean_date_format, preprocess_data
from funda_scraper.utils import logger


class FileRepository(object):
    DATA_DIR = "data"
    LISTPAGES_DIR = 'listpages'
    DETAILPAGES_DIR = 'detailpages'

    def __init__(self) -> None:
        self._ensure_dir(self.DATA_DIR)

    def _ensure_dir(self, dir_name: str):
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)

    def get_list_pages(self, run_id: str) -> List[str]:
        pages = []

        list_pages_dir = self._get_list_pages_dir_name(run_id)

        for f in os.listdir(list_pages_dir):
            file_path = os.path.join(list_pages_dir, f)

            if os.path.isfile(file_path):
                with open(file_path, 'r') as file:
                    content = file.read()
                    pages.append(content)

        return pages

    def get_detail_pages(self, run_id: str) -> List[str]:
        pages = []

        detail_pages_dir = self._get_detail_pages_dir_name(run_id)

        for f in os.listdir(detail_pages_dir):
            file_path = os.path.join(detail_pages_dir, f)

            if os.path.isfile(file_path):
                with open(file_path, 'r') as file:
                    content = file.read()
                    pages.append(content)

        return pages

    def save_list_page(self, content: str, index: int, run_id: str):
        list_pages_dir = self._get_list_pages_dir_name(run_id)
        self._ensure_dir(list_pages_dir)

        file_path = os.path.join(list_pages_dir, f"listpage_{index}.html")

        with open(file_path, 'w') as file:
                file.write(content)

    def save_detail_page(self, content: str, index: int, run_id: str):
        detail_pages_dir = self._get_detail_pages_dir_name(run_id)
        self._ensure_dir(detail_pages_dir)

        file_path = os.path.join(detail_pages_dir, f"detailpage_{index}.html")

        with open(file_path, 'w') as file:
            file.write(content)

    def _get_list_pages_dir_name(self, run_id: str):
        return os.path.join(self.DATA_DIR, run_id, self.LISTPAGES_DIR)

    def _get_detail_pages_dir_name(self, run_id: str):
        return os.path.join(self.DATA_DIR, run_id, self.DETAILPAGES_DIR)






