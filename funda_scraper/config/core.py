"""Configuration used for scraping """

from pathlib import Path

import yaml
from diot import Diot
from yaml.loader import SafeLoader

import funda_scraper

PACKAGE_ROOT = Path(funda_scraper.__file__).resolve().parent
CONFIG_PATH = PACKAGE_ROOT / "config/config.yaml"

with open(CONFIG_PATH) as f:
    data = yaml.load(f, Loader=SafeLoader)

config = Diot(data)
