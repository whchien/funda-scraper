from pathlib import PosixPath

from diot import Diot

from funda_scraper.config.core import config, PACKAGE_ROOT, CONFIG_PATH


def test_config():
    assert isinstance(config, Diot)
    assert config.base_url == 'https://www.funda.nl/en'


def test_env():
    assert isinstance(PACKAGE_ROOT, PosixPath)
    assert isinstance(CONFIG_PATH, PosixPath)
