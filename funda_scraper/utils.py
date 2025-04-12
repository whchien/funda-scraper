"""Utilities for modules"""

import logging
import time
import pickle
import os
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By


COOKIE_PATH = Path(__file__).parent.joinpath("secrets")
URL = (
    "https://www.funda.nl/en/zoeken/huur?"
    + "selected_area=[%22eindhoven%22]&price=%22500-2000%22"
)
    

logger = logging.getLogger("funda_scraper")
logger.setLevel(logging.INFO)

# Create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

logger.addHandler(ch)


# Cookies fetch
def get_selenium_web_driver():
    """Create a Chrome driver instance with selenium webdriver."""
    chrome_options = Options()

    return webdriver.Chrome(options=chrome_options)


def get_cookies(cookie_path : Path = COOKIE_PATH, url : str = URL):
    """Prompt user to solve captcha puzzle on an interactive web. Get and save cookies for later requests."""
    logger.info("Prompting interactive web session to extract cookies...")
    driver = get_selenium_web_driver()
    driver.get(url)
    
    try:
        button = driver.find_element(By.ID, "didomi-notice-agree-button")
        button.click()
    except Exception as e:
        logger.info(e)
        logger.info("Unable to accept policy, wait for user manually click...")

    while "Je bent bijna op de pagina die je zoekt" in driver.page_source:
        time.sleep(5)
    
    logger.info("Captcha is solved, extracting cookies...")

    # Save cookies after solving CAPTCHA
    cookies = driver.get_cookies()
    if not os.path.isdir(cookie_path):
        os.makedirs(cookie_path)
    with open(cookie_path.joinpath("cookies.pkl").__str__(), "wb") as file:
        pickle.dump(cookies, file)

    driver.quit()

    return cookies


if __name__=="__main__":
    get_cookies()
