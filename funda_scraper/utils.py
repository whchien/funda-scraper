"""Utilities for modules"""
import logging

logger = logging.getLogger("funda_scraper")
logger.setLevel(logging.INFO)

# # create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

logger.addHandler(ch)
