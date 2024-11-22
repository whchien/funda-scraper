"""Preprocess raw data scraped from Funda"""

import re
from datetime import datetime, timedelta
from typing import List, Union

import pandas as pd
import numpy as np
from dateutil.parser import parse

from funda_scraper.config.core import config

def clean_price(x: str) -> int:
    """Clean the 'price' and transform from string to integer."""
    try:
        return int(str(x).split(" ")[1].replace(".", ""))
    except ValueError:
        return 0
    except IndexError:
        return 0

def clean_year(x: str) -> int:
    """Clean the 'year' and transform from string to integer"""
    if len(x) == 4:
        return int(x)
    elif x.find("-") != -1:
        return int(x.split("-")[0])
    elif x.find("before") != -1:
        return int(x.split(" ")[1])
    else:
        return 0

def clean_m2(x: str) -> int:
    """Clean the 'm2' and transform from string to integer"""
    try:
        return int(str(x).replace(".", "").split(" m²")[0])
    except ValueError:
        return 0
    except IndexError:
        return 0

def clean_garden(x: str) -> int:
    """Clean the 'gardensize' and transform to readable format m2"""
    try:
        pattern = r"(\d{1,5}\s{1}m2{0,1})"
        return find_keyword_from_regex(str(x).replace(" m²", " m2"), pattern)
    except ValueError:
        return 0
    except IndexError:
        return 0
    
def clean_m3(x: str) -> int:
    """Clean the 'm3' and transform from string to integer"""
    try:
        return int(str(x).replace(".", "").split(" m³")[0])
    except ValueError:
        return 0
    except IndexError:
        return 0

def find_garden_depth(x: str) -> float:
    """Find the number of bathrooms from a string"""
    pattern = r"(\d{1,2},\d{1,2}\s{1}meter diep{0,1})|(\d{1,2},\d{1,2}\s{1}metre deep{0,1})"
    return float_find_keyword_from_regex(x, pattern)

def find_garden_width(x: str) -> float:
    """Find the number of bathrooms from a string"""
    pattern = r"(\d{1,2},\d{1,2}\s{1}meter breed{0,1})|(\d{1,2},\d{1,2}\s{1}metre wide{0,1})"
    return float_find_keyword_from_regex(x, pattern)

def find_keyword_from_regex(x: str, pattern: str) -> int:
    result = re.findall(pattern, x)
    if len(result) > 0:
        result = "".join(result[0])
        x = result.split(" ")[0]
    else:
        x = 0
    return int(x)

def float_find_keyword_from_regex(x: str, pattern: str) -> float:
    result = re.findall(pattern, x)
    if len(result) > 0:
        result = "".join(result[0])
        x = result.split(" ")[0]
        x = x.replace(",", ".")
    else:
        x = 0
    return float(x)

def find_n_bathroom(x: str) -> int:
    """Find the number of bathrooms from a string"""
    pattern = r"(\d{1,2}\s{1}badkamers{0,1})|(\d{1,2}\s{1}bathrooms{0,1})"
    return find_keyword_from_regex(x, pattern)

def find_n_toilets(x: str) -> int:
    """Find the number of bathrooms from a string"""
    pattern = r"(\d{1,2}\s{1}apart{0,1})|(\d{1,2}\s{1}seperate{0,1})"
    return find_keyword_from_regex(x, pattern)

def map_dutch_month(x: str) -> str:
    """Map the month from Dutch to English."""
    month_mapping = {
        "januari": "January",
        "februari": "February",
        "maart": "March",
        "mei": "May",
        "juni": "June",
        "juli": "July",
        "augustus": "August",
        "oktober": "October",
    }
    for k, v in month_mapping.items():
        if x.find(k) != -1:
            x = x.replace(k, v)
    return x

def clean_energy_label(x: str) -> str:
    """Clean the energy labels."""
    try:
        x = x.split(" ")[0]
        if x.find("A+") != -1:
            x = ">A+"
        return x
    except IndexError:
        return x

def clean_date_format(x: str) -> Union[datetime, str]:
    """Transform the date from string to datetime object."""

    x = x.replace("weken", "week")
    x = x.replace("maanden", "month")
    x = x.replace("Vandaag", "Today")
    x = x.replace("+", "")
    x = map_dutch_month(x)

    def delta_now(d: int):
        t = timedelta(days=d)
        return datetime.now() - t

    weekdays_dict = {
        "maandag": "Monday",
        "dinsdag": "Tuesday",
        "woensdag": "Wednesday",
        "donderdag": "Thursday",
        "vrijdag": "Friday",
        "zaterdag": "Saturday",
        "zondag": "Sunday",
    }

    try:
        if x.lower() in weekdays_dict.keys():
            date_string = weekdays_dict.get(x.lower())
            parsed_date = parse(date_string, fuzzy=True)
            delta = datetime.now().weekday() - parsed_date.weekday()
            x = delta_now(delta)

        elif x.find("month") != -1:
            x = delta_now(int(x.split("month")[0].strip()[0]) * 30)
        elif x.find("week") != -1:
            x = delta_now(int(x.split("week")[0].strip()[0]) * 7)
        elif x.find("Today") != -1:
            x = delta_now(1)
        elif x.find("day") != -1:
            x = delta_now(int(x.split("day")[0].strip()))
        else:
            x = datetime.strptime(x, "%d %B %Y")
        return x

    except ValueError:
        return "na"


def preprocess_data(
    df: pd.DataFrame, is_past: bool, keep_extra_cols: List[str] = None
) -> pd.DataFrame:
    """
    Clean the raw dataframe from scraping.
    Indicate whether the historical data is included since the columns would be different.

    :param df: raw dataframe from scraping
    :param is_past: whether it scraped past data
    :param keep_extra_cols: specify additional column names to keep in the final df
    :return: clean dataframe
    """

    df = df.dropna()
    if not is_past:
        keep_cols = config.keep_cols.selling_data
    else:
        keep_cols = config.keep_cols.selling_data + config.keep_cols.sold_data

    if keep_extra_cols is not None:
        keep_cols.extend(keep_extra_cols)

    # Info
    df["house_id"] = df["url"].apply(lambda x: int(x.split("/")[-2].split("-")[1]))
    df["house_type"] = df["url"].apply(lambda x: x.split("/")[-2].split("-")[0])
    df = df[df["house_type"].isin(["appartement", "huis"])]

    # Areas
    df["volume"] = df["volume"].apply(clean_m3)
    df["balcony_size"] = df["balcony_size"].apply(clean_m2)
    df["other_interior_area"] = df["other_interior_area"].apply(clean_m2)
    df["other_exterior_area"] = df["other_exterior_area"].apply(clean_m2)
    df["cadastralarea"] = df["cadastralarea"].apply(clean_m2)
    df["exteriors"] = df["exteriors"].apply(clean_m2)
    df['property_area'] = df['property_area'].apply(clean_m2)
    df["living_area"] = df["living_area"].apply(clean_m2)
    df["garden_width"] = df["garden_size"].apply(find_garden_width) # first before cleaning garden_size
    df["garden_depth"] = df["garden_size"].apply(find_garden_depth) # first before cleaning garden_size
    df["garden_size"] = df["garden_size"].apply(clean_garden)
    # Price
    price_col = "price_sold" if is_past else "price"
    df["price"] = df[price_col].apply(clean_price)
    df = df[df["price"] != 0]
    df = df[df["living_area"] != 0]
    df["price_m2"] = round(df.price / df.living_area, 1)
    df["price_m2grond"] = round(df.price / df.property_area,1)
    df["price_m2grond"] = df["price_m2grond"].replace(np.inf, 0)

    # Location
    df["zip"] = df["zip_code"].apply(lambda x: x[:7])

    # House layout
    df["rooms"] = df["num_of_rooms"]
    df["bedroom"] = df["num_of_bedrooms"]
    df["bathroom"] = df["num_of_bathrooms"].apply(find_n_bathroom)
    df["toilets"] = df["num_of_bathrooms"].apply(find_n_toilets)
    df["energy_label"] = df["energy_label"].apply(clean_energy_label)

    # Time
    df["year_built"] = df["year_built"].apply(clean_year).astype(int)
    df["house_age"] = datetime.now().year - df["year_built"]

    if is_past:
        # Only check past data
        df = df[df["date_sold"] != "na"]
        df["date_sold"] = df["date_sold"].apply(clean_date_format)
        df = df.dropna()
        df["date_sold"] = pd.to_datetime(df["date_sold"])
        df["ym_sold"] = df["date_sold"].apply(lambda x: x.to_period("M").to_timestamp())
        df["year_sold"] = df["date_sold"].apply(lambda x: x.year)

    return df[keep_cols].reset_index(drop=True)
