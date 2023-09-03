"""Preprocess raw data scraped from Funda"""
import re
from datetime import datetime, timedelta
from typing import Union

import pandas as pd
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


def clean_living_area(x: str) -> int:
    """Clean the 'living_area' and transform from string to integer"""
    try:
        return int(str(x).replace(",", "").split(" m²")[0])
    except ValueError:
        return 0
    except IndexError:
        return 0


def find_keyword_from_regex(x: str, pattern: str) -> int:
    result = re.findall(pattern, x)
    if len(result) > 0:
        result = "".join(result[0])
        x = result.split(" ")[0]
    else:
        x = 0
    return int(x)


def find_n_room(x: str) -> int:
    """Find the number of rooms from a string"""
    pattern = r"(\d{1,2}\s{1}kamers{0,1})|(\d{1,2}\s{1}rooms{0,1})"
    return find_keyword_from_regex(x, pattern)


def find_n_bedroom(x: str) -> int:
    """Find the number of bedrooms from a string"""
    pattern = r"(\d{1,2}\s{1}slaapkamers{0,1})|(\d{1,2}\s{1}bedrooms{0,1})"
    return find_keyword_from_regex(x, pattern)


def find_n_bathroom(x: str) -> int:
    """Find the number of bathrooms from a string"""
    pattern = r"(\d{1,2}\s{1}badkamers{0,1})|(\d{1,2}\s{1}bathrooms{0,1})"
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


def get_neighbor(x: str) -> str:
    """Find the neighborhood name."""
    city = x.split("/")[0].replace("-", " ")
    return x.lower().split(city)[-1]


def clean_energy_label(x: str) -> str:
    """Clean the energy labels."""
    try:
        x = x.split(" ")[0]
        if x.find("A+") != -1:
            x = ">A+"
        return x
    except IndexError:
        return x


def clean_list_date(x: str) -> Union[datetime, str]:
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


def preprocess_data(df: pd.DataFrame, is_past: bool) -> pd.DataFrame:
    """
    Clean the raw dataframe from scraping.
    Indicate whether the historical data is included since the columns would be different.

    :param df: raw dataframe from scraping
    :param is_past: whether it scraped past data
    :return: clean dataframe
    """

    df = df.dropna()
    keep_cols = config.keep_cols.selling_data
    keep_cols_sold = keep_cols + config.keep_cols.sold_data

    # Info
    df["house_id"] = df["url"].apply(lambda x: int(x.split("/")[-2].split("-")[1]))
    df["house_type"] = df["url"].apply(lambda x: x.split("/")[-2].split("-")[0])
    df = df[df["house_type"].isin(["appartement", "huis"])]

    # Price
    price_col = "price_sold" if is_past else "price"
    df["price"] = df[price_col].apply(clean_price)
    df = df[df["price"] != 0]
    df["living_area"] = df["living_area"].apply(clean_living_area)
    df = df[df["living_area"] != 0]
    df["price_m2"] = round(df.price / df.living_area, 1)

    # Location
    df["zip"] = df["zip_code"].apply(lambda x: x[:4])

    # House layout
    df["room"] = df["num_of_rooms"].apply(find_n_room)
    df["bedroom"] = df["num_of_rooms"].apply(find_n_bedroom)
    df["bathroom"] = df["num_of_bathrooms"].apply(find_n_bathroom)
    df["energy_label"] = df["energy_label"].apply(clean_energy_label)

    # Time
    df["year_built"] = df["year"].apply(clean_year).astype(int)
    df["house_age"] = datetime.now().year - df["year_built"]

    if is_past:
        # Only check past data
        df = df[(df["date_sold"] != "na") & (df["date_list"] != "na")]
        df["date_list"] = df["date_list"].apply(clean_list_date)
        df["date_sold"] = df["date_sold"].apply(clean_list_date)
        df = df.dropna()
        df["date_list"] = pd.to_datetime(df["date_list"])
        df["date_sold"] = pd.to_datetime(df["date_sold"])
        df["ym_sold"] = df["date_sold"].apply(lambda x: x.to_period("M").to_timestamp())
        df["year_sold"] = df["date_sold"].apply(lambda x: x.year)

        # Term
        df["term_days"] = df["date_sold"] - df["date_list"]
        df["term_days"] = df["term_days"].apply(lambda x: x.days)
        keep_cols = keep_cols_sold
        df["date_sold"] = df["date_sold"].dt.date

    else:
        # Only check current data
        df["date_list"] = df["listed_since"].apply(clean_list_date)
        df = df[df["date_list"] != "na"]
        df["date_list"] = pd.to_datetime(df["date_list"])

    df["ym_list"] = df["date_list"].apply(lambda x: x.to_period("M").to_timestamp())
    df["year_list"] = df["date_list"].apply(lambda x: x.year)
    df["date_list"] = df["date_list"].dt.date

    return df[keep_cols].reset_index(drop=True)
