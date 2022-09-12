import pandas as pd
from scraper.config import config
from datetime import datetime
from datetime import timedelta


def clean_price(x):
    try:
        return int(str(x).split(" ")[1].replace(",", ""))
    except ValueError:
        return 0
    except IndexError:
        return 0


def clean_year(x):
    if len(x) == 4:
        return int(x)
    elif x.find("-") != -1:
        return int(x.split("-")[0])
    elif x.find("before") != -1:
        return int(x.split(" ")[1])
    else:
        return 0


def clean_living_area(x):
    try:
        return int(str(x).replace(",", "").split(" m²")[0])
    except ValueError:
        return 0
    except IndexError:
        return 0


def find_n_room(x):
    if x.find("room") != -1:
        return int(str(x).split("room")[0].strip())
    else:
        return 0


def find_n_bedroom(x):
    if x.find("bedroom") != -1:
        return int(x.split(" ")[2].replace("(", ""))
    else:
        return 0


def find_n_bathroom(x):
    if x.find("bathroom") != -1:
        return int(str(x).split("bathroom")[0].strip())
    else:
        return 0


def fix_typo(x) -> str:
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


def get_neighbor(x):
    city = x.split("/")[0].replace("-", " ")
    return x.lower().split(city)[-1]


def clean_energy_label(x):
    try:
        x = x.split(" ")[0]
        if x.find("A+") != -1:
            return ">A+"
        else:
            return x
    except IndexError:
        return x


def clean_list_date(x):
    def delta_now(d):
        t = timedelta(days=d)
        return datetime.now() - t

    if x.find("€") != -1 or x.find("na") != -1:
        return "na"
    elif x.find("month") != -1:
        return delta_now(int(x.split("month")[0].strip()[0]) * 30)
    elif x.find("week") != -1:
        return delta_now(int(x.split("month")[0].strip()[0]) * 7)
    elif x.find("Today") != -1:
        return delta_now(1)
    elif x.find("day") != -1:
        return delta_now(int(x.split("month")[0].strip()))
    else:
        return datetime.strptime(x, "%B %d, %Y")


def preprocess_data(df: pd.DataFrame, is_past: bool) -> pd.DataFrame:

    df = df.dropna()
    keep_cols = config.keep_cols.selling_data

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
    df["temp"] = df["city"] + "/" + df["zip_code"]
    df["neighborhood"] = df["temp"].apply(get_neighbor)

    # House layout
    df["room"] = df["num_of_rooms"].apply(find_n_room)
    df["bedroom"] = df["num_of_rooms"].apply(find_n_bedroom)
    df["bathroom"] = df["num_of_bathrooms"].apply(find_n_bathroom)
    df["energy_label"] = df["energy_label"].apply(clean_energy_label)
    df["has_balcony"] = df["exteriors"].apply(
        lambda x: 1 if str(x).find("Balcony present") != -1 else 0
    )
    df["has_garden"] = df["exteriors"].apply(
        lambda x: 1 if str(x).find("garden") != -1 else 0
    )

    # Time
    df["year_built"] = df["year"].apply(clean_year).astype(int)
    df["house_age"] = 2023 - df["year_built"]

    if not is_past:
        df["date_list"] = df.listed_since.apply(clean_list_date)
        df = df[df["date_list"] != "na"]
        df["date_list"] = pd.to_datetime(df["date_list"])

    else:
        df = df[(df["date_sold"] != "na") & (df["date_list"] != "na")]
        df["date_sold"] = df["date_sold"].apply(fix_typo)
        df = df.dropna()
        df["date_list"] = pd.to_datetime(df["date_list"])
        df["date_sold"] = pd.to_datetime(df["date_sold"])
        df["ym_sold"] = df["date_sold"].apply(lambda x: x.to_period("M").to_timestamp())
        df["year_sold"] = df["date_sold"].apply(lambda x: x.year)

        # Term
        df["term_days"] = df["date_sold"] - df["date_list"]
        df["term_days"] = df["term_days"].apply(lambda x: x.days)

        keep_cols += config.keep_cols.sold_data

    df["ym_list"] = df["date_list"].apply(lambda x: x.to_period("M").to_timestamp())
    df["year_list"] = df["date_list"].apply(lambda x: x.year)
    keep_cols = list(set(keep_cols))

    return df[keep_cols].reset_index(drop=True)
